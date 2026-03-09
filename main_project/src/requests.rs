use std::collections::HashMap;
use crate::types::{ElevatorFSM, Order};
use tokio::process::Command;
use std::process::Stdio;
use crate::types::*;
use crate::config;
use std::net::IpAddr;
use std::str::FromStr;
use tokio::time::Instant;

use driver_rust::elevio::elev::{DIRN_DOWN, DIRN_STOP, DIRN_UP};
use std::sync::Arc;

impl RequestAssigner {

    pub fn new(id: String, role: Roles, message: Message) -> Self {
        Self {
            message,
            id,
            role,
            last_published_assignments: HashMap::new(),
            last_seen: HashMap::new(),
            peer_states: HashMap::new(),
            peer_ttl: config::MASTER_ELECTION_TIMEOUT,
        }
    }

    pub async fn cost_function(&self) -> HashMap<String, Vec<Order>> {
        let json_str = serde_json::to_string_pretty(&self.message).unwrap();
        println!("[COST_FUNC] Input: {}", json_str);
        let child = Command::new("./hall_request_assigner")
            .arg("--input")
            .arg(&json_str)
            .arg("--includeCab") // include cab requests so output has 3 columns per floor
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let output = child.wait_with_output().await.unwrap();

        if !output.status.success() {
            println!(
                "hall_request_assigner feilet. stderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return HashMap::new();
        }

        let raw: HashMap<String, Vec<Vec<bool>>> = serde_json::from_slice(&output.stdout).unwrap();
        
        println!("[COST_FUNC] Raw output: {:?}", raw);

        let mut assignments: HashMap<String, Vec<Order>> = HashMap::new();

        for (id, per_floor) in raw {
            let mut orders: Vec<Order> = Vec::new();

            for (floor, cols) in per_floor.iter().enumerate() {
                if cols.get(0).copied().unwrap_or(false) {
                    orders.push(Order {
                        floor: floor as u8,
                        order_type: ButtonType::HallUp,
                    });
                }
                if cols.get(1).copied().unwrap_or(false) {
                    orders.push(Order {
                        floor: floor as u8,
                        order_type: ButtonType::HallDown,
                    });
                }
                if cols.get(2).copied().unwrap_or(false) {
                    orders.push(Order {
                        floor: floor as u8,
                        order_type: ButtonType::CabCall,
                    });
                }
            }
            assignments.insert(id, orders);
        }
        println!("[COST_FUNC] Parsed assignments: {:?}", assignments);
        assignments
    }


    pub fn build_message_from_gossip(&mut self, own_heartbeat: &HeartbeatMSG) {
        let num_floors = crate::config::NUM_FLOORS as usize;

        self.message.states.clear();
        self.message.hall_requests = vec![[false, false]; num_floors];

        // Always include self
        self.insert_state_from_heartbeat(own_heartbeat, num_floors);
        self.merge_external_orders(own_heartbeat, num_floors);
        
        // Include all live peers (using cached states; clone to avoid borrow conflict)
        let peer_states: Vec<_> = self.peer_states
            .values()
            .filter(|hb| hb.id != self.id)
            .cloned()
            .collect();

        for heartbeat in &peer_states {
            self.insert_state_from_heartbeat(heartbeat, num_floors);
            self.merge_external_orders(heartbeat, num_floors);
        }
    }

    fn insert_state_from_heartbeat(&mut self, heartbeat: &HeartbeatMSG, num_floors: usize) {
        let mut cab = vec![false; num_floors];
        for order in heartbeat.internal_orders.iter() {
            if matches!(order.order_type, ButtonType::CabCall) {
                let floor = order.floor as usize;
                if floor < num_floors { cab[floor] = true; }
            }
        }

        let st = ElevatorState {
            behaviour: heartbeat.status.clone(),
            floor: heartbeat.floor,
            direction: match heartbeat.direction {
                DIRN_STOP => Direction::Stop,
                DIRN_UP   => Direction::Up,
                DIRN_DOWN => Direction::Down,
                _         => Direction::Stop,
            },
            cab_requests: cab,
        };

        self.message.states.insert(heartbeat.id.clone(), st);
    }

    fn merge_external_orders(&mut self, heartbeat: &HeartbeatMSG, num_floors: usize) {
        for order in heartbeat.external_orders.iter() {
            let floor = order.floor as usize;
            if floor >= num_floors { continue; }
            match order.order_type {
                ButtonType::HallUp => self.message.hall_requests[floor][0] = true,
                ButtonType::HallDown => self.message.hall_requests[floor][1] = true,
                _ => {}
            }
        }
    }

    pub fn recover_cab_orders_from_gossip(&self, gossip: &[HeartbeatMSG], network: &mut Heartbeat) {
        let my_id = network.id().to_string();
        for heartbeat in gossip {
            if let Some(cabs) = heartbeat.all_cab_orders.get(&my_id) {
                for order in cabs {
                    if !network.msg.internal_orders.contains(order) {
                        println!("[RECOVER] Cab order f{} from peer {}", order.floor, heartbeat.id);
                        network.msg.internal_orders.push(order.clone());
                        network.msg.counter += 1;
                    }
                }
            }
        }
    }

    async fn enqueue_orders(
        &self,
        fsm: &Arc<ElevatorFSM>,
        orders: &[Order],
    ) {
        let currently_serving = {
            let inner = fsm.inner.lock().await;
            inner.currently_serving.clone()
        };

        // Build the new queue, but PRESERVE currently serving order
        let mut new_queue: Vec<Order> = orders.to_vec();
        
        // If we're currently serving an order, make sure it stays in the queue
        if let Some(ref serving) = currently_serving {
            if !new_queue.contains(serving) {
                new_queue.insert(0, serving.clone());
            }
        }

        let mut q = fsm.queue.lock().await;
        if *q != new_queue {
            *q = new_queue;
        }
    }   

    pub async fn elect_master(
        &mut self, 
        gossip_heartbeats: Vec<HeartbeatMSG>, 
        network: &mut Heartbeat,
    ) {
        
        let now = Instant::now();

        self.last_seen.insert(self.id.clone(), now);
        for heartbeat in &gossip_heartbeats {
            self.last_seen.insert(heartbeat.id.clone(), now);
            // Cache the latest state from each peer
            self.peer_states.insert(heartbeat.id.clone(), heartbeat.clone());
        }

        let ttl = self.peer_ttl;
        
        // Find peers that timed out BEFORE removing them
        let timed_out_peers: Vec<String> = self.last_seen
            .iter()
            .filter(|(_, t)| now.duration_since(**t) > ttl)
            .map(|(id, _)| id.clone())
            .collect();
        
        // Clear assignments from timed-out peers so orders can be reassigned
        for peer_id in &timed_out_peers {
            if self.last_published_assignments.remove(peer_id).is_some() {
                println!("[MASTER] Peer {} timed out - clearing their assignments for reassignment", peer_id);
            }
        }
        
        self.last_seen.retain(|_, t| now.duration_since(*t) <= ttl);
        // Remove stale peer states
        self.peer_states.retain(|id, _| self.last_seen.contains_key(id));

        let candidates: Vec<String> = self.last_seen.keys().cloned().collect();

        let elected = candidates
            .iter()
            .min_by_key(|id| IpAddr::from_str(id).unwrap_or(IpAddr::from([255, 255, 255, 255])))
            .unwrap()
            .clone();

        let new_role = if elected == self.id {
            Roles::Master
        } else {
            Roles::Slave
        };

        if self.role != new_role {
            println!(
                "Role change: {:?} -> {:?} (elected master: {})",
                self.role, new_role, elected
            );
            self.role = new_role.clone();
            network.msg.role = new_role;
            network.msg.counter += 1;
        } else {
            network.msg.role = new_role;
        }
    }


    pub async fn master(
        &mut self,
        gossip: &[HeartbeatMSG],
        network: &mut Heartbeat,
        fsm: Arc<ElevatorFSM>,
    ) {
        // Collect all cleared orders (from peers + self) and remove them from
        // master-specific tracking structures (external/internal already cleared
        // by clear_completed_orders_from_gossip and order_completed).
        let all_cleared: Vec<Order> = std::iter::once(&network.msg)
            .chain(gossip.iter())
            .filter_map(|hb| hb.cleared_order.clone())
            .collect();

        for cleared in &all_cleared {
            for cached_hb in self.peer_states.values_mut() {
                cached_hb.external_orders.retain(|o| o != cleared);
            }
            for orders in network.msg.assignments.values_mut() {
                orders.retain(|o| o != cleared);
            }
            for orders in self.last_published_assignments.values_mut() {
                orders.retain(|o| o != cleared);
            }
        }

        self.build_message_from_gossip(&network.msg);
        
        // Collect orders that are already assigned to someone
        let already_assigned: Vec<Order> = self.last_published_assignments
            .values()
            .flatten()
            .cloned()
            .collect();
        
        // Remove already-assigned orders from hall_requests before calling cost function
        // This prevents the cost function from reassigning orders mid-execution
        for order in &already_assigned {
            let floor = order.floor as usize;
            if floor < self.message.hall_requests.len() {
                match order.order_type {
                    ButtonType::HallUp => self.message.hall_requests[floor][0] = false,
                    ButtonType::HallDown => self.message.hall_requests[floor][1] = false,
                    _ => {}
                }
            }
        }

        // Debug: show what we're sending to cost function
        println!("[MASTER] gossip: {} | peers: {} | hall_requests: {:?}", 
            gossip.len(), 
            self.peer_states.len(),
            self.message.hall_requests);
        
        // Only run cost function if there are new HALL orders to assign
        // Cab orders are handled directly (each elevator enqueues its own cabs)
        let has_new_hall_orders = self.message.hall_requests.iter().any(|floor| floor[0] || floor[1]);
        
        let new_assignments = if has_new_hall_orders {
            self.cost_function().await
        } else {
            HashMap::new()
        };
        
        // Merge new assignments with existing ones
        let mut merged_assignments = self.last_published_assignments.clone();
        for (id, new_orders) in new_assignments {
            let entry = merged_assignments.entry(id).or_default();
            for order in new_orders {
                if !entry.contains(&order) {
                    entry.push(order);
                }
            }
        }

        let self_id = network.id().to_string();

        if merged_assignments != self.last_published_assignments {
            self.last_published_assignments = merged_assignments.clone();
            network.msg.assignments = merged_assignments.clone();
            network.msg.counter += 1;
        }

        // Debug: show what's assigned to us vs others
        let my_orders = network.msg.assignments.get(&self_id);
        println!("[MASTER {}] My assigned orders: {:?}", self_id, my_orders);
        
        // Combine assigned hall orders with local cab orders
        let mut all_my_orders: Vec<Order> = my_orders.cloned().unwrap_or_default();
        for cab_order in &network.msg.internal_orders {
            if !all_my_orders.contains(cab_order) {
                all_my_orders.push(cab_order.clone());
            }
        }
        
        self.enqueue_orders(&fsm, &all_my_orders).await;
        
        // Update button lights to match all orders (external + internal)
        fsm.set_button_light(&network.msg.external_orders, &network.msg.internal_orders).await;
    }

    pub async fn slave(&mut self, gossip: &[HeartbeatMSG], network: &mut Heartbeat, fsm: Arc<ElevatorFSM>) {
        if let Some(master_heartbeat) = gossip.iter().find(|heartbeat| matches!(heartbeat.role, Roles::Master)) {
            let my_id = network.id().to_string();
            let my_orders = master_heartbeat.assignments.get(&my_id);
            println!("[SLAVE {}] My assigned orders from master: {:?}", my_id, my_orders);

            // Filter out recently-cleared orders to prevent flicker
            let cleared = network.collect_cleared_orders(gossip);
            let not_cleared = |o: &&Order| !cleared.contains(o);

            let mut all_my_orders: Vec<Order> = my_orders
                .into_iter().flatten().filter(not_cleared).cloned().collect();
            for cab in network.msg.internal_orders.iter().filter(not_cleared) {
                if !all_my_orders.contains(cab) { all_my_orders.push(cab.clone()); }
            }

            self.enqueue_orders(&fsm, &all_my_orders).await;

            let ext: Vec<Order> = master_heartbeat.external_orders.iter().filter(not_cleared).cloned().collect();
            let int: Vec<Order> = network.msg.internal_orders.iter().filter(not_cleared).cloned().collect();
            fsm.set_button_light(&ext, &int).await;
        } else {
            // Even if no master, still handle local cab orders
            self.enqueue_orders(&fsm, &network.msg.internal_orders).await;
            println!("[SLAVE] No master found in gossip!");
        }
    }

    /// Called by both master and slave to clear orders that any peer has completed
    pub fn clear_completed_orders_from_gossip(&mut self, gossip: &[HeartbeatMSG], network: &mut Heartbeat) {
        let my_id = &network.msg.id;

        for heartbeat in gossip {
            if let Some(cleared) = &heartbeat.cleared_order {
                // External (hall) orders: clear from everyone
                network.msg.external_orders.retain(|order| order != cleared);

                // Internal (cab) orders: only clear if it's MY cleared order
                // Cabs are private - other elevators shouldn't clear my cabs
                if &heartbeat.id == my_id {
                    network.msg.internal_orders.retain(|order| order != cleared);
                }

                // Remove from all_cab_orders so peers stop re-broadcasting the stale order
                if let Some(cabs) = network.msg.all_cab_orders.get_mut(&heartbeat.id) {
                    cabs.retain(|o| o != cleared);
                }

                // Clear from cached peer_states (only external orders)
                for (_, cached_hb) in self.peer_states.iter_mut() {
                    cached_hb.external_orders.retain(|o| o != cleared);
                }
            }
        }
    }
}