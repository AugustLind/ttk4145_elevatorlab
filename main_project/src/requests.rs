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

    pub fn new(id: String) -> Self {
        Self {
            message: Message {
                hall_requests: vec![[false, false]; config::NUM_FLOORS as usize],
                states: HashMap::new(),
            },
            id,
            role: Roles::Slave,
            last_published_assignments: HashMap::new(),
            last_seen: HashMap::new(),
            cached_peers: HashMap::new(),
            peer_ttl: config::MASTER_ELECTION_TIMEOUT,
        }
    }

    /// Run the external hall_request_assigner binary and parse its output into
    /// a map from elevator ID to assigned orders.
    async fn assign_hall_orders(&self) -> HashMap<String, Vec<Order>> {
        let json_str = serde_json::to_string_pretty(&self.message).unwrap();
        println!("[ASSIGNER] Input: {}", json_str);

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
            eprintln!(
                "hall_request_assigner failed. stderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return HashMap::new();
        }

        let raw_json_assignments: HashMap<String, Vec<Vec<bool>>> = serde_json::from_slice(&output.stdout).unwrap();
        println!("[ASSIGNER] Raw output: {:?}", raw_json_assignments);

        let assignments = raw_json_assignments.into_iter().map(|(id, per_floor)| {
            let orders = per_floor.iter().enumerate().flat_map(|(floor, cols)| {
                [
                    (cols.get(0), ButtonType::HallUp),
                    (cols.get(1), ButtonType::HallDown),
                    (cols.get(2), ButtonType::CabCall),
                ]
                .into_iter()
                .filter_map(move |(flag, btn)| {
                    flag.copied().unwrap_or(false).then_some(Order { floor: floor as u8, order_type: btn })
                })
            }).collect();
            (id, orders)
        }).collect();

        println!("[ASSIGNER] Parsed assignments: {:?}", assignments);
        assignments
    }


    /// Build the Message struct that will be fed into the hall_request_assigner binary.
    /// Populates elevator states from our own state and all live cached peers.
    fn build_cost_input(&mut self, own_state: &GossipMsg) {
        let num_floors = config::NUM_FLOORS as usize;

        self.message.states.clear();
        self.message.hall_requests = vec![[false, false]; num_floors];

        self.add_elevator_state(own_state, num_floors);
        self.add_hall_requests_from_peer(own_state, num_floors);

        // Clone to avoid borrow conflict with self.cached_peers and self.message
        let gossip: Vec<GossipMsg> = self.cached_peers
            .values()
            .filter(|p| p.id != self.id)
            .cloned()
            .collect();

        for peer in &gossip {
            self.add_elevator_state(peer, num_floors);
            self.add_hall_requests_from_peer(peer, num_floors);
        }
    }

    fn add_elevator_state(&mut self, peer: &GossipMsg, num_floors: usize) {
        let mut cab_requests = vec![false; num_floors];
        for order in &peer.cab_orders {
            if matches!(order.order_type, ButtonType::CabCall) {
                let floor = order.floor as usize;
                if floor < num_floors { cab_requests[floor] = true; }
            }
        }

        let state = ElevatorState {
            behaviour: peer.behaviour.clone(),
            floor: peer.floor,
            direction: match peer.direction {
                DIRN_STOP => Direction::Stop,
                DIRN_UP   => Direction::Up,
                DIRN_DOWN => Direction::Down,
                _         => Direction::Stop,
            },
            cab_requests,
        };

        self.message.states.insert(peer.id.clone(), state);
    }

    fn add_hall_requests_from_peer(&mut self, peer: &GossipMsg, num_floors: usize) {
        for order in &peer.hall_orders {
            let floor = order.floor as usize;
            if floor >= num_floors { continue; }
            match order.order_type {
                ButtonType::HallUp   => self.message.hall_requests[floor][0] = true,
                ButtonType::HallDown => self.message.hall_requests[floor][1] = true,
                _ => {}
            }
        }
    }


    /// On startup: restore our own cab orders from peers who still hold our pre-crash state.
    pub fn recover_cab_orders_from_gossip(&self, gossip: &[GossipMsg], network: &mut Network) {
        let my_id = network.id().to_string();
        for peer in gossip {
            if let Some(cabs) = peer.peer_cab_orders.get(&my_id) {
                for order in cabs {
                    if !network.state.cab_orders.contains(order) {
                        println!("[RECOVER] Cab order f{} from peer {}", order.floor, peer.id);
                        network.state.cab_orders.push(order.clone());
                        network.state.counter += 1;
                    }
                }
            }
        }
    }


    /// Replace the FSM queue with the given orders, preserving any order currently being served.
    async fn enqueue_orders(&self, fsm: &Arc<ElevatorFSM>, orders: &[Order]) {
        let currently_serving = {
            let inner = fsm.inner.lock().await;
            inner.currently_serving.clone()
        };

        let mut new_queue: Vec<Order> = orders.to_vec();
        // Do not interrupt an order already in progress
        if let Some(ref serving) = currently_serving {
            if !new_queue.contains(serving) {
                new_queue.insert(0, serving.clone());
            }
        }

        let mut queue = fsm.queue.lock().await;
        if *queue != new_queue {
            *queue = new_queue;
        }
    }


    /// Elect the master as the peer with the lowest IP address among all live peers.
    pub async fn elect_master(&mut self, gossip: Vec<GossipMsg>, network: &mut Network) {
        let now = Instant::now();

        self.last_seen.insert(self.id.clone(), now);
        for peer in &gossip {
            self.last_seen.insert(peer.id.clone(), now);
            self.cached_peers.insert(peer.id.clone(), peer.clone());
        }

        let ttl = self.peer_ttl;

        // Clear assignments for timed-out peers so their orders get reassigned
        let timed_out: Vec<String> = self.last_seen
            .iter()
            .filter(|(_, t)| now.duration_since(**t) > ttl)
            .map(|(id, _)| id.clone())
            .collect();

        for id in &timed_out {
            if self.last_published_assignments.remove(id).is_some() {
                println!("[ELECTION] Peer {} timed out — reassigning their orders", id);
            }
        }

        self.last_seen.retain(|_, t| now.duration_since(*t) <= ttl);
        self.cached_peers.retain(|id, _| self.last_seen.contains_key(id));

        let elected = self.last_seen
            .keys()
            .min_by_key(|id| IpAddr::from_str(id).unwrap_or(IpAddr::from([255, 255, 255, 255])))
            .unwrap()
            .clone();

        let new_role = if elected == self.id { Roles::Master } else { Roles::Slave };

        if self.role != new_role {
            println!("Role change: {:?} → {:?} (master: {})", self.role, new_role, elected);
            self.role = new_role.clone();
            network.state.counter += 1;
        }
        network.state.role = new_role;
    }


    pub async fn master_execute_tasks(&mut self, gossip: &[GossipMsg], network: &mut Network, fsm: Arc<ElevatorFSM>) {
        // Remove completed orders from master-specific tracking structures.
        // (hall_orders / cab_orders on network.state are already cleared by
        //  clear_completed_orders_from_gossip and order_completed before we get here.)
        let all_cleared: Vec<Order> = std::iter::once(&network.state)
            .chain(gossip.iter())
            .filter_map(|p| p.cleared_order.clone())
            .collect();

        for cleared in &all_cleared {
            for peer in self.cached_peers.values_mut() {
                peer.hall_orders.retain(|o| o != cleared);
            }
            for orders in network.state.assignments.values_mut() {
                orders.retain(|o| o != cleared);
            }
            for orders in self.last_published_assignments.values_mut() {
                orders.retain(|o| o != cleared);
            }
        }

        self.build_cost_input(&network.state);

        // Mask already-assigned orders so the cost function only sees NEW requests.
        // This prevents reassigning mid-execution.
        for order in self.last_published_assignments.values().flatten() {
            let floor = order.floor as usize;
            if floor < self.message.hall_requests.len() {
                match order.order_type {
                    ButtonType::HallUp   => self.message.hall_requests[floor][0] = false,
                    ButtonType::HallDown => self.message.hall_requests[floor][1] = false,
                    _ => {}
                }
            }
        }

        println!("[MASTER] peers: {} | unassigned hall_requests: {:?}",
            self.cached_peers.len(), self.message.hall_requests);

        let has_new_hall_orders = self.message.hall_requests.iter().any(|r| r[0] || r[1]);
        let new_assignments = if has_new_hall_orders {
            self.assign_hall_orders().await
        } else {
            HashMap::new()
        };

        // Merge new assignments on top of existing ones
        let mut merged = self.last_published_assignments.clone();
        for (id, orders) in new_assignments {
            let entry = merged.entry(id).or_default();
            for order in orders {
                if !entry.contains(&order) { entry.push(order); }
            }
        }

        if merged != self.last_published_assignments {
            self.last_published_assignments = merged.clone();
            network.state.assignments = merged;
            network.state.counter += 1;
        }

        let my_id = network.id().to_string();
        let hall_assigned = network.state.assignments.get(&my_id).cloned().unwrap_or_default();
        println!("[MASTER {}] Assigned hall orders: {:?}", my_id, hall_assigned);

        let mut my_orders = hall_assigned;
        for cab in &network.state.cab_orders {
            if !my_orders.contains(cab) { my_orders.push(cab.clone()); }
        }

        self.enqueue_orders(&fsm, &my_orders).await;
        fsm.set_button_light(&network.state.hall_orders, &network.state.cab_orders).await;
    }


    pub async fn slave_execute_tasks(&mut self, gossip: &[GossipMsg], network: &mut Network, fsm: Arc<ElevatorFSM>) {
        let cleared = network.collect_cleared_orders(gossip);
        let not_cleared = |o: &&Order| !cleared.contains(o);

        if let Some(master) = gossip.iter().find(|p| matches!(p.role, Roles::Master)) {
            let my_id = network.id().to_string();
            let assigned = master.assignments.get(&my_id);
            println!("[SLAVE {}] Assigned hall orders from master: {:?}", my_id, assigned);

            let mut my_orders: Vec<Order> = assigned
                .into_iter().flatten().filter(not_cleared).cloned().collect();
            for cab in network.state.cab_orders.iter().filter(not_cleared) {
                if !my_orders.contains(cab) { my_orders.push(cab.clone()); }
            }

            self.enqueue_orders(&fsm, &my_orders).await;

            let hall: Vec<Order> = master.hall_orders.iter().filter(not_cleared).cloned().collect();
            let cab:  Vec<Order> = network.state.cab_orders.iter().filter(not_cleared).cloned().collect();
            fsm.set_button_light(&hall, &cab).await;
        } else {
            // No master visible — serve our own cab orders at minimum
            self.enqueue_orders(&fsm, &network.state.cab_orders).await;
            println!("[SLAVE] No master found in peer list");
        }
    }


    /// Clear orders that any peer has marked as completed.
    /// Hall orders are cleared globally; cab orders only if it's our own elevator's completion.
    pub fn clear_completed_orders_from_gossip(&mut self, gossip: &[GossipMsg], network: &mut Network) {
        let my_id = &network.state.id.clone();

        for peer in gossip {
            if let Some(cleared) = &peer.cleared_order {
                network.state.hall_orders.retain(|o| o != cleared);

                if &peer.id == my_id {
                    network.state.cab_orders.retain(|o| o != cleared);
                }

                if let Some(cabs) = network.state.peer_cab_orders.get_mut(&peer.id) {
                    cabs.retain(|o| o != cleared);
                }

                for cached in self.cached_peers.values_mut() {
                    cached.hall_orders.retain(|o| o != cleared);
                }
            }
        }
    }
}
