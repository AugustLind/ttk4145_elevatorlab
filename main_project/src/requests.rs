use std::{collections::HashMap, net::IpAddr, str::FromStr, process::Stdio, sync::Arc};
use tokio::{process::Command, time::Instant};
use crate::types::*;

use crate::config;

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
            new_peer_joined: false,
        }
    }

    async fn assign_hall_orders(&self) -> HashMap<String, Vec<Order>> {
        let json_str = serde_json::to_string_pretty(&self.message).unwrap();
        println!("[ASSIGNER] Input: {}", json_str);

        let child = Command::new("./hall_request_assigner")
            .arg("--input")
            .arg(&json_str)
            .arg("--includeCab") 
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

        let raw: HashMap<String, Vec<Vec<bool>>> = serde_json::from_slice(&output.stdout).unwrap();
        println!("[ASSIGNER] Raw output: {:?}", raw);

        let assignments = raw.into_iter().map(|(id, per_floor)| {
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

    fn build_cost_input(&mut self, own_state: &GossipMsg) {
        let num_floors = config::NUM_FLOORS as usize;

        self.message.states.clear();
        self.message.hall_requests = vec![[false, false]; num_floors];

        self.add_elevator_state(own_state, num_floors);
        self.add_hall_requests_from_peer(own_state, num_floors);

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

        let state = AssignmentState {
            behaviour: peer.behaviour.clone(),
            floor: peer.floor,
            direction: peer.direction,
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

    pub async fn recover_cab_orders_from_gossip(&self, network: &mut Network) {
        let my_id = network.id().to_string();
        let mut rx = network.incoming.subscribe();
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        let mut received_any = false;

        println!("[RECOVER] Listening for cab orders (max 2s or until first heartbeat)...");

        while tokio::time::Instant::now() < deadline {
            let remaining = deadline - tokio::time::Instant::now();
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Ok(peer)) if peer.id != my_id => {
                    received_any = true;
                    if let Some(cabs) = peer.peer_cab_orders.get(&my_id) {
                        for order in cabs {
                            if !network.state.cab_orders.contains(order) {
                                println!("[RECOVER] Cab order f{} from peer {}", order.floor, peer.id);
                                network.state.cab_orders.push(order.clone());
                                network.state.counter += 1;
                            }
                        }
                    }
                    break;
                }
                Ok(Err(_)) => break, 
                Err(_) => break,    
                _ => {}
            }
        }

        if received_any {
            println!("[RECOVER] Received heartbeat, recovery complete");
        } else {
            println!("[RECOVER] No heartbeat received within 2s, proceeding without recovery");
        }
    }

    pub async fn run_election(&mut self, gossip: Vec<GossipMsg>, network: &mut Network) {
        let now = Instant::now();

        self.last_seen.insert(self.id.clone(), now);
        for peer in &gossip {
            if !self.last_seen.contains_key(&peer.id) {
                self.new_peer_joined = true;
            }
            self.last_seen.insert(peer.id.clone(), now);
            self.cached_peers.insert(peer.id.clone(), peer.clone());
        }
        let ttl = self.peer_ttl;
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

    fn remove_cleared_from_tracking(&mut self, gossip: &[GossipMsg], network: &mut Network) {
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
    }

    fn mask_assigned_orders(&mut self) {
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
    }

    pub async fn run_as_master(&mut self, gossip: &[GossipMsg], network: &mut Network, fsm: Arc<ElevatorGuard>) {
        self.remove_cleared_from_tracking(gossip, network);
        self.build_cost_input(&network.state);
        if !self.new_peer_joined {
            self.mask_assigned_orders();
        }

        println!("[MASTER] peers: {} | unassigned hall_requests: {:?}",
            self.cached_peers.len(), self.message.hall_requests);

        let has_new_hall_orders = self.message.hall_requests.iter().any(|r| r[0] || r[1]);
        let new_assignments = if has_new_hall_orders || self.new_peer_joined {
            self.assign_hall_orders().await
        } else {
            HashMap::new()
        };

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
        self.new_peer_joined = false;

        let my_id = network.id().to_string();
        let hall_assigned = network.state.assignments.get(&my_id).cloned().unwrap_or_default();
        println!("[MASTER {}] Assigned hall orders: {:?}", my_id, hall_assigned);

        let mut my_orders = hall_assigned;
        for cab in &network.state.cab_orders {
            if !my_orders.contains(cab) { my_orders.push(cab.clone()); }
        }

        fsm.set_queue(&my_orders).await;
        fsm.set_button_light(&network.state.hall_orders, &network.state.cab_orders).await;
    }

    pub async fn run_as_slave(&mut self, gossip: &[GossipMsg], network: &mut Network, fsm: Arc<ElevatorGuard>) {
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

            fsm.set_queue(&my_orders).await;

            let hall: Vec<Order> = master.hall_orders.iter().filter(not_cleared).cloned().collect();
            let cab:  Vec<Order> = network.state.cab_orders.iter().filter(not_cleared).cloned().collect();
            fsm.set_button_light(&hall, &cab).await;
        } else {
            fsm.set_queue(&network.state.cab_orders).await;
            println!("[SLAVE] No master found in peer list");
        }
    }

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
