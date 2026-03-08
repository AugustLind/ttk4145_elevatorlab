use driver_rust::elevio::elev::Elevator;
use driver_rust::elevio::elev::{DIRN_DOWN, DIRN_STOP, DIRN_UP};
use std::time::Duration;
use tokio::time::sleep;
use crate::config::NUM_FLOORS;
use crate::types::*;
use strum::IntoEnumIterator;

impl ElevatorInner {
    async fn new(addr: &str) -> Self {
        let mut driver = Elevator::init(addr, NUM_FLOORS).unwrap();
        loop {
            Elevator::motor_direction(&mut driver, DIRN_DOWN);
            if let Some(floor) = Elevator::floor_sensor(&driver) {
                Elevator::motor_direction(&mut driver, DIRN_STOP);
                Elevator::floor_indicator(&mut driver, floor);
                return Self {
                    obstruction: Elevator::obstruction(&driver),
                    driver,
                    prev_floor: floor,
                    direction: DIRN_STOP,
                    elev_id: addr.to_string(),
                    state: ElevState::Init,
                    last_received_msg_counter: 0,
                    currently_serving: None,
                };
            }
        }
    }

    fn update_floor_sensor(&mut self) {
        if let Some(floor) = Elevator::floor_sensor(&self.driver) {
            self.prev_floor = floor;
            Elevator::floor_indicator(&self.driver, floor);
        }
    }

    fn stop(&mut self) {
        self.direction = DIRN_STOP;
        self.state = ElevState::Idle;
        self.currently_serving = None;
        Elevator::motor_direction(&self.driver, DIRN_STOP);
    }

    fn move_toward(&mut self, target: u8, queue: &[Order]) {
        self.state = ElevState::WorkingOrder;
        self.currently_serving = queue.iter().find(|o| o.floor == target).cloned();
        self.direction = if self.prev_floor < target { DIRN_UP } else { DIRN_DOWN };
        Elevator::motor_direction(&self.driver, self.direction);
    }


    fn serve_order(&mut self, queue: &mut Vec<Order>) -> Option<Order> {
        let travel_dir = self.direction;
        self.state = ElevState::WorkingOrder;
        Elevator::motor_direction(&self.driver, DIRN_STOP);

        let pos = queue.iter().position(|o| {
            o.floor == self.prev_floor && match travel_dir {
                DIRN_UP => o.order_type != ButtonType::HallDown,
                DIRN_DOWN => o.order_type != ButtonType::HallUp,
                _ => true,
            }
        })?;

        let served = queue.remove(pos);
        self.currently_serving = None;

        self.direction = if queue.iter().any(|o| match travel_dir {
            DIRN_UP => o.floor > self.prev_floor,
            DIRN_DOWN => o.floor < self.prev_floor,
            _ => false,
        }) { travel_dir } else { DIRN_STOP };

        Some(served)
    }
}

impl ElevatorFSM {
    pub async fn new(addr: &str) -> Self {
        Self {
            queue: tokio::sync::Mutex::new(Vec::new()),
            inner: tokio::sync::Mutex::new(ElevatorInner::new(addr).await),
        }
    }

    // Replace the queue with new assignments, preserving the order
    // currently being executed so it doesn't vanish mid-movement.
    pub async fn replace_queue(&self, orders: &[Order]) {
        let serving = self.inner.lock().await.currently_serving.clone();
        let mut new_queue: Vec<Order> = orders.to_vec();
        if let Some(ref s) = serving {
            if !new_queue.contains(s) {
                new_queue.insert(0, s.clone());
            }
        }
        *self.queue.lock().await = new_queue;
    }

    pub async fn poll_buttons(&self) -> Vec<Order> {
        let inner = self.inner.lock().await;
        let mut pressed = Vec::new();
        for floor in 0..NUM_FLOORS {
            for button in ButtonType::iter() {
                if Elevator::call_button(&inner.driver, floor, button as u8) {
                    pressed.push(Order { floor, order_type: button });
                }
            }
        }
        pressed
    }

    /// Drive toward target floor, but stop at intermediate floors if there are orders there.
    pub async fn go_to_floor(&self, target: u8) -> u8 {
        loop {
            // First check floor sensor
            let floor_opt = {
                let mut inner = self.inner.lock().await;
                let f = Elevator::floor_sensor(&inner.driver);
                if let Some(floor) = f {
                    inner.prev_floor = floor;
                    Elevator::floor_indicator(&inner.driver, floor);
                }
                f
            };
            
            if let Some(floor) = floor_opt {
                // Check if we should stop at this floor
                let has_order_here = {
                    let q = self.queue.lock().await;
                    q.iter().any(|o| o.floor == floor)
                };
                
                let should_stop = floor == target || has_order_here;
                
                let mut inner = self.inner.lock().await;
                if should_stop {
                    inner.direction = DIRN_STOP;
                    Elevator::motor_direction(&inner.driver, DIRN_STOP);
                    return floor;
                }
                
                let dir = if floor < target { DIRN_UP } else { DIRN_DOWN };
                inner.direction = dir;
                Elevator::motor_direction(&inner.driver, dir);
            }

            sleep(Duration::from_millis(100)).await;
        }

        // Fallback: if no direction-compatible order found, pick closest
        best.unwrap_or_else(|| {
            queue.iter()
                .map(|o| o.floor)
                .min_by_key(|&f| dist(f))
                .unwrap_or(current_floor)
        })
    }

    pub async fn process_next_order(&self) -> Option<Order> {
        let order = {
            let mut q = self.queue.lock().await;
            if q.is_empty() {
                None
            } else {
                let o = q.remove(0);
                Some(o)
            }
        };

            let mut inner = self.inner.lock().await;
            inner.update_floor_sensor();

            let mut queue = self.queue.lock().await;
            if queue.is_empty() {
                inner.stop();
                return None;
            }

            let actual_floor = self.go_to_floor(order.floor).await;
            
            // Determine which order we actually served (might be intermediate stop)
            let served_order = if actual_floor != order.floor {
                // Stopped at intermediate floor - find and remove that order from queue
                let intermediate = {
                    let mut q = self.queue.lock().await;
                    q.iter().position(|o| o.floor == actual_floor)
                        .map(|pos| q.remove(pos))
                        .unwrap_or(Order { floor: actual_floor, order_type: ButtonType::CabCall })
                };
                // Put original target back in queue
                {
                    let mut q = self.queue.lock().await;
                    if !q.iter().any(|o| o.floor == order.floor && o.order_type == order.order_type) {
                        q.push(order);
                    }
                }
                intermediate
            } else {
                order
            };
            
            self.open_door_and_wait().await;

            println!(
                "[FSM] Served f{} {:?} | queue: {:?}",
                served.floor,
                served.order_type,
                queue.iter().map(|x| x.floor).collect::<Vec<_>>()
            );

            drop(queue);
            drop(inner);
            self.open_door_and_wait().await;
            self.inner.lock().await.state = ElevState::Idle;

            return Some(served);
        }
    }

    pub async fn get_state(&self) -> (u8, u8, Behaviour) {
        let inner = self.inner.lock().await;
        let behaviour = match inner.state {
            ElevState::WorkingOrder if inner.direction == DIRN_STOP => Behaviour::DoorOpen,
            ElevState::WorkingOrder                                 => Behaviour::Moving,
            _                                                       => Behaviour::Idle,
        };
        (inner.prev_floor, inner.direction, behaviour)
    }

    pub async fn open_door_and_wait(&self) {
        let inner = self.inner.lock().await;
        Elevator::door_light(&inner.driver, true);
        drop(inner);
        sleep(Duration::from_secs(3)).await;
        loop {
            let inner = self.inner.lock().await;
            if !Elevator::obstruction(&inner.driver) {
                Elevator::door_light(&inner.driver, false);
                return;
            }
            drop(inner);
            sleep(Duration::from_millis(40)).await;
        }
    }

    pub async fn set_button_light(&self, external_orders: &[Order], internal_orders: &[Order]) {
        let inner = self.inner.lock().await;
        for floor in 0..NUM_FLOORS {
            for button in ButtonType::iter() {
                let active = external_orders.iter().chain(internal_orders)
                    .any(|o| o.floor == floor && o.order_type == button);
                Elevator::call_button_light(&inner.driver, floor, button as u8, active);
            }
        }
    }
}
