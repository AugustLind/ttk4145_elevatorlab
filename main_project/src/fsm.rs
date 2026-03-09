use driver_rust::elevio::elev::Elevator;
use driver_rust::elevio::elev::{DIRN_DOWN, DIRN_STOP, DIRN_UP};
use std::time::Duration;
use tokio::time::sleep;
use crate::config::NUM_FLOORS;
use crate::types::*;
use strum::IntoEnumIterator;

impl ElevatorInner {
    async fn new(addr: &str) -> Self {
        let driver = Elevator::init(addr, NUM_FLOORS).unwrap();
        Elevator::motor_direction(&driver, DIRN_DOWN);
        loop {
            if let Some(floor) = Elevator::floor_sensor(&driver) {
                Elevator::motor_direction(&driver, DIRN_STOP);
                Elevator::floor_indicator(&driver, floor);

                for f in 0..NUM_FLOORS {
                    for btn in 0..3u8 {
                        Elevator::call_button_light(&driver, f, btn, false);
                    }
                }

                while Elevator::obstruction(&driver) { sleep(Duration::from_millis(100)).await; }

                Elevator::door_light(&driver, false);

                return Self {
                    driver,
                    prev_floor: floor,
                    direction: DIRN_STOP,
                    state: ElevState::Init,
                    currently_serving: None,
                };
            }
            sleep(Duration::from_millis(20)).await;
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

    fn get_next_order(queue: &[Order], current: u8, direction: u8) -> u8 {
        let dist = |f: u8| (f as i16 - current as i16).unsigned_abs();

        // Pick the best order compatible with current direction:
        // DIRN_UP:   nearest ahead, skipping HallDown
        // DIRN_DOWN: nearest ahead (going down), skipping HallUp
        // DIRN_STOP: nearest overall
        let best = queue.iter().filter_map(|o| {
            let skip = match direction {
                DIRN_UP   => o.floor < current || o.order_type == ButtonType::HallDown,
                DIRN_DOWN => o.floor > current || o.order_type == ButtonType::HallUp,
                _         => false,
            };
            if skip { None } else { Some(o.floor) }
        }).min_by_key(|&f| match direction {
            DIRN_UP   => f as i16,
            DIRN_DOWN => -(f as i16),
            _         => dist(f) as i16,
        });

        // Fallback: if no direction-compatible order found, pick closest
        best.unwrap_or_else(|| {
            queue.iter().map(|o| o.floor).min_by_key(|&f| dist(f)).unwrap_or(current)
        })
    }

    pub async fn process_next_order(&self) -> OrderResult {
        let order_start = std::time::Instant::now();
        loop {
            sleep(Duration::from_millis(100)).await;

            let mut inner = self.inner.lock().await;
            inner.update_floor_sensor();

            let mut queue = self.queue.lock().await;
            if queue.is_empty() {
                inner.stop();
                return OrderResult::Empty;
            }

            if order_start.elapsed() > crate::config::ORDER_TIMEOUT {
                println!("[FSM] ORDER TIMEOUT — restarting");
                inner.stop();
                return OrderResult::Failed;
            }

            let next = Self::get_next_order(&queue, inner.prev_floor, inner.direction);

            // Not at target yet — keep moving
            if inner.prev_floor != next {
                inner.move_toward(next, &queue);
                continue;
            }

            // At target — serve order
            let Some(served) = inner.serve_order(&mut queue) else {
                // No direction-compatible order at this floor (e.g. only HallDown
                // while going up). Reset to DIRN_STOP so the next iteration uses
                // the DIRN_STOP branch in get_next_order, which matches any order
                // type and will serve the order on the next pass.
                inner.direction = DIRN_STOP;
                continue;
            };

            println!(
                "[FSM] Served f{} {:?} | queue: {:?}",
                served.floor,
                served.order_type,
                queue.iter().map(|x| x.floor).collect::<Vec<_>>()
            );

            drop(queue);
            drop(inner);
            if !self.open_door_and_wait().await {
                return OrderResult::Failed;
            }
            self.inner.lock().await.state = ElevState::Idle;

            return OrderResult::Completed(served);
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

    pub async fn open_door_and_wait(&self) -> bool {
        let inner = self.inner.lock().await;
        Elevator::door_light(&inner.driver, true);
        drop(inner);
        sleep(Duration::from_secs(3)).await;
        let obstruction_start = std::time::Instant::now();
        loop {
            let inner = self.inner.lock().await;
            if !Elevator::obstruction(&inner.driver) {
                Elevator::door_light(&inner.driver, false);
                return true;
            }
            drop(inner);
            if obstruction_start.elapsed() > crate::config::OBSTRUCTION_TIMEOUT {
                println!("[FSM] OBSTRUCTION TIMEOUT — restarting");
                return false;
            }
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
