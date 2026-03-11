use driver_rust::elevio::elev::Elevator;
use driver_rust::elevio::elev::{DIRN_DOWN, DIRN_STOP, DIRN_UP};
use std::sync::Arc;
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
                    last_floor: floor,
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
            self.last_floor = floor;
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
        self.direction = if self.last_floor < target { DIRN_UP } else { DIRN_DOWN };
        Elevator::motor_direction(&self.driver, self.direction);
    }

    fn serve_order(&mut self, queue: &mut Vec<Order>) -> Option<Order> {
        let travel_dir = self.direction;
        self.state = ElevState::WorkingOrder;
        Elevator::motor_direction(&self.driver, DIRN_STOP);

        let pos = queue.iter().position(|o| {
            o.floor == self.last_floor && match travel_dir {
                DIRN_UP   => o.order_type != ButtonType::HallDown,
                DIRN_DOWN => o.order_type != ButtonType::HallUp,
                _         => true,
            }
        })?;

        let served = queue.remove(pos);
        self.currently_serving = None;

        // Keep travel direction if there are more orders ahead; otherwise stop.
        self.direction = if queue.iter().any(|o| match travel_dir {
            DIRN_UP   => o.floor > self.last_floor,
            DIRN_DOWN => o.floor < self.last_floor,
            _         => false,
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

    /// SCAN-like next target floor selection.
    /// DIRN_UP/DOWN: nearest order ahead that matches the direction.
    /// Falls back to closest order if none are direction-compatible.
    fn get_next_order(queue: &[Order], current: u8, direction: u8) -> u8 {
        let dist = |f: u8| (f as i16 - current as i16).unsigned_abs();

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

        best.unwrap_or_else(|| {
            queue.iter().map(|o| o.floor).min_by_key(|&f| dist(f)).unwrap_or(current)
        })
    }

    /// Drive toward the next queued order, serving it when we arrive.
    /// Returns Completed(order), Empty (queue was empty), or Failed (timeout).
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

            let target = Self::get_next_order(&queue, inner.last_floor, inner.direction);

            if inner.last_floor != target {
                inner.move_toward(target, &queue);
                continue;
            }

            // At target floor — attempt to serve an order.
            let Some(served) = inner.serve_order(&mut queue) else {
                // No direction-compatible order here (e.g. only HallDown while going up).
                // Reset direction so next iteration picks the closest order unconditionally.
                inner.direction = DIRN_STOP;
                continue;
            };

            println!(
                "[FSM] Served f{} {:?} | remaining: {:?}",
                served.floor,
                served.order_type,
                queue.iter().map(|o| o.floor).collect::<Vec<_>>()
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
            ElevState::WorkingOrder                                  => Behaviour::Moving,
            _                                                        => Behaviour::Idle,
        };
        (inner.last_floor, inner.direction, behaviour)
    }

    /// Open the door for 3 seconds, then wait for the obstruction switch to clear.
    /// Returns false if the obstruction timeout is exceeded (triggers a restart).
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

    /// Spawn the order-runner and button-poller background tasks.
    /// Returns (completed_orders, button_presses, failure_signal) receivers.
    pub fn spawn_tasks(self: Arc<Self>) -> (
        tokio::sync::mpsc::UnboundedReceiver<Order>,
        tokio::sync::mpsc::UnboundedReceiver<Vec<Order>>,
        tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        use tokio::sync::mpsc;
        let (completed_tx, completed_rx) = mpsc::unbounded_channel::<Order>();
        let (button_tx,    button_rx)    = mpsc::unbounded_channel::<Vec<Order>>();
        let (fail_tx,      fail_rx)      = mpsc::unbounded_channel::<()>();

        tokio::spawn({
            let fsm = Arc::clone(&self);
            async move {
                loop {
                    match fsm.process_next_order().await {
                        OrderResult::Completed(order) => { let _ = completed_tx.send(order); }
                        OrderResult::Failed           => { let _ = fail_tx.send(()); return; }
                        OrderResult::Empty            => {}
                    }
                    sleep(Duration::from_millis(50)).await;
                }
            }
        });

        tokio::spawn(async move {
            loop {
                let pressed = self.poll_buttons().await;
                if !pressed.is_empty() { let _ = button_tx.send(pressed); }
                sleep(Duration::from_millis(50)).await;
            }
        });

        (completed_rx, button_rx, fail_rx)
    }

    pub async fn emergency_stop(&self) {
        let inner = self.inner.lock().await;
        Elevator::motor_direction(&inner.driver, DIRN_STOP);
    }

    pub async fn log_queue(&self) {
        let queue = self.queue.lock().await;
        let contents: Vec<String> = queue.iter().map(|o| format!("f{} {:?}", o.floor, o.order_type)).collect();
        println!("[FSM] queue ({}): [{}]", queue.len(), contents.join(", "));
    }

    pub async fn set_button_light(&self, hall_orders: &[Order], cab_orders: &[Order]) {
        let inner = self.inner.lock().await;
        for floor in 0..NUM_FLOORS {
            for button in ButtonType::iter() {
                let active = hall_orders.iter().chain(cab_orders)
                    .any(|o| o.floor == floor && o.order_type == button);
                Elevator::call_button_light(&inner.driver, floor, button as u8, active);
            }
        }
    }
}
