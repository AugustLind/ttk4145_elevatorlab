mod config;
mod fsm;
pub mod types;
mod networkhandler;
mod requests;

use std::sync::Arc;
use types::*;
use tokio::time::Duration;

fn restart_self() -> ! {
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe().expect("Failed to get current executable");
    let args: Vec<String> = std::env::args().skip(1).collect();
    println!("[RESTART] Re-executing {:?} with args {:?}", exe, args);
    std::thread::sleep(Duration::from_secs(2));
    let err = std::process::Command::new(exe)
        .args(&args)
        .exec();
    panic!("Failed to restart: {}", err);
}


// entry point of the elevator application.
// creates the FSM, network and request assigner, spawns a queue runner and
// then loops handling networking, elections, button presses, and queue state.
// returns `()` implicitly; invoked by the Tokio runtime.
#[tokio::main]
async fn main() {
    println!("Main started");

    let port: u16 = std::env::args()
        .nth(1)
        .map(|s| s.parse().expect("Invalid port number"))
        .unwrap_or(config::ELEVATOR_PORT);

    let addr = format!("localhost:{}", port);
    println!("Connecting to elevator simulator at {}", addr);

    let fsm = Arc::new(ElevatorFSM::new(&addr).await);

    let message = Message {
        hall_requests: vec![[false, false]; config::NUM_FLOORS as usize],
        states: std::collections::HashMap::new(),
    };

    let mut network = Heartbeat::new().await;

    let mut request_assigner =
        RequestAssigner::new(network.id().to_string(), Roles::Slave, message);

    // One-time cab order recovery on startup: keep broadcasting and listening
    // until we hear from at least one peer (up to 5 seconds, then give up).
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut gossip = Vec::new();
        while tokio::time::Instant::now() < deadline {
            network.network_controller().await;
            gossip = network.collect_gossip_heartbeats().await;
            if !gossip.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        request_assigner.recover_cab_orders_from_gossip(&gossip, &mut network);
    }

    let (completed_tx, mut completed_rx) = tokio::sync::mpsc::unbounded_channel::<Order>();
    let (button_tx, mut button_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<Order>>();
    let (fail_tx, mut fail_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let mut clear_completed_after = None::<tokio::time::Instant>;

    tokio::spawn({
        let fsm = Arc::clone(&fsm);
        async move {
            loop {
                match fsm.process_next_order().await {
                    OrderResult::Completed(order) => { let _ = completed_tx.send(order); }
                    OrderResult::Failed => { let _ = fail_tx.send(()); return; }
                    OrderResult::Empty => {}
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });

    tokio::spawn({
        let fsm = Arc::clone(&fsm);
        async move {
            loop {
                let pressed = fsm.poll_buttons().await;
                if !pressed.is_empty() {
                    let _ = button_tx.send(pressed);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });

    loop {
        // Check for failure signal (obstruction/order timeout)
        if fail_rx.try_recv().is_ok() {
            println!("[MAIN] Failure detected — stopping motor and restarting");
            let inner = fsm.inner.lock().await;
            driver_rust::elevio::elev::Elevator::motor_direction(&inner.driver, driver_rust::elevio::elev::DIRN_STOP);
            drop(inner);
            restart_self();
        }

        let (floor, direction, status) = fsm.get_state().await;
        network.msg.floor = floor;
        network.msg.direction = direction;
        network.msg.status = status;

        network.network_controller().await;

        let gossip = network.collect_gossip_heartbeats().await;

        // Clear orders that any peer has completed (works for both master and slave)
        request_assigner.clear_completed_orders_from_gossip(&gossip, &mut network);

        request_assigner.elect_master(gossip.clone(), &mut network).await;

        while let Ok(orders) = button_rx.try_recv() {
            for order in orders {
                let target = match order.order_type {
                    ButtonType::CabCall => &mut network.msg.internal_orders,
                    _ => &mut network.msg.external_orders,
                };
                if !target.contains(&order) {
                    target.push(order);
                    network.msg.counter += 1;
                }
            }
        }

        while let Ok(order) = completed_rx.try_recv() {
            println!("[MAIN] Order completed: f{} {:?}", order.floor, order.order_type);
            network.order_completed(order);
            clear_completed_after = Some(tokio::time::Instant::now() + Duration::from_secs(1));
        }

        // Aggregate external orders from all peers so every elevator knows all hall orders.
        // This ensures hall orders survive master failure: when a slave becomes master,
        // it already has the complete set.
        for heartbeat in &gossip {
            for order in &heartbeat.external_orders {
                if !network.msg.external_orders.contains(order) {
                    network.msg.external_orders.push(order.clone());
                }
            }
        }


        let my_id = network.id().to_string();
        network.msg.all_cab_orders.insert(my_id.clone(), network.msg.internal_orders.clone());
        for heartbeat in &gossip {
            for (id, cabs) in &heartbeat.all_cab_orders {
                if id == &my_id { continue; } // We own our own entry; peers must not override it
                let entry = network.msg.all_cab_orders.entry(id.clone()).or_default();
                for order in cabs {
                    if !entry.contains(order) {
                        entry.push(order.clone());
                    }
                }
            }
        }

        match request_assigner.role {
            Roles::Master => request_assigner.master(&gossip, &mut network, fsm.clone()).await,
            Roles::Slave  => request_assigner.slave(&gossip, &mut network, fsm.clone()).await,
        }

        {
            let q = fsm.queue.lock().await;
            let contents: Vec<String> = q.iter().map(|o| format!("f{} {:?}", o.floor, o.order_type)).collect();
            println!("[MAIN] queue ({}): [{}]", q.len(), contents.join(", "));
        }

        // Clear cleared_order after 1 second of broadcasting
        if let Some(clear_time) = clear_completed_after {
            if tokio::time::Instant::now() >= clear_time && network.msg.cleared_order.is_some() {
                network.msg.cleared_order = None;
                network.msg.counter += 1;
                clear_completed_after = None;
            }
        }
    }
}
