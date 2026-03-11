mod config;
mod fsm;
pub mod types;
mod networkhandler;
mod requests;

use std::sync::Arc;
use types::*;
use tokio::time::Duration;

fn get_elevator_addr() -> String {
    let port: u16 = std::env::args()
        .nth(1)
        .map(|s| s.parse().expect("Invalid port number"))
        .unwrap_or(config::ELEVATOR_PORT);
    format!("localhost:{}", port)
}

fn restart_ownElevator() -> ! {
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe().expect("Failed to get current executable");
    let args: Vec<String> = std::env::args().skip(1).collect();
    println!("[RESTART] Re-executing {:?} with args {:?}", exe, args);
    std::thread::sleep(Duration::from_secs(2));
    let err = std::process::Command::new(exe).args(&args).exec();
    panic!("Failed to restart: {}", err);
}


#[tokio::main]
async fn main() {
    let addr = get_elevator_addr();
    println!("Connecting to elevator simulator at {}", addr);

    let ownElevator = Arc::new(ElevatorFSM::new(&addr).await);
    let mut network = Network::new().await;
    let mut assigner = RequestAssigner::new(network.id().to_string());
    assigner.recover_cab_orders_from_gossip(&network.collect_gossip().await, &mut network);

    let (mut completed_rx, mut button_rx, mut fail_rx) = ownElevator.clone().spawn_tasks();

    loop {
        if fail_rx.try_recv().is_ok() {
            println!("[MAIN] Failure detected — stopping motor and restarting");
            ownElevator.emergency_stop().await;
            restart_ownElevator();
        }

        network.update_state(ownElevator.get_state().await);
        network.broadcast_state().await;

        let gossip = network.collect_gossip().await;

        assigner.clear_completed_orders_from_gossip(&gossip, &mut network);
        assigner.elect_master(gossip.clone(), &mut network).await;

        while let Ok(orders) = button_rx.try_recv() {
            for order in orders { network.add_order(order); }
        }

        while let Ok(order) = completed_rx.try_recv() {
            println!("[MAIN] Order completed: f{} {:?}", order.floor, order.order_type);
            network.order_completed(order);
        }

        network.merge_gossip_orders(&gossip);

        match assigner.role {
            Roles::Master => assigner.master_execute_tasks(&gossip, &mut network, ownElevator.clone()).await,
            Roles::Slave  => assigner.slave_execute_tasks(&gossip, &mut network, ownElevator.clone()).await,
        }

        ownElevator.log_queue().await;
        network.tick_cleared_order();
    }
}
