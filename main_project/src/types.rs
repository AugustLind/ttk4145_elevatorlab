use crossbeam_channel::Sender;
use driver_rust::elevio::elev::{Elevator, DIRN_DOWN, DIRN_STOP, DIRN_UP};
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::time::{Duration, Instant};
use strum_macros::EnumIter;


pub struct ElevatorFSM {
    pub driver:    Elevator,
    pub floor:     u8,
    pub direction: Direction,
    pub behaviour: Behaviour,
    pub serving:   Option<Order>,
}

pub struct ElevatorGuard {
    pub(crate) state: Mutex<ElevatorFSM>,
    pub(crate) queue: Mutex<Vec<Order>>,
}

pub enum OrderResult {
    Completed(Order),
    Empty,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Order {
    pub floor: u8,
    pub order_type: ButtonType,
}

#[repr(u8)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, EnumIter, Copy)]
pub enum ButtonType {
    CabCall  = 2,
    HallUp   = 0,
    HallDown = 1,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Roles {
    Master,
    Slave,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Behaviour {
    #[serde(rename = "idle")]
    Idle,
    #[serde(rename = "moving")]
    Moving,
    #[serde(rename = "doorOpen")]
    DoorOpen,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Stop = DIRN_STOP,
    Up   = DIRN_UP,
    Down = DIRN_DOWN,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipMsg {
    pub id: String,
    pub hall_orders: Vec<Order>,
    pub cab_orders: Vec<Order>,
    pub floor: u8,
    pub direction: Direction,
    pub behaviour: Behaviour,
    pub counter: i32,
    pub role: Roles,
    pub assignments: HashMap<String, Vec<Order>>,
    pub peer_cab_orders: HashMap<String, Vec<Order>>,
    #[serde(rename = "clearedOrder")]
    pub cleared_order: Option<Order>,
}

pub struct Network {
    pub state: GossipMsg,
    pub incoming: tokio::sync::broadcast::Sender<GossipMsg>,
    pub udp_tx: Sender<GossipMsg>,
    pub cleared_at: Option<Instant>,
}


#[derive(Serialize)]
pub struct Message {
    #[serde(rename = "hallRequests")]
    pub hall_requests: Vec<[bool; 2]>,
    pub states: HashMap<String, AssignmentState>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssignmentState {
    pub behaviour: Behaviour,
    pub floor: u8,
    pub direction: Direction,
    #[serde(rename = "cabRequests")]
    pub cab_requests: Vec<bool>,
}

pub struct RequestAssigner {
    pub message: Message,
    pub id: String,
    pub role: Roles,
    pub last_published_assignments: HashMap<String, Vec<Order>>,
    pub last_seen: HashMap<String, Instant>,
    pub cached_peers: HashMap<String, GossipMsg>,
    pub peer_ttl: Duration,
    pub new_peer_joined: bool,
}
