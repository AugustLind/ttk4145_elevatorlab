use crossbeam_channel;
use driver_rust::elevio::elev::Elevator;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::time::{Instant, Duration};
use strum_macros::EnumIter;


// --- FSM types ---

#[derive(Copy, Clone, Debug)]
pub enum ElevState {
    Init,
    WorkingOrder,
    Idle,
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
    CabCall = 2,
    HallUp = 0,
    HallDown = 1,
}

use tokio::sync::Mutex;

pub struct ElevatorInner {
    pub driver: Elevator,
    pub prev_floor: u8,
    pub direction: u8,
    pub state: ElevState,
    pub currently_serving: Option<Order>,
}

pub struct ElevatorFSM {
    pub queue: Mutex<Vec<Order>>,
    pub inner: Mutex<ElevatorInner>,
}

// --- Shared types 

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Roles {
    Master,
    Slave,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Behaviour {
    #[serde(rename = "idle")]
    Idle,
    #[serde(rename = "moving")]
    Moving,
    #[serde(rename = "doorOpen")]
    DoorOpen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Up,
    Down,
    Stop,
}

// --- Network types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMSG {
    pub id: String,
    pub external_orders: Vec<Order>,
    pub internal_orders: Vec<Order>,
    pub floor: u8,
    pub direction: u8,
    pub status: Behaviour,
    pub counter: i32,
    pub role: Roles,
    pub assignments: std::collections::HashMap<String, Vec<Order>>,
    pub all_cab_orders: std::collections::HashMap<String, Vec<Order>>,
    #[serde(rename = "clearedOrder")]
    pub cleared_order: Option<Order>,
}

pub struct Heartbeat {
    pub msg: HeartbeatMSG,
    pub tx_broadcast: tokio::sync::broadcast::Sender<HeartbeatMSG>,
    pub tx_udp: crossbeam_channel::Sender<HeartbeatMSG>,
}

// --- Request assigner types ---

#[derive(Serialize)]
pub struct Message {
    #[serde(rename = "hallRequests")]
    pub hall_requests: Vec<[bool; 2]>,
    pub states: HashMap<String, ElevatorState>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ElevatorState {
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
    pub peer_states: HashMap<String, HeartbeatMSG>,  // Cache last known state of each peer
    pub peer_ttl: Duration,
}