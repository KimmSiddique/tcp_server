//! Contains implementation for structs and enums which include the following:
//!
//!     Enum:
//!         LogLevel - Used to let the server know the type of the log message
//!         ServerCommand - Contains Kick and Log at the moment, kick allows to kick Clients while log provides helpful messages
//!
//!     Struct:
//!         ServerDetails - The attributes possessed by the server which include: (1) Vec<Client> (2) ClientCount (3) server_tx (mpsc channel)

use crate::server::client::{Client, ClientDetails, ClientID, Control, ClientIP};
use core::net::SocketAddr;
use rand::Rng;
use tokio::sync::mpsc;
use std::fmt;

#[derive(Debug)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}

pub enum ServerCommand {
    Kick { client_id: ClientID, reason: String },
    Log { level: LogLevel, message: String },
    ClientDisconnected(ClientID),
    ClientConnected(ClientID, ClientIP),
}

impl fmt::Debug for ServerCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerCommand::Kick { client_id, reason } => {
                write!(f, "[CMD: KICK] Client: {client_id} kicked for {reason}")
            }
            ServerCommand::ClientDisconnected(client_id) => {
                write!(f, "[CMD: DISCONNECT] Client: {client_id} disconnected")
            }
            ServerCommand::ClientConnected(client_id, client_ip) => {
                write!(f, "[CMD: CONNECT] Client: {client_id} connected with IP: {client_ip}")
            }
            ServerCommand::Log { level, message } => {
                let log_level = match level {
                    LogLevel::Info => "Info",
                    LogLevel::Error => "Error",
                    LogLevel::Warn => "Warn",
                };
                write!(f, "[CMD: LOG] {log_level}: {message}")
            }
        }
    }
}

// Implementation for ServerCommand
impl ServerCommand {
    pub fn kick(client_id: ClientID, reason: impl Into<String>) -> Self {
        ServerCommand::Kick {
            client_id,
            reason: reason.into(),
        }
    }
    pub fn log(level: LogLevel, message: impl Into<String>) -> Self {
        ServerCommand::Log {
            level,
            message: message.into(),
        }
    }
}
pub struct ServerDetails {
    pub(crate) clients: Vec<Client>,
    pub(crate) client_count: u16,
    pub(crate) server_tx: mpsc::Sender<ServerCommand>,
}

impl ServerDetails {
    pub fn new(server_tx: mpsc::Sender<ServerCommand>) -> Self {
        Self {
            clients: Vec::new(),
            client_count: 0,
            server_tx,
        }
    }

    pub(crate) fn add_client(&mut self, client: Client) {
        self.clients.push(client);
    }

    pub(crate) fn increment_client_count(&mut self) {
        self.client_count += 1;
    }

    pub(crate) fn decrement_client_count(&mut self) {
        self.client_count -= 1;
    }

    pub(crate) fn get_client_count(&self) -> u16 {
        self.client_count
    }
    pub(crate) fn is_client_id_vacant(&self, client_id: ClientID) -> bool {
        self.clients
            .iter()
            .any(|client| client.get_client_id() == client_id)
    }

    pub(crate) fn generate_random_id(&self) -> u16 {
        let mut rng = rand::rng();
        let mut id: u16 = rng.random_range(1000..=9999);
        while !self.is_client_id_vacant(id) {
            id = rng.random_range(1000..=9999)
        }
        id
    }

    pub(crate) fn create_client(
        &self,
        client_addr: SocketAddr,
        control_tx: mpsc::Sender<Control>,
    ) -> Client {
        let generated_client_id = self.generate_random_id();
        Client::new(ClientDetails::new(
            client_addr,
            generated_client_id,
            control_tx,
        ))
    }

    pub(crate) fn get_server_tx_clone(&self) -> mpsc::Sender<ServerCommand> {
        self.server_tx.clone()
    }
}
