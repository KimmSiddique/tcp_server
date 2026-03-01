//! Implementation for Server methods
//!
//! Responsibilities:
//!     - Creating a new server instance
//!     - Certain methods such as adding a client, incrementing client count or decrementing client count and creating clients

use super::server_details::{ServerCommand, ServerDetails};
pub use crate::server::client::Client;
pub use crate::server::client::{ClientID, Control};
pub use std::error::Error;
pub use std::net::SocketAddr;
pub use tokio::net::{TcpListener, TcpStream};
pub use tokio::sync::mpsc;

pub struct Server {
    pub server_details: ServerDetails,
}

impl Server {
    pub fn new(server_details: ServerDetails) -> Self {
        Self { server_details }
    }

    pub fn add_client(&mut self, client: Client) {
        self.server_details.add_client(client);
        self.increment_client_count();
    }

    pub fn increment_client_count(&mut self) {
        self.server_details.increment_client_count();
    }

    pub fn decrement_client_count(&mut self) {
        self.server_details.decrement_client_count();
    }

    pub fn create_client(
        &self,
        client_addr: SocketAddr,
        control_tx: mpsc::Sender<Control>,
    ) -> Client {
        self.server_details.create_client(client_addr, control_tx)
    }

    pub fn get_server_tx_clone(&self) -> mpsc::Sender<ServerCommand> {
        self.server_details.get_server_tx_clone()
    }
}
