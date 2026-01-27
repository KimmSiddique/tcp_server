use crate::server::client::{Client, ClientDetails, ClientID, Control};
use core::net::SocketAddr;
use rand::Rng;
use tokio::sync::mpsc;

pub enum ServerCommand {
    Kick(ClientID),
    Log(String),

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
            server_tx
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
}
