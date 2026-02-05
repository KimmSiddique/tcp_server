use core::net::SocketAddr;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

// TYPE OF CLIENT ID DECLARED HERE AS u16
pub type ClientID = u16;
pub type ClientIP = SocketAddr;

pub enum RequestType {
    GetList,
    GetFile,
    SendMessage,
    GetOkay,
    GetErr,
}

pub enum Control {
    Bytes(Vec<u8>),
    Request {
        request_type: RequestType,
        text: String,
        client_id: ClientID,
    },
}

impl Control {
    pub fn request_from(request_type: RequestType, text: impl Into<String>, client_id: ClientID) -> Self {
        Self::Request { request_type, text: text.into(), client_id }
    }
}

pub struct Client {
    client_details: ClientDetails,
    client_state: ClientState,
}

// Client Details struct, contains a mpsc sender channel
// Will send commands which will be recieved later and dealt with
pub struct ClientDetails {
    client_id: ClientID,
    client_addr: SocketAddr,
    control_tx: mpsc::Sender<Control>,
    cancel_token: CancellationToken,
}

#[derive(PartialEq, Eq)]
pub enum ClientState {
    Free,
    Processing,
}

// Implementation for ClientDetails
impl ClientDetails {
    pub fn new(
        client_addr: SocketAddr,
        client_id: ClientID,
        control_tx: mpsc::Sender<Control>,
    ) -> Self {
        Self {
            client_id,
            client_addr,
            control_tx,
            cancel_token: CancellationToken::new(),
        }
    }

    fn get_client_id(&self) -> u16 {
        self.client_id
    }

    fn get_client_addr(&self) -> &SocketAddr {
        &self.client_addr
    }

    fn get_cancellation_token_clone(&self) -> CancellationToken {
        self.cancel_token.clone()
    }
}

// Implementation for Client
impl Client {
    pub fn new(client_details: ClientDetails) -> Self {
        Self {
            client_details,
            client_state: ClientState::Free,
        }
    }

    pub fn get_client_id(&self) -> u16 {
        self.client_details.get_client_id()
    }

    pub fn get_client_addr(&self) -> &SocketAddr {
        self.client_details.get_client_addr()
    }

    pub fn get_client_details(&self) -> &ClientDetails {
        &self.client_details
    }

    pub fn get_cancellation_token_clone(&self) -> CancellationToken {
        self.client_details.get_cancellation_token_clone()
    }
}
