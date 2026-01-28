//! Server main loop and orchestration
//!
//! Responsibilities:
//!     - Initializes server and binds address to TCP listener
//!     - Starts server and creates mpsc channels for server -> server_tx and server_rx
//!     - Server run function controls the main looping of the server with tokio::select! and delegates tasks to other functions
//!
//!
//! TODO
//! [] - Implement method to deal with client stream and control_rx
//! [] - Complete match statement for server_rx.recv()
//! [] - Implement handle_client_task()
//! [] - ...

use super::server::{ClientID, Control, Error, Server, TcpListener, TcpStream, mpsc};
use super::server_details::{ServerCommand, ServerDetails};

impl Server {
    async fn init() -> Result<(Server, TcpListener, mpsc::Receiver<ServerCommand>), Box<dyn Error>>
    {
        // Create new Server and bind to specific address
        let (server_tx, server_rx) = mpsc::channel::<ServerCommand>(32);
        let server = Server::new(ServerDetails::new(server_tx));
        // Setting the address for the server using an environment variable
        let address = std::env::var("ADDRESS").expect("ADDRESS not set");
        let server_listener = tokio::net::TcpListener::bind(address.clone()).await?;
        println!("Listening on address: {address}");
        Ok((server, server_listener, server_rx))
    }

    async fn start_server(&mut self) -> Result<(), Box<dyn Error>> {
        // Init server
        let (server, server_listener, server_rx) = Self::init().await?;

        // Self running server function that encapsulates everything
        self.server_run(server, server_listener, server_rx).await?;
        Ok(())
    }

    async fn server_run(
        &mut self,
        server: Server,
        server_listener: TcpListener,
        mut server_rx: mpsc::Receiver<ServerCommand>,
    ) -> Result<(), Box<dyn Error>> {
        loop {
            // VERY IMPORTANT: tokio::select! acts kind of like a match statement that auto awaits your async functions

            tokio::select! {
                accept_client = server_listener.accept() => {
                    let (client_stream, client_addr) = accept_client?;
                    let (control_tx, control_rx) = mpsc::channel::<Control>(32);
                    let client = self.create_client(client_addr, control_tx);
                    let client_id_copy = client.get_client_id();
                    self.add_client(client);

                    tokio::spawn(async move {
                        // do work here...
                        Self::handle_client_task(client_stream, control_rx).await;
                    });

                }
                cmd = server_rx.recv() => {
                    match cmd {
                        Some(command) => {
                            todo!("Will implement this later...");
                            // Server clones the client's transmitter (control_tx) and then will be recieved by control_rx in handle_client_task...

                        }
                        None => {

                        }
                    }

                }

            }
        }
    }

    async fn handle_client_task(client_stream: TcpStream, control_rx: mpsc::Receiver<Control>) {
        // We have access to the server's transmitter, which will be used to send messages to the receiver...
        // control_rx here will be used to
        let buffer: Vec<u8> = Vec::new();
    }
}
