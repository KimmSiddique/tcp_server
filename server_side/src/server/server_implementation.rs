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
use protocol::ftcp::Command;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::OwnedReadHalf;

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
                    // Accepting client stream and address
                    let (client_stream, client_addr) = accept_client?;
                    // Breaking down client stream into read & write
                    let (read_half, write_half) = client_stream.into_split();
                    let (control_tx, control_rx) = mpsc::channel::<Control>(32);
                    let client = self.create_client(client_addr, control_tx);
                    let client_id_copy = client.get_client_id();
                    self.add_client(client);

                    tokio::spawn(async move {
                        // do work here...
                        Self::client_reader_task(read_half, control_rx).await;
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

    async fn client_reader_task(
        mut read_half: OwnedReadHalf,
        mut control_rx: mpsc::Receiver<Control>,
    ) {
        // We have access to the server's transmitter, which will be used to send messages to the receiver...
        // control_rx here will be used to receive messages from the transmitter...
        // MIGHT HAVE TO ADD USE FOR CONTROL_RX LATER!

        let mut command_buf = [0u8; 4];
        let mut payload_buf = [0u8; 4];
        // Protocol design: reads exactly 4 bytes for the server command, and u32 (4 bytes as well) for the length of the payload.
        if let Err(err) = read_half.read_exact(&mut command_buf).await {
            eprintln!("Error reading command buffer: {err}");
            return;
        }
        let cmd = match Command::from_bytes(command_buf) {
            Ok(cmd) => cmd,
            Err(err) => {
                eprintln!("Error converting bytes to Command: {err}");
                return;
            }
        };

        if let Err(err) = read_half.read_exact(&mut payload_buf).await {
            eprintln!("Error reading payload: {err}");
            return;
        }

        const MAX_PAYLOAD_SIZE: usize = 1024;
        let payload_size = u32::from_be_bytes(payload_buf) as usize;

        if payload_size > MAX_PAYLOAD_SIZE {
            eprintln!("Payload size exceeds max limit of {MAX_PAYLOAD_SIZE} bytes");
            return;
        }

        let mut text_buf = vec![0u8; payload_size];
        if let Err(err) = read_half.read_exact(&mut text_buf).await {
            eprintln!("Error reading text buffer of client: {err}");
            return;
        }
        let text = String::from_utf8_lossy(&text_buf);
        Self::dispatch_command(cmd, &text).await;
    }

    async fn dispatch_command(cmd: Command, text: &str) {
        match cmd {
            Command::List => (),
            Command::Get => (),
            Command::Send => (),
            Command::Okay => (),
            Command::Err => (),
            // Will finish later...
        }
    }

    async fn handle_okay() {
        println!("Okay!");
    }
    
    async fn handle_everything_else() {
        println!("Doing something else!");
    }
}
