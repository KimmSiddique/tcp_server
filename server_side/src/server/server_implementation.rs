//! Server main loop and orchestration
//!
//! Responsibilities:
//!     - Initializes server and binds address to TCP listener
//!     - Starts server and creates mpsc channels for server -> server_tx and server_rx
//!     - Server run function controls the main looping of the server with tokio::select! and delegates tasks to other functions
//!
//!
//! TODO
//! [x] - Implement method to deal with client stream and control_rx
//! [] - Complete match statement for server_rx.recv()
//! [x] - Implement handle_client_task()
//! [] - ...

use crate::server::client::{Client, RequestType};

use super::client::RequestType::*;
use super::server::{ClientID, Control, Error, Server, TcpListener, TcpStream, mpsc};
use super::server_details::{ServerCommand, ServerDetails};
use protocol::ftcp::Command;
use std::{error, fs};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::sync::CancellationToken;

impl Server {
    async fn init() -> Result<
        (
            Server,
            TcpListener,
            mpsc::Sender<ServerCommand>,
            mpsc::Receiver<ServerCommand>,
        ),
        Box<dyn Error>,
    > {
        // Create directory 'files' if it does not exist else ignore
        fs::create_dir_all("files")?;

        // Create new Server and bind to specific address
        let (server_tx, server_rx) = mpsc::channel::<ServerCommand>(32);
        let server_tx_clone = server_tx.clone();
        let server = Server::new(ServerDetails::new(server_tx));
        // Setting the address for the server using an environment variable
        let address = std::env::var("ADDRESS").expect("ADDRESS not set");
        let server_listener = tokio::net::TcpListener::bind(address.clone()).await?;
        println!("Listening on address: {address}");
        Ok((server, server_listener, server_tx_clone, server_rx))
    }

    async fn start_server(&mut self) -> Result<(), Box<dyn Error>> {
        // Init server
        let (server, server_listener, server_tx, server_rx) = Self::init().await?;

        // Self running server function that encapsulates everything
        self.server_run(server, server_listener, server_tx, server_rx)
            .await?;
        Ok(())
    }

    async fn server_run(
        &mut self,
        server: Server,
        server_listener: TcpListener,
        server_tx: mpsc::Sender<ServerCommand>,
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
                    let control_tx_clone = control_tx.clone();
                    let client_addr_clone = client_addr.clone();

                    let client = self.create_client(client_addr, control_tx);
                    let client_id_copy = client.get_client_id();

                    server_tx.send(ServerCommand::ClientConnected(client_id_copy, client_addr_clone)).await?;

                    let read_half_cancel_token = client.get_cancellation_token_clone();
                    let write_half_cancel_token = client.get_cancellation_token_clone();

                    let server_tx_read_clone = self.get_server_tx_clone();
                    let server_tx_write_clone = self.get_server_tx_clone();

                    self.add_client(client);

                    // Spawns 2 tasks in parallel, one will be used to read the client's stream, the other will be used to write to the client stream (i.e. sending files to the user)
                    tokio::spawn(async move {
                        Self::client_reader_task(read_half, client_id_copy, read_half_cancel_token, server_tx_read_clone, control_tx_clone).await;
                    });

                    tokio::spawn(async move {
                        if let Err(err) = Self::client_writer_task(write_half, write_half_cancel_token, control_rx, server_tx_write_clone).await {
                            eprintln!("Error with client writer task function: {err}");
                        }
                    });

                }
                cmd = server_rx.recv() => {
                    // Prints out the command in debug format (implemented manually), will need to implement server command Kick here specifically to check if Some user exists before trying to kick, this prevents kicking the same user twice
                    println!("{:?}", cmd);
                    match cmd {
                        Some(command) => {
                            todo!("Will implement this later...");
                            // Need implementation for kick



                        }
                        None => {
                            // This indicates that the channel has been closed
                            return Ok(())
                        }
                    }

                }


            }
        }
    }

    async fn client_writer_task(
        mut write_half: OwnedWriteHalf,
        cancel_token: CancellationToken,
        mut control_rx: mpsc::Receiver<Control>,
        server_tx: mpsc::Sender<ServerCommand>,
    ) -> Result<(), Box<dyn Error>> {
        loop {
            tokio::select! {
                _check_token_cancelled = cancel_token.cancelled() => {
                    drop(write_half);
                    return Ok(())
                }
                cmd = control_rx.recv() => {
                    match cmd {
                        Some(command) => {
                            if let Control::Request { request_type, text, client_id } = command {
                                match request_type {
                                    RequestType::GetList => {

                                        let mut dir = tokio::fs::read_dir("files").await?;
                                        let mut file_names: Vec<String> = Vec::new();

                                        while let Some(entry) = dir.next_entry().await? {
                                            file_names.push(entry.file_name().to_string_lossy().to_string());
                                        }

                                        // joining each file with \n to be seperated later
                                        let payload = file_names.join("\n");
                                        let payload_len = payload.len() as u64;

                                        // send the length of the payload so client knows how much to read
                                        write_half.write_all(&payload_len.to_be_bytes()).await?;

                                        // send the actual payload to the client
                                        write_half.write_all(payload.as_bytes()).await?;

                                    }
                                    RequestType::GetFile => {


                                    }
                                    RequestType::SendMessage => {


                                    }
                                    RequestType::GetOkay => {

                                    }
                                    RequestType::GetErr => {

                                    }
                                }

                            }
                        }
                        None => {
                            // Means channel has been dropped will get cleaned automatically by Rust
                        }

                    }
                }
            }
        }
    }

    async fn client_reader_task(
        mut read_half: OwnedReadHalf,
        client_id: ClientID,
        cancel_token: CancellationToken,
        server_tx: mpsc::Sender<ServerCommand>,
        control_tx: mpsc::Sender<Control>,
    ) {
        // We have access to the server's transmitter, which will be used to send messages to the receiver...
        // control_rx here will be used to receive messages from the transmitter...
        // MIGHT HAVE TO ADD USE FOR CONTROL_RX LATER!

        loop {
            let mut command_buf = [0u8; 4];
            let mut payload_buf = [0u8; 4];
            // Protocol design: reads exactly 4 bytes for the server command, and u32 (4 bytes as well) for the length of the payload.

            tokio::select! {
                _check_token_cancelled = cancel_token.cancelled() => {
                    drop(read_half);
                    return;
                }
                read_command = read_half.read_exact(&mut command_buf) => {
                    match read_command {
                        Ok(_) => (),
                        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                            cancel_token.cancel();
                            let _ = server_tx.send(ServerCommand::ClientDisconnected(client_id)).await;
                            drop(read_half);
                            return;
                        }
                        Err(all_other_errors) => {
                            eprintln!("Client: {client_id} -> Error reading command buffer: {all_other_errors}");
                            return;
                        }

                    }
                }
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
            let text = String::from_utf8_lossy(&text_buf).to_string();
            let control_tx_clone = control_tx.clone();
            Self::dispatch_command(cmd, text, control_tx_clone, client_id).await;
        }
    }

    async fn dispatch_command(
        cmd: Command,
        text: String,
        control_tx: mpsc::Sender<Control>,
        client_id: ClientID,
    ) {
        match cmd {
            Command::List => {
                if let Err(err) = control_tx
                    .send(Control::request_from(GetList, text, client_id))
                    .await
                {
                    eprintln!("Error dispatching command List: {err}");
                    return;
                }
            }
            Command::Get => {
                if let Err(err) = control_tx
                    .send(Control::request_from(GetFile, text, client_id))
                    .await
                {
                    eprintln!("Error dispatching command Get: {err}");
                    return;
                }
            }
            Command::Send => {
                if let Err(err) = control_tx
                    .send(Control::request_from(SendMessage, text, client_id))
                    .await
                {
                    eprintln!("Error dispatching command Send: {err}");
                    return;
                }
            }
            Command::Okay => {
                if let Err(err) = control_tx
                    .send(Control::request_from(GetOkay, text, client_id))
                    .await
                {
                    eprintln!("Error dispatching command Okay: {err}");
                    return;
                }
            }
            Command::Err => {
                if let Err(err) = control_tx
                    .send(Control::request_from(GetList, text, client_id))
                    .await
                {
                    eprintln!("Error dispatching command Err: {err}");
                    return;
                }
            }
        };
    }
}
