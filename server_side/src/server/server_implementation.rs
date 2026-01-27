use super::server::{Server, TcpListener, TcpStream, ClientID, Error, Control, mpsc};
use super::server_details::{ServerDetails, ServerCommand};

impl Server {
    async fn init() -> Result<(Server, TcpListener, mpsc::Receiver<ServerCommand>), Box<dyn Error>> {
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

        self.handle_client(server_listener).await?;
        Ok(())
    }

    async fn handle_client(&mut self, server_listener: TcpListener) -> Result<(), Box<dyn Error>> {
        let (client_stream, client_addr) = server_listener.accept().await?;
        let (control_tx, control_rx) = mpsc::channel::<Control>(32);
        let client = self.create_client(client_addr, control_tx);
        let client_id_copy = client.get_client_id();

        // Adding client and incrementing client count
        self.add_client(client);
        self.increment_client_count();
        self.handle_client_task(client_id_copy, client_stream, control_rx)
            .await?;
        Ok(())
    }
    async fn handle_client_task(
        &mut self,
        id: ClientID,
        mut client_stream: TcpStream,
        mut control_rx: mpsc::Receiver<Control>,
    ) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}
