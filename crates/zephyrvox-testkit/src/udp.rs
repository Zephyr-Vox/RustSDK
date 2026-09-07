//! Local connected UDP pairs for voice integration tests.

use std::io;

use tokio::net::UdpSocket;

/// Two locally connected UDP sockets suitable for a voice client/server test.
pub struct UdpPair {
    client: UdpSocket,
    server: UdpSocket,
}

impl UdpPair {
    /// Binds two loopback sockets and connects each side to the other.
    pub async fn bind() -> io::Result<Self> {
        let client = UdpSocket::bind(("127.0.0.1", 0)).await?;
        let server = UdpSocket::bind(("127.0.0.1", 0)).await?;
        let client_addr = client.local_addr()?;
        let server_addr = server.local_addr()?;
        client.connect(server_addr).await?;
        server.connect(client_addr).await?;
        Ok(Self { client, server })
    }

    /// Splits the pair into the client and server sockets.
    pub fn into_parts(self) -> (UdpSocket, UdpSocket) {
        (self.client, self.server)
    }
}
