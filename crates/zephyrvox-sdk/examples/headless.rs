//! Minimal headless host for manual CommunityServer interoperability checks.

use std::{env, error::Error};

use zephyrvox_sdk::{Client, ClientConfig, LoginRequest, ServerCard};

/// Discovers the server and optionally opens an authenticated control session.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let card_value = env::var("ZEPHYRVOX_SERVER_CARD")?;
    let card = ServerCard::parse(&card_value)?;
    let client = Client::new(ClientConfig::new(card))?;

    let metadata = client.discover().await?;
    println!(
        "protocol={} codecs={} voice={}:{}",
        metadata.protocol_version,
        metadata.codecs.len(),
        metadata.voice_endpoint.host,
        metadata.voice_endpoint.port
    );

    if let (Ok(username), Ok(password)) = (
        env::var("ZEPHYRVOX_USERNAME"),
        env::var("ZEPHYRVOX_PASSWORD"),
    ) {
        let device_id = env::var("ZEPHYRVOX_DEVICE_ID").unwrap_or_else(|_| "headless".to_owned());
        client
            .login(LoginRequest {
                username,
                password,
                device_id,
            })
            .await?;
        let control = client.connect().await?;
        control.wait_until_live().await?;
        println!("control connection is live");
    }

    client.shutdown().await?;
    Ok(())
}
