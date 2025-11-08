use iroh::{Endpoint, SecretKey, discovery::mdns::MdnsDiscovery};
use orbit::network::APLN;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let secret = SecretKey::generate(rand::rngs::OsRng);
    let endpoint = Endpoint::builder()
        .discovery(Box::new(MdnsDiscovery::new(secret.public())?))
        .secret_key(secret)
        .alpns(vec![APLN.as_bytes().to_vec()])
        .bind()
        .await?;
    println!("Node ID is {}", endpoint.node_id());
    while let Some(incoming) = endpoint.accept().await {
        let connection = incoming.accept()?.await?;
        let (_send_stream, mut recv_stream) = connection.accept_bi().await?;

        while let Some(chunk) = recv_stream.read_chunk(1500, true).await? {
            println!("Received chunk: {:?}", chunk);
        }
    }
    Ok(())
}
