use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::Error;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Deserialize, Debug)]
struct Trade {
    e: String,
    E: u64,
    s: String,
    t: u64,
    p: String,
    q: String,
    T: u64,
    m: bool,
    M: bool,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let url = "wss://stream.binance.com:9443/ws/btcusdt@trade";
    let (mut ws_stream, response) = connect_async(url).await.expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");
    println!("{:?}", response);

    while let Some(msg) = ws_stream.next().await {
        let msg = msg.expect("Error receiving message");
        match msg {
            Message::Text(text) => {
                let text_str = text.as_str();
                let result: Trade = serde_json::from_str(text_str)?;
                println!("{:?}", result);
            }
            Message::Binary(_) => {}
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}
