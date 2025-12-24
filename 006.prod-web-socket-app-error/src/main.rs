use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::Error as SerdeJsonError;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug)]
enum AppError {
    Ws(TungsteniteError),
    Json(SerdeJsonError),
}

impl std::error::Error for AppError {}

impl Display for AppError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            AppError::Ws(e) => write!(f, "WebSocket error: {}", e),
            AppError::Json(e) => write!(f, "JSON error: {}", e),
        }
    }
}

impl From<serde_json::Error> for AppError {
    fn from(value: serde_json::Error) -> Self {
        AppError::Json(value)
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for AppError {
    fn from(value: tokio_tungstenite::tungstenite::Error) -> Self {
        AppError::Ws(value)
    }
}

#[allow(dead_code)]
#[allow(non_snake_case)]
#[derive(Deserialize, Debug)]
struct MessageStruct {
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
async fn main() -> Result<(), AppError> {
    let url = "wss://stream.binance.com:9443/ws/btcusdt@trade";

    let (mut ws_stream_conn, _) = connect_async(url).await.expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");
    while let Some(ws_stream_result) = ws_stream_conn.next().await {
        let msg = ws_stream_result?;
        match msg {
            Message::Text(text) => {
                let message_json: MessageStruct = serde_json::from_str(&text)?;
                println!("{:?}", message_json);
            }
            _ => {}
        }
    }
    Ok(())
}
