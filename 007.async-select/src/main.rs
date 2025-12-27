use futures_util::stream::StreamExt;
use serde::Deserialize;
use std::fmt;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::select;
use tokio::signal::ctrl_c;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

#[allow(dead_code)]
#[allow(non_snake_case)]
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

#[derive(Error)]
pub enum AppError {
    #[error("App Error: {0}")]
    Tls(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("{0}")]
    Msg(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("JSON error: {0}")]
    Io(#[from] std::io::Error),
}

impl fmt::Debug for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

async fn get_msg(
    wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<Option<Trade>, AppError> {
    let msg_option = wss.next().await;
    let msg_result = msg_option.ok_or_else(|| AppError::Msg("No message".into()))?;
    let msg = msg_result?;
    match msg {
        Message::Text(text) => {
            let trade: Trade = serde_json::from_str(&text)?;
            Ok(Some(trade))
        }
        Message::Ping(_) | Message::Pong(_) => Ok(None),
        Message::Close(_) => Err(AppError::Msg("Connection closed by server".into())),
        _ => Err(AppError::Msg("Unexpected message type".into())),
    }
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let btc_url = "wss://stream.binance.com:9443/ws/btcusdt@trade";
    let eth_url = "wss://stream.binance.com:9443/ws/ethusdt@trade";

    let (mut wss_btc, _) = connect_async(btc_url).await?;
    let (mut wss_eth, _) = connect_async(eth_url).await?;

    loop {
        select! {
            biased;

            _ = ctrl_c() => {
                println!("Shutting down...");
                break;
            },

            result_btc = get_msg(&mut wss_btc) => {
                let result = result_btc?;
                match result {
                    Some(trade) => println!("Trade: {:?}", trade),
                    None => {},
                }
            },

            result_eth = get_msg(&mut wss_eth) => {
                let result = result_eth?;
                match result {
                    Some(trade) => println!("Trade: {:?}", trade),
                    None => {},
                }
            },

        }
    }

    let close_frame_option_btc = Some(CloseFrame {
        code: CloseCode::Normal,
        reason: Utf8Bytes::from("Goodbye from BTC wss"),
    });
    let close_frame_option_eth = Some(CloseFrame {
        code: CloseCode::Normal,
        reason: Utf8Bytes::from("Goodbye from BTC wss"),
    });

    let _ = wss_btc.close(close_frame_option_btc).await;
    let _ = wss_eth.close(close_frame_option_eth).await;
    println!("Gracefully closed sockets");

    Ok(())
}
