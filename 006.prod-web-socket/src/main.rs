use futures_util::StreamExt;
use serde::Deserialize;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Duration;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::Utf8Bytes;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

#[derive(Debug)]
enum AppError {
    Ws(TungsteniteError),
    Json(serde_json::Error),
    Str(String),
}

impl std::error::Error for AppError {}

impl Display for AppError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            AppError::Ws(e) => write!(f, "WebSocket error: {}", e),
            AppError::Json(e) => write!(f, "JSON error: {}", e),
            AppError::Str(e) => write!(f, "Str error: {}", e),
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

impl From<&str> for AppError {
    fn from(value: &str) -> Self {
        AppError::Str(value.to_string())
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
    let urls = vec![
        "wss://nonexistent.com",                          // Wrong domain
        "wss://stream.binance.com:9999/ws/btcusdt@trade", // Wrong port
        "wss://definitely-not-real-123456.binance.com",   // DNS Failure
        "wss://expired.badssl.com",                       // Bad ssl
        "wss://stream.binance.com:9443/ws/INVALID",       // Invalid strem
        "wss://stream.binance.com:9443/ws/btcusdt@trade", // Correct web socket
    ];
    let sleep_times = vec![1000, 2000, 3000, 4000, 4000, 5000];
    let mut url_index = 0;

    loop {
        if url_index == urls.len() {
            println!("Breaking, no more streams to test");
            break;
        }
        println!("Connecting to {}", urls[url_index]);
        let conn_attempt_with_timeout =
            tokio::time::timeout(Duration::from_secs(20), connect_async(urls[url_index])).await;
        match conn_attempt_with_timeout {
            Ok(Ok((mut ws_stream_conn, _))) => {
                println!("WebSocket handshake has been successfully completed");
                let mut msg_index = 0;

                while let Ok(Some(ws_stream_result)) =
                    tokio::time::timeout(Duration::from_secs(10), ws_stream_conn.next()).await
                    && msg_index < 50
                {
                    let msg = ws_stream_result?;
                    match msg {
                        Message::Text(text) => {
                            let message_json: MessageStruct = serde_json::from_str(&text)?;
                            println!("{:?}", message_json);
                        }
                        _ => {}
                    }
                    msg_index += 1;
                }
                let close_frame_option = Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: Utf8Bytes::from("Goodbye"),
                });
                let close_result = ws_stream_conn.close(close_frame_option).await;
                println!("Close result: {:?}", close_result);
                if msg_index == 0 {
                    url_index += 1;
                }
            }
            Ok(Err(ws_error)) => {
                match ws_error {
                    TungsteniteError::Io(err) => {
                        println!("Web Socket IO Error: {}", err);
                    }
                    TungsteniteError::Http(err_box) => {
                        let response = *err_box;
                        let (parts, body) = response.into_parts();
                        let body_string_result = String::from_utf8(body.ok_or("Error")?);
                        match body_string_result {
                            Ok(body_string) => {
                                println!("WebParts Socket HTTP Error Parts: {:?}", parts);
                                println!("Web Socket HTTP Error Body: {:?}", body_string);
                            }
                            Err(err) => {
                                println!("Web Socket HTTP Error Body decode error: {:?}", err);
                            }
                        }
                    }
                    TungsteniteError::Tls(err) => {
                        println!("Web Socket TSL Err: {:?}", err);
                    }
                    _ => {
                        println!("WebSocket other error: {}", ws_error);
                    }
                }
                sleep(Duration::from_millis(sleep_times[url_index])).await;
                url_index += 1;
            }
            Err(timeout_error) => {
                println!("Timeout error: {}", timeout_error);
                url_index += 1;
            }
        }
    }
    Ok(())
}
