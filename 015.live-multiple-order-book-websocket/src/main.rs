use circular_buffer::CircularBuffer;
use futures_util::lock::Mutex;
use futures_util::{SinkExt, StreamExt};
use live_order_book::errors::AppError;
use live_order_book::order_book::OrderBook;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::{
    select,
    time::{Duration, interval, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type String2 = (String, String);
type String3 = (String, String, String);

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct BinanceMessageDeserialized {
    lastUpdateId: u64,
    bids: Vec<String2>,
    asks: Vec<String2>,
}
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum KrakenData {
    Snapshot {
        #[serde(rename = "as")]
        as_: Vec<String3>,
        bs: Vec<String3>,
    },
    UpdateAsk {
        a: Vec<String3>,
    },
    UpdateBid {
        b: Vec<String3>,
    },
    Other(Value),
}

fn percentiles(buffer: &CircularBuffer<200, u128>) -> Option<(u128, u128, u128)> {
    if buffer.is_empty() {
        return None;
    }

    let mut buf_vec: Vec<u128> = buffer.iter().copied().collect();
    buf_vec.sort_unstable();

    let len = buf_vec.len();
    let p50 = buf_vec[len * 50 / 100];
    let p95 = buf_vec[len * 95 / 100];
    let p99 = buf_vec[len * 99 / 100];

    Some((p50, p95, p99))
}

#[derive(Clone, Serialize)]
struct OrderBookUpdate {
    book: OrderBook,
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    // binance connection
    let binance_url = "wss://stream.binance.com:9443/ws/btcusdt@depth10@100ms";
    let (mut binance_ws_socket, _) =
        timeout(Duration::from_millis(5000), connect_async(binance_url)).await??;

    // kraken connection
    let kraken_url = "wss://ws.kraken.com";
    let (mut kraken_ws_socket, _) =
        timeout(Duration::from_millis(5000), connect_async(kraken_url)).await??;
    let sub_msg = json!({
        "event": "subscribe",
        "pair": ["XBT/USD"],
        "subscription": {"name": "book", "depth": 10}
    });
    let sub_msg_text = serde_json::to_string(&sub_msg)?;
    kraken_ws_socket
        .send(Message::Text(sub_msg_text.into()))
        .await?;

    let binance_order_book = Arc::new(Mutex::new(OrderBook::new()));
    let kraken_order_book = Arc::new(Mutex::new(OrderBook::new()));
    let mut interval = interval(Duration::from_secs(1));
    let mut binance_buf = CircularBuffer::<200, u128>::new();
    let mut kraken_buf = CircularBuffer::<200, u128>::new();
    let (tx, _) = broadcast::channel(200);
    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    loop {
        select! {
            Some(msg_res) = binance_ws_socket.next() => {
                let msg_recv = msg_res?;
                match msg_recv {
                    Message::Text(msg_recv_text) => {
                        let now = Instant::now();
                        let msg: BinanceMessageDeserialized = serde_json::from_str(&msg_recv_text)?;
                        let mut binance_order_book_locked = binance_order_book.lock().await;
                        binance_order_book_locked.asks.clear();
                        binance_order_book_locked.bids.clear();
                        binance_order_book_locked.update_last_update_id(msg.lastUpdateId as f64);
                        for pair in msg.asks.iter() {
                            binance_order_book_locked.update_ask(pair.0.parse()?, pair.1.parse()?, "Binance".to_string());
                        }
                        for pair in msg.bids.iter() {
                            binance_order_book_locked.update_bid(pair.0.parse()?, pair.1.parse()?, "Binance".to_string());
                        }
                        binance_buf.push_back(now.elapsed().as_micros());
                    }
                    Message::Ping(_) | Message::Pong(_) => {}
                    _ => {}
                }
            }
            Some(msg_res) = kraken_ws_socket.next() => {
                let msg_recv = msg_res?;
                match msg_recv {
                    Message::Text(msg_recv_text) => {
                        let now = Instant::now();
                        let data_value: Value = serde_json::from_str(&msg_recv_text)?;
                        if let Value::Array(arr) = data_value {
                            let data_content = arr[1].as_object().ok_or(AppError::KrakenWrongFormat)?;
                            let kraken_data: KrakenData =
                                serde_json::from_value(Value::Object(data_content.clone()))
                                    .map_err(|_| AppError::KrakenWrongFormat)?;
                            match kraken_data {
                                KrakenData::Snapshot{as_, bs} => {
                                    let mut kraken_order_book_locked = kraken_order_book.lock().await;
                                    kraken_order_book_locked.asks.clear();
                                    kraken_order_book_locked.bids.clear();
                                    let mut max_ts = 0f64;

                                    for (price, volume, ts) in as_ {
                                        kraken_order_book_locked.update_ask(price.parse()?, volume.parse()?, "Kraken".to_string());
                                        max_ts = max_ts.max(ts.parse::<f64>()?);
                                    }
                                    for (price, volume, ts) in bs {
                                        kraken_order_book_locked.update_bid(price.parse()?, volume.parse()?, "Kraken".to_string());
                                        max_ts = max_ts.max(ts.parse::<f64>()?);
                                    }

                                    kraken_order_book_locked.last_update_id = max_ts;
                                },
                                KrakenData::UpdateAsk { a } => {
                                    let mut kraken_order_book_locked = kraken_order_book.lock().await;
                                    let mut max_ts: f64 = kraken_order_book_locked.last_update_id;

                                    for (price, volume, ts) in a {
                                        let p: f64 = price.parse()?;
                                        let q: f64 = volume.parse()?;
                                        let mut kraken_order_book_locked = kraken_order_book.lock().await;
                                        kraken_order_book_locked.update_ask(p, q, "Kraken".to_string());

                                        let t: f64 = ts.parse()?;
                                        if t > max_ts {
                                            max_ts = t;
                                        }
                                    }

                                    kraken_order_book_locked.last_update_id = max_ts;
                                }
                                KrakenData::UpdateBid { b } => {
                                    let mut kraken_order_book_locked = kraken_order_book.lock().await;
                                    let mut max_ts: f64 = kraken_order_book_locked.last_update_id;

                                    for (price, volume, ts) in b {
                                        let p: f64 = price.parse()?;
                                        let q: f64 = volume.parse()?;
                                        kraken_order_book_locked.update_bid(p, q, "Kraken".to_string());

                                        let t: f64 = ts.parse()?;
                                        if t > max_ts {
                                            max_ts = t;
                                        }
                                    }

                                    kraken_order_book_locked.last_update_id = max_ts;
                                }
                                KrakenData::Other(_) => {}
                            }
                            kraken_buf.push_back(now.elapsed().as_micros());
                        }
                    }
                   Message::Ping(_) | Message::Pong(_) => {}
                    _ => {}
                }
            }
            Ok((tcp_stream, _)) = listener.accept() => {
                let mut rx = tx.subscribe();
                tokio::spawn(async move {
                    if let Ok(mut ws_stream) = tokio_tungstenite::accept_async(tcp_stream).await {
                        while let Ok(update) = rx.recv().await {
                            let json = serde_json::to_string(&update).unwrap();
                            let msg = Message::text(json);
                            if ws_stream.send(msg).await.is_err() {
                                break;
                            }
                        }
                    }
                });
            }
            _ = interval.tick() => {
                // ticks += 1;
                let binance_book = binance_order_book.lock().await;
                let kraken_book = kraken_order_book.lock().await;
                tx.send(OrderBookUpdate {
                    book: binance_book.clone()
                }).ok();
                tx.send(OrderBookUpdate {
                    book: kraken_book.clone()
                }).ok();
            }
        }
    }
}
