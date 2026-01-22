use circular_buffer::CircularBuffer;
use futures_util::{SinkExt, StreamExt};
use ordered_float::OrderedFloat;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use std::time::Instant;
use std::{collections::BTreeMap, fmt};
use tokio::{
    select,
    time::{Duration, error::Elapsed, interval, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const DEPTH: usize = 10;
type String2 = (String, String);
type String3 = (String, String, String);

#[allow(dead_code)]
enum AppError {
    Ws(tokio_tungstenite::tungstenite::Error),
    Timeout(Elapsed),
    Serde(serde_json::Error),
    FloatParse(std::num::ParseFloatError),
    IntParse(std::num::ParseIntError),
    KrakenWrongFormat,
}

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

#[allow(dead_code)]
struct OrderBook {
    bids: BTreeMap<OrderedFloat<f64>, (f64, String)>,
    asks: BTreeMap<OrderedFloat<f64>, (f64, String)>,
    last_update_id: f64,
}

impl fmt::Display for OrderBook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = writeln!(f, "ASK LEN: {:?}", self.asks.len());
        let _ = writeln!(f, "BIDS LEN: {:?}", self.bids.len());
        let mut out = format!(
            "=== Order Book (Updated at: {})===\n ASKS:\n",
            self.last_update_id
        );
        for (price, data) in self.asks.iter().take(DEPTH) {
            out.push_str(&format!(
                "  {:>8.2} | {:>6.5} | {:<8}\n",
                price.into_inner(),
                data.0,
                data.1
            ));
        }
        if let Some(spread) = self.spread() {
            out.push_str(&format!("---SPREAD: {:.2}---\n", spread));
        }
        if let Some(mid_price) = self.mid_price() {
            out.push_str(&format!("---MID PRICE: {:.2}---\n", mid_price));
        }
        out.push_str(" BIDS:\n");
        for (price, data) in self.bids.iter().rev().take(DEPTH) {
            out.push_str(&format!(
                "  {:>8.2} | {:>6.5} | {:<8}\n",
                price.into_inner(),
                data.0,
                data.1
            ));
        }
        write!(f, "{}", out.as_str())
    }
}

impl OrderBook {
    fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0.0,
        }
    }

    fn update_last_update_id(&mut self, last_update_id: f64) {
        self.last_update_id = last_update_id;
    }

    fn update_bid(&mut self, price: f64, quantity: f64, exchange: String) {
        if quantity == 0. {
            self.bids.remove(&OrderedFloat(price));
            return;
        };
        self.bids.insert(OrderedFloat(price), (quantity, exchange));
    }

    fn update_ask(&mut self, price: f64, quantity: f64, exchange: String) {
        if quantity == 0. {
            self.asks.remove(&OrderedFloat(price));
            return;
        };
        self.asks.insert(OrderedFloat(price), (quantity, exchange));
    }

    fn best_bid(&self) -> Option<f64> {
        let bid = self.bids.last_key_value().map(|(k, _v)| k);
        bid.map(|val| (*val).into_inner())
    }

    fn best_ask(&self) -> Option<f64> {
        let ask = self.asks.first_key_value().map(|(k, _v)| k);
        ask.map(|val| (*val).into_inner())
    }

    fn spread(&self) -> Option<f64> {
        match (self.best_ask(), self.best_bid()) {
            (Some(ask), Some(bid)) => Some(ask - bid),
            _ => None,
        }
    }

    fn mid_price(&self) -> Option<f64> {
        match (self.best_ask(), self.best_bid()) {
            (Some(ask), Some(bid)) => Some((ask + bid) / 2.),
            _ => None,
        }
    }
}
impl std::fmt::Debug for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self) // Delegates to Display
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AppError::Ws(e) => write!(f, "WebSocket error: {}", e),
            AppError::Timeout(e) => write!(f, "Connection timeout: {}", e),
            AppError::Serde(e) => write!(f, "JSON parse error: {}", e),
            AppError::FloatParse(e) => write!(f, "Float parse error: {}", e),
            AppError::IntParse(e) => write!(f, "Int parse error: {}", e),
            AppError::KrakenWrongFormat => write!(f, "Kraken sent different format"),
        }
    }
}

impl std::error::Error for AppError {}

impl From<tokio_tungstenite::tungstenite::Error> for AppError {
    fn from(value: tokio_tungstenite::tungstenite::Error) -> Self {
        AppError::Ws(value)
    }
}

impl From<Elapsed> for AppError {
    fn from(value: Elapsed) -> Self {
        AppError::Timeout(value)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(value: serde_json::Error) -> Self {
        AppError::Serde(value)
    }
}

impl From<std::num::ParseFloatError> for AppError {
    fn from(value: std::num::ParseFloatError) -> Self {
        AppError::FloatParse(value)
    }
}

impl From<std::num::ParseIntError> for AppError {
    fn from(value: std::num::ParseIntError) -> Self {
        AppError::IntParse(value)
    }
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
    let mut binance_order_book: OrderBook = OrderBook::new();
    let mut kraken_order_book: OrderBook = OrderBook::new();
    let mut interval = interval(Duration::from_secs(1));
    let mut binance_msg_no = 0;
    let mut kraken_msg_no = 0;
    let mut ticks = 0;
    let mut binance_buf = CircularBuffer::<200, u128>::new();
    let mut kraken_buf = CircularBuffer::<200, u128>::new();
    loop {
        select! {
            Some(msg_res) = binance_ws_socket.next() => {
                let msg_recv = msg_res?;
                match msg_recv {
                    Message::Text(msg_recv_text) => {
                        let now = Instant::now();
                        binance_msg_no += 1;
                        let msg: BinanceMessageDeserialized = serde_json::from_str(&msg_recv_text)?;
                        binance_order_book.asks.clear();
                        binance_order_book.bids.clear();
                        binance_order_book.update_last_update_id(msg.lastUpdateId as f64);
                        for pair in msg.asks.iter() {
                            binance_order_book.update_ask(pair.0.parse()?, pair.1.parse()?, "Binance".to_string());
                        }
                        for pair in msg.bids.iter() {
                            binance_order_book.update_bid(pair.0.parse()?, pair.1.parse()?, "Binance".to_string());
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
                        kraken_msg_no += 1;
                        let data_value: Value = serde_json::from_str(&msg_recv_text)?;
                        if let Value::Array(arr) = data_value {
                            let data_content = arr[1].as_object().ok_or(AppError::KrakenWrongFormat)?;
                            let kraken_data: KrakenData =
                                serde_json::from_value(Value::Object(data_content.clone()))
                                    .map_err(|_| AppError::KrakenWrongFormat)?;
                            match kraken_data {
                                KrakenData::Snapshot{as_, bs} => {
                                    kraken_order_book.asks.clear();
                                    kraken_order_book.bids.clear();
                                    let mut max_ts = 0f64;

                                    for (price, volume, ts) in as_ {
                                        kraken_order_book.update_ask(price.parse()?, volume.parse()?, "Kraken".to_string());
                                        max_ts = max_ts.max(ts.parse::<f64>()?);
                                    }
                                    for (price, volume, ts) in bs {
                                        kraken_order_book.update_bid(price.parse()?, volume.parse()?, "Kraken".to_string());
                                        max_ts = max_ts.max(ts.parse::<f64>()?);
                                    }

                                    kraken_order_book.last_update_id = max_ts;
                                },
                                KrakenData::UpdateAsk { a } => {
                                    let mut max_ts: f64 = kraken_order_book.last_update_id;

                                    for (price, volume, ts) in a {
                                        let p: f64 = price.parse()?;
                                        let q: f64 = volume.parse()?;
                                        kraken_order_book.update_ask(p, q, "Kraken".to_string());

                                        let t: f64 = ts.parse()?;
                                        if t > max_ts {
                                            max_ts = t;
                                        }
                                    }

                                    kraken_order_book.last_update_id = max_ts;
                                }
                                KrakenData::UpdateBid { b } => {
                                    let mut max_ts: f64 = kraken_order_book.last_update_id;

                                    for (price, volume, ts) in b {
                                        let p: f64 = price.parse()?;
                                        let q: f64 = volume.parse()?;
                                        kraken_order_book.update_bid(p, q, "Kraken".to_string());

                                        let t: f64 = ts.parse()?;
                                        if t > max_ts {
                                            max_ts = t;
                                        }
                                    }

                                    kraken_order_book.last_update_id = max_ts;
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
            _ = interval.tick() => {
                ticks += 1;
                println!("Mesages received from binance / sec: {}", binance_msg_no/ticks);
                println!("Percentiles Binance: P50, P95, P99 (microseconds) {:?}", percentiles(&binance_buf));
                println!("{}", binance_order_book);
                println!("Mesages received from kraken: / sec: {}", kraken_msg_no/ticks);
                println!("Percentiles Kraken: P50, P95, P99 (microseconds) {:?}", percentiles(&kraken_buf));
                println!("{}", kraken_order_book);
            }
        }
    }
}
