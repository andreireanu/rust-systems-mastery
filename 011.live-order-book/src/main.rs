use futures_util::StreamExt;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use std::{collections::BTreeMap, fmt};
use tokio::{
    select,
    time::{Duration, error::Elapsed, interval, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[allow(dead_code)]
enum AppError {
    Ws(tokio_tungstenite::tungstenite::Error),
    Timeout(Elapsed),
    Serde(serde_json::Error),
    Parse(std::num::ParseFloatError),
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct MessageDeseriealized {
    lastUpdateId: u64,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

#[allow(dead_code)]
struct OrderBook {
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    last_update_id: u64,
}

impl fmt::Display for OrderBook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = format!(
            "=== Order Book (Updated at: {})===\n ASKS:\n",
            self.last_update_id
        );
        for (price, qty) in self.asks.iter() {
            out.push_str(&format!("  {:>8.2} | {:>6.5}\n", price.into_inner(), qty));
        }
        if let Some(spread) = self.spread() {
            out.push_str(&format!("---SPREAD: {:.2}---\n", spread));
        }
        if let Some(mid_price) = self.mid_price() {
            out.push_str(&format!("---MID PRICE: {:.2}---\n", mid_price));
        }
        out.push_str(" BIDS:\n");
        for (price, qty) in self.bids.iter().rev() {
            out.push_str(&format!("  {:>8.2} | {:>6.5}\n", price.into_inner(), qty));
        }
        write!(f, "{}", out.as_str())
    }
}

impl OrderBook {
    fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
        }
    }

    fn update_last_update_id(&mut self, last_update_id: u64) {
        self.last_update_id = last_update_id;
    }

    fn update_bid(&mut self, price: f64, quantity: f64) {
        if quantity == 0. {
            self.bids.remove(&OrderedFloat(price));
            return;
        };
        self.bids.insert(OrderedFloat(price), quantity);
    }

    fn update_ask(&mut self, price: f64, quantity: f64) {
        if quantity == 0. {
            self.asks.remove(&OrderedFloat(price));
            return;
        };
        self.asks.insert(OrderedFloat(price), quantity);
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
            AppError::Parse(e) => write!(f, "Float parse error: {}", e),
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
        AppError::Parse(value)
    }
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let url = "wss://stream.binance.com:9443/ws/btcusdt@depth10@100ms";
    let (mut ws_socket, _) = timeout(Duration::from_millis(5000), connect_async(url)).await??;
    let mut order_book: OrderBook = OrderBook::new();
    let mut interval = interval(Duration::from_secs(1));
    loop {
        select! {
            Some(msg_res) = ws_socket.next() => {
                order_book = OrderBook::new();
                let msg_recv = msg_res?;
                match msg_recv {
                    Message::Text(msg_recv_text) => {
                        let msg: MessageDeseriealized = serde_json::from_str(&msg_recv_text)?;
                        order_book.update_last_update_id(msg.lastUpdateId);
                        for pair in msg.asks.iter() {
                            order_book.update_ask(pair.0.parse()?, pair.1.parse()?);
                        }
                        for pair in msg.bids.iter() {
                            order_book.update_bid(pair.0.parse()?, pair.1.parse()?);
                        }
                    }
                    Message::Ping(_) | Message::Pong(_) => {
                        println!("PING or PONG");
                    }
                    _ => {}
                }
            }
            _ = interval.tick() => {
                println!("{}", order_book);
            }
        }
    }
}
