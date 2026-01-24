use ordered_float::OrderedFloat;
use std::{collections::BTreeMap, fmt};

const DEPTH: usize = 10;

#[allow(dead_code)]
pub struct OrderBook {
    pub bids: BTreeMap<OrderedFloat<f64>, (f64, String)>,
    pub asks: BTreeMap<OrderedFloat<f64>, (f64, String)>,
    pub last_update_id: f64,
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
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0.0,
        }
    }

    pub fn update_last_update_id(&mut self, last_update_id: f64) {
        self.last_update_id = last_update_id;
    }

    pub fn update_bid(&mut self, price: f64, quantity: f64, exchange: String) {
        if quantity == 0. {
            self.bids.remove(&OrderedFloat(price));
            return;
        };
        self.bids.insert(OrderedFloat(price), (quantity, exchange));
    }

    pub fn update_ask(&mut self, price: f64, quantity: f64, exchange: String) {
        if quantity == 0. {
            self.asks.remove(&OrderedFloat(price));
            return;
        };
        self.asks.insert(OrderedFloat(price), (quantity, exchange));
    }

    pub fn best_bid(&self) -> Option<f64> {
        let bid = self.bids.last_key_value().map(|(k, _v)| k);
        bid.map(|val| (*val).into_inner())
    }

    pub fn best_ask(&self) -> Option<f64> {
        let ask = self.asks.first_key_value().map(|(k, _v)| k);
        ask.map(|val| (*val).into_inner())
    }

    pub fn spread(&self) -> Option<f64> {
        match (self.best_ask(), self.best_bid()) {
            (Some(ask), Some(bid)) => Some(ask - bid),
            _ => None,
        }
    }

    pub fn mid_price(&self) -> Option<f64> {
        match (self.best_ask(), self.best_bid()) {
            (Some(ask), Some(bid)) => Some((ask + bid) / 2.),
            _ => None,
        }
    }
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}
