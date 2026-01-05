use ordered_float::OrderedFloat;
use std::collections::BTreeMap;
use std::fmt;

#[allow(dead_code)]
struct OrderBook {
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    last_update_id: u64,
}

impl fmt::Display for OrderBook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = String::from("=== Order Book ===\n ASKS:\n");
        for (price, qty) in self.asks.iter() {
            out.push_str(&format!("  {:>8.2} | {:>6.2}\n", price.into_inner(), qty));
        }
        if let Some(spread) = self.spread() {
            out.push_str(&format!("---SPREAD: {:.2}---\n", spread));
        }
        out.push_str(" BIDS:\n");
        for (price, qty) in self.bids.iter().rev() {
            out.push_str(&format!("  {:>8.2} | {:>6.2}\n", price.into_inner(), qty));
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

fn main() {
    let mut ob = OrderBook::new();

    // Add some bids
    ob.update_bid(100.0, 5.0); // price 100, quantity 5
    ob.update_bid(99.5, 10.0);
    ob.update_bid(99.0, 7.5);

    // Add some asks
    ob.update_ask(100.5, 3.0);
    ob.update_ask(101.0, 8.0);
    ob.update_ask(101.5, 12.0);

    println!("Best bid: {}", ob.best_bid().unwrap()); // Should be 100.0
    println!("Best ask: {}", ob.best_ask().unwrap()); // Should be 100.5
    println!("Spread: {}", ob.spread().unwrap()); // Should be 0.5
    println!("Mid price: {}", ob.mid_price().unwrap()); // Should be 100.25

    println!("{}", ob);

    ob.update_bid(100.0, 8.0); // Change quantity from 5.0 to 8.0

    // Remove level (quantity = 0)
    ob.update_ask(101.5, 0.0); // Remove ask at 101.5

    // Add new level
    ob.update_bid(100.2, 15.0);

    println!("{}", ob);
}
