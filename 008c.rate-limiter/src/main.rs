use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use thiserror::Error;
use tokio::{time, time::Duration, time::Instant};

#[derive(Error, Debug)]
pub enum AppError {
    #[error("reqwest fail")]
    Message(#[from] reqwest::Error),

    #[error("serde fail: {0}")]
    SerdeFromStr(#[from] serde_json::Error),

    #[error("custom fail")]
    Custom(String),
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct USD {
    usd: f64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct Coin {
    #[serde(flatten)]
    prices: HashMap<String, USD>,
}

async fn get(client: &Client, coin: &str) -> Result<(), AppError> {
    let url = format!(
        "https://api.coingecko.com/api/v3/simple/price?ids={}&vs_currencies=usd",
        coin
    );
    let body = client.get(url).send().await?.text().await?;
    let coin_get: Coin = serde_json::from_str(&body)?;
    println!("{:?}: {:?}", coin, coin_get);
    Ok(())
}

#[tokio::main]
async fn main() {
    let mut interval = time::interval(Duration::from_millis(400));
    let client = reqwest::Client::new();
    let mut requests = vec![];
    for _ in 0..7 {
        requests.push("bitcoin");
        requests.push("ethereum");
        requests.push("solana");
    }
    let start = Instant::now();
    while let Some(coin) = requests.pop() {
        interval.tick().await;
        println!("Processing {} at {:?}", coin, start.elapsed());
        match get(&client, coin).await {
            Ok(_) => {}
            Err(e) => eprintln!("Error for {}: {}", coin, e),
        }
    }
}
