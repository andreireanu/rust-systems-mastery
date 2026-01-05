use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use thiserror::Error;
use tokio::{join, time::Instant, try_join};

#[derive(Error, Debug)]
pub enum AppError {
    #[error("reqwest fail")]
    Message(#[from] reqwest::Error),

    #[error("serde fail")]
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
async fn main() -> Result<(), AppError> {
    let client = reqwest::Client::new();

    println!("-----");
    let time = Instant::now();
    get(&client, "bitcoin").await?;
    get(&client, "ethereum").await?;
    get(&client, "solana").await?;
    let elapsed_seq = time.elapsed();
    println!("Elapsed SEQUENTALLY: {:?}", elapsed_seq);

    println!("-----");
    let time = Instant::now();
    let _ = try_join!(
        get(&client, "bitcoin"),
        get(&client, "ethereums"),
        get(&client, "solana"),
        async {
            Err::<(), AppError>(AppError::Custom("Custom error".into())) // Force error
        }
    );

    let elapsed_try_join = time.elapsed();
    println!("Elapsed on TRY JOIN: {:?}", elapsed_try_join);

    println!("-----");
    let time = Instant::now();
    let _ = join!(
        get(&client, "bitcoin"),
        get(&client, "ethereum"),
        get(&client, "solana")
    );
    let elapsed_join = time.elapsed();
    println!("Elapsed on JOIN: {:?}", elapsed_join);

    Ok(())
}

// -----
// "bitcoin": Coin { prices: {"bitcoin": USD { usd: 87790.0 }} }
// "ethereum": Coin { prices: {"ethereum": USD { usd: 2940.96 }} }
// "solana": Coin { prices: {"solana": USD { usd: 124.49 }} }
// Elapsed SEQUENTALLY: 459.017034ms
// -----
// "ethereum": Coin { prices: {"ethereum": USD { usd: 2940.75 }} }
// "solana": Coin { prices: {"solana": USD { usd: 124.5 }} }
// "bitcoin": Coin { prices: {"bitcoin": USD { usd: 87794.0 }} }
// Elapsed on JOIN: 81.309467ms
// -----
// "solana": Coin { prices: {"solana": USD { usd: 124.5 }} }
// "ethereum": Coin { prices: {"ethereum": USD { usd: 2940.75 }} }
// "bitcoin": Coin { prices: {"bitcoin": USD { usd: 87794.0 }} }
// Elapsed on TRY JOIN: 12.037605ms
