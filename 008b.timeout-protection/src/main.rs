use serde::Deserialize;
use thiserror::Error;
use tokio::select;
use tokio::time::{Duration, sleep};

#[derive(Error, Debug)]
pub enum AppError {
    #[error("data store disconnected")]
    SendError(#[from] reqwest::Error),

    #[error("serde error")]
    SerdeError(#[from] serde_json::Error),
}

#[derive(Debug, Deserialize)]
pub struct USD {
    usd: f64,
}

#[derive(Debug, Deserialize)]
pub struct Bitcoin {
    bitcoin: USD,
}

async fn get_price(timeout: u64) -> Result<f64, AppError> {
    sleep(Duration::from_millis(timeout)).await;
    let url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd";
    let client = reqwest::Client::new();
    let response = client.get(url).send().await?.text().await?;
    let btc: Bitcoin = serde_json::from_str(&response)?;
    Ok(btc.bitcoin.usd)
}

async fn sleep_some_sec(timeout: u64) {
    sleep(Duration::from_millis(timeout)).await;
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    println!("Test A: Normal speed (no added sleep)");
    select! {
        price = get_price(0) => {
            let p = price?;
            println!("Got price: {}", p);
        },
        _ =  sleep_some_sec(2000) => {
            println!("Request timed out!");
        },
    }

    println!("Test B: Artificially slow (add 3-second sleep)");
    select! {
        price = get_price(3000) => {
            let p = price?;
            println!("Got price: {}", p);
        },
        _ = sleep_some_sec(2000) => {
            println!("Request timed out!");
        },
    }
    Ok(())
}
