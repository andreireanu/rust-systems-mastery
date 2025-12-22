use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Instant;
use tokio::try_join;

#[derive(Deserialize, Debug)]
struct USD {
    usd: f64,
}

#[derive(Deserialize, Debug)]
struct Coins {
    #[serde(flatten)]
    prices: HashMap<String, USD>,
}

async fn get_coin_price(client: &Client, coin_id: &str) -> Result<f64> {
    let url = format!(
        "https://api.coingecko.com/api/v3/simple/price?ids={}&vs_currencies=usd",
        coin_id
    );

    let result_text = client.get(&url).send().await?.text().await?;
    let coins: Coins = serde_json::from_str(&result_text)?;

    if let Some(price) = coins.prices.get(coin_id) {
        println!("{} Price: {}", coin_id.to_uppercase(), price.usd);
        Ok(price.usd)
    } else {
        anyhow::bail!("Coin {} not found in response", coin_id);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = reqwest::Client::new();
    let seq_time = Instant::now();
    get_coin_price(&client, &"bitcoin").await?;
    get_coin_price(&client, &"ethereum").await?;
    get_coin_price(&client, &"cardano").await?;
    get_coin_price(&client, &"polkadot").await?;
    get_coin_price(&client, &"solana").await?;
    let elapsed_seq = seq_time.elapsed();
    println!("Elapsed sequentually: {:?}", elapsed_seq);
    let thread_async = Instant::now();
    try_join!(
        get_coin_price(&client, &"bitcoin"),
        get_coin_price(&client, &"ethereum"),
        get_coin_price(&client, &"cardano"),
        get_coin_price(&client, &"polkadot"),
        get_coin_price(&client, &"solana"),
    )?;
    let elapsed_async = thread_async.elapsed();
    println!("Elapsed concurrent async: {:?}", elapsed_async);
    Ok(())
}

/*
Results:

BTC: {"bitcoin":{"usd":89979}}
BTC Price: 89979.0
ETH: {"ethereum":{"usd":3050.52}}
ETH Price: 3050.52
ADA: {"cardano":{"usd":0.371117}}
ADA Price: 0.371117
SOL: {"solana":{"usd":127.05}}
SOL Price: 127.05
DOT: {"polkadot":{"usd":1.83}}
DOT Price: 1.83
Elapsed sequentually: 340.975102ms

ETH: {"ethereum":{"usd":3052.6}}
ETH Price: 3052.6
SOL: {"solana":{"usd":127.02}}
SOL Price: 127.02
DOT: {"polkadot":{"usd":1.83}}
DOT Price: 1.83
BTC: {"bitcoin":{"usd":90125}}
BTC Price: 90125.0
ADA: {"cardano":{"usd":0.372723}}
ADA Price: 0.372723
Elapsed concurrent async: 173.147748ms
*/
