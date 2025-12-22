use anyhow::Result;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct USD {
    usd: u64,
}

#[derive(Deserialize, Debug)]
struct BTC {
    bitcoin: USD,
}

async fn get() -> Result<String> {
    let body =
        reqwest::get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd")
            .await?
            .text()
            .await?;
    Ok(body)
}

#[tokio::main]
async fn main() -> Result<()> {
    let body_result = get().await;
    match body_result {
        Err(err) => println!("Error when getting the price: {:?}", err),
        Ok(result) => {
            println!("Result: {}", result);
            let btc: BTC = serde_json::from_str(&result)?;
            println!("BTC Price: {:?}", btc.bitcoin.usd);
        }
    }
    Ok(())
}
