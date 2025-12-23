use std::io::Error;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;

async fn read_file(name: &str) -> Result<String, Error> {
    let path = format!("files/{}.csv", name);
    let file = File::open(path).await?;
    let mut contents = String::new();
    let reader = BufReader::with_capacity(1, file);
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        contents.push_str(&line);
    }
    Ok(contents)
}

fn parse_contents(contents: String) -> Result<f64, Error> {
    let (_, price) = contents
        .split_once(',')
        .ok_or_else(|| Error::new(std::io::ErrorKind::InvalidData, "missing comma"))?;

    let price: f64 = price
        .parse()
        .map_err(|_| Error::new(std::io::ErrorKind::InvalidData, "invalid number"))?;

    Ok(price)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut total_price: f64 = 0.;

    let files = vec!["btc", "eth", "ada", "dot", "sol"];
    let mut handles = Vec::new();
    for filename in files {
        let handle = tokio::spawn(async move { read_file(filename).await });
        handles.push(handle);
    }

    for h in handles.into_iter() {
        let result = h.await??;
        let price = parse_contents(result)?;
        total_price += price;
    }

    println!("Total price: {}", total_price);
    Ok(())
}
