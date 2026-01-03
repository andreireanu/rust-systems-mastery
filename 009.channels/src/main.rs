use std::time::Duration;
use std::time::SystemTime;
use tokio::spawn;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Instant, sleep};

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct Message {
    producer_id: u8,
    message_number: u8,
    timestamp: SystemTime,
}

#[tokio::main]
async fn main() {
    // Multiple Producer Single Consumer
    let (tx, mut rx) = mpsc::channel::<Message>(10);
    let mut handles = Vec::new();
    for i in 0..5 {
        let tx_clone = tx.clone();
        let handle = spawn(async move {
            for j in 0..20 {
                let timestamp = SystemTime::now();
                tx_clone
                    .send(Message {
                        producer_id: i,
                        message_number: j,
                        timestamp,
                    })
                    .await
                    .unwrap();
                sleep(Duration::from_millis(10)).await;
            }
        });
        handles.push(handle);
    }

    let handle = tokio::spawn(async move {
        let mut index = 1;
        while let Some(message) = rx.recv().await {
            let time = Instant::now();
            println!("GOT index = {}: {:?}", index, message);
            sleep(Duration::from_millis(50)).await;
            index += 1;
            println!("Elapsed: {:?}", time.elapsed());
        }
    });
    handles.push(handle);

    drop(tx);

    for h in handles.into_iter() {
        h.await.unwrap();
    }

    // Broadcast: Multi Producer Multiple Consumer
    let (tx, _) = broadcast::channel::<Message>(10);

    let mut handles = Vec::new();

    for i in 0..3 {
        let mut rx_subscribe = tx.subscribe();
        let handle = tokio::spawn(async move {
            loop {
                let message = rx_subscribe.recv().await;
                match message {
                    Ok(msg) => println!("Message received by receiver {}: {:?}", i, msg),
                    Err(RecvError::Lagged(n)) => println!("Lagged for: {:?}", n),
                    Err(RecvError::Closed) => {
                        println!("Closed channel");
                        break;
                    }
                }
            }
        });
        handles.push(handle);
    }

    let handle = tokio::spawn(async move {
        for j in 0..20 {
            let timestamp = SystemTime::now();
            let sent = tx.send(Message {
                producer_id: 0,
                message_number: j,
                timestamp,
            });
            match sent {
                Ok(_) => {}
                Err(SendError(msg)) => {
                    println!("Err sending message: {:?}", msg)
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    });
    handles.push(handle);

    for h in handles.into_iter() {
        h.await.unwrap();
    }
}
