use tokio::spawn;
use tokio::time::{Duration, sleep};

async fn run1() {
    println!("Starting 1...");
    sleep(Duration::from_millis(1000)).await;
    println!("1000 !");
    sleep(Duration::from_millis(1000)).await;
    println!("2000 !");
    sleep(Duration::from_millis(1000)).await;
    println!("3000 !");
    sleep(Duration::from_millis(1000)).await;
    println!("4000 !");
    sleep(Duration::from_millis(1000)).await;
    println!("5000 !");
}

// BLOCKS THE WORKER THREAD
async fn run2() {
    println!("Starting 2...");
    std::thread::sleep(Duration::from_millis(5000));
    println!("Done 2 !");
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let handle1 = spawn(async move {
        run1().await;
    });

    let handle2 = spawn(async move {
        run2().await;
    });

    let result = tokio::join!(handle1, handle2);
}
