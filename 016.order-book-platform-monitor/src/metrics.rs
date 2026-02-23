// info!("Mesages received from binance / sec: {}", binance_msg_no/ticks);
// info!("Percentiles Binance: P50, P95, P99 (microseconds) {:?}", percentiles(&binance_buf));
// info!("{}", *binance_order_book.lock().await);
// info!("Mesages received from kraken: / sec: {}", kraken_msg_no/ticks);
// info!("Percentiles Kraken: P50, P95, P99 (microseconds) {:?}", percentiles(&kraken_buf));
// info!("{}", *kraken_order_book.lock().await);

use std::sync::atomic::AtomicU64;

struct ExchangeMetrics {
    msg_no: AtomicU64,
}
