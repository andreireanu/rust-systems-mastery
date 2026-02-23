// info!("Mesages received from binance / sec: {}", binance_msg_no/ticks);
// info!("Percentiles Binance: P50, P95, P99 (microseconds) {:?}", percentiles(&binance_buf));
// info!("{}", *binance_order_book.lock().await);
// info!("Mesages received from kraken: / sec: {}", kraken_msg_no/ticks);
// info!("Percentiles Kraken: P50, P95, P99 (microseconds) {:?}", percentiles(&kraken_buf));
// info!("{}", *kraken_order_book.lock().await);

use std::sync::atomic::{AtomicU64, Ordering};

use circular_buffer::CircularBuffer;

pub struct ExchangeMetrics {
    pub msg_no: AtomicU64,
    pub buffer: CircularBuffer<200, u128>,
    pub percentiles: Option<(u128, u128, u128)>,
}

impl ExchangeMetrics {
    pub fn new() -> Self {
        Self {
            msg_no: AtomicU64::new(0),
            buffer: CircularBuffer::<200, u128>::new(),
            percentiles: None,
        }
    }

    pub fn increment_msg_no(&mut self) {
        self.msg_no.fetch_add(1, Ordering::Relaxed);
    }

    pub fn buffer_push_back(&mut self, val: u128) {
        self.buffer.push_back(val);
    }

    pub fn percentiles(&mut self) {
        if self.buffer.is_empty() {
            return self.percentiles = None;
        }

        let mut buf_vec: Vec<u128> = self.buffer.iter().copied().collect();
        buf_vec.sort_unstable();

        let len = buf_vec.len();
        let p50 = buf_vec[len * 50 / 100];
        let p95 = buf_vec[len * 95 / 100];
        let p99 = buf_vec[len * 99 / 100];

        self.percentiles = Some((p50, p95, p99));
    }
}
