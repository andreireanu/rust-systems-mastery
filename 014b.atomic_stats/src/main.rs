use rand::prelude::*;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::thread;
use std::time::Duration;

struct AtomicStats {
    count: AtomicU64,
    sum: AtomicU64,
    min: AtomicU64,
    max: AtomicU64,
}

impl AtomicStats {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            min: AtomicU64::new(u64::MAX),
            max: AtomicU64::new(0),
        }
    }

    fn record(&self, value: u64) {
        loop {
            let count = self.count.load(Relaxed);
            let new_count = count + 1;
            if self
                .count
                .compare_exchange(count, new_count, Relaxed, Relaxed)
                .is_ok()
            {
                break;
            }
        }

        loop {
            let sum = self.sum.load(Relaxed);
            let new_sum = sum + value;
            if self
                .sum
                .compare_exchange(sum, new_sum, Relaxed, Relaxed)
                .is_ok()
            {
                break;
            }
        }

        loop {
            let min = self.min.load(Relaxed);
            if value > min {
                break;
            }
            if self
                .min
                .compare_exchange(min, value, Relaxed, Relaxed)
                .is_ok()
            {
                break;
            }
        }

        loop {
            let max = self.max.load(Relaxed);
            if value < max {
                break;
            }
            if self
                .max
                .compare_exchange(max, value, Relaxed, Relaxed)
                .is_ok()
            {
                break;
            }
        }
        use rand::prelude::*;
        use std::sync::Arc;
        use std::sync::atomic::AtomicU64;
        use std::sync::atomic::Ordering::Relaxed;
        use std::thread;
        use std::time::Duration;

        struct AtomicStats {
            count: AtomicU64,
            sum: AtomicU64,
            min: AtomicU64,
            max: AtomicU64,
        }

        impl AtomicStats {
            fn new() -> Self {
                Self {
                    count: AtomicU64::new(0),
                    sum: AtomicU64::new(0),
                    min: AtomicU64::new(u64::MAX),
                    max: AtomicU64::new(0),
                }
            }

            fn record(&self, value: u64) {
                loop {
                    let count = self.count.load(Relaxed);
                    let new_count = count + 1;
                    if self
                        .count
                        .compare_exchange(count, new_count, Relaxed, Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }

                loop {
                    let sum = self.sum.load(Relaxed);
                    let new_sum = sum + value;
                    if self
                        .sum
                        .compare_exchange(sum, new_sum, Relaxed, Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }

                loop {
                    let min = self.min.load(Relaxed);
                    if value > min {
                        break;
                    }
                    if self
                        .min
                        .compare_exchange(min, value, Relaxed, Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }

                loop {
                    let max = self.max.load(Relaxed);
                    if value < max {
                        break;
                    }
                    if self
                        .max
                        .compare_exchange(max, value, Relaxed, Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }
            }

            fn get_count(&self) -> u64 {
                self.count.load(Relaxed)
            }

            fn get_average(&self) -> u64 {
                let count = self.count.load(Relaxed);
                if count == 0 {
                    return 0;
                }
                self.sum.load(Relaxed) / count
            }

            fn get_min(&self) -> u64 {
                self.min.load(Relaxed)
            }

            fn get_max(&self) -> u64 {
                self.max.load(Relaxed)
            }
        }

        fn main() {
            let atomic_stats = Arc::new(AtomicStats::new());
            thread::scope(|s| {
                for _ in 0..10 {
                    let stats = atomic_stats.clone();
                    s.spawn(move || {
                        loop {
                            let mut rng = rand::rng();
                            let value: u64 = rng.random_range(88000..92000);
                            stats.record(value);
                            thread::sleep(Duration::from_millis(200));
                        }
                    });
                }
                let stats = atomic_stats.clone();
                s.spawn(move || {
                    loop {
                        thread::sleep(Duration::from_secs(1));

                        println!("Count: {}", stats.get_count());
                        println!("Average: {}", stats.get_average());
                        println!("Min: {}", stats.get_min());
                        println!("Max: {}", stats.get_max());
                        println!("-------------------------");
                    }
                });
            });
        }
    }

    fn get_count(&self) -> u64 {
        self.count.load(Relaxed)
    }

    fn get_average(&self) -> u64 {
        let count = self.count.load(Relaxed);
        if count == 0 {
            return 0;
        }
        self.sum.load(Relaxed) / count
    }

    fn get_min(&self) -> u64 {
        self.min.load(Relaxed)
    }

    fn get_max(&self) -> u64 {
        self.max.load(Relaxed)
    }
}

fn main() {
    let atomic_stats = Arc::new(AtomicStats::new());
    thread::scope(|s| {
        for _ in 0..10 {
            let stats = atomic_stats.clone();
            s.spawn(move || {
                loop {
                    let mut rng = rand::rng();
                    let value: u64 = rng.random_range(88000..92000);
                    stats.record(value);
                    thread::sleep(Duration::from_millis(200));
                }
            });
        }
        let stats = atomic_stats.clone();
        s.spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(1));

                println!("Count: {}", stats.get_count());
                println!("Average: {}", stats.get_average());
                println!("Min: {}", stats.get_min());
                println!("Max: {}", stats.get_max());
                println!("-------------------------");
            }
        });
    });
}
