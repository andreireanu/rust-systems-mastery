use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::Ordering::SeqCst;
use std::{
    sync::{Arc, Mutex, atomic::AtomicU64},
    thread,
    time::{Duration, Instant},
};

fn main() {
    let shared_mutex = Arc::new(Mutex::new(0));
    let time = Instant::now();
    thread::scope(|s| {
        for _ in 0..10 {
            s.spawn(|| {
                for _ in 0..10000 {
                    let mut inner = shared_mutex.lock().unwrap();
                    *inner += 1;
                }
            });
        }
    });
    println!(
        "Mutex Done in {:?}, value is: {:?}",
        Duration::from_micros(time.elapsed().as_micros().try_into().unwrap()),
        shared_mutex
    );

    let shared_atomic_relaxed = Arc::new(AtomicU64::new(0));
    let time_atomic = Instant::now();
    thread::scope(|s| {
        for _ in 0..10 {
            s.spawn(|| {
                for _ in 0..10000 {
                    shared_atomic_relaxed.fetch_add(1, Relaxed);
                }
            });
        }
    });
    println!(
        "Atomic Relaxed Done in {:?}, value is: {:?}",
        Duration::from_micros(time_atomic.elapsed().as_micros().try_into().unwrap()),
        shared_atomic_relaxed
    );

    let shared_atomic_seqcst = Arc::new(AtomicU64::new(0));
    let time_atomic_seqcst = Instant::now();
    thread::scope(|s| {
        for _ in 0..10 {
            s.spawn(|| {
                for _ in 0..10000 {
                    shared_atomic_seqcst.fetch_add(1, SeqCst);
                }
            });
        }
    });
    println!(
        "Atomic SeqCst Done in {:?}, value is: {:?}",
        Duration::from_micros(time_atomic_seqcst.elapsed().as_micros().try_into().unwrap()),
        shared_atomic_seqcst
    );
}
