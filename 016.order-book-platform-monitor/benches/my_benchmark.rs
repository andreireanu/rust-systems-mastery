use criterion::{Criterion, criterion_group, criterion_main};
use live_order_book::order_book::OrderBook;

fn create_populated_order_book() -> OrderBook {
    let mut ob = OrderBook::new();

    for i in 0..200 {
        ob.update_bid(
            90000.0 + i as f64,
            1.0 + (i as f64 * 0.1),
            "Test".to_string(),
        );
    }

    for i in 0..200 {
        ob.update_ask(
            91000.0 + i as f64,
            1.0 + (i as f64 * 0.1),
            "Test".to_string(),
        );
    }

    ob
}

fn update_bid_benchmark(c: &mut Criterion) {
    c.bench_function("update_bid realistic", |b| {
        b.iter_batched(
            || create_populated_order_book(),
            |mut ob| ob.update_bid(93250.0, 0.01, "Binance".to_string()),
            criterion::BatchSize::SmallInput,
        );
    });
}

fn delete_bid_benchmark(c: &mut Criterion) {
    c.bench_function("delete_bid realistic", |b| {
        b.iter_batched(
            || create_populated_order_book(),
            |mut ob| ob.update_bid(90050.0, 0.0, "Test".to_string()), // Delete existing
            criterion::BatchSize::SmallInput,
        );
    });
}

fn best_bid_benchmark(c: &mut Criterion) {
    let ob = create_populated_order_book();
    c.bench_function("best_bid realistic", |b| {
        b.iter(|| ob.best_bid());
    });
}

criterion_group!(
    benches,
    update_bid_benchmark,
    best_bid_benchmark,
    delete_bid_benchmark
);
criterion_main!(benches);
