use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use strev::{Message, Publisher, Subscriber, Topic};
use strev_channel::Channel;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

fn message_lifecycle(c: &mut Criterion) {
    c.bench_function("message_new_ack", |b| {
        b.iter(|| {
            let msg = Message::new(Bytes::from_static(b"payload"));
            std::hint::black_box(msg.ack());
        });
    });
}

fn channel_roundtrip(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let topic = Topic::new("bench");
    let (channel, stream) = rt.block_on(async {
        let channel = Channel::new(1024);
        let stream = Subscriber::subscribe(&channel, &topic).await.unwrap();
        (channel, stream)
    });
    let stream = Mutex::new(stream);

    c.bench_function("channel_roundtrip", |b| {
        b.to_async(&rt).iter(|| async {
            Publisher::publish(
                &channel,
                &topic,
                vec![Message::new(Bytes::from_static(b"x"))],
            )
            .await
            .unwrap();
            let mut stream = stream.lock().await;
            let received = stream.next().await.unwrap();
            let _ = received.ack();
        });
    });
}

fn channel_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let topic = Topic::new("bench-batch");
    let (channel, stream) = rt.block_on(async {
        let channel = Channel::new(4096);
        let stream = Subscriber::subscribe(&channel, &topic).await.unwrap();
        (channel, stream)
    });
    let stream = Mutex::new(stream);
    const BATCH: usize = 1000;

    let mut group = c.benchmark_group("channel_throughput");
    group.throughput(criterion::Throughput::Elements(BATCH as u64));
    group.bench_function("publish_consume_1000", |b| {
        b.to_async(&rt).iter(|| async {
            let messages = (0..BATCH)
                .map(|_| Message::new(Bytes::from_static(b"x")))
                .collect();
            Publisher::publish(&channel, &topic, messages)
                .await
                .unwrap();
            let mut stream = stream.lock().await;
            for _ in 0..BATCH {
                let received = stream.next().await.unwrap();
                let _ = received.ack();
            }
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    message_lifecycle,
    channel_roundtrip,
    channel_throughput
);
criterion_main!(benches);
