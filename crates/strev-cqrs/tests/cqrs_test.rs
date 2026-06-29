use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use strev::{Router, ShutdownSignal, Subscriber};
use strev_channel::Channel;
use strev_cqrs::{
    Command, CommandBus, CommandProcessor, Context, Event, EventBus, EventProcessor,
    SubscriberFactory,
};
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
struct CreateOrder {
    id: u64,
}
impl Command for CreateOrder {
    const NAME: &'static str = "CreateOrder";
}

#[derive(Serialize, Deserialize)]
struct OrderShipped {
    id: u64,
}
impl Event for OrderShipped {
    const NAME: &'static str = "OrderShipped";
}

fn factory(channel: &Channel) -> SubscriberFactory {
    let channel = channel.clone();
    Arc::new(move |_group| Box::new(channel.clone()) as Box<dyn Subscriber>)
}

#[tokio::test]
async fn command_is_dispatched_to_its_handler() {
    let channel = Channel::new(64);
    let handled = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    let mut processor = CommandProcessor::new(factory(&channel));
    let counter = handled.clone();
    processor
        .add_handler("create-order", move |cmd: CreateOrder, _ctx: Context| {
            let counter = counter.clone();
            async move {
                assert_eq!(cmd.id, 7);
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
    processor.register(&mut router);

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let bus = CommandBus::new(Box::new(channel.clone()));
    bus.send(CreateOrder { id: 7 }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(handled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn event_fans_out_to_every_handler() {
    let channel = Channel::new(64);
    let a = Arc::new(AtomicU32::new(0));
    let b = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    let mut processor = EventProcessor::new(factory(&channel));
    let counter_a = a.clone();
    processor.add_handler("notify", move |evt: OrderShipped, _ctx: Context| {
        let counter_a = counter_a.clone();
        async move {
            let _ = evt;
            counter_a.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });
    let counter_b = b.clone();
    processor.add_handler("audit", move |evt: OrderShipped, _ctx: Context| {
        let counter_b = counter_b.clone();
        async move {
            let _ = evt;
            counter_b.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });
    processor.register(&mut router);

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let bus = EventBus::new(Box::new(channel.clone()));
    bus.publish(OrderShipped { id: 1 }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(a.load(Ordering::SeqCst), 1);
    assert_eq!(b.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn duplicate_command_handler_is_rejected() {
    let channel = Channel::new(64);
    let mut processor = CommandProcessor::new(factory(&channel));
    processor
        .add_handler("first", |_cmd: CreateOrder, _ctx: Context| async { Ok(()) })
        .unwrap();
    let result = processor.add_handler("second", |_cmd: CreateOrder, _ctx: Context| async {
        Ok(())
    });
    assert!(result.is_err());
}
