use std::sync::Arc;
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
struct PlaceOrder {
    order_id: u64,
}
impl Command for PlaceOrder {
    const NAME: &'static str = "PlaceOrder";
}

#[derive(Serialize, Deserialize)]
struct OrderPlaced {
    order_id: u64,
}
impl Event for OrderPlaced {
    const NAME: &'static str = "OrderPlaced";
}

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);
    let factory: SubscriberFactory = {
        let channel = channel.clone();
        Arc::new(move |_group| Box::new(channel.clone()) as Box<dyn Subscriber>)
    };

    let event_bus = Arc::new(EventBus::new(Box::new(channel.clone())));

    let mut router = Router::new();

    // Command handler: placing an order emits an OrderPlaced event.
    let mut commands = CommandProcessor::new(factory.clone());
    let bus = event_bus.clone();
    commands
        .add_handler("place-order", move |cmd: PlaceOrder, ctx: Context| {
            let bus = bus.clone();
            async move {
                println!("placing order {} (msg {})", cmd.order_id, ctx.message_id());
                bus.publish(OrderPlaced {
                    order_id: cmd.order_id,
                })
                .await
                .map_err(|e| strev::HandlerError::Processing(Box::new(e)))?;
                Ok(())
            }
        })
        .unwrap();
    commands.register(&mut router);

    // Two independent event handlers both react to OrderPlaced.
    let mut events = EventProcessor::new(factory);
    events.add_handler("ship", |evt: OrderPlaced, _ctx: Context| async move {
        println!("  shipping order {}", evt.order_id);
        Ok(())
    });
    events.add_handler("invoice", |evt: OrderPlaced, _ctx: Context| async move {
        println!("  invoicing order {}", evt.order_id);
        Ok(())
    });
    events.register(&mut router);

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let command_bus = CommandBus::new(Box::new(channel.clone()));
    for order_id in 0..3 {
        command_bus.send(PlaceOrder { order_id }).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
}
