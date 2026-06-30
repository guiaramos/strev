use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rdkafka::Message as KafkaMessage;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Headers;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use strev::{
    CloseError, ConsumerLag, Disposition, LagError, Message, MessageStream, Metadata,
    SubscribeError, Topic,
};

pub struct KafkaSubscriberConfig {
    pub brokers: String,
    pub group_id: String,
    pub buffer_size: usize,
    pub auto_offset_reset: String,
    pub options: Vec<(String, String)>,
}

impl KafkaSubscriberConfig {
    pub fn new(brokers: impl Into<String>, group_id: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            group_id: group_id.into(),
            buffer_size: 64,
            auto_offset_reset: "earliest".to_string(),
            options: Vec::new(),
        }
    }

    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.push((key.into(), value.into()));
        self
    }
}

pub struct KafkaSubscriber {
    config: KafkaSubscriberConfig,
}

impl KafkaSubscriber {
    pub fn new(config: KafkaSubscriberConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl strev::Subscriber for KafkaSubscriber {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &self.config.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", &self.config.auto_offset_reset)
            .set("allow.auto.create.topics", "true");
        for (key, value) in &self.config.options {
            client_config.set(key, value);
        }

        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        consumer
            .subscribe(&[topic.as_str()])
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        let (tx, stream) = MessageStream::channel(self.config.buffer_size);

        tokio::spawn(async move {
            loop {
                let received = tokio::select! {
                    biased;
                    _ = tx.closed() => break,
                    received = consumer.recv() => received,
                };

                match received {
                    Ok(borrowed) => {
                        let payload = borrowed
                            .payload()
                            .map(Bytes::copy_from_slice)
                            .unwrap_or_default();

                        let mut metadata = Metadata::new();
                        if let Some(headers) = borrowed.headers() {
                            for header in headers.iter() {
                                if header.key == "strev-uuid" {
                                    continue;
                                }
                                if let Some(value) = header.value
                                    && let Ok(text) = std::str::from_utf8(value)
                                {
                                    metadata.set(header.key, text);
                                }
                            }
                        }

                        let msg_topic = borrowed.topic().to_string();
                        let partition = borrowed.partition();
                        let offset = borrowed.offset();
                        drop(borrowed);

                        let (msg, ack) = Message::with_metadata(payload, metadata).leased();
                        if tx.send(msg).await.is_err() {
                            break;
                        }

                        match ack.recv().await {
                            Disposition::Ack => {
                                let mut tpl = TopicPartitionList::new();
                                let _ = tpl.add_partition_offset(
                                    &msg_topic,
                                    partition,
                                    Offset::Offset(offset + 1),
                                );
                                let _ = consumer.commit(&tpl, CommitMode::Async);
                            }
                            Disposition::Nack => {
                                let _ = consumer.seek(
                                    &msg_topic,
                                    partition,
                                    Offset::Offset(offset),
                                    Duration::from_secs(5),
                                );
                            }
                        }
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        });

        Ok(stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}

#[async_trait]
impl ConsumerLag for KafkaSubscriber {
    async fn lag(&self, topic: &Topic) -> Result<u64, LagError> {
        let brokers = self.config.brokers.clone();
        let group_id = self.config.group_id.clone();
        let options = self.config.options.clone();
        let topic_name = topic.as_str().to_string();

        let lag = tokio::task::spawn_blocking(move || -> Result<i64, LagError> {
            let mut client_config = ClientConfig::new();
            client_config
                .set("bootstrap.servers", &brokers)
                .set("group.id", &group_id)
                .set("enable.auto.commit", "false");
            for (key, value) in &options {
                client_config.set(key, value);
            }
            let consumer: BaseConsumer = client_config.create()?;

            let timeout = Duration::from_secs(5);
            let metadata = consumer.fetch_metadata(Some(&topic_name), timeout)?;

            let mut partitions = TopicPartitionList::new();
            for meta_topic in metadata.topics() {
                if meta_topic.name() == topic_name {
                    for partition in meta_topic.partitions() {
                        partitions.add_partition(&topic_name, partition.id());
                    }
                }
            }
            if partitions.count() == 0 {
                return Ok(0);
            }

            let committed = consumer.committed_offsets(partitions, timeout)?;

            let mut lag = 0i64;
            for element in committed.elements() {
                let (low, high) =
                    consumer.fetch_watermarks(&topic_name, element.partition(), timeout)?;
                let consumed = match element.offset() {
                    Offset::Offset(offset) => offset,
                    _ => low,
                };
                lag += (high - consumed).max(0);
            }
            Ok(lag)
        })
        .await
        .map_err(|e| Box::new(e) as LagError)??;

        Ok(lag as u64)
    }
}
