use bytes::Bytes;
use cloudevents::{AttributesReader, Data, Event, EventBuilder, EventBuilderV10};
use strev::{Message, Metadata};

const CONTENT_TYPE: &str = "content-type";
const STRUCTURED_JSON: &str = "application/cloudevents+json";
const DEFAULT_DATA_CONTENT_TYPE: &str = "application/json";

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("cloudevent type is required: set a codec default or ce-type metadata")]
    MissingType,
    #[error("failed to build cloudevent: {0}")]
    Build(String),
    #[error("failed to encode cloudevent: {0}")]
    Encode(serde_json::Error),
    #[error("failed to decode cloudevent: {0}")]
    Decode(serde_json::Error),
}

#[derive(Clone)]
pub struct CloudEventCodec {
    source: String,
    event_type: Option<String>,
    data_content_type: String,
}

impl CloudEventCodec {
    pub fn new(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            event_type: None,
            data_content_type: DEFAULT_DATA_CONTENT_TYPE.to_string(),
        }
    }

    pub fn event_type(mut self, event_type: impl Into<String>) -> Self {
        self.event_type = Some(event_type.into());
        self
    }

    pub fn data_content_type(mut self, data_content_type: impl Into<String>) -> Self {
        self.data_content_type = data_content_type.into();
        self
    }

    pub fn encode(&self, msg: &Message) -> Result<Message, CodecError> {
        let meta = msg.metadata();

        let id = meta
            .get("ce-id")
            .map(str::to_string)
            .unwrap_or_else(|| msg.uuid().to_string());
        let source = meta.get("ce-source").unwrap_or(self.source.as_str());
        let event_type = meta
            .get("ce-type")
            .or(self.event_type.as_deref())
            .ok_or(CodecError::MissingType)?;
        let content_type = meta
            .get("ce-datacontenttype")
            .unwrap_or(self.data_content_type.as_str());

        let data = if content_type.contains("json") {
            match serde_json::from_slice::<serde_json::Value>(msg.payload()) {
                Ok(value) => Data::Json(value),
                Err(_) => Data::Binary(msg.payload().to_vec()),
            }
        } else {
            Data::Binary(msg.payload().to_vec())
        };

        let mut builder = EventBuilderV10::new()
            .id(id)
            .source(source)
            .ty(event_type)
            .data(content_type.to_string(), data);

        if let Some(subject) = meta.get("ce-subject") {
            builder = builder.subject(subject);
        }

        let event = builder
            .build()
            .map_err(|e| CodecError::Build(e.to_string()))?;
        let bytes = serde_json::to_vec(&event).map_err(CodecError::Encode)?;

        let mut out = Message::new(Bytes::from(bytes));
        out.metadata_mut().set(CONTENT_TYPE, STRUCTURED_JSON);
        Ok(out)
    }

    pub fn decode(&self, msg: &Message) -> Result<Message, CodecError> {
        let event: Event = serde_json::from_slice(msg.payload()).map_err(CodecError::Decode)?;

        let payload = match event.data() {
            Some(Data::Binary(bytes)) => Bytes::copy_from_slice(bytes),
            Some(Data::String(text)) => Bytes::from(text.clone().into_bytes()),
            Some(Data::Json(value)) => {
                Bytes::from(serde_json::to_vec(value).map_err(CodecError::Encode)?)
            }
            None => Bytes::new(),
        };

        let mut metadata = Metadata::new();
        metadata.set("ce-id", event.id());
        metadata.set("ce-source", event.source());
        metadata.set("ce-type", event.ty());
        metadata.set("ce-specversion", event.specversion().to_string());
        if let Some(subject) = event.subject() {
            metadata.set("ce-subject", subject);
        }
        if let Some(content_type) = event.datacontenttype() {
            metadata.set("ce-datacontenttype", content_type);
        }
        if let Some(time) = event.time() {
            metadata.set("ce-time", time.to_rfc3339());
        }

        Ok(Message::with_metadata(payload, metadata))
    }
}
