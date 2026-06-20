use bytes::Bytes;
use strev::{Message, Metadata};

pub trait Marshaller: Send + Sync {
    fn marshal(&self, msg: &Message) -> Vec<(String, Vec<u8>)>;
    fn unmarshal(&self, fields: &[(String, redis::Value)]) -> Option<(Bytes, Metadata)>;
}

pub struct DefaultMarshaller;

const PAYLOAD_KEY: &str = "payload";
const UUID_KEY: &str = "uuid";
const METADATA_PREFIX: &str = "meta:";

impl Marshaller for DefaultMarshaller {
    fn marshal(&self, msg: &Message) -> Vec<(String, Vec<u8>)> {
        let mut fields = Vec::new();

        fields.push((UUID_KEY.to_string(), msg.uuid().to_string().into_bytes()));
        fields.push((PAYLOAD_KEY.to_string(), msg.payload().to_vec()));

        for (k, v) in msg.metadata().iter() {
            fields.push((format!("{METADATA_PREFIX}{k}"), v.as_bytes().to_vec()));
        }

        fields
    }

    fn unmarshal(&self, fields: &[(String, redis::Value)]) -> Option<(Bytes, Metadata)> {
        let mut payload = None;
        let mut metadata = Metadata::new();

        for (key, value) in fields {
            match key.as_str() {
                PAYLOAD_KEY => {
                    payload = value_to_bytes(value);
                }
                UUID_KEY => {}
                k if k.starts_with(METADATA_PREFIX) => {
                    let meta_key = &k[METADATA_PREFIX.len()..];
                    if let Some(val) = value_to_string(value) {
                        metadata.set(meta_key, val);
                    }
                }
                _ => {}
            }
        }

        Some((Bytes::from(payload?), metadata))
    }
}

fn value_to_bytes(v: &redis::Value) -> Option<Vec<u8>> {
    match v {
        redis::Value::BulkString(bytes) => Some(bytes.clone()),
        redis::Value::SimpleString(s) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

fn value_to_string(v: &redis::Value) -> Option<String> {
    match v {
        redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        redis::Value::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
}
