use crate::protocol::message::Message;
use crate::types::error::FrameworkError;
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

pub struct MessageCodec {
    inner: LengthDelimitedCodec,
}

impl Default for MessageCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageCodec {
    pub fn new() -> Self {
        Self {
            // Configure max frame length to 70MB to allow 64MB chunks + overhead
            inner: LengthDelimitedCodec::builder()
                .max_frame_length(70 * 1024 * 1024)
                .new_codec(),
        }
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = FrameworkError;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let serialized =
            bincode::serialize(&item).map_err(|e| FrameworkError::Serialization(e.to_string()))?;
        let bytes = bytes::Bytes::from(serialized);
        self.inner.encode(bytes, dst).map_err(FrameworkError::Io)
    }
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = FrameworkError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.inner.decode(src) {
            Ok(Some(bytes)) => {
                let message: Message = bincode::deserialize(&bytes)
                    .map_err(|e| FrameworkError::Serialization(e.to_string()))?;
                Ok(Some(message))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(FrameworkError::Io(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::message::Message;
    use bytes::BytesMut;

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut codec = MessageCodec::new();
        let mut buf = BytesMut::new();

        let msg = Message::Pong;
        codec.encode(msg.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_encode_decode_large_chunk() {
        let mut codec = MessageCodec::new();
        let mut buf = BytesMut::new();

        let msg = Message::ObjectData {
            object_id: uuid::Uuid::new_v4(),
            chunk: vec![0u8; 1024 * 1024], // 1MB chunk
            offset: 0,
        };
        codec.encode(msg.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, msg);
    }
}
