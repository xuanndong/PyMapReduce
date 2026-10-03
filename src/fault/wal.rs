use crate::protocol::message::Message;
use crate::types::error::{FrameworkError, Result};
use std::path::PathBuf;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct WriteAheadLog {
    path: PathBuf,
    file: Option<File>,
}

impl WriteAheadLog {
    pub async fn new(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(FrameworkError::Io)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(FrameworkError::Io)?;

        Ok(Self {
            path,
            file: Some(file),
        })
    }

    pub async fn append(&mut self, message: &Message) -> Result<()> {
        let serialized = bincode::serialize(message)
            .map_err(|e| FrameworkError::Serialization(e.to_string()))?;

        let mut length_prefix = [0u8; 4];
        length_prefix.copy_from_slice(&(serialized.len() as u32).to_be_bytes());

        if let Some(file) = &mut self.file {
            file.write_all(&length_prefix)
                .await
                .map_err(FrameworkError::Io)?;
            file.write_all(&serialized)
                .await
                .map_err(FrameworkError::Io)?;

            // Allow disabling sync for high-throughput scheduler benchmarks
            if std::env::var("DISABLE_WAL_SYNC").is_err() {
                file.sync_data().await.map_err(FrameworkError::Io)?;
            }
        }

        Ok(())
    }

    pub async fn replay(&mut self) -> Result<Vec<Message>> {
        let mut file = File::open(&self.path).await.map_err(FrameworkError::Io)?;
        let mut messages = Vec::new();

        loop {
            let mut length_buf = [0u8; 4];
            let bytes_read = file
                .read(&mut length_buf)
                .await
                .map_err(FrameworkError::Io)?;

            if bytes_read == 0 {
                break; // EOF
            }
            if bytes_read < 4 {
                return Err(FrameworkError::Other(
                    "Corrupted WAL: incomplete length prefix".into(),
                ));
            }

            let length = u32::from_be_bytes(length_buf) as usize;
            let mut data_buf = vec![0u8; length];
            file.read_exact(&mut data_buf)
                .await
                .map_err(FrameworkError::Io)?;

            let message: Message = bincode::deserialize(&data_buf)
                .map_err(|e| FrameworkError::Serialization(e.to_string()))?;
            messages.push(message);
        }

        Ok(messages)
    }

    pub async fn truncate(&mut self) -> Result<()> {
        self.file = None;

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .await
            .map_err(FrameworkError::Io)?;

        self.file = Some(file);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_wal_append_replay() {
        let path = std::env::temp_dir().join(Uuid::new_v4().to_string());

        let mut wal = WriteAheadLog::new(path.clone()).await.unwrap();

        let msg1 = Message::JobAccepted {
            job_id: Uuid::new_v4(),
        };
        let msg2 = Message::CancelJob {
            job_id: Uuid::new_v4(),
        };

        wal.append(&msg1).await.unwrap();
        wal.append(&msg2).await.unwrap();

        let messages = wal.replay().await.unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], msg1);
        assert_eq!(messages[1], msg2);

        wal.truncate().await.unwrap();
        let messages_after = wal.replay().await.unwrap();
        assert_eq!(messages_after.len(), 0);

        let _ = tokio::fs::remove_file(path).await;
    }
}
