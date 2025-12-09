use crate::config::MongoDbConfig;
use crate::consumers::{BoxFuture, CommitFunc, MessageConsumer};
use crate::model::CanonicalMessage;
use crate::publishers::MessagePublisher;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::StreamExt;
use mongodb::{
    bson::{doc, to_document, Binary, Bson, Document},
    change_stream::ChangeStream,
    error::ErrorKind,
};
use mongodb::{
    change_stream::event::ChangeStreamEvent,
    options::{FindOneAndUpdateOptions, ReturnDocument},
};
use mongodb::{Client, Collection};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::time::{Duration, SystemTime};
use tracing::{debug, info, warn};

#[derive(Serialize, Deserialize, Debug)]
struct MongoMessageInternal {
    #[serde(flatten)]
    msg: CanonicalMessage,
    locked_until: Option<i64>,
}

/// A helper struct for deserialization that matches the BSON structure exactly.
/// The payload is read as a BSON Binary type, which we then manually convert.
#[derive(Deserialize, Debug)]
struct MongoMessageRaw {
    message_id: i64,
    payload: Binary,
    metadata: Document,
    locked_until: Option<i64>,
}

impl TryFrom<MongoMessageRaw> for MongoMessageInternal {
    type Error = anyhow::Error;

    fn try_from(raw: MongoMessageRaw) -> Result<Self, Self::Error> {
        let metadata = mongodb::bson::from_document(raw.metadata)
            .context("Failed to deserialize metadata from BSON document")?;

        Ok(MongoMessageInternal {
            // BSON uses i64, our model uses u64. We cast, assuming IDs are positive.
            msg: CanonicalMessage {
                message_id: raw.message_id as u64,
                payload: raw.payload.bytes,
                metadata,
            },
            locked_until: raw.locked_until,
        })
    }
}

/// A publisher that inserts messages into a MongoDB collection.
pub struct MongoDbPublisher {
    collection: Collection<Document>,
}

impl MongoDbPublisher {
    pub async fn new(config: &MongoDbConfig, collection_name: &str) -> anyhow::Result<Self> {
        let client = Client::with_uri_str(&config.url).await?;
        let db = client.database(&config.database);
        let collection = db.collection(collection_name);
        info!(database = %config.database, collection = %collection_name, "MongoDB publisher connected");
        Ok(Self { collection })
    }
}

#[async_trait]
impl MessagePublisher for MongoDbPublisher {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        let mut msg_with_metadata = message.clone();

        // Manually construct the document to handle u64 message_id for BSON.
        // BSON only supports i64, so we do a wrapping conversion.
        let doc = doc! {
            "message_id": message.message_id as i64, // Convert u64 to i64
            "payload": Bson::Binary(mongodb::bson::Binary { subtype: mongodb::bson::spec::BinarySubtype::Generic, bytes: message.payload }),
            "metadata": to_document(&message.metadata)?,
            "locked_until": null
        };

        let result = self.collection.insert_one(doc.clone()).await?;
        let object_id = result
            .inserted_id
            .as_object_id()
            .ok_or_else(|| anyhow!("Could not get inserted_id as ObjectId"))?;

        // Add the mongodb_object_id to the message headers
        msg_with_metadata
            .metadata
            .insert("mongodb_object_id".to_string(), object_id.to_string());
        self.collection
            .update_one(
                doc! {"_id": object_id},
                doc! {"$set": {"metadata": to_document(&msg_with_metadata.metadata)?}},
            )
            .await?;

        Ok(Some(msg_with_metadata))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A consumer that receives messages from a MongoDB collection, treating it like a queue.
pub struct MongoDbConsumer {
    collection: Collection<Document>,
    change_stream: Option<tokio::sync::Mutex<ChangeStream<ChangeStreamEvent<Document>>>>,
    polling_interval: Duration,
}

impl MongoDbConsumer {
    pub async fn new(config: &MongoDbConfig, collection_name: &str) -> anyhow::Result<Self> {
        let client = Client::with_uri_str(&config.url).await?;
        // The first operation will trigger connection and topology discovery.
        client.list_database_names().await?;

        let db = client.database(&config.database);
        let collection = db.collection(collection_name);

        // Attempt to create a change stream. If it fails because it's a standalone instance,
        // fall back to polling.
        let pipeline = [doc! { "$match": { "operationType": "insert" } }];
        let change_stream_result = collection.watch().pipeline(pipeline).await;

        let change_stream = match change_stream_result {
            Ok(stream) => {
                info!("MongoDB is a replica set/sharded cluster. Using change stream.");
                Some(tokio::sync::Mutex::new(stream))
            }
            Err(e) if matches!(*e.kind, ErrorKind::Command(ref cmd_err) if cmd_err.code == 40573) =>
            {
                warn!("MongoDB is a single instance (ChangeStream support check failed). Falling back to polling for consumer.");
                None
            }
            Err(e) => return Err(e.into()), // For any other error, we propagate it.
        };

        info!(database = %config.database, collection = %collection_name, "MongoDB consumer connected and watching for changes");

        Ok(Self {
            collection,
            change_stream,
            polling_interval: Duration::from_secs(1),
        })
    }
}

#[async_trait]
impl MessageConsumer for MongoDbConsumer {
    async fn receive(&mut self) -> anyhow::Result<(CanonicalMessage, CommitFunc)> {
        loop {
            // Atomically find a message that is not locked or whose lock has expired,
            // and lock it for this consumer for 60 seconds.
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs() as i64;
            let lock_duration_secs = 60;
            let locked_until = now + lock_duration_secs;

            let filter = doc! {
                "$or": [
                    { "locked_until": { "$exists": false } },
                    { "locked_until": null },
                    { "locked_until": { "$lt": now } }
                ]
            };
            let update = doc! { "$set": { "locked_until": locked_until } };
            let options = FindOneAndUpdateOptions::builder()
                .return_document(ReturnDocument::After)
                .build();

            match self
                .collection
                .find_one_and_update(filter, update)
                .with_options(options)
                .await
            {
                Ok(Some(doc)) => {
                    let raw_msg: MongoMessageRaw = mongodb::bson::from_document(doc.clone())
                        .context("Failed to deserialize MongoDB document")?;
                    let msg: MongoMessageInternal = raw_msg.try_into()?;

                    let object_id = doc
                        .get_object_id("_id")
                        .map_err(|_| anyhow!("Could not find or parse _id in returned document"))?;

                    let collection_clone = self.collection.clone();

                    // The commit function will delete the message from the collection.
                    let commit = Box::new(move |_response| {
                        Box::pin(async move {
                            match collection_clone.delete_one(doc! { "_id": object_id }).await {
                                Ok(delete_result) => {
                                    if delete_result.deleted_count == 1 {
                                        debug!(mongodb_object_id = %object_id, "MongoDB message acknowledged and deleted");
                                    } else {
                                        warn!(mongodb_object_id = %object_id, "Attempted to ack/delete MongoDB message, but it was not found (already deleted?)");
                                    }
                                }
                                Err(e) => {
                                    tracing::error!(mongodb_object_id = %object_id, error = %e, "Failed to ack/delete MongoDB message");
                                }
                            }
                        }) as BoxFuture<'static, ()>
                    });

                    return Ok((msg.msg, commit));
                }
                Ok(None) => {
                    // No available messages, wait for a new one to be inserted.
                    if let Some(stream_mutex) = &self.change_stream {
                        // We wait on the change stream for a new insert event.
                        let mut stream = stream_mutex.lock().await;
                        if stream.next().await.is_none() {
                            return Err(anyhow!("MongoDB change stream ended"));
                        }
                    } else {
                        // Fallback to polling if not using a change stream.
                        tokio::time::sleep(self.polling_interval).await;
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
