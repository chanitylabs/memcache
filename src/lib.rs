use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct Data {
    pub value: Vec<u8>,
    pub expires_at: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct MemCache {
    storage: Arc<RwLock<HashMap<String, Data>>>,
}

const DEFAULT_TTL: Duration = Duration::from_secs(60 * 60);

impl Default for MemCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MemCache {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get<K, V>(&self, key: K) -> Option<V>
    where
        K: Into<String>,
        V: for<'de> Deserialize<'de>,
    {
        let key: String = key.into();

        let storage = self.storage.read().await;
        let data = storage.get(&key)?;
        if let Some(expires_at) = data.expires_at {
            if expires_at < Instant::now() {
                return None;
            }
        }

        let data: V = bincode::deserialize(&data.value).ok()?;
        Some(data)
    }

    pub async fn set<K: Into<String>, V: Serialize>(
        &self,
        key: K,
        value: V,
        expires_in: Option<Duration>,
    ) -> Option<V> {
        let key: String = key.into();
        let value_data = bincode::serialize(&value).ok()?;

        let expires_in = Some(expires_in.unwrap_or(DEFAULT_TTL));

        {
            let mut storage = self.storage.write().await;
            storage.insert(
                key.clone(),
                Data {
                    value: value_data,
                    expires_at: expires_in.map(|expires_in| Instant::now() + expires_in),
                },
            );
        }

        if let Some(expires_in) = expires_in {
            let storage = self.storage.clone();
            tokio::spawn(async move {
                tokio::time::sleep(expires_in).await;
                let mut storage = storage.write().await;
                storage.remove(&key);
            });
        }

        Some(value)
    }

    pub async fn cached<F, Fut, K, V, E>(
        &self,
        action_fn: F,
        key: K,
        expires_in: Option<Duration>,
    ) -> Result<V, E>
    where
        K: Into<String>,
        V: for<'de> Deserialize<'de> + Serialize,
        Fut: Future<Output = Result<V, E>>,
        F: FnOnce() -> Fut + Send + 'static,
    {
        let key: String = key.into();
        let data: Option<V> = self.get(key.clone()).await;
        if let Some(data) = data {
            return Ok(data);
        }

        match action_fn().await {
            Err(err) => Err(err),
            Ok(value) => {
                let value = self
                    .set(key, value, expires_in)
                    .await
                    .expect("failed to set cache");

                Ok(value)
            }
        }
    }
}
