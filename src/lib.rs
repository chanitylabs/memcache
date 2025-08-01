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
    prefix: String,
}

impl Default for MemCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MemCache {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
            prefix: "main".into(),
        }
    }

    pub fn subgroup(&self, prefix: impl Into<String>) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            prefix: format!("{}::{}", self.prefix, prefix.into()),
        }
    }

    pub async fn get<K, V>(&self, key: K) -> Option<V>
    where
        K: Serialize,
        V: for<'de> Deserialize<'de>,
    {
        let key = self._key(key);
        self._get(key).await
    }

    pub async fn set<K: Serialize, V: Serialize>(
        &self,
        key: K,
        value: V,
        expires_in: Option<Duration>,
    ) -> Option<V> {
        let key = self._key(key);
        self._set(key, value, expires_in).await
    }

    pub async fn remove<K: Serialize>(&self, key: K) {
        let key = self._key(key);
        self._remove(key).await
    }

    pub async fn cached<F, Fut, K, V, E>(
        &self,
        action_fn: F,
        key: K,
        expires_in: Option<Duration>,
    ) -> Result<V, E>
    where
        K: Serialize,
        V: for<'de> Deserialize<'de> + Serialize,
        Fut: Future<Output = Result<V, E>>,
        F: FnOnce() -> Fut + Send + 'static,
    {
        let key = self._key(key);

        let data = self._get(key.clone()).await;
        if let Some(data) = data {
            return Ok(data);
        }
        let value = action_fn().await?;
        let value = self
            ._set(key, value, expires_in)
            .await
            .expect("failed to set cache");

        Ok(value)
    }

    async fn _get<V>(&self, key: String) -> Option<V>
    where
        V: for<'de> Deserialize<'de>,
    {
        let mut storage = self.storage.write().await;
        let data = storage.get(&key)?;

        if let Some(expiry) = data.expires_at {
            if expiry <= Instant::now() {
                storage.remove(&key);
                return None;
            }
        }

        bincode::deserialize(&data.value).ok()
    }

    async fn _set<V>(&self, key: String, value: V, expires_in: Option<Duration>) -> Option<V>
    where
        V: Serialize,
    {
        let value_data = bincode::serialize(&value).ok()?;
        let mut storage = self.storage.write().await;
        storage.insert(
            key.clone(),
            Data {
                value: value_data,
                expires_at: expires_in.map(|expires_in| Instant::now() + expires_in),
            },
        );

        Some(value)
    }

    async fn _remove(&self, key: String) {
        let mut storage = self.storage.write().await;
        storage.remove(&key);
    }

    fn _key<K: Serialize>(&self, key: K) -> String {
        let key = serde_json::to_string(&key).expect("failed to serialize key");
        let key = format!("{}::{}", self.prefix, key);
        key
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use tokio::time::{Duration, sleep};

    use super::*;

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
    struct MyStruct {
        name: String,
        id: u32,
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let cache = MemCache::new();
        let value = MyStruct {
            name: "Alice".into(),
            id: 1,
        };

        let _ = cache.set("user1", value.clone(), None).await;
        let retrieved: Option<MyStruct> = cache.get("user1").await;

        assert_eq!(retrieved, Some(value));
    }

    #[tokio::test]
    async fn test_expiration() {
        let cache = MemCache::new();
        let _ = cache
            .set("temp", 123_i32, Some(Duration::from_millis(100)))
            .await;
        sleep(Duration::from_millis(150)).await;
        let result: Option<i32> = cache.get("temp").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_remove() {
        let cache = MemCache::new();
        let _ = cache.set("user2", "hello".to_string(), None).await;
        cache.remove("user2").await;
        let result: Option<String> = cache.get("user2").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_subgroup_prefix_isolation() {
        let base = MemCache::new();
        let group = base.subgroup("admin");

        let _ = base.set("id", 1_u32, None).await;
        let _ = group.set("id", 42_u32, None).await;

        let base_val: Option<u32> = base.get("id").await;
        let group_val: Option<u32> = group.get("id").await;

        assert_eq!(base_val, Some(1));
        assert_eq!(group_val, Some(42));
    }

    #[tokio::test]
    async fn test_cached_returns_existing() {
        let cache = MemCache::new();
        let _ = cache.set("cached_key", 99_i32, None).await;

        let result = cache
            .cached(|| async { Ok::<_, ()>(123) }, "cached_key", None)
            .await
            .unwrap();

        assert_eq!(result, 99);
    }

    #[tokio::test]
    async fn test_cached_executes_on_miss() {
        let cache = MemCache::new();

        let result = cache
            .cached(|| async { Ok::<_, ()>(777) }, "new_key", None)
            .await
            .unwrap();

        assert_eq!(result, 777);

        let stored: Option<i32> = cache.get("new_key").await;
        assert_eq!(stored, Some(777));
    }

    #[tokio::test]
    async fn test_cached_error() {
        let cache = MemCache::new();

        let result: Result<i32, &str> = cache
            .cached(|| async { Err("fail") }, "err_key", None)
            .await;

        assert_eq!(result, Err("fail"));
    }
}
