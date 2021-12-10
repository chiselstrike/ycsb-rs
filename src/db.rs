use crate::chiselstore::ChiselStore;
use crate::sqlite::SQLite;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait DB {
    async fn init(&self) -> Result<()>;
    async fn create_schema(&self) -> Result<()>;
    async fn insert(
        &self,
        table: String,
        key: String,
        values: HashMap<String, String>,
    ) -> Result<()>;
    async fn read(&self, table: String, key: String) -> Result<HashMap<String, String>>;
}

pub async fn create_db(db: &str) -> Result<Arc<dyn DB + Send + Sync>> {
    match db {
        "chiselstore" => Ok(Arc::new(ChiselStore::new().await?)),
        "sqlite" => Ok(Arc::new(SQLite::new()?)),
        db => Err(anyhow!("{} is an invalid database name", db)),
    }
}
