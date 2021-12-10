use crate::db::DB;

use anyhow::Result;
use async_trait::async_trait;
use chiselstore::rpc::proto::rpc_server::RpcServer;
use chiselstore::{
    rpc::{RpcService, RpcTransport},
    Consistency, StoreServer,
};
use lazy_static::lazy_static;
use sql_builder::SqlBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Once;

use tonic::transport::Server;

const PRIMARY_KEY: &str = "y_id";

lazy_static! {
    static ref STORE_SERVER: Arc<StoreServer<RpcTransport>> = {
        let id = 1;
        let peers = vec![2, 3];
        let transport = RpcTransport::new(Box::new(node_rpc_addr));
        let server = StoreServer::start(id, peers, transport).unwrap();
        Arc::new(server)
    };
}

/// Node authority (host and port) in the cluster.
fn node_authority(id: usize) -> (&'static str, u16) {
    let host = "127.0.0.1";
    let port = 50000 + (id as u16);
    (host, port)
}

/// Node RPC address in cluster.
fn node_rpc_addr(id: usize) -> String {
    let (host, port) = node_authority(id);
    format!("http://{}:{}", host, port)
}

static START: Once = Once::new();

pub struct ChiselStore {
    //    conn: Connection,
}

impl ChiselStore {
    pub async fn new() -> Result<Self> {
        START.call_once(|| {
            tokio::task::spawn(async move {
                let id = 1;
                let (host, port) = node_authority(id);
                let server = STORE_SERVER.clone();
                let f = {
                    let server = server.clone();
                    tokio::task::spawn_blocking(move || {
                        server.run();
                    })
                };
                let rpc_listen_addr = format!("{}:{}", host, port).parse().unwrap();
                let rpc = RpcService::new(server.clone());
                let g = tokio::task::spawn(async move {
                    println!("RPC listening to {} ...", rpc_listen_addr);
                    let ret = Server::builder()
                        .add_service(RpcServer::new(rpc))
                        .serve(rpc_listen_addr)
                        .await;
                    ret
                });
                let results = tokio::try_join!(f, g).unwrap();
                results.1.unwrap();
            });
        });
        Ok(ChiselStore {})
    }
}

#[async_trait]
impl DB for ChiselStore {
    async fn init(&self) -> Result<()> {
        let server = STORE_SERVER.clone();

        server.wait_for_leader().await;

        Ok(())
    }

    async fn create_schema(&self) -> Result<()> {
        let server = STORE_SERVER.clone();

        server.wait_for_leader().await;

        server.query("CREATE TABLE IF NOT EXISTS usertable (y_id VARCHAR PRIMARY KEY, field0 VARCHAR, field1 VARCHAR, field2 VARCHAR, field3 VARCHAR, field4 VARCHAR, field5 VARCHAR, field6 VARCHAR, field7 VARCHAR, field8 VARCHAR, field9 VARCHAR)", Consistency::Strong).await?;

        Ok(())
    }

    async fn insert(
        &self,
        table: String,
        key: String,
        values: HashMap<String, String>,
    ) -> Result<()> {
        // TODO: cache prepared statement
        let mut sql = SqlBuilder::insert_into(table);
        let mut vals: Vec<String> = Vec::new();
        sql.field(PRIMARY_KEY);
        vals.push(key);
        for (key, value) in values {
            sql.field(key);
            vals.push(format!("'{}'", value));
        }
        sql.values(&vals);
        let sql = sql.sql()?;
        let server = STORE_SERVER.clone();
        server.query(sql, Consistency::Strong).await?;
        Ok(())
    }

    async fn read(&self, table: String, key: String) -> Result<HashMap<String, String>> {
        // TODO: cache prepared statement
        let mut sql = SqlBuilder::select_from(table);
        sql.field("*");
        // TODO: fields
        sql.and_where(format!("{} = '{}'", PRIMARY_KEY, key));
        let sql = sql.sql()?;
        let server = STORE_SERVER.clone();
        server.query(sql, Consistency::RelaxedReads).await?;
        let result = HashMap::new();
        Ok(result)
    }
}
