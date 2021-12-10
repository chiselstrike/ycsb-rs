use crate::db::DB;
use crate::workload::Workload;
use anyhow::{bail, Result};
use futures::future::join_all;
use properties::Properties;
use std::fs;
use std::sync::Arc;
use std::time::Instant;
use structopt::StructOpt;
use workload::CoreWorkload;

pub mod chiselstore;
pub mod db;
pub mod generator;
pub mod properties;
pub mod sqlite;
pub mod workload;

#[derive(StructOpt, Debug)]
#[structopt(name = "ycsb")]
struct Opt {
    #[structopt(name = "COMMANDS")]
    commands: Vec<String>,
    #[structopt(short, long)]
    database: String,
    #[structopt(short, long)]
    workload: String,
    #[structopt(short, long, default_value = "1")]
    threads: usize,
}

async fn load(wl: Arc<CoreWorkload>, db: Arc<dyn DB + Send + Sync>, operation_count: usize) {
    for _ in 0..operation_count {
        wl.do_insert(db.clone()).await;
    }
}

async fn run(wl: Arc<CoreWorkload>, db: Arc<dyn DB + Send + Sync>, operation_count: usize) {
    for _ in 0..operation_count {
        wl.do_transaction(db.clone()).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();

    let raw_props = fs::read_to_string(&opt.workload)?;

    let props: Properties = toml::from_str(&raw_props)?;

    let props = Arc::new(props);

    let wl = Arc::new(CoreWorkload::new(&props));

    if opt.commands.is_empty() {
        bail!("no command specified");
    }

    let database = opt.database.clone();
    let thread_operation_count = props.operation_count as usize / opt.threads;
    for cmd in opt.commands {
        let mut dbs: Vec<Arc<dyn DB + Sync + Send>> = vec![];
        for _ in 0..opt.threads {
            let db = db::create_db(&database).await.unwrap();
            db.init().await.unwrap();
            dbs.push(db);
        }
        let start = Instant::now();
        let mut threads = vec![];
        for _ in 0..opt.threads {
            let wl = wl.clone();
            let cmd = cmd.clone();
            let db = dbs.remove(0);
            let task = tokio::task::spawn(async move {
                let wl = wl.clone();

                match &cmd[..] {
                    "load" => {
                        db.create_schema().await.unwrap();
                        load(wl.clone(), db.clone(), thread_operation_count as usize).await;
                    }
                    "run" => run(wl.clone(), db.clone(), thread_operation_count as usize).await,
                    cmd => panic!("invalid command: {}", cmd),
                };
            });
            threads.push(task);
        }
        join_all(threads).await;
        let runtime = start.elapsed().as_millis();
        println!("[OVERALL], ThreadCount, {}", opt.threads);
        println!("[OVERALL], RunTime(ms), {}", runtime);
        let throughput = props.operation_count as f64 / (runtime as f64 / 1000.0);
        println!("[OVERALL], Throughput(ops/sec), {}", throughput);
    }

    Ok(())
}
