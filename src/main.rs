mod db;
mod fs;
mod s3;
mod worker;

use std::{env, str::FromStr};

use anyhow::Result;
use clap::Parser;
use clap_verbosity::{ErrorLevel, Verbosity};
use db::DB;
use tokio::task::JoinSet;
use tracing::{debug, error, info, instrument, level_filters::LevelFilter, Level};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

use worker::Worker;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    paths: Vec<String>,

    #[arg(short, long)]
    identifier: Option<String>,

    #[command(flatten)]
    verbose: Verbosity<ErrorLevel>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let log_level = args
        .verbose
        .log_level()
        .and_then(|l| Level::from_str(&l.to_string()).ok())
        .unwrap_or(Level::WARN);

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(LevelFilter::from_level(log_level))
        .init();

    let mut joinset = JoinSet::new();
    for path in &args.paths {
        let path = path.clone();
        let identifier = args.identifier.clone();
        let db = DB::new(&env::var("DATABASE_URL")?).await?;

        joinset.spawn(async move {
            process(identifier.as_deref(), &path, db)
                .await
                .inspect_err(|error| {
                    // if let Err(error) = process(identifier.as_deref(), &path, db).await {
                    error!(?path, ?error, "Error processing path")
                })
        });
    }

    // wait for all tasks to complete
    joinset
        .join_all()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    debug!(?args, ?log_level);
    Ok(())
}

#[instrument(skip(db))]
async fn process(identifier: Option<&str>, path: &str, db: DB) -> Result<()> {
    let worker: Box<dyn Worker> = s3::Worker::from_path(db.clone(), identifier, path)
        .await
        .inspect_err(|e| info!(?e, "cannot construct s3 worker"))
        .map(|w| Box::new(w) as Box<dyn Worker>)
        .or_else(|_| {
            fs::Worker::from_path(db, identifier, path).map(|w| Box::new(w) as Box<dyn Worker>)
        })?;
    worker.walk().await
}
