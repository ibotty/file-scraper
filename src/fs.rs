use std::time::Duration;

use anyhow::Result;
use async_walkdir::{DirEntry, WalkDir};
use itertools::Itertools;
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::{debug, error, instrument};

use crate::{
    db::{FileInfo, DB},
    worker,
};

#[derive(Debug)]
pub struct Worker {
    pub path: String,
    pub db: DB,
}

impl Worker {
    #[instrument(skip_all)]
    pub fn from_path(path: &str, db: DB) -> Result<Self> {
        let path = path.to_string();
        let worker = Worker { path, db };
        debug!(?worker, "created worker from path");

        Ok(worker)
    }

    #[instrument(skip_all)]
    async fn handle_entries(&self, entries: Vec<DirEntry>) -> () {
        debug!(?entries);
        let files: Vec<_> = entries
            .iter()
            .map(|entry| FileInfo {
                name: entry.path().to_string_lossy().to_string(),
            })
            .collect();

        if let Err(e) = self.db.record_files(&self.path, files.as_slice()).await {
            error!(?e)
        }
    }
}

#[async_trait::async_trait]
impl worker::Worker for Worker {
    #[instrument(skip_all)]
    async fn walk(&self) -> Result<()> {
        let entries = WalkDir::new(&self.path);
        let batch_entries = entries.chunks_timeout(200, Duration::from_secs(1));
        pin!(batch_entries);

        while let Some(batch) = batch_entries.next().await {
            let (files, errors): (Vec<_>, Vec<_>) = batch.into_iter().partition_result();
            let filtered = tokio_stream::iter(files.into_iter())
                .then(filter_file)
                .filter_map(|x| x)
                .collect()
                .await;
            self.handle_entries(filtered).await;
            for e in errors {
                error!(?e);
            }
        }
        Ok(())
    }
}

async fn filter_file(entry: DirEntry) -> Option<DirEntry> {
    if entry
        .file_type()
        .await
        .map(|t| t.is_file())
        .unwrap_or(false)
    {
        Some(entry)
    } else {
        None
    }
}