use std::time::Duration;

use anyhow::Result;
use async_walkdir::{DirEntry, WalkDir};
use futures_util::future::join_all;
use gethostname::gethostname;
use itertools::Itertools;
use sqlx::{Postgres, Transaction};
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::{debug, instrument};

use crate::{
    db::{DB, FileInfo},
    worker,
};

#[derive(Debug)]
pub struct Worker {
    pub identifier: String,
    pub path: String,
    pub db: DB,
}

impl Worker {
    #[instrument(skip_all)]
    pub fn from_path(db: DB, identifier: Option<&str>, path: &str) -> Result<Self> {
        let identifier = identifier
            .map(str::to_string)
            .unwrap_or_else(|| Self::default_identifier(path));
        let path = path.to_string();
        let worker = Worker {
            identifier,
            path,
            db,
        };
        debug!(?worker, "created worker from path");

        Ok(worker)
    }

    #[instrument(skip_all)]
    pub fn default_identifier(path: &str) -> String {
        let hostname = gethostname();
        let hostname = hostname.to_str().unwrap();
        format!("{}:{}", hostname, path)
    }

    #[instrument]
    async fn get_fileinfo(entry: &DirEntry) -> Result<Option<FileInfo>> {
        let path = entry.path().parent().unwrap().to_string_lossy().to_string();
        let filename = entry.file_name().to_string_lossy().to_string();
        let metadata = entry.metadata().await?;

        let mime_type: Option<String> = if metadata.file_type().is_file() {
            mime_guess::from_path(entry.path())
                .first()
                .map(|m| m.essence_str().to_string())
        } else {
            // do not record directories
            return Ok(None);
        };

        let size = metadata.len();
        let created = Some(metadata.created()?);
        let modified = metadata.modified()?;

        Ok(Some(FileInfo {
            path,
            filename,
            mime_type,
            created,
            modified,
            size,
        }))
    }

    #[instrument(skip_all)]
    async fn handle_entries(
        &self,
        tx: &mut Transaction<'static, Postgres>,
        entries: Vec<DirEntry>,
    ) -> Result<()> {
        debug!(?entries);
        let files = join_all(entries.iter().map(Self::get_fileinfo))
            .await
            .into_iter()
            .filter_map_ok(|f| f)
            .collect::<Result<Vec<_>>>()?;

        DB::record_files(tx, &self.identifier, files).await
    }
}

#[async_trait::async_trait]
impl worker::Worker for Worker {
    #[instrument(skip_all)]
    async fn walk(&self) -> Result<()> {
        let entries = WalkDir::new(&self.path);
        let batch_entries = entries.chunks_timeout(200, Duration::from_secs(1));
        pin!(batch_entries);

        let mut tx = self.db.begin().await?;

        DB::create_temp_table(&mut tx).await?;

        while let Some(batch) = batch_entries.next().await {
            let files: Vec<_> = batch.into_iter().try_collect()?;
            self.handle_entries(&mut tx, files).await?;
        }

        DB::mark_deleted_files(&mut tx, &self.identifier).await?;

        tx.commit().await?;

        Ok(())
    }
}
