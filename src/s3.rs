use anyhow::{Context, Result};
use regex_lite::Regex;
use tracing::debug;

use crate::worker;

#[derive(Debug)]
pub struct Worker {
    pub s3_bucket: String,
    pub path: String,
}

impl Worker {
    pub fn from_path(s: &str) -> Result<Self> {
        let re = Regex::new(r"^s3://(?<bucket>[[:alnum:]]+)(|/(?<path>.*))$")
            .context("Cannot create regex")?;
        let captures = re.captures(s).context("s3: Cannot parse s3 URL")?;
        let s3_bucket = captures.name("bucket").unwrap().as_str().to_string();
        let path = captures
            .name("path")
            .map(|c| c.as_str())
            .unwrap_or("")
            .to_string();

        let worker = Worker { s3_bucket, path };
        debug!(?worker, "created worker from path");

        Ok(worker)
    }
}

#[async_trait::async_trait]
impl worker::Worker for Worker {
    async fn walk(&self) -> Result<()> {
        todo!();
    }
}
