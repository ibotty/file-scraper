use std::{env, time::SystemTime};

use anyhow::{Context, Result};
use aws_sdk_s3::{Client, operation::list_objects_v2::ListObjectsV2Output, types::Object};
use regex_lite::Regex;
use tracing::{debug, instrument};

use crate::{
    db::{DB, FileInfo},
    worker,
};

#[derive(Debug)]
pub struct Worker {
    pub client: Client,
    pub identifier: String,
    pub s3_bucket: String,
    pub path: String,
    pub db: DB,
}

impl Worker {
    #[instrument]
    pub async fn client_from_env() -> Result<Client> {
        let aws_config = aws_config::load_from_env().await;
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

        let endpoint_url = env::var("AWS_ENDPOINT_URL").ok();
        let aws_region = env::var("AWS_REGION").ok();
        let aws_force_path_style = matches!(
            env::var("AWS_S3_FORCE_PATH_STYLE")
                .unwrap_or("false".to_string())
                .as_str(),
            "true" | "TRUE" | "1"
        );

        if let Some(aws_endpoint_url) = endpoint_url {
            s3_config_builder = s3_config_builder.endpoint_url(aws_endpoint_url);
        };

        s3_config_builder = s3_config_builder.force_path_style(aws_force_path_style);

        if let Some(aws_region) = aws_region {
            let region = aws_config::Region::new(aws_region.clone());
            s3_config_builder = s3_config_builder.region(region);
        };

        Ok(Client::from_conf(s3_config_builder.build()))
    }

    #[instrument]
    fn parse_s3_url(s3_url: &str) -> Result<(String, String)> {
        let re = Regex::new(r"^s3://(?<bucket>[[:alnum:]-_]+)(|/(?<path>.*))$")
            .context("Cannot create regex")?;
        let captures = re.captures(s3_url).context("s3: Cannot parse s3 URL")?;
        let s3_bucket = captures.name("bucket").unwrap().as_str().to_string();
        let path = captures
            .name("path")
            .map(|c| c.as_str())
            .unwrap_or("")
            .to_string();
        Ok((s3_bucket, path))
    }

    #[instrument(skip_all, fields(s3_url, identifier))]
    pub async fn from_path(db: DB, identifier: Option<&str>, s3_url: &str) -> Result<Self> {
        debug!("trying to create worker from path");

        let (s3_bucket, path) = Self::parse_s3_url(s3_url)?;

        let identifier = identifier.unwrap_or(s3_url).to_string();

        let client = Self::client_from_env().await?;

        let worker = Worker {
            client,
            identifier,
            s3_bucket,
            path,
            db,
        };
        debug!(?worker, "created worker from path");

        Ok(worker)
    }

    async fn list_next(&self, continuation_token: Option<String>) -> Result<ListObjectsV2Output> {
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.s3_bucket)
            .prefix(&self.path);

        if let Some(token) = continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;
        Ok(resp)
    }
}

fn fileinfo_from_object(f: &Object) -> FileInfo {
    let (path, filename) = f
        .key()
        .expect("key is None")
        .rsplit_once('/')
        .expect("path does not contain /");

    let mime_type = mime_guess::from_path(filename)
        .first()
        .map(|m| m.essence_str().to_string());

    let modified = SystemTime::try_from(f.last_modified.expect("last_modified is None"))
        .expect("Cannot get SystemTime from DateTime");
    let size = f.size.expect("size is None") as u64;

    FileInfo {
        filename: filename.to_string(),
        path: path.to_string(),
        mime_type,
        modified,
        created: None,
        size,
    }
}

#[async_trait::async_trait]
impl worker::Worker for Worker {
    async fn walk(&self) -> Result<()> {
        let mut tx = self.db.begin().await?;

        let mut continuation_token = None;
        loop {
            let list_next = self.list_next(continuation_token).await?;
            let resp = list_next;

            let items: Vec<_> = resp.contents().iter().map(fileinfo_from_object).collect();
            debug!(items_len = items.len(), "got s3 list_objects_v2 response");

            DB::record_files(&mut tx, &self.identifier, items).await?;

            if resp.is_truncated().context("is_truncated returned None")? {
                continuation_token = resp.next_continuation_token().map(str::to_string);
            } else {
                break;
            }
        }

        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    const S3_URLS: [(&str, &str, &str); 3] = [
        ("s3://test_bucket/test", "test_bucket", "/test"),
        ("s3://test-bucket", "test-bucket", "/"),
        ("s3://test-bucket_0253/", "test-bucket_0253", "/"),
    ];

    #[test]
    fn test_parse_s3_url() {
        for (s3_url, bucket, path) in S3_URLS {
            let (parsed_bucket, parsed_path) = super::Worker::parse_s3_url(s3_url).unwrap();
            assert_eq!(parsed_bucket, bucket);
            assert_eq!(parsed_path, path);
        }
    }
}
