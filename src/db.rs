use anyhow::Result;
use sqlx::postgres::{PgPool as Pool, PgPoolOptions};

#[derive(Debug, sqlx::FromRow)]
pub struct FileInfo {
    pub name: String,
}

#[derive(Debug)]
pub struct DB {
    pool: Pool,
}

impl DB {
    pub async fn new(db_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(db_url)
            .await?;
        Ok(Self { pool })
    }
    pub async fn record_files(&self, external_source: &str, files: &[FileInfo]) -> Result<()> {
        let filenames: Vec<_> = files
            .iter()
            .map(|f| f.name.as_str())
            .map(|s| s.to_string())
            .collect();
        sqlx::query!(
            r#"INSERT INTO external_file(external_source, path) SELECT $1, * from UNNEST($2::text[])"#,

            external_source,
            &filenames
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
