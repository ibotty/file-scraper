use std::time::SystemTime;

use anyhow::Result;
use sqlx::{
    postgres::{PgPool as Pool, PgPoolOptions},
    Postgres, Transaction,
};

#[derive(Debug, sqlx::FromRow)]
pub struct FileInfo {
    pub path: String,
    pub filename: String,
    pub mime_type: Option<String>,
    pub created: SystemTime,
    pub modified: SystemTime,
    pub size: u64,
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

    pub async fn begin(&self) -> Result<Transaction<'static, Postgres>> {
        Ok(self.pool.begin().await?)
    }

    pub async fn record_files(
        tx: &mut Transaction<'static, Postgres>,
        external_source: &str,
        files: Vec<FileInfo>,
    ) -> Result<()> {
        let mut filenames = vec![];
        let mut pathes = vec![];
        let mut mime_types = vec![];
        let mut createds = vec![];
        let mut modifieds = vec![];
        let mut sizes = vec![];

        files.into_iter().for_each(|file| {
            filenames.push(file.filename);
            pathes.push(file.path);
            mime_types.push(file.mime_type);
            createds.push(file.created.into());
            modifieds.push(file.modified.into());
            sizes.push(file.size.try_into().ok());
        });

        sqlx::query!(
            r#"INSERT INTO external_file(external_source, filename, path, mime_type, created, modified, size)
                SELECT $1, * from UNNEST(
                    $2::text[],
                    $3::text[],
                    $4::text[],
                    $5::timestamptz[],
                    $6::timestamptz[],
                    $7::bigint[]
                )"#,
            external_source,
            &filenames,
            &pathes,
            &mime_types as &Vec<Option<String>>,
            &createds,
            &modifieds,
            &sizes as &Vec<Option<i64>>,
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub async fn clean_table(
        tx: &mut Transaction<'static, Postgres>,
        external_source: &str,
    ) -> Result<()> {
        sqlx::query!(
            r#"DELETE FROM external_file WHERE external_source = $1"#,
            external_source
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}
