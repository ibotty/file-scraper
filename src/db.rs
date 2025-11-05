use std::time::SystemTime;

use anyhow::Result;
use sqlx::{
    Postgres, Transaction,
    postgres::{PgPool as Pool, PgPoolOptions},
    types::time::OffsetDateTime,
};

#[derive(Debug, sqlx::FromRow)]
pub struct FileInfo {
    pub path: String,
    pub filename: String,
    pub mime_type: Option<String>,
    pub created: Option<SystemTime>,
    pub modified: SystemTime,
    pub size: u64,
}

#[derive(Debug, Clone)]
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
            createds.push(file.created.map(|f| f.into()));
            modifieds.push(file.modified.into());
            sizes.push(file.size.try_into().ok());
        });

        // This query does not update `mime_type` when `created`, `modified` or `size` are
        // unchanged.  Because the mime type detection might have been wrong and we don't want to
        // overwrite a correct mime type.
        sqlx::query!(
            r#"INSERT INTO external_file(external_source, filename, path, mime_type, created, modified, size)
                SELECT $1, * from UNNEST(
                    $2::text[],
                    $3::text[],
                    $4::text[],
                    $5::timestamptz[],
                    $6::timestamptz[],
                    $7::bigint[]
                )
                ON CONFLICT ON CONSTRAINT external_file_unique_constraint
                DO UPDATE
                SET
                    mime_type = EXCLUDED.mime_type,
                    created = EXCLUDED.created,
                    modified = EXCLUDED.modified,
                    size = EXCLUDED.size
                WHERE
                    (external_file.created, external_file.modified, external_file.size)
                    <> (EXCLUDED.created, EXCLUDED.modified, EXCLUDED.size)
                "#,
            external_source,
            &filenames,
            &pathes,
            &mime_types as &Vec<Option<String>>,
            &createds as &Vec<Option<OffsetDateTime>>,
            &modifieds,
            &sizes as &Vec<Option<i64>>,
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}
