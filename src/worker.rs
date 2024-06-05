use anyhow::Result;

#[async_trait::async_trait]
pub trait Worker: Send + Sync {
    async fn walk(&self) -> Result<()>;
}
