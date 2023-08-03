use async_trait::async_trait;
use futures_util::Future;

#[async_trait]
pub trait ExternalCallDelegate {
    async fn delegate<Fut, T>(&self, future: Fut) -> T
    where Fut: Future<Output = T> + Send;
}
#[async_trait]
impl ExternalCallDelegate for () {
    async fn delegate<Fut, T>(&self, future: Fut) -> T
    where Fut: Future<Output = T> + Send {
        future.await
    }
}
