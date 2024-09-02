use tokio::sync::watch;

pub struct Sender<T> {
    watch_tx: watch::Sender<Option<T>>,
}

impl<T> Sender<T> {
    fn send(self, obj: T) {
        self.watch_tx.send(Some(obj));
    }
}

#[derive(Clone)]
pub struct Receiver<T> {
    watch_rx: watch::Receiver<Option<T>>,
}

#[derive(Debug)]
pub struct Cancelled;

impl<T: Clone> Receiver<T> {
    async fn receive(mut self) -> Result<T, Cancelled> {
        let result_opt = self.watch_rx
            .wait_for(|result_opt| result_opt.is_some())
            .await
            .map_err(|_| Cancelled)?;
        Ok(result_opt.clone().unwrap())
    }
}

pub fn broadcast_oneshot_channel<T>() -> (Sender<T>, Receiver<T>) {
    let (watch_tx, watch_rx) = watch::channel(None);
    let sender = Sender { watch_tx};
    let receiver = Receiver { watch_rx };
    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::broadcast_oneshot_channel;

    #[tokio::test]
    async fn test_broadcast_oneshot_channel_rx() {
        let (sender, receiver) = broadcast_oneshot_channel::<u32>();
        let receiver_2 = receiver.clone();
        let join_handle_rx = tokio::spawn(async move {
            receiver_2.receive().await.unwrap()
        });
        sender.send(42);
        assert_eq!(receiver.receive().await.unwrap(), 42);
        assert_eq!(join_handle_rx.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_broadcast_oneshot_channel_rx_cancel() {
        let (sender, receiver) = broadcast_oneshot_channel::<u32>();
        let receiver_2 = receiver.clone();
        let join_handle_rx = tokio::spawn(async move {
            receiver_2.receive().await
        });
        drop(sender);
        assert!(receiver.receive().await.is_err());
        assert!(join_handle_rx.await.unwrap().is_err());
    }
}
