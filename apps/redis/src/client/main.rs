use bytes::Bytes;
use mini_redis::{client, Result};
use tokio::sync::oneshot;

#[derive(Debug)]
enum Command {
    Get { key: String, resp Respon },
    Set { key: String, val: Bytes },
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let tx2 = tx.clone();

    let t1 = tokio::spawn(async move {
        tx.send(Command::Get{key: "hello".to_string() }).await.unwrap();
    });

    let t2 = tokio::spawn(async move {
        tx2.send(Command::Set{key: "foo".to_string(), val: "bar".into()}).await.unwrap();
    });

    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Get { key } => {
                    client.get(&key).await;
                }
                Command::Set { key, val } => {
                    client.set(&key, val).await;
                }
            }
        }
    });

    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();
    Ok(())
}
