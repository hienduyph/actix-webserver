use mini_redis::{Connection, Frame};
use tokio::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use bytes::Bytes;

type DB = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(&addr).await.unwrap();
    println!("Listen on {}", addr);

    let db: DB = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let db = db.clone();
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: DB) {
    use mini_redis::Command::{Get, Set, self };
    let mut connection = Connection::new(socket);


    while let Some(frame) = connection.read_frame().await.unwrap() {
        // response with an error
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            },
            Get(cmd) => {
                let db = db.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            },
            cmd => panic!("un impl {:?}", cmd),
        };
        connection.write_frame(&response).await.unwrap();
    }
}
