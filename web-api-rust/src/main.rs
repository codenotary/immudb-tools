use std::collections::HashMap;
use clap::Parser;
use reqwest;
use rand::distributions::{Alphanumeric, DistString};
use faker_rand::en_us::names::{FirstName, LastName};
use faker_rand::en_us::addresses::Address;
use faker_rand::en_us::internet::Email;
use futures::{stream, StreamExt};

// Configuration
#[derive(Parser,Clone)]
struct Cfg {
    #[arg(short, long, default_value = "127.0.0.1")]
    address: String,
    #[arg(short = 'P', long, default_value_t = 8080)]
    port: u16,
    #[arg(short, long, default_value = "immudb")]
    username: String,
    #[arg(short = 'p', long, default_value = "immudb")]
    password: String,
    #[arg(short, long, default_value = "defaultdb")]
    database: String,
    #[arg(short = 'b', long, default_value_t = 1)]
    batchsize: u16,
    #[arg(short = 'n', long, default_value_t = 1)]
    batchnum: u16,
    #[arg(short = 'w', long, default_value_t = 1)]
    workers: u16,
    #[arg(short = 't', long)]
    duration: Option<u16>,
}

#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct ImmudbSession {
    sessionID: String,
    // serverUUID: String,
    // inactivityTimestamp: u64,
}

async fn open_session(cfg: &Cfg, client: &reqwest::Client) -> String {
    let mut map = HashMap::new();
    map.insert("username", &cfg.username);
    map.insert("password", &cfg.password);
    map.insert("database", &cfg.database);
    let url = format!("http://{}:{}/api/v2/authorization/session/open", cfg.address, cfg.port);
    let res = client.post(url)
        .json(&map)
        .send()
        .await.expect("Unable to open session");
    let sess = res.json::<ImmudbSession>().await.expect("Unable to get response");
    sess.sessionID
}

async fn create_collection(cfg: &Cfg, client: &reqwest::Client, session: &str, collection: &str) {
    println!("Creating collection: {:?}", collection);
    let url = format!("http://{}:{}/api/v2/collection/{}", cfg.address, cfg.port, collection);
    for attempt in 0..100 {
        let payload = format!(r#"{{
                "name": "{}",
                "documentIdFieldName": "person_id",
                "fields": [
                    {{ "name": "first_name" }},
                    {{ "name": "last_name" }},
                    {{ "name": "address" }},
                    {{ "name": "email" }}
                ]
            }}"#, collection);
        let res = client.post(&url)
            .header("grpc-metadata-sessionid", session)
            .header("content-type", "application/json")
            .body(payload)
            .send()
            .await.expect("Unable to create collection");
        if res.status().is_success() {
            break
        }
        println!("Error: {}\n{}", res.status().as_u16(), res.text().await.expect("Unable to fetch response body"));
        tokio::time::sleep(tokio::time::Duration::from_millis(1*(attempt as u64))).await;
    }
}

#[derive(serde::Serialize)]
struct DocumentStruct {
    first_name: String,
    last_name: String,
    address: String,
    email: String,
}

#[derive(serde::Serialize)]
struct DocumentInsert {
    documents: Vec<DocumentStruct>,
}


async fn insert_document(cfg: &Cfg, client: &reqwest::Client, session: &str, collection: &str, idx: u16) {
    let mut docs = DocumentInsert {
        documents: Vec::<DocumentStruct>::new(),
    };
    for _i in 0..cfg.batchsize {
        let doc = DocumentStruct {
            first_name: rand::random::<FirstName>().to_string(),
            last_name: rand::random::<LastName>().to_string(),
            address: rand::random::<Address>().to_string(),
            email: rand::random::<Email>().to_string(),
        };
        println!("WORKER {}: Inserting doc for {} {}", idx, &doc.first_name, &doc.last_name);
        docs.documents.push(doc);
    }
    let url = format!("http://{}:{}/api/v2/collection/{}/documents", cfg.address, cfg.port, collection);
    let res = client.post(url)
        .header("grpc-metadata-sessionid", session)
        .header("content-type", "application/json")
        .json(&docs)
        .send()
        .await.expect("Unable to insert document");
    if !res.status().is_success() {
        println!("Error: {}\n{}", res.status().as_u16(), res.text().await.expect("Unable to fetch response body"))
    }
}

async fn insert_n_documents(cfg: &Cfg, client: &reqwest::Client, session: &str, collection: &str, idx: u16) {
    for _i in 0..cfg.batchnum {
        insert_document(&cfg, &client, &session, &collection, idx).await;
    }
}

async fn insert_t_documents(cfg: &Cfg, client: &reqwest::Client, session: &str, collection: &str, idx: u16) {
    let begin = std::time::Instant::now();
    let end = std::time::Duration::new(cfg.duration.unwrap().into(),0);
    while begin.elapsed() < end {
        insert_document(&cfg, &client, &session, &collection, idx).await;
    }
}

async fn async_run(cfg: &Cfg, client: &reqwest::Client, idx: u16) {
    let sess = open_session(&cfg, &client).await;
    println!("WORKER {} Session: {:?}", idx, sess);
    let coll = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    create_collection(&cfg, &client, &sess, &coll).await;
    match cfg.duration {
        None => insert_n_documents(&cfg, &client, &sess, &coll, idx).await,
        Some(_x) => insert_t_documents(&cfg, &client, &sess, &coll, idx).await,
    };
    println!("WORKER {} End:", idx);
}

async fn spawner(cfg: &Cfg) {
    let client = reqwest::Client::new();
    let workers = stream::iter(0..cfg.workers)
        .map(|idx| {
            let ccfg = cfg.clone();
            let cclient = client.clone();
            tokio::spawn(async move {
                async_run(&ccfg, &cclient, idx).await;
            })
        }).buffer_unordered(cfg.workers.into());
    workers.for_each(|b| async {
        match b {
            Ok(_) => println!("Worker done"),
            Err(e) => println!("Worker error: {}", e),
        }
    }).await;
}

#[tokio::main]
async fn main() {
    let cfg = Cfg::parse();
    async_std::task::block_on(spawner(&cfg));
}
