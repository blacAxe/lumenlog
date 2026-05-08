use prost::Message;
use chrono::Utc;
use std::collections::HashMap;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::ClientConfig;
use std::time::Duration;

use serde::{Deserialize};
use warp::Filter;

pub mod lumenlog {
    include!(concat!(env!("OUT_DIR"), "/lumenlog.rs"));
}

use lumenlog::LogEvent;

#[derive(Debug, Deserialize)]
struct SecurityEvent {
    user_id: String,
    attack_type: String,
    action: String,
    ip: Option<String>,
    path: Option<String>,
    method: Option<String>,
    query: Option<String>,
    timestamp: Option<i64>,
}

#[tokio::main]
async fn main() {

    println!("Rust Security Agent listening on :7777");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "redpanda:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let producer_filter = warp::any().map(move || producer.clone());

    let route = warp::post()
        .and(warp::path("event"))
        .and(warp::body::json())
        .and(producer_filter)
        .and_then(handle_event);

    warp::serve(route)
        .run(([0, 0, 0, 0], 7777))
        .await;
}

async fn handle_event(
    event: SecurityEvent,
    producer: FutureProducer,
) -> Result<impl warp::Reply, warp::Rejection> {

    println!(
        "🚨 SECURITY EVENT: user={} attack={}",
        event.user_id,
        event.attack_type
    );

    let log_event = LogEvent {
        service_name: "sentinel-proxy".to_string(),
        host: "waf-node".to_string(),
        level: "SECURITY".to_string(),
        message: format!(
            "{} attack blocked for user {}",
            event.attack_type,
            event.user_id
        ),
        timestamp: Utc::now().timestamp(),
        user_id: event.user_id.clone(),
        attack_type: event.attack_type.clone(),
        action: event.action.clone(),
        metadata: HashMap::new(),
    };

    let mut buf = Vec::new();

    log_event
        .encode(&mut buf)
        .expect("protobuf encode failed");

    let result = producer.send(
        FutureRecord::to("security-events")
            .payload(&buf)
            .key(&event.user_id),
        Duration::from_secs(0),
    ).await;

    match result {
        Ok(_) => println!("Event shipped to Kafka"),
        Err((e, _)) => println!("Kafka error: {:?}", e),
    }

    Ok(warp::reply::json(&"ok"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn placeholder_test() {
        assert_eq!(2 + 2, 4);
    }
}