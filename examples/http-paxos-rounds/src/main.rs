#[macro_use]
extern crate log;

mod kv_http;
mod service;
mod transport;

use hyper::{
    Server,
    service::{make_service_fn, service_fn},
};
use paxos::{Configuration, NodeId};
use std::{env::args, net::SocketAddr, process::exit};

fn config() -> Configuration<String> {
    let node_id_str = match args().skip(1).next() {
        Some(node_id_str) => node_id_str,
        None => {
            error!("Must supply node ID as the first argument (0, 1, 2)");
            exit(1);
        }
    };

    let node_id = match u32::from_str_radix(&node_id_str, 10) {
        Ok(node_id) => node_id,
        Err(_) => {
            error!("Must supply integer node ID as the first argument (0, 1, 2)");
            exit(1);
        }
    };

    if node_id > 2 {
        error!("Must supply integer node ID as the first argument (0, 1, 2)");
        exit(1);
    }

    Configuration::new(
        node_id,
        (0u32..3)
            .filter(|n| *n != node_id)
            .map(|n| (n as NodeId, format!("http://127.0.0.1:808{}/paxos", n))),
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::init();

    let conf = config();
    let addr: SocketAddr = format!("127.0.0.1:808{}", conf.current()).parse().unwrap();
    let handler = service::Handler::new(conf);
    let tasks = handler.spawn_tasks();

    let service = make_service_fn(move |_| {
        let handler = handler.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let handler = handler.clone();
                async move { handler.handle(req).await }
            }))
        }
    });

    let server = Server::bind(&addr).serve(service);
    if let Err(e) = server.await {
        drop(tasks);
        eprintln!("server error: {}", e);
    }
}
