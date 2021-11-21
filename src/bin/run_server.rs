use clap;
use log::info;
use naive_kv::catalog::CatalogViewer;
use naive_kv::logger;
use naive_kv::protos::messages;
use naive_kv::thread_pool::ThreadPool;
use naive_kv::types::Result;
use naive_kv::utils;
use naive_kv::NaiveKV;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

const DEFAULT_FOLDER_PATH: &str = "/tmp/naive_kv/";
const DEFAULT_NUM_THREADS: usize = 8;
const DEFAULT_SOCKET_IP: &str = "127.0.0.1";
const DEFAULT_SOCKET_PORT: &str = "1024";

// TODO Create a config type to incorporate the following params.
const MIN_RETRY_DELAY_MS: u64 = 100;
const MAX_RETRY_TIMES: usize = 3;
const MEMTABLE_COMPACTION_THRESHOLD: usize = 1 << 20; // 1MB
const GENERATION_GEOMETRIC_RATIO: usize = 8;
const COMPACTION_DAEMON_CYCLE_S: u64 = 1; // 1 sec

fn main() -> Result<()> {
    logger::init()?;
    let flag_matches = clap::App::new("NaiveKV Server")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .arg(
            clap::Arg::with_name("folder_path")
                .long("directory")
                .takes_value(true)
                .help("The directory for holding the storage"),
        )
        .arg(
            clap::Arg::with_name("num_threads")
                .long("workers")
                .takes_value(true)
                .help("The number of worker threads"),
        )
        .arg(
            clap::Arg::with_name("socket_ip")
                .long("ip")
                .takes_value(true)
                .help("The IPv4 address of the server"),
        )
        .arg(
            clap::Arg::with_name("socket_port")
                .long("port")
                .takes_value(true)
                .help("The port of the server"),
        )
        .get_matches();

    let folder_path = flag_matches
        .value_of("folder_path")
        .unwrap_or(DEFAULT_FOLDER_PATH);
    let num_threads = flag_matches
        .value_of("num_threads")
        .map(|s| s.parse::<usize>().expect("Cannot parse num_threads."))
        .unwrap_or(DEFAULT_NUM_THREADS);
    let socket_ip = flag_matches
        .value_of("socket_ip")
        .unwrap_or(DEFAULT_SOCKET_IP);
    let socket_port = flag_matches
        .value_of("socket_port")
        .unwrap_or(DEFAULT_SOCKET_PORT);

    let naive_kv = NaiveKV::open(
        folder_path,
        MEMTABLE_COMPACTION_THRESHOLD,
        GENERATION_GEOMETRIC_RATIO,
        COMPACTION_DAEMON_CYCLE_S,
    )?;
    info!("Started the NaiveKV instance.");

    let servers = ThreadPool::new(num_threads);
    info!("Started the server threads.");

    let listener = TcpListener::bind(format!("{}:{}", socket_ip, socket_port))?;
    info!("Started the TCP listener.");

    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            let catalog_viewer = naive_kv.catalog_viewer()?;
            servers.add_task(move || {
                let _ = serve_client(catalog_viewer, stream);
            })?;
        }
    }
    Ok(())
}

fn serve_client(mut catalog_viewer: CatalogViewer, mut stream: TcpStream) -> Result<()> {
    let client_address = stream.peer_addr()?;
    info!("Start serving client {}.", client_address);
    loop {
        let mut response = messages::Response::new();
        match utils::read_message::<messages::Request, TcpStream>(&mut stream) {
            Ok(Some(request)) => {
                handle_request(
                    &client_address,
                    &mut catalog_viewer,
                    &request,
                    &mut response,
                );
            }
            Ok(None) => {
                break;
            }
            Err(error) => {
                log::error!("Failed to receive or deserialize request: {:?}", error);
                response.set_status(messages::Status::OPERATION_NOT_SUPPORTED);
            }
        }
        let mut retry_delay_ms = MIN_RETRY_DELAY_MS;
        for _ in 0..MAX_RETRY_TIMES {
            match utils::write_message(&response, &mut stream) {
                Ok(()) => {
                    break;
                }
                Err(error) => {
                    log::error!("Failed to serialize or send response: {:?}", error);
                }
            }
            thread::sleep(Duration::from_millis(retry_delay_ms));
            retry_delay_ms *= 2;
        }
    }
    info!("End serving client {}.", client_address);
    Ok(())
}

fn handle_request(
    client_address: &SocketAddr,
    catalog_viewer: &mut CatalogViewer,
    request: &messages::Request,
    response: &mut messages::Response,
) {
    let key = request.get_key();
    response.set_status(messages::Status::OK);
    response.set_id(request.get_id());
    match request.get_operation() {
        messages::Operation::GET => {
            info!(
                "CLIENT={} REQUEST_ID={} GET {}",
                client_address,
                request.get_id(),
                key
            );
            match catalog_viewer.get(key) {
                Ok(Some(value)) => {
                    response.set_value(value);
                }
                Ok(None) => {
                    response.set_status(messages::Status::KEY_NOT_FOUND);
                }
                Err(_) => {
                    response.set_status(messages::Status::INTERNAL_ERROR);
                }
            }
        }
        messages::Operation::SET => {
            if !request.has_value() {
                response.set_status(messages::Status::VALUE_MISSING);
                return;
            }
            let value = request.get_value();
            info!(
                "CLIENT={} REQUEST_ID={} SET {} {}",
                client_address,
                request.get_id(),
                key,
                value
            );
            if let Err(_) = catalog_viewer.set(key.to_string(), value.to_string()) {
                response.set_status(messages::Status::INTERNAL_ERROR);
            }
        }
        messages::Operation::REMOVE => {
            info!(
                "CLIENT={} REQUEST_ID={} REMOVE {}",
                client_address,
                request.get_id(),
                key
            );
            if let Err(_) = catalog_viewer.remove(key.to_string()) {
                response.set_status(messages::Status::INTERNAL_ERROR);
            }
        }
    }
}
