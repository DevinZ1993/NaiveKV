use clap;
use naive_kv::protos::messages;
use naive_kv::thread_pool::ThreadPool;
use naive_kv::types::Result;
use naive_kv::utils;
use naive_kv::NaiveKV;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const DEFAULT_FOLDER_PATH: &str = "/tmp/naive_kv/";
const DEFAULT_NUM_THREADS: usize = 8;
const DEFAULT_SOCKET_IP: &str = "127.0.0.1";
const DEFAULT_SOCKET_PORT: &str = "1024";
const MIN_RETRY_DELAY_MS: u64 = 100;
const MAX_RETRY_TIMES: usize = 3;

fn main() -> Result<()> {
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

    let naive_kv = Arc::new(NaiveKV::open(folder_path)?);
    let servers = ThreadPool::new(num_threads);

    let listener = TcpListener::bind(format!("{}:{}", socket_ip, socket_port))?;
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            let naive_kv = naive_kv.clone();
            servers.add_task(|| {
                let _ = serve_client(naive_kv, stream);
            })?;
        }
    }
    Ok(())
}

fn serve_client(naive_kv: Arc<NaiveKV>, mut stream: TcpStream) -> Result<()> {
    let client_address = stream.peer_addr()?;
    println!("Start serving client {}.", client_address);
    loop {
        let mut response = messages::Response::new();
        match utils::read_message::<messages::Request, TcpStream>(&mut stream) {
            Ok(Some(request)) => {
                handle_request(&client_address, &*naive_kv, &request, &mut response);
            }
            Ok(None) => {
                break;
            }
            Err(error) => {
                println!("Failed to receive or deserialize request: {:?}", error);
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
                    println!("Failed to serialize or send response: {:?}", error);
                }
            }
            thread::sleep(Duration::from_millis(retry_delay_ms));
            retry_delay_ms *= 2;
        }
    }
    println!("End serving client {}.", client_address);
    Ok(())
}

fn handle_request(
    client_address: &SocketAddr,
    naive_kv: &NaiveKV,
    request: &messages::Request,
    response: &mut messages::Response,
) {
    let key = request.get_key();
    response.set_status(messages::Status::OK);
    response.set_id(request.get_id());
    print!(
        "client={}, request_id={}: ",
        client_address,
        request.get_id()
    );
    match request.get_operation() {
        messages::Operation::GET => {
            println!("GET {}", key);
        }
        messages::Operation::SET => {
            if !request.has_value() {
                response.set_status(messages::Status::VALUE_MISSING);
                return;
            }
            let value = request.get_value();
            println!("SET {} {}", key, value);
        }
        messages::Operation::REMOVE => {
            println!("REMOVE {}", key);
        }
    }
}
