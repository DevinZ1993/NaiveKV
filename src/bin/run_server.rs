use clap;
use naive_kv::protos::commands;
use naive_kv::storage::NaiveKV;
use naive_kv::thread_pool::ThreadPool;
use naive_kv::types::{NaiveError, Record, Result};
use naive_kv::utils;
use std::io::{Read, Write};
use std::net::ToSocketAddrs;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

const BUFFER_SIZE: usize = 1024;
const DEFAULT_FOLDER_PATH: &str = "/tmp/naive_kv/";
const DEFAULT_NUM_THREADS: usize = 8;
const DEFAULT_SOCKET_IP: &str = "127.0.0.1";
const DEFAULT_SOCKET_PORT: &str = "1024";

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
            servers.add_task(|| serve_client(naive_kv, stream))?;
        }
    }
    Ok(())
}

fn serve_client(naive_kv: Arc<NaiveKV>, mut stream: TcpStream) {
    let mut buffer = [0u8; BUFFER_SIZE];
    let mut bytes = Vec::new();

    loop {
        let mut response = commands::Response::new();
        match utils::get_message_from_stream::<commands::Request>(
            &mut stream,
            &mut buffer,
            &mut bytes,
        ) {
            Ok(request) => {
                handle_request(&*naive_kv, &request, &mut response);
            }
            Err(NaiveError::TcpReadError) => {
                break;
            }
            Err(_) => {
                response.set_status(commands::Status::OPERATION_NOT_SUPPORTED);
            }
        }
        utils::send_message_to_stream(&response, &mut stream);
    }
}

fn handle_request(
    naive_kv: &NaiveKV,
    request: &commands::Request,
    response: &mut commands::Response,
) {
    let key = request.get_key();
    response.set_status(commands::Status::OK);
    match request.get_operation() {
        commands::Operation::GET => {
            println!("get {}", key);
        }
        commands::Operation::SET => {
            if !request.has_value() {
                response.set_status(commands::Status::VALUE_MISSING);
                return;
            }
            let value = request.get_value();
            println!("set {} {}", key, value);
        }
        commands::Operation::REMOVE => {
            println!("remove {}", key);
        }
    }
}
