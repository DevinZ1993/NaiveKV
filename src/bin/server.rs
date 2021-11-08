use std::net::{TcpListener, TcpStream};

use naive_kv::protos::commands;
use naive_kv::storage::NaiveKV;
use naive_kv::thread_pool::ThreadPool;
use naive_kv::types::{NaiveError, Record, Result};
use protobuf;
use protobuf::Message;
use std::io::{Read, Write};
use std::sync::Arc;

const BUFFER_SIZE: usize = 1024;

fn main() -> Result<()> {
    let naive_kv = Arc::new(NaiveKV::open("")?);
    let servers = ThreadPool::new(8);

    let listener = TcpListener::bind("127.0.0.1:1024")?;
    for stream in listener.incoming() {
        let stream = stream?;
        let naive_kv = naive_kv.clone();
        servers.add_task(|| process_stream(naive_kv, stream))?;
    }
    Ok(())
}

fn process_stream(naive_kv: Arc<NaiveKV>, mut stream: TcpStream) {
    let mut buffer = [0u8; BUFFER_SIZE];
    let mut bytes = Vec::new();
    while let Ok(num_bytes) = stream.read(&mut buffer) {
        bytes.reserve(bytes.len() + num_bytes);
        for i in 0..num_bytes {
            bytes.push(buffer[i]);
        }
    }

    let mut response = commands::Response::new();
    match commands::Request::parse_from_bytes(&bytes) {
        Err(_) => {
            response.set_status(commands::Status::COMMAND_NOT_SUPPORTED);
        }
        Ok(request) => {
            handle_request(&*naive_kv, &request, &mut response);
        }
    }

    if let Ok(bytes) = response.write_to_bytes() {
        stream.write(&bytes);
    }
}

fn handle_request(
    naive_kv: &NaiveKV,
    request: &commands::Request,
    response: &mut commands::Response,
) {
}
