use clap;
use naive_kv::types::Result;
use std::net::TcpStream;

const BUFFER_SIZE: usize = 65536;
const DEFAULT_SERVER_IP: &str = "127.0.0.1";
const DEFAULT_SERVER_PORT: &str = "1024";

fn main() -> Result<()> {
    let flag_matches = clap::App::new("NaiveKV Client")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .arg(
            clap::Arg::with_name("server_ip")
                .long("ip")
                .takes_value(true)
                .help("The IPv4 address of the server"),
        )
        .arg(
            clap::Arg::with_name("server_port")
                .long("port")
                .takes_value(true)
                .help("The port of the server"),
        )
        .get_matches();

    let server_ip = flag_matches
        .value_of("server_ip")
        .unwrap_or(DEFAULT_SERVER_IP);
    let server_port = flag_matches
        .value_of("server_port")
        .unwrap_or(DEFAULT_SERVER_PORT);

    let stream = TcpStream::connect(format!("{}{}", server_ip, server_port))?;

    let mut buffer = [0u8; BUFFER_SIZE];
    //let mut bytes = Vec::new();
    loop {
        // TODO: interpreter
    }
    Ok(())
}
