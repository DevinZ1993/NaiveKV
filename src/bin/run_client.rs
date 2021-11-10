use clap;
use naive_kv::protos::commands;
use naive_kv::types::Result;
use naive_kv::utils;
use std::io::{stdin, stdout, BufRead, Read, Write};
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

    // TODO Decide whether to build the TCP connection once for all or for each single request.
    let mut stream = TcpStream::connect(format!("{}:{}", server_ip, server_port))?;
    let mut buffer = [0u8; BUFFER_SIZE];

    let stdin = stdin();
    let mut user_commands = stdin.lock().lines();
    print_greetings();
    print_help();

    let mut read_user_command = || {
        print!(">>> ");
        stdout().flush().unwrap();
        user_commands.next()
    };
    while let Some(command) = read_user_command() {
        let command = command?;
        let tokens = command
            .split(" ")
            .filter(|tok| !tok.is_empty())
            .collect::<Vec<&str>>();
        if tokens.is_empty() {
            continue;
        }

        macro_rules! check_arguments {
            ($actual_number:expr, $expected_number:expr) => {
                if $actual_number != $expected_number {
                    println!(
                        "[ERROR] Invalid Arguments: expect {} but got {}.",
                        $expected_number, $actual_number
                    );
                    continue;
                }
            };
        }

        match tokens[0] {
            "help" => {
                check_arguments!(tokens.len() - 1, 0);
                print_help();
            }
            "exit" => {
                check_arguments!(tokens.len() - 1, 0);
                break;
            }
            "get" => {
                check_arguments!(tokens.len() - 1, 1);
                let mut request = commands::Request::new();
                request.set_operation(commands::Operation::GET);
                request.set_key(tokens[1].to_owned());
                send_request(request, &mut stream, &mut buffer);
            }
            "set" => {
                check_arguments!(tokens.len() - 1, 2);
                let mut request = commands::Request::new();
                request.set_operation(commands::Operation::SET);
                request.set_key(tokens[1].to_owned());
                request.set_value(tokens[2].to_owned());
                send_request(request, &mut stream, &mut buffer);
            }
            "remove" => {
                check_arguments!(tokens.len() - 1, 1);
                let mut request = commands::Request::new();
                request.set_operation(commands::Operation::REMOVE);
                request.set_key(tokens[1].to_owned());
                send_request(request, &mut stream, &mut buffer);
            }
            _ => {
                println!("[ERROR] Command not found.");
            }
        }
    }
    Ok(())
}

fn send_request(request: commands::Request, stream: &mut TcpStream, buffer: &mut [u8]) {
    match utils::send_message_to_stream(&request, stream) {
        Ok(mut bytes) => {
            // Re-use the write buffer for read.
            match utils::get_message_from_stream::<commands::Response>(stream, buffer, &mut bytes) {
                Ok(response) => {
                    print!("Status: {:?} ", response.get_status());
                    if response.has_value() {
                        print!("Value: {}", response.get_value());
                    }
                    if response.has_error() {
                        print!("Error: {:?}", response.get_error());
                    }
                    println!("");
                }
                Err(error) => {
                    println!("[ERROR] Failed to deserialize the response: {:?}.", error);
                }
            }
        }
        Err(error) => {
            println!("[ERROR] Failed to serialize the request: {:?}.", error);
        }
    };
}

fn print_help() {
    println!("This is an interactive session for querying the NaiveKV server.\n");
    println!("Supported commands:");
    println!("  get [KEY]            Get the value for a key.");
    println!("  set [KEY] [VALUE]    Set the value for a key.");
    println!("  remove [KEY]         Remove a key.");
    println!("  exit                 Exit the interactive session.");
    println!("  help                 Display this help info.");
}

fn print_greetings() {
    println!(
        r#" __        __   _                            _          _   _       _           _  ____     __  _
 \ \      / /__| | ___ ___  _ __ ___   ___  | |_ ___   | \ | | __ _(_)_   _____| |/ /\ \   / / | |
  \ \ /\ / / _ \ |/ __/ _ \| '_ ` _ \ / _ \ | __/ _ \  |  \| |/ _` | \ \ / / _ \ ' /  \ \ / /  | |
   \ V  V /  __/ | (_| (_) | | | | | |  __/ | || (_) | | |\  | (_| | |\ V /  __/ . \   \ V /   |_|
    \_/\_/ \___|_|\___\___/|_| |_| |_|\___|  \__\___/  |_| \_|\__,_|_| \_/ \___|_|\_\   \_/    (_)
                                                                                                  "#
    );
}
