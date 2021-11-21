use clap;
use naive_kv::protos::messages;
use naive_kv::types::Result;
use naive_kv::utils;
use std::io::{stdin, stdout, BufRead, Write};
use std::net::TcpStream;

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

    let stdin = stdin();
    let mut user_messages = stdin.lock().lines();
    print_greetings();
    print_help();

    let mut read_user_command = || {
        print!(">>> ");
        stdout().flush().unwrap();
        user_messages.next()
    };
    let mut request_id = 1; // Cannot start from 0, otherwise the response would not be serialized.
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
                        "Invalid Arguments: expect {} but got {}.",
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
                let mut request = messages::Request::new();
                request.set_id(request_id);
                request_id += 1;
                request.set_operation(messages::Operation::GET);
                request.set_key(tokens[1].to_owned());
                send_request(request, &mut stream);
            }
            "set" => {
                check_arguments!(tokens.len() - 1, 2);
                let mut request = messages::Request::new();
                request.set_id(request_id);
                request_id += 1;
                request.set_operation(messages::Operation::SET);
                request.set_key(tokens[1].to_owned());
                request.set_value(tokens[2].to_owned());
                send_request(request, &mut stream);
            }
            "remove" => {
                check_arguments!(tokens.len() - 1, 1);
                let mut request = messages::Request::new();
                request.set_id(request_id);
                request_id += 1;
                request.set_operation(messages::Operation::REMOVE);
                request.set_key(tokens[1].to_owned());
                send_request(request, &mut stream);
            }
            _ => {
                println!("Command not found.");
            }
        }
    }
    Ok(())
}

fn send_request(request: messages::Request, stream: &mut TcpStream) {
    match utils::write_message(&request, stream) {
        Ok(()) => match utils::read_message::<messages::Response, TcpStream>(stream) {
            Ok(response) => {
                let response = response.unwrap_or(messages::Response::new());
                if response.get_id() != request.get_id() {
                    println!(
                        "Invalid Response: expected id = {}, but got id = {}.",
                        request.get_id(),
                        response.get_id()
                    );
                }
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
                println!(
                    "Internal Error: failed to receive or deserialize the response: {:?}.",
                    error
                );
            }
        },
        Err(error) => {
            println!(
                "Internal Error: failed to serialize or send the request: {:?}.",
                error
            );
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
