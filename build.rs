extern crate protoc_rust;

use protoc_rust::Codegen;

fn main() {
    Codegen::new()
        .out_dir("src/protos")
        .inputs(&["src/protos/messages.proto"])
        .include("src/protos")
        .run()
        .expect("Failed to run protoc_rust.");
}
