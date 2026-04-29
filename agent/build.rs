fn main() {
    // This tells Rust: "Look one folder up, find the proto, and turn it into Rust code"
    prost_build::compile_protos(&["../proto/log_event.proto"], &["../proto/"]).unwrap();
}