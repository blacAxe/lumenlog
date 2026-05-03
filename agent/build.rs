fn main() {
    prost_build::compile_protos(
        &["../proto/log.proto"],
        &["../proto"],
    ).unwrap();
}