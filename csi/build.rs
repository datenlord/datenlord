fn main() {
    // grpcio depends on cmake, g++ and protoc,
    // run the following command to install:
    // `sudo apt install cmake g++ libprotobuf-dev protobuf-compiler`
    protoc_grpcio::compile_grpc_protos(
        &["csi.proto", "datenlord_worker.proto"], // inputs
        &["."],                                   // includes
        "src",                                    // output
        None,                                     // customizations
    )
    .unwrap_or_else(|e| panic!("Failed to compile gRPC definitions, the error is: {}", e));
}
