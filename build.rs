use std::env;
use std::ffi::{OsStr, OsString};
use std::io;
use std::process::{Command, ExitStatus};

const REQUIRED_BUILD_TOOLS: &[(&str, &str)] =
    &[("cmake", "cmake"), ("protoc", "protobuf-compiler")];

fn main() {
    println!("cargo:rerun-if-env-changed=CXX");
    println!("cargo:rerun-if-changed=src/csi/proto/csi.proto");
    println!("cargo:rerun-if-changed=src/csi/proto/datenlord_worker.proto");

    check_build_tools();

    protoc_grpcio::compile_grpc_protos(
        &[
            "./src/csi/proto/csi.proto",
            "./src/csi/proto/datenlord_worker.proto",
        ], // inputs
        &["./src/csi/proto"], // includes
        "src/csi/proto",      // output
        None,                 // customizations
    )
    .unwrap_or_else(|e| panic!("Failed to compile gRPC definitions, the error is: {}", e));
}

fn check_build_tools() {
    let mut unavailable_tools = REQUIRED_BUILD_TOOLS
        .iter()
        .filter_map(|(tool, package)| check_tool(tool, package))
        .collect::<Vec<_>>();

    let cxx = env::var_os("CXX").unwrap_or_else(|| OsString::from("g++"));
    if let Some(error) = check_tool(&cxx, "g++") {
        unavailable_tools.push(error);
    }

    if !unavailable_tools.is_empty() {
        panic!(
            "DatenLord build requires cmake, protoc and a C++ compiler.\n\
             Missing or unusable tools:\n  - {}\n\
             On Debian or Ubuntu, install them with:\n  \
             sudo apt install cmake g++ libprotobuf-dev protobuf-compiler",
            unavailable_tools.join("\n  - "),
        );
    }
}

fn check_tool(tool: impl AsRef<OsStr>, package: &str) -> Option<String> {
    let tool = tool.as_ref();
    match Command::new(tool).arg("--version").status() {
        Ok(status) if status.success() => None,
        Ok(status) => Some(format_unusable_tool(tool, package, status)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Some(format!(
            "`{}` was not found; install package `{package}`",
            tool.to_string_lossy(),
        )),
        Err(e) => Some(format!(
            "failed to run `{}`; install package `{package}` or fix the command: {e}",
            tool.to_string_lossy(),
        )),
    }
}

fn format_unusable_tool(tool: &OsStr, package: &str, status: ExitStatus) -> String {
    format!(
        "`{} --version` exited with {status}; install package `{package}` or fix the command",
        tool.to_string_lossy(),
    )
}
