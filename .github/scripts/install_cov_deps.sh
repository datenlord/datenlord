#/bin/bash
apt-get install -y cmake g++

# install protoc
wget https://github.com/protocolbuffers/protobuf/releases/download/v21.10/protoc-21.10-linux-x86_64.zip
unzip protoc-21.10-linux-x86_64.zip -d .local
mv "$(pwd)/.local/bin/protoc" /bin/

# install cargo-llvm-cov
cargo install cargo-llvm-cov