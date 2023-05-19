#/bin/bash
apt-get install -y cmake g++

# install protoc
wget https://github.com/protocolbuffers/protobuf/releases/download/v21.10/protoc-21.10-linux-x86_64.zip
unzip protoc-21.10-linux-x86_64.zip -d .local
mv "$(pwd)/.local/bin/protoc" /bin/

# install grcov
wget https://github.com/mozilla/grcov/releases/download/v0.8.13/grcov-x86_64-unknown-linux-gnu.tar.bz2
tar -xjvf grcov-x86_64-unknown-linux-gnu.tar.bz2
mv grcov /bin/