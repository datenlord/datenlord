# DatenLord C SDK

Current datenlord implementation is based on the fuse filesystem and support basic POSIX api for file operations, but it will introduce some overheads for the data transfer.
This demo is to show how to use the datenlord sdk to implement a user space client for datenlord, and serve datenlord as daemon process, current demo support c and python language.

### c language demo

Use `cargo build --release` to get dynamic library `libdatenlordsdk.so` in `target/release/`.

Go to `examples` to run the c demo.
```bash
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:../target/release
g++ -o main test_datenlord_sdk.c -L../target/release -ldatenlordsdk -ldl
./main
```