# DatenLord Python SDK

The current implementation of DatenLord is based on the FUSE filesystem and supports basic POSIX API for file operations. While this approach may introduce some overhead for data transfer, it still provides an efficient user-space filesystem interface.

This example demonstrates how to use the DatenLord SDK to implement a user-space client and serve DatenLord as a daemon process. The current examples support both C and Python languages.

### Python Example

#### Installation
Before using the Python SDK, make sure you have installed the `datenlordsdk` package. You can install it from PyPI (if published) or build and install it locally using `maturin`.

1. Install from PyPI (assuming the package is published):
   ```bash
   pip install datenlordsdk
   ```

2. Build and install from source:
   First, build the SDK with `maturin`:
   ```bash
   maturin build
   ```
   Then, install the generated `.whl` file using `pip`:
   ```bash
   pip install path/to/your/datenlordsdk.whl
   ```

#### Building and Running Python Example
Make sure you have correctly built the SDK dynamic library and added its path to your environment variables.

1. Build the SDK dynamic library (`libdatenlordsdk.so`) using:
   ```bash
   cargo build --release
   ```

2. Before running the Python example, ensure that the built library path is added to the `LD_LIBRARY_PATH` environment variable:
   ```bash
   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:../target/release
   ```

3. Run the Python example script:
   ```bash
   python test_datenlord_sdk.py
   ```

#### Dependencies

- Python version: `>=3.11, < 3.13`
- Runtime dependency: DatenLord Rust dynamic library (`libdatenlordsdk.so`)

### Contributing
If you encounter any issues or have suggestions for improvements, please submit an issue or pull request to help enhance the SDK.