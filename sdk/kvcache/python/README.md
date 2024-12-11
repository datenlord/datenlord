# DatenLord Distributed KVCache Python SDK Documentation

The `DatenLord Distributed KVCache Python SDK` allows developers to interact with a distributed KVCache system implemented in Rust. This document outlines the usage, installation, and API references for the SDK.

---

## Overview

The Python SDK provides an interface to interact with the distributed key-value cache (`KVCache`) implemented in the DatenLord project. The SDK is powered by a Rust backend, leveraging its robust concurrency and performance features.

---

## Installation

### Prerequisites
- Python: `>=3.11, <3.13`
- Rust: Installed for building the SDK if not using a prebuilt binary.

### Install from PyPI (if published):
```bash
pip install datenlordsdk
```

### Build and Install from Source:
1. Clone the repository:
   ```bash
   git clone https://github.com/datenlord/datenlord.git
   cd datenlord
   ```
2. Build the Rust SDK:
   ```bash
   maturin build
   ```
3. Install the Python SDK:
   ```bash
   pip install target/wheels/datenlordsdk-*.whl
   ```

---

## Usage

### Initialize the SDK

```python
from datenlordsdk import DatenLordSDK

# Initialize the SDK with a configuration file
sdk = DatenLordSDK(config_path="/path/to/config/file")

# Use the SDK in an asynchronous context manager
async with sdk as client:
    # Perform operations using the client
    pass

# Explicitly close the SDK (optional when using context manager)
sdk.close()
```

### Example Operations

#### Load a KV Cache Block
```python
matched_prefix, buffer = await sdk.try_load(prefix="some/key/prefix")
if matched_prefix:
    print(f"Matched Prefix: {matched_prefix}, Data: {buffer}")
else:
    print("Key not found in KV cache.")
```

#### Insert a KV Cache Block
```python
from datenlordsdk import Buffer

key = "my/key"
data = Buffer(b"My cache data")
await sdk.insert(key, data)
```

---

## API Reference

### Class: `DatenLordSDK`

The `DatenLordSDK` class provides methods to interact with the distributed KVCache.

#### Methods:

1. **`__init__(config_path: str)`**
   - Initialize the SDK with a configuration file.
   - **Parameters:**
     - `config_path`: Path to the configuration file.

2. **`try_load(prefix: str) -> Tuple[str, bytes]`**
   - Tries to load a block from the distributed KVCache.
   - **Parameters:**
     - `prefix`: The prefix of the key to search for.
   - **Returns:**
     - A tuple `(matched_prefix, buffer)` if the block is found; otherwise, returns `None`.

3. **`insert(key: str, data: Buffer) -> None`**
   - Inserts a block into the distributed KVCache.
   - **Parameters:**
     - `key`: The key to associate with the block.
     - `data`: The value (as `Buffer`) to store in the block.

4. **`close() -> None`**
   - Explicitly close the SDK, releasing all resources.

5. **`__aenter__()`**
   - Supports asynchronous context management for initializing the SDK.

6. **`__aexit__()`**
   - Ensures proper cleanup when exiting the context.

---

## Dependencies

- **Runtime Library:** The Rust-based `libdatenlordsdk.so` dynamic library.
- **Python Dependencies:** Managed by `pip` during installation.

---

## Development

### Building the Rust Dynamic Library
To use the SDK, ensure the dynamic library (`libdatenlordsdk.so`) is built:
```bash
cargo build --release
```

Add the library path to your environment:
```bash
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/path/to/target/release
```

### Running the Python Example
```bash
python examples/test_datenlord_sdk.py
```

#### Dependencies

- Python version: `>=3.11, < 3.13`
- Runtime dependency: DatenLord Rust dynamic library (`libdatenlordsdk.so`)

### Contributing
If you encounter any issues or have suggestions for improvements, please submit an issue or pull request to help enhance the SDK.