# DatenLordSDK Distribute KVCache Python Package README

## Overview
The `DatenLordSDK` is a Python class that provides an interface to interact with the DatenLord filesystem.

## Classes and Methods

### (一) `Buffer` Class
- **Function**: A bytes-like object that implements the buffer protocol and is a reference to the `opendal` library.
- **Methods**:
  - `get_len(self, /)`: Gets the length of the buffer.

### (二) `DatenLordSDK` Class
1. **Initialization Method**
   - `DatenLordSDK(block_size, kv_engine_address, log_level)`: Creates an instance of `DatenLordSDK`. The parameter `block_size` represents the block size, `kv_engine_address` represents the key-value engine address, and `log_level` represents the log level.
2. **Instance Methods**
   - `__aenter__(self)`: Supports the async context manager.
   - `__aexit__(self, _exc_type, _exc_value, _traceback)`: Supports the async context manager.
   - `close(self)`: Closes the SDK and releases all resources. It will block until all tasks are completed. Since the current `pyo3` does not support the `__del__` method, this function should be called explicitly.
   - `insert(self, key, data)`: Inserts a block into the distributed key-value cache. The `key` is the key of the block to be inserted, and the `data` is the value of the block to be inserted.
   - `try_load(self, prefix)`: Tries to load a key-value cache block. If found, it returns `(matched_prefix, buffer)`; otherwise, it returns `None`. Here, `matched_prefix` is the longest prefix of the key that matches the block, and `buffer` is the value of the block.

## Data
- `__all__ = ['DatenLordSDK', 'Buffer']`: Defines the classes that can be exported in the module.

## Notes
1. When using the `DatenLordSDK` class, pay attention to calling the `close` method at the appropriate time to release resources.
2. Due to the limitations of `pyo3`, the `__del__` method cannot be used to automatically manage resource release for now, and it needs to be handled manually.