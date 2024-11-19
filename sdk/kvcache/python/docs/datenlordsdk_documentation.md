# DatenLordSDK Package Documentation

## Package: `datenlordsdk`

### Contents
- **Module**: `datenlordsdk`
- **Classes**:
  - [`Buffer`](#class-buffer)
  - [`DatenLordSDK`](#class-datenlordsdk)

---

## Classes

### Class: `Buffer`

A bytes-like object that implements the buffer protocol. This class is a reference to the `opendal` library.

#### Methods:
- **`get_len(self)`**:
  Returns the length of the buffer.

#### Static Methods:
- **`__new__(*args, **kwargs)`**:
  Creates and returns a new object. For details, refer to the [Python `type` documentation](https://docs.python.org/3/library/functions.html#type).

---

### Class: `DatenLordSDK`

`DatenLordSDK(config_path)` is a Python class that provides an interface to interact with the DatenLord filesystem.

#### Methods:

- **`__aenter__(self)`**:
  Supports asynchronous context management.

- **`__aexit__(self, _exc_type, _exc_value, _traceback)`**:
  Ensures proper cleanup when exiting the context manager.

- **`close(self)`**:
  Shuts down the SDK and releases all resources. This method blocks until all tasks are finished.
  **Note**: Due to `pyo3` limitations, this method should be called explicitly.
  Reference: [pyo3 customization](https://pyo3.rs/v0.22.3/class/protocols.html?highlight=__del#class-customizations).

- **`insert(self, key, data)`**:
  Inserts a block into the distributed KV cache.
  - **Parameters**:
    - `key`: The key of the block to be inserted.
    - `data`: The value of the block to be inserted.

- **`try_load(self, prefix)`**:
  Attempts to load a KV cache block.
  - **Returns**:
    - A tuple `(matched_prefix, buffer)` if the block is found.
    - `None` otherwise.
  - **Details**:
    - `matched_prefix`: The longest prefix of the key that matches the block.
    - `buffer`: The value of the block.

#### Static Methods:
- **`__new__(*args, **kwargs)`**:
  Creates and returns a new object. Refer to the [Python `type` documentation](https://docs.python.org/3/library/functions.html#type).

---

## Data
- **`__all__`**:
  A list of public API components: `['DatenLordSDK', 'Buffer']`.

---

## File Location
This package is located at:
`/home/lvbo/miniconda3/envs/py311/lib/python3.11/site-packages/datenlordsdk/__init__.py`