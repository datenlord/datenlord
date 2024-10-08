# DatenLordSDK Documentation

# PACKAGE CONTENTS
- `datenlordsdk`

# CLASSES

## builtins.object
- `builtins.Buffer`
- `builtins.DatenLordSDK`
- `builtins.Entry`
- `builtins.File`

---

## Class `Buffer(object)`
A bytes-like object that implements buffer protocol.
Reference to opendal lib.

### Static Methods Defined Here
- `__new__(*args, **kwargs)`: Create and return a new object. See `help(type)` for an accurate signature.

---

## Class `DatenLordSDK(object)`
`DatenLordSDK(config_path)`

`DatenLordSDK` is a Python class that provides an interface to interact with the DatenLord filesystem.

### Methods Defined Here
- `__aenter__(self, /)`: Support async context manager.
- `__aexit__(self, /, _exc_type, _exc_value, _traceback)`: Support async context manager.
- `close(self, /)`:
  - Close the SDK.
  - This function will shut down the SDK and release all resources.
  - Will block until all tasks are finished.
  - Note: Current pyo3 does not support `__del__` method, so this function should be called explicitly.
    [Reference](https://pyo3.rs/v0.22.3/class/protocols.html?highlight=__del#class-customizations)
- `exists(self, /, path)`: Check if a path exists.
- `listdir(self, /, path)`:
  - List directories and files in a directory.
  - Returns a list containing the names of the files in the directory.
- `mkdir(self, /, path)`: Create a directory.
- `mknod(self, /, path, mode=420)`:
  - Create a node in the file system.
  - The node can be a file.
  - The `mode` parameter is used to set the permissions of the node.
- `open(self, /, path, mode)`:
  - Open a file and return a `File` object.
  - `mode` is a string that represents the file open mode and can be:
    - "r": Read mode
    - "w": Write mode
    - "a": Append mode
    - "rw": Read/Write mode
- `read_file(self, /, path)`:
  - Read an entire file.
  - The `path` is the path to the file to be read.
- `rename(self, /, src, dst)`:
  - Rename a file or directory.
  - `src` is the path to the file or directory to be renamed.
  - `dst` is the new path for the file or directory.
- `rmdir(self, /, path, recursive=False)`:
  - Remove a directory.
  - Not recommended to remove all directories and files, support to remove one directory at a time.
- `stat(self, /, path)`:
  - Perform a stat system call on the given path.
  - `Path` to be examined; can be a string.
- `write_file(self, /, path, data)`:
  - Write an entire file.
  - The `path` is the path to the file to be written.

### Static Methods Defined Here
- `__new__(*args, **kwargs)`: Create and return a new object. See `help(type)` for an accurate signature.

---

## Class `Entry(object)`
### Methods Defined Here
- `__repr__(self, /)`: Return `repr(self)`.
- `__str__(self, /)`: Return `str(self)`.

### Static Methods Defined Here
- `__new__(*args, **kwargs)`: Create and return a new object. See `help(type)` for an accurate signature.

### Data Descriptors Defined Here
- `file_type`
- `ino`
- `name`

---

## Class `File(object)`
### Methods Defined Here
- `__aenter__(self, /)`: Support async context manager.
- `__aexit__(self, /, _exc_type, _exc_value, _traceback)`: Support async context manager.
- `close(self, /)`: Close the file.
- `read(self, /, size=None)`:
  - Read and return at most `size` bytes, or if `size` is not given, until EOF.
  - Read with `seek(offset)`.
- `seek(self, /, offset, whence=0)`:
  - Seek to the given offset in the file.
  - Options:
    - `0`: Start of the file, offset should be positive
    - `1`: Current file position
    - `2`: End of the file, offset can be negative
- `tell(self, /)`:
  - Tell the current file position, start from `0`.
- `write(self, /, data)`:
  - Write the given bytes-like object, return the number of bytes written.

### Static Methods Defined Here
- `__new__(*args, **kwargs)`: Create and return a new object. See `help(type)` for an accurate signature.

---

# DATA
`__all__ = ['DatenLordSDK', 'File', 'Buffer', 'Entry']`