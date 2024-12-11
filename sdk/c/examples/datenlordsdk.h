#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

/// DatenLord SDK core data structure
/// This structure is used to store the SDK instance, which is used to interact with the DatenLord SDK.
/// We need to use init_sdk to initialize the SDK and free_sdk to release the SDK manually.
struct datenlord_sdk {
  void *datenlordfs;
};

/// File attributes
/// This structure is used to store the file attributes, which are used to store the file metadata.
struct datenlord_file_stat {
  /// Inode number
  uint64_t ino;
  /// Size in bytes
  uint64_t size;
  /// Size in blocks
  uint64_t blocks;
  /// Permissions
  uint16_t perm;
  /// Number of hard links
  uint32_t nlink;
  /// User id
  uint32_t uid;
  /// Group id
  uint32_t gid;
  /// Rdev
  uint32_t rdev;
};

extern "C" {

/// Open a file return current fd
///
/// sdk: datenlord_sdk
/// pathname: file path
/// mode_t: file mode and permission bits
///
/// If the file is opened successfully, return the file descriptor
/// Otherwise, return -1.
long long dl_open(datenlord_sdk *sdk, const char *pathname, mode_t mode);

/// Close a opened file
///
/// sdk: datenlord_sdk
/// ino: file inode, which is returned by stat
/// fd: file descriptor, which is returned by dl_open
///
/// If the file is closed successfully, return 0
/// Otherwise, return -1.
long long dl_close(datenlord_sdk *sdk, unsigned long long ino, unsigned long long fd);

/// Write to a opened file
///
/// sdk: datenlord_sdk
/// ino: file inode, which is returned by stat
/// fd: file descriptor, which is returned by dl_open
/// buf: data to write
/// count: data size
///
/// If the file is written successfully, return 0
/// Otherwise, return -1.
long long dl_write(datenlord_sdk *sdk,
                   unsigned long long ino,
                   unsigned long long fd,
                   const uint8_t *buf,
                   unsigned long long count);

/// Read from a opened file
///
/// sdk: datenlord_sdk
/// ino: file inode, which is returned by stat
/// fd: file descriptor, which is returned by dl_open
/// buf: buffer to store read data
/// count: buffer size
///
/// If the file is read successfully, return the read size
/// Otherwise, return -1.
long long dl_read(datenlord_sdk *sdk,
                  unsigned long long ino,
                  unsigned long long fd,
                  uint8_t *buf,
                  unsigned long long count);

/// Initialize the DatenLord SDK by the given config file
///
/// config: path to the config file
datenlord_sdk *dl_init_sdk(const char *config);

/// Free the SDK instance
///
/// sdk: datenlord_sdk instance
void dl_free_sdk(datenlord_sdk *sdk);

/// Check if the given path exists
///
/// sdk: datenlord_sdk instance
/// dir_path: path to the directory
///
/// Return: true if the path exists, otherwise false
bool dl_exists(datenlord_sdk *sdk, const char *dir_path);

/// Create a directory
///
/// sdk: datenlord_sdk instance
/// dir_path: path to the directory
///
/// If the directory is created successfully, return the inode number, otherwise -1
long long dl_mkdir(datenlord_sdk *sdk, const char *dir_path);

/// Remove a directory
///
/// sdk: datenlord_sdk instance
/// dir_path: path to the directory
/// recursive: whether to remove the directory recursively, current not used
///
/// If the directory is removed successfully, return 0, otherwise -1
long long dl_rmdir(datenlord_sdk *sdk, const char *dir_path, bool recursive);

/// Remove a file
///
/// sdk: datenlord_sdk instance
/// file_path: path to the file
///
/// If the file is removed successfully, return 0, otherwise -1
long long dl_remove(datenlord_sdk *sdk, const char *file_path);

/// Rename a file
///
/// sdk: datenlord_sdk instance
/// src_path: source file path
/// dest_path: destination file path
///
/// If the file is renamed successfully, return 0, otherwise -1
long long dl_rename(datenlord_sdk *sdk, const char *src_path, const char *dest_path);

/// Create a file
///
/// sdk: datenlord_sdk instance
/// file_path: path to the file
///
/// If the file is created successfully, return the inode number, otherwise -1
long long dl_mknod(datenlord_sdk *sdk, const char *file_path);

/// Get the file attributes
///
/// sdk: datenlord_sdk instance
/// file_path: path to the file
/// file_metadata: datenlord_file_stat instance
///
/// If the file attributes are retrieved successfully, return 0, otherwise -1
long long dl_stat(datenlord_sdk *sdk, const char *file_path, datenlord_file_stat *file_metadata);

/// Write data to a file
///
/// sdk: datenlord_sdk instance
/// file_path: path to the file
/// buf: buffer to store the file content
/// count: the size of the buffer
///
/// If the file is written successfully, return the number of bytes written, otherwise -1
long long dl_write_file(datenlord_sdk *sdk,
                        const char *file_path,
                        const uint8_t *buf,
                        unsigned long long count);

/// Read a hole file
///
/// sdk: datenlord_sdk instance
/// file_path: path to the file
/// buf: buffer to store the file content
/// count: the size of the buffer
///
/// If the file is read successfully, return the number of bytes read, otherwise -1
long long dl_read_file(datenlord_sdk *sdk,
                       const char *file_path,
                       const uint8_t *buf,
                       unsigned long long count);

} // extern "C"
