#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

struct datenlord_sdk {
  void *datenlordfs;
};

struct datenlord_bytes {
  const uint8_t *data;
  uintptr_t len;
};

/// File attributes
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
/// mode_t is a type that represents file mode and permission bits
unsigned long long dl_open(datenlord_sdk *sdk, const char *file_path, mode_t mode);

/// Close a opened file
long long dl_close(datenlord_sdk *sdk, unsigned long long ino, unsigned long long fd);

/// Write to a opened file
long long dl_write(datenlord_sdk *sdk,
                   unsigned long long ino,
                   unsigned long long fd,
                   datenlord_bytes content);

/// Read from a opened file
long long dl_read(datenlord_sdk *sdk,
                  unsigned long long ino,
                  unsigned long long fd,
                  datenlord_bytes *out_content,
                  unsigned int size);

datenlord_sdk *dl_init_sdk(const char *config);

void dl_free_sdk(datenlord_sdk *sdk);

bool dl_exists(datenlord_sdk *sdk, const char *dir_path);

long long dl_mkdir(datenlord_sdk *sdk, const char *dir_path);

long long dl_rmdir(datenlord_sdk *sdk, const char *dir_path, bool recursive);

long long dl_remove(datenlord_sdk *sdk, const char *file_path);

long long dl_rename(datenlord_sdk *sdk, const char *src_path, const char *dest_path);

long long dl_mknod(datenlord_sdk *sdk, const char *file_path);

long long dl_stat(datenlord_sdk *sdk, const char *file_path, datenlord_file_stat *file_metadata);

long long dl_write_file(datenlord_sdk *sdk, const char *file_path, datenlord_bytes content);

long long dl_read_file(datenlord_sdk *sdk, const char *file_path, datenlord_bytes *out_content);

} // extern "C"
