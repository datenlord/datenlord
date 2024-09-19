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

struct datenlord_error {
  unsigned int code;
  datenlord_bytes message;
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

datenlord_sdk *init(const char *config);

void free_sdk(datenlord_sdk *sdk);

bool exists(datenlord_sdk *sdk, const char *dir_path);

datenlord_error *mkdir(datenlord_sdk *sdk, const char *dir_path);

datenlord_error *deldir(datenlord_sdk *sdk, const char *dir_path, bool recursive);

datenlord_error *rename_path(datenlord_sdk *sdk, const char *src_path, const char *dest_path);

datenlord_error *copy_from_local_file(datenlord_sdk *sdk,
                                      bool overwrite,
                                      const char *local_file_path,
                                      const char *dest_file_path);

datenlord_error *copy_to_local_file(datenlord_sdk *sdk,
                                    const char *src_file_path,
                                    const char *local_file_path);

datenlord_error *create_file(datenlord_sdk *sdk, const char *file_path);

datenlord_error *stat(datenlord_sdk *sdk,
                      const char *file_path,
                      datenlord_file_stat *file_metadata);

datenlord_error *write_file(datenlord_sdk *sdk, const char *file_path, datenlord_bytes content);

datenlord_error *read_file(datenlord_sdk *sdk, const char *file_path, datenlord_bytes *out_content);

} // extern "C"
