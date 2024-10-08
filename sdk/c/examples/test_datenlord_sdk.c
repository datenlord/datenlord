#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "datenlordsdk.h"

#define FILE_SIZE_MB 20
#define FILE_SIZE (FILE_SIZE_MB * 1024 * 1024)
#define READ_BUFFER_SIZE FILE_SIZE

double get_time_diff(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
}

void print_file_stat(const datenlord_file_stat *stat) {
    printf("File Attributes:\n");
    printf("Inode: %lu\n", stat->ino);
    printf("Size: %lu bytes\n", stat->size);
    printf("Blocks: %lu\n", stat->blocks);
    printf("Permissions: %u\n", stat->perm);
    printf("Number of Hard Links: %u\n", stat->nlink);
    printf("User ID: %u\n", stat->uid);
    printf("Group ID: %u\n", stat->gid);
    printf("Rdev: %u\n", stat->rdev);
}

int main() {
    // printf("size %ld \n", sizeof(int));
    datenlord_sdk *sdk = dl_init_sdk("config.toml");
    if (!sdk) {
        printf("Failed to initialize SDK\n");
        return 1;
    }
    printf("SDK initialized successfully\n");

    const char *file_path = "test_file.bin";
    const char *dir_path = "test_directory";
    const char *new_dir_path = "renamed_directory";
    const char *new_file_path = "renamed_file.bin";

    printf("\n=== File Operation Tests ===\n");

    if (dl_exists(sdk, file_path)) {
        printf("File %s already exists, removing it...\n", file_path);
        dl_remove(sdk, file_path);
    }

    if (dl_mknod(sdk, file_path) < 0) {
        printf("Failed to create file");
        dl_free_sdk(sdk);
        return 1;
    }
    printf("File %s created successfully\n", file_path);

    unsigned long long fd = dl_open(sdk, file_path, O_RDWR);
    if (fd == 0) {
        printf("Failed to open file");
        dl_free_sdk(sdk);
        return 1;
    }
    printf("File %s opened successfully with descriptor %llu\n", file_path, fd);

    datenlord_file_stat file_stat;
    if (dl_stat(sdk, file_path, &file_stat) < 0) {
        printf("Failed to get file stat");
        dl_close(sdk, 0, fd);
        dl_free_sdk(sdk);
        return 1;
    }
    print_file_stat(&file_stat);

    uint8_t *data = (uint8_t *)malloc(FILE_SIZE);
    if (!data) {
        printf("Failed to allocate memory");
        dl_close(sdk, file_stat.ino, fd);
        dl_free_sdk(sdk);
        return 1;
    }
    memset(data, 'A', FILE_SIZE);

    struct timespec start_time, end_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    if (dl_write(sdk, file_stat.ino, fd, data, FILE_SIZE) < 0) {
        printf("Failed to write to file");
        dl_close(sdk, file_stat.ino, fd);
        free(data);
        dl_free_sdk(sdk);
        return 1;
    }
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    double write_time = get_time_diff(start_time, end_time);
    printf("Write operation completed in %.6f seconds\n", write_time);

    data = (uint8_t *)malloc(READ_BUFFER_SIZE);

    clock_gettime(CLOCK_MONOTONIC, &start_time);
    if (dl_read(sdk, file_stat.ino, fd, data, READ_BUFFER_SIZE) < 0) {
        printf("Failed to read file");
        dl_close(sdk, file_stat.ino, fd);
        // free(read_content.data);
        free(data);
        dl_free_sdk(sdk);
        return 1;
    }
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    double read_time = get_time_diff(start_time, end_time);
    printf("Read operation completed in %.6f seconds\n", read_time);

    clock_gettime(CLOCK_MONOTONIC, &start_time);
    if (dl_close(sdk, file_stat.ino, fd) < 0) {
        printf("Failed to close file");
        // free(read_content.data);
        free(data);
        dl_free_sdk(sdk);
        return 1;
    }
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    double close_time = get_time_diff(start_time, end_time);
    printf("File close operation completed in %.6f seconds\n", close_time);

    free(data);
    // free(read_content.data);

    if (dl_rename(sdk, file_path, new_file_path) < 0) {
        printf("Failed to rename file");
    } else {
        printf("File %s renamed to %s successfully\n", file_path, new_file_path);
    }

    if (dl_remove(sdk, new_file_path) < 0) {
        printf("Failed to remove file");
    } else {
        printf("File %s removed successfully\n", new_file_path);
    }

    printf("\n=== Directory Operation Tests ===\n");

    if (dl_exists(sdk, dir_path)) {
        printf("Directory %s already exists, removing it...\n", dir_path);
        dl_rmdir(sdk, dir_path, true);
    }

    if (dl_mkdir(sdk, dir_path) < 0) {
        printf("Failed to create directory");
    } else {
        printf("Directory %s created successfully\n", dir_path);
    }

    if (dl_rename(sdk, dir_path, new_dir_path) < 0) {
        printf("Failed to rename directory");
    } else {
        printf("Directory %s renamed to %s successfully\n", dir_path, new_dir_path);
    }

    if (dl_rmdir(sdk, new_dir_path, true) < 0) {
        printf("Failed to remove directory");
    } else {
        printf("Directory %s removed successfully\n", new_dir_path);
    }

    dl_free_sdk(sdk);
    printf("SDK resources freed successfully\n");

    return 0;
}
