#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "datenlordsdk.h"

#define FILE_SIZE_MB 100
#define FILE_SIZE (FILE_SIZE_MB * 1024 * 1024)

double get_time_diff(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
}

int main() {
    datenlord_sdk *sdk = dl_init_sdk("config.toml");
    if (!sdk) {
        printf("Failed to initialize SDK\n");
        return 1;
    }
    printf("SDK initialized successfully\n");

    uint8_t *data = (uint8_t *)malloc(FILE_SIZE);
    if (!data) {
        printf("Failed to allocate memory\n");
        dl_free_sdk(sdk);
        return 1;
    }
    memset(data, 'A', FILE_SIZE);

    const char *file_path = "benchmark_test_file.bin";

    if (dl_exists(sdk, file_path)) {
        printf("File already exists\n");
        long long res = dl_remove(sdk, file_path);
        if (res < 0) {
            printf("Failed to remove file\n");
            free(data);
            dl_free_sdk(sdk);
            return 1;
        }
        printf("File removed successfully\n");
    }

    long long res = dl_mknod(sdk, file_path);
    if (res < 0) {
        printf("Failed to create file\n");
        free(data);
        dl_free_sdk(sdk);
        return 1;
    }
    printf("File created successfully\n");

    unsigned long long fd = dl_open(sdk, file_path, O_RDWR);
    if (fd == 0) {
        printf("Failed to open file\n");
        free(data);
        dl_free_sdk(sdk);
        return 1;
    }
    printf("File opened successfully with file descriptor: %llu\n", fd);

    datenlord_file_stat file_stat;
    res = dl_stat(sdk, file_path, &file_stat);
    if (res < 0) {
        printf("Failed to get file stat information\n");
        dl_close(sdk, 0, fd);
        free(data);
        dl_free_sdk(sdk);
        return 1;
    }
    printf("File stat retrieved successfully, inode number: %lu\n", file_stat.ino);

    struct timespec start_time, end_time;
    double write_latency[5];

    for (int i = 0; i < 5; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);

        res = dl_write(sdk, file_stat.ino, fd, data, FILE_SIZE);

        clock_gettime(CLOCK_MONOTONIC, &end_time);
        if (res < 0) {
            printf("Failed to write to file\n");
            dl_close(sdk, file_stat.ino, fd);
            free(data);
            dl_free_sdk(sdk);
            return 1;
        }

        write_latency[i] = get_time_diff(start_time, end_time);
    }

    clock_gettime(CLOCK_MONOTONIC, &start_time);
    res = dl_close(sdk, file_stat.ino, fd);
    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (res < 0) {
        printf("Failed to close file\n");
        free(data);
        dl_free_sdk(sdk);
        return 1;
    }

    double total_write_latency = 0;
    for (int i = 0; i < 5; i++) {
        total_write_latency += write_latency[i];
    }
    double avg_write_latency = total_write_latency / 5;

    printf("Write 5 times, average time: %.6f seconds\n", avg_write_latency);

    double close_latency = get_time_diff(start_time, end_time);
    printf("File close operation time: %.6f seconds\n", close_latency);

    free(data);
    dl_free_sdk(sdk);

    return 0;
}
