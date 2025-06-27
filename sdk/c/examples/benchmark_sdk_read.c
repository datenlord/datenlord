#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
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

    if (!dl_exists(sdk, file_path)) {
        printf("File is not exists\n");
        free(data);
        dl_free_sdk(sdk);
        return 1;
    }

    unsigned long long fd = dl_open(sdk, file_path, O_RDWR);
    if (fd == 0) {
        printf("Failed to open file\n");
        free(data);
        dl_free_sdk(sdk);
        return 1;
    }
    printf("File opened successfully with file descriptor: %llu\n", fd);

    datenlord_file_stat file_stat;
    long long res = dl_stat(sdk, file_path, &file_stat);
    if (res < 0) {
        printf("Failed to get file stat information\n");
        dl_close(sdk, 0);
        free(data);
        dl_free_sdk(sdk);
        return 1;
    }
    printf("File stat retrieved successfully, inode number: %lu\n", file_stat.ino);

    struct timespec start_time, end_time;

    double read_latency[5];

    data = (uint8_t *)malloc(FILE_SIZE);
    if (!data) {
        printf("Failed to allocate memory for read content\n");
        dl_close(sdk, file_stat.ino);
        free(data);
        dl_free_sdk(sdk);
        return 1;
    }

    for (int i = 0; i < 5; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);

        res = dl_read(sdk, file_stat.ino, data, FILE_SIZE);

        clock_gettime(CLOCK_MONOTONIC, &end_time);
        if (res < 0) {
            printf("Failed to read from file\n");
            dl_close(sdk, file_stat.ino);
            free(data);
            dl_free_sdk(sdk);
            return 1;
        }

        read_latency[i] = get_time_diff(start_time, end_time);
    }

    double total_read_latency = 0;
    // Igonre the first read operation for preheat
    for (int i = 1; i < 5; i++) {
        total_read_latency += read_latency[i];
    }
    double avg_read_latency = total_read_latency / 4;
    printf("Read 5 times, average time: %.6f seconds\n", avg_read_latency);

    clock_gettime(CLOCK_MONOTONIC, &start_time);
    res = dl_close(sdk, file_stat.ino);
    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (res < 0) {
        printf("Failed to close file\n");
        free(data);
        // free((void *)read_content.data);
        dl_free_sdk(sdk);
        return 1;
    }

    double close_latency = get_time_diff(start_time, end_time);
    printf("File close operation time: %.6f seconds\n", close_latency);

    free(data);
    // free((void *)read_content.data);
    dl_free_sdk(sdk);

    return 0;
}
