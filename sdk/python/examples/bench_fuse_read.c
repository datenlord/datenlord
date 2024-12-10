#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// #define FILE_PATH "/home/lvbo/tmp/256MB_file.bin" // 要读取的文件路径
#define FILE_PATH "/home/lvbo/data/datenlord_cache/256mb_file_0.bin" // 要读取的文件路径
#define READ_COUNT 5 // 读取次数

void read_file(const char *file_path, double *elapsed_time) {
    FILE *file = fopen(file_path, "rb"); // 以二进制模式打开文件
    if (!file) {
        perror("Error opening file");
        return;
    }

    // 获取文件大小
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    fseek(file, 0, SEEK_SET);

    // 分配内存以读取文件
    char *buffer = (char *)malloc(file_size);
    if (!buffer) {
        perror("Memory allocation failed");
        fclose(file);
        return;
    }

    // 读取文件并计时
    clock_t start_time = clock();
    size_t bytes_read = fread(buffer, 1, file_size, file);
    clock_t end_time = clock();

    if (bytes_read != file_size) {
        perror("Error reading file");
        free(buffer);
        fclose(file);
        return;
    }

    *elapsed_time = (double)(end_time - start_time) / CLOCKS_PER_SEC; // 计算读取时间
    printf("Read %s in %.6f seconds\n", file_path, *elapsed_time);

    // 清理资源
    free(buffer);
    fclose(file);
}

int compare(const void *a, const void *b) {
    return (*(double *)a - *(double *)b); // 比较函数用于 qsort
}

int main() {
    double read_latencies[READ_COUNT] = {0}; // 存储每次读取的时间

    for (int i = 0; i < READ_COUNT; i++) {
        read_file(FILE_PATH, &read_latencies[i]); // 读取文件并记录时间
    }

    // 计算平均时间、最大时间、最小时间和中位数
    double sum = 0.0, min_time = read_latencies[0], max_time = read_latencies[0];

    for (int i = 0; i < READ_COUNT; i++) {
        sum += read_latencies[i];
        if (read_latencies[i] < min_time) {
            min_time = read_latencies[i]; // 计算最小时间
        }
        if (read_latencies[i] > max_time) {
            max_time = read_latencies[i]; // 计算最大时间
        }
    }

    double avg_time = sum / READ_COUNT; // 计算平均时间

    // 排序以便计算中位数
    qsort(read_latencies, READ_COUNT, sizeof(double), compare);
    double median_time = (READ_COUNT % 2 == 0)
                        ? (read_latencies[READ_COUNT / 2 - 1] + read_latencies[READ_COUNT / 2]) / 2
                        : read_latencies[READ_COUNT / 2];

    // 输出结果
    printf("Read %d files:\n", READ_COUNT);
    printf("Average time: %.6f seconds\n", avg_time);
    printf("Max time: %.6f seconds\n", max_time);
    printf("Min time: %.6f seconds\n", min_time);
    printf("Median time: %.6f seconds\n", median_time);

    return 0;
}
