#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "datenlordsdk.h"


// Utils functions to free current error
void handle_error(datenlord_error *err) {
    if (err != NULL) {
        printf("Error code: %d, message: %.*s\n", err->code, (int)err->message.len, (const char*)err->message.data);
        free(err);
    }
}

int main() {
    // Init sdk
    datenlord_sdk* sdk = init("example_config");
    if (sdk == NULL) {
        printf("Failed to initialize SDK\n");
        return 1;
    }
    printf("SDK initialized successfully\n");

    // Check current dir is available
    bool dir_exists = exists(sdk, "/datenlord_sdk");
    printf("Directory exists: %d\n", dir_exists);

    // Mkdir /example_dir
    datenlord_error* err = mkdir(sdk, "example_dir/");
    if (err == NULL) {
        printf("Directory created successfully\n");
    } else {
        handle_error(err);
    }

    // Create file
    err = create_file(sdk, "/example_dir/example_file.txt");
    if (err == NULL) {
        printf("File created successfully\n");
    } else {
        handle_error(err);
    }

    // Write file
    const char* file_path = "/example_dir/example_file.txt";
    const char* file_content = "Hello, Datenlord!";
    datenlord_bytes content = { (const uint8_t*)file_content, strlen(file_content) };
    err = write_file(sdk, file_path, content);
    if (err == NULL) {
        printf("File written successfully\n");
    } else {
        handle_error(err);
    }

    // Read file
    size_t buffer_size = 1024;
    uint8_t *buffer = (uint8_t *)malloc(buffer_size);
    if (buffer == NULL) {
        printf("Failed to allocate buffer\n");
        return 1;
    }
    datenlord_bytes out_content = { buffer, buffer_size };
    err = read_file(sdk, file_path, &out_content);
    if (err == NULL) {
        printf("File read successfully: %.*s\n", (int)out_content.len, (const char*)out_content.data);
    } else {
        handle_error(err);
    }

    // Stat file
    datenlord_file_stat file_stat;
    err = stat(sdk, "/example_dir/renamed_file.txt", &file_stat);
    if (err == NULL) {
        printf("File stat: %ld %d %d %d %d %d %d %d %d %d %d\n", file_stat.blocks, file_stat.gid, file_stat.ino, file_stat.nlink, file_stat.perm, file_stat.rdev, file_stat.size, file_stat.uid);
    }

    // Rename file
    err = rename_path(sdk, "/example_dir/example_file.txt", "/example_dir/renamed_file.txt");
    if (err == NULL) {
        printf("File renamed successfully\n");
    } else {
        handle_error(err);
    }

    // Delete dir
    err = deldir(sdk, "/example_dir", 1);
    if (err == NULL) {
        printf("Directory deleted successfully\n");
    } else {
        handle_error(err);
    }

    // Release sdk
    free_sdk(sdk);
    printf("SDK released successfully\n");

    return 0;
}
