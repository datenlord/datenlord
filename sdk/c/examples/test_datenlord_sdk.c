#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "datenlordsdk.h"

// Utils functions to free current error
void handle_error(datenlord_error *err) {
    if (err != NULL) {
        printf("Error code: %d, message: %.*s\n", err->code, (int)err->message.len, (const char*)err->message.data);
        free(err);
    }
}

int main() {
    // Init SDK
    datenlord_sdk* sdk = init("config.toml");
    if (sdk == NULL) {
        printf("Failed to initialize SDK\n");
        return 1;
    }
    printf("SDK initialized successfully\n");

    // Check if directory exists
    const char* test_dir = "datenlord_sdk";
    // Delete the directory recursively
    datenlord_error* err = deldir(sdk, test_dir, true);
    if (err == NULL) {
        printf("Directory deleted successfully\n");
    } else {
        handle_error(err);
    }

    bool dir_exists = exists(sdk, test_dir);
    printf("Directory exists: %d\n", dir_exists);

    // Mkdir test_sdk
    err = mkdir(sdk, test_dir);
    if (err == NULL) {
        printf("Directory created successfully\n");
    } else {
        handle_error(err);
    }

    // Check if directory exists
    dir_exists = exists(sdk, test_dir);
    printf("Directory exists: %d\n", dir_exists);

    // Create a new file
    const char* file_path = "datenlord_sdk/test_file.txt";
    // Check if directory exists
    dir_exists = exists(sdk, file_path);
    printf("Directory exists: %d\n", dir_exists);

    err = create_file(sdk, file_path);
    if (err == NULL) {
        printf("File created successfully\n");
    } else {
        handle_error(err);
    }

    // Check if file exists
    dir_exists = exists(sdk, file_path);
    printf("File exists: %d\n", dir_exists);

    // Stat the file
    datenlord_file_stat file_stat;
    err = stat(sdk, file_path, &file_stat);
    if (err == NULL) {
        printf("File stat: inode=%ld, size=%ld, blocks=%ld, perm=o%o, nlink=%d, uid=%d, gid=%d\n",
               file_stat.ino, file_stat.size, file_stat.blocks, file_stat.perm, file_stat.nlink,
               file_stat.uid, file_stat.gid);
    } else {
        handle_error(err);
    }

    // Write data to the file
    const char* file_content = "Hello, Datenlord!";
    datenlord_bytes content = { (const uint8_t*)file_content, strlen(file_content) };
    err = write_file(sdk, file_path, content);
    if (err == NULL) {
        printf("File written successfully\n");
    } else {
        handle_error(err);
    }

    // Read the file
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
    free(buffer);

    // Stat the file
    file_stat;
    err = stat(sdk, file_path, &file_stat);
    if (err == NULL) {
        printf("File stat: inode=%ld, size=%ld, blocks=%ld, perm=o%o, nlink=%d, uid=%d, gid=%d\n",
               file_stat.ino, file_stat.size, file_stat.blocks, file_stat.perm, file_stat.nlink,
               file_stat.uid, file_stat.gid);
    } else {
        handle_error(err);
    }

    // Rename the file
    const char* rename_file_path = "datenlord_sdk/test_file_rename.txt";
    err = rename_path(sdk, file_path, rename_file_path);
    if (err == NULL) {
        printf("File renamed successfully\n");
    } else {
        handle_error(err);
    }

    err = rename_path(sdk, rename_file_path, file_path);
    if (err == NULL) {
        printf("File renamed successfully\n");
    } else {
        handle_error(err);
    }

    // Copy file from local to SDK
    const char* local_file_path = "/bin/cat";
    const char* remote_file_path = "cat";
    err = copy_from_local_file(sdk, true, local_file_path, remote_file_path);
    if (err == NULL) {
        printf("File copied from local to SDK successfully\n");
    } else {
        handle_error(err);
    }

    // Copy file from SDK to local
    const char* local_file_path_tmp = "/tmp/cat";
    err = copy_to_local_file(sdk, remote_file_path, local_file_path_tmp);
    if (err == NULL) {
        printf("File copied to local successfully\n");
    } else {
        handle_error(err);
    }

    // Check if the copied file exists
    dir_exists = exists(sdk, remote_file_path);
    printf("Copied file exists: %d\n", dir_exists);

    // // Delete the directory recursively
    // err = deldir(sdk, test_dir, true);
    // if (err == NULL) {
    //     printf("Directory deleted successfully\n");
    // } else {
    //     handle_error(err);
    // }
    // // Check if directory exists
    // dir_exists = exists(sdk, test_dir);
    // printf("Directory exists: %d\n", dir_exists);

    // Release the SDK
    free_sdk(sdk);
    printf("SDK released successfully\n");

    return 0;
}
