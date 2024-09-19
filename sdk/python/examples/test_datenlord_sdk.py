import os
import time
from datenlordsdk import DatenLordSDK

def handle_error(err):
    print(f"Error: {err}")

def main():
    # Initialize SDK
    sdk = DatenLordSDK("config.toml")
    print("SDK initialized successfully")

    # Check if directory exists
    test_dir = "datenlord_sdk"
    # Delete the directory recursively if it exists
    if sdk.exists(test_dir):
        sdk.deldir(test_dir, recursive=True)
        print("Directory deleted successfully")

    # Create directory
    try:
        sdk.mkdir(test_dir)
        print("Directory created successfully")
    except Exception as e:
        handle_error(e)

    # Check if directory exists
    if sdk.exists(test_dir):
        print("Directory exists")

    # Create a new file
    file_path = f"{test_dir}/test_file.txt"
    if sdk.exists(file_path):
        print("File already exists")
    else:
        try:
            sdk.create_file(file_path)
            print("File created successfully")
        except Exception as e:
            handle_error(e)

    # Write data to the file
    file_content = "Hello, Datenlord!"
    try:
        sdk.write_file(file_path, file_content.encode())
        print("File written successfully")
    except Exception as e:
        handle_error(e)

    # Read the file
    try:
        content = sdk.read_file(file_path)
        print(f"File read successfully: {content.decode()}")
    except Exception as e:
        handle_error(e)

    # Stat the file
    try:
        file_stat = sdk.stat(file_path)
        print(f"File stat: {file_stat}")
    except Exception as e:
        handle_error(e)

    # Rename the file
    rename_file_path = f"{test_dir}/test_file_rename.txt"
    try:
        sdk.rename_path(file_path, rename_file_path)
        print("File renamed successfully")
    except Exception as e:
        handle_error(e)

    # Copy file from local to SDK
    local_file_path = "/bin/cat"
    remote_file_path = "cat"
    try:
        sdk.copy_from_local_file(local_file_path, remote_file_path, overwrite=True)
        print("File copied from local to SDK successfully")
    except Exception as e:
        handle_error(e)

    # Copy file from SDK to local
    local_file_path_tmp = "/tmp/cat"
    try:
        sdk.copy_to_local_file(remote_file_path, local_file_path_tmp)
        print("File copied to local successfully")
    except Exception as e:
        handle_error(e)

    # Check if the copied file exists
    if sdk.exists(remote_file_path):
        print("Copied file exists")

    # Delete the directory recursively
    try:
        sdk.deldir(test_dir, recursive=True)
        print("Directory deleted successfully")
    except Exception as e:
        handle_error(e)

    # Check if directory exists
    if not sdk.exists(test_dir):
        print("Directory does not exist")

    print("SDK released successfully")

if __name__ == "__main__":
    main()
