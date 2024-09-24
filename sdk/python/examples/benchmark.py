import sys
import asyncio
import time

from datenlordsdk import DatenLordSDK

# clear the command line arguments
sys.argv = [sys.argv[0]]

def handle_error(err):
    print(f"Error: {err}")

async def main():
    sdk = DatenLordSDK("config.toml")
    print("SDK initialized successfully")

    # Check if directory exists
    test_dir = "datenlord_sdk_111"
    is_exists = await sdk.exists(test_dir)
    print(f"{test_dir} directory exists: {is_exists}")

    if is_exists:
        print(f"{test_dir} directory exists")
        try:
            await sdk.deldir(test_dir, recursive=True)
            print(f"{test_dir} directory deleted successfully")
        except Exception as e:
            handle_error(e)

    try:
        res = await sdk.mkdir(test_dir)
        print(f"{test_dir} directory created successfully", res)
        is_exists = await sdk.exists(test_dir)
        if is_exists:
            print(f"{test_dir} directory exists")
        else:
            print(f"{test_dir} directory does not exist")
    except Exception as e:
        handle_error(e)

    # Create a new file
    file_path = f"{test_dir}/test_file.txt"
    try:
        await sdk.create_file(file_path)
        print(f"{file_path} file created successfully")
    except Exception as e:
        handle_error(e)

    # Write data to the file
    # file_content = "Hello, Datenlord!"
    # Write 20m
    file_content = "a" * 20 * 1024 * 1024
    try:
        await sdk.write_file(file_path, file_content.encode())
        print(f"{file_path} file written content {len(file_content)} successfully")
    except Exception as e:
        handle_error(e)

    # Read the file
    try:
        content = await sdk.read_file(file_path)
        # Convert to string
        print(f"{file_path} file read successfully, content: {len(content)}")
    except Exception as e:
        handle_error(e)

    # Read with 10 times and calculate the average time
    start_time = time.time()
    for i in range(10):
        try:
            content = await sdk.read_file(file_path)
            # print(f"{file_path} file read successfully, content: {bytes(content)}")
        except Exception as e:
            handle_error(e)
    end_time = time.time()

    print(f"Read file 10 times, average time: {(end_time - start_time) / 10}")

    try:
        is_closed = await sdk.close()
        print(f"SDK closed: {is_closed}")
    except Exception as e:
        handle_error(e)

if __name__ == "__main__":
    asyncio.run(main())
