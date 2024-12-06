import sys
import asyncio
from datenlordsdk import DatenLordSDK

# clear the command line arguments
sys.argv = [sys.argv[0]]


def handle_error(err):
    print(f"Error: {err}")


async def main():
    sdk = DatenLordSDK("config.toml")
    print("SDK initialized successfully")

    # Check if directory exists
    test_dir = "datenlord_cache"
    is_exists = await sdk.exists(test_dir)
    print(f"{test_dir} directory exists: {is_exists}")

    if not is_exists:
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

    # Check if directory exists
    test_dir_subdir = f"{test_dir}/subdir"
    is_exists = await sdk.exists(test_dir_subdir)
    print(f"{test_dir_subdir} directory exists: {is_exists}")

    if is_exists:
        print(f"{test_dir_subdir} directory exists")
        try:
            await sdk.rmdir(test_dir_subdir)
            print(f"{test_dir_subdir} directory deleted successfully")
        except Exception as e:
            handle_error(e)

    # Create a new directory
    try:
        await sdk.mkdir(test_dir_subdir)
        print(f"{test_dir}/subdir directory created successfully")
    except Exception as e:
        handle_error(e)

    # Check if file exists
    test_dir_file = f"{test_dir}/test_file.txt"
    is_exists = await sdk.exists(test_dir_file)
    print(f"{test_dir_file} file exists: {is_exists}")

    if is_exists:
        print(f"{test_dir_file} file exists")
        try:
            await sdk.remove(test_dir_file)
            print(f"{test_dir_file} file deleted successfully")
        except Exception as e:
            handle_error(e)

    # Create a new file
    file_path = f"{test_dir}/test_file.txt"
    try:
        await sdk.mknod(file_path)
        print(f"{file_path} file created successfully")
    except Exception as e:
        handle_error(e)

    # Check if file exists
    file_path = f"{test_dir}/test_file2.txt"
    is_exists = await sdk.exists(file_path)
    print(f"{file_path} file exists: {is_exists}")

    if is_exists:
        print(f"{file_path} file exists")
        try:
            await sdk.remove(file_path)
            print(f"{file_path} file deleted successfully")
        except Exception as e:
            handle_error(e)

    # Create a new file
    try:
        await sdk.mknod(file_path)
        print(f"{file_path} file created successfully")
    except Exception as e:
        handle_error(e)

    # Write data to the file
    file_content = "Hello, Datenlord!"
    try:
        await sdk.write_file(file_path, file_content.encode())
        print(f"{file_path} file written content {file_content} successfully")
    except Exception as e:
        handle_error(e)

    # Read the file
    try:
        content = await sdk.read_file(file_path)
        # Convert to string
        print(f"{file_path} file read successfully, content: {bytes(content)}")
    except Exception as e:
        handle_error(e)

    # Read the file with file handle
    async with sdk.open(file_path, "rw") as f:
        try:
            await f.seek(0)
            content = await f.read()
            print(
                f"{file_path} file async read successfully, content: {bytes(content)}"
            )

            pos = await f.tell()
            print(f"Position: {pos}")

            await f.seek(0)
            pos = await f.tell()
            print(f"Position: {pos}")
        except Exception as e:
            handle_error(e)

    # Stat the file
    try:
        file_stat = await sdk.stat(file_path)
        print(f"{file_path} file stat: {file_stat}")
    except Exception as e:
        handle_error(e)

    # Rename the file
    rename_file_path = f"{test_dir}/test_file_rename.txt"
    try:
        await sdk.rename(file_path, rename_file_path)
        print(f"{file_path} file renamed successfully to {rename_file_path}")
    except Exception as e:
        handle_error(e)

    # Stat the file
    try:
        file_stat = await sdk.stat(rename_file_path)
        print(f"{rename_file_path} file stat: {file_stat}")
    except Exception as e:
        handle_error(e)

    # Read dir info
    try:
        dir_info = await sdk.listdir(test_dir)
        print(f"{test_dir} directory info: {dir_info}")
        for item in dir_info:
            print(f"Item name: {item.name} ino: {item.ino} type: {item.file_type}")
    except Exception as e:
        handle_error(e)

    try:
        is_closed = await sdk.close()
        print(f"SDK closed: {is_closed}")
    except Exception as e:
        handle_error(e)


if __name__ == "__main__":
    asyncio.run(main())
