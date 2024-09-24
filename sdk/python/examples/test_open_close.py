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
    file_content = "Hello, Datenlord!!!"
    fd = sdk.open(file_path, "w")
    await fd.write(file_content.encode())
    print(f"{file_path} file written content {file_content} successfully")
    fd.close()

    # Read the file
    fd = sdk.open(file_path, "r")
    content = await fd.read()
    print(f"{file_path} file read successfully, content: {bytes(content)}")
    fd.close()

    # Stat the file
    try:
        file_stat = await sdk.stat(file_path)
        print(f"{file_path} file stat: {file_stat}")
    except Exception as e:
        handle_error(e)

    # Rename the file
    rename_file_path = f"{test_dir}/test_file_rename.txt"
    try:
        await sdk.rename_path(file_path, rename_file_path)
        print(f"{file_path} file renamed successfully to {rename_file_path}")
    except Exception as e:
        handle_error(e)

    # Stat the file
    try:
        file_stat = await sdk.stat(rename_file_path)
        print(f"{rename_file_path} file stat: {file_stat}")
    except Exception as e:
        handle_error(e)

    try:
        is_closed = await sdk.close()
        print(f"SDK closed: {is_closed}")
    except Exception as e:
        handle_error(e)

if __name__ == "__main__":
    asyncio.run(main())
