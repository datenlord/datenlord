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

    # Delete directory if exists
    if is_exists:
        print(f"{test_dir} directory exists")
        try:
            await sdk.deldir(test_dir, recursive=True)
            print(f"{test_dir} directory deleted successfully")
        except Exception as e:
            handle_error(e)

    # Create directory
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

    file_base = "20mb_file11111"
    write_latency = []
    read_latency = []

    # Create and write 5 files
    for i in range(5):
        file_path = f"{test_dir}/{file_base}_{i}.bin"
        file_content = "a" * 100 * 1024 * 1024  # 20 MB
        start_time = time.time()
        try:
            await sdk.create_file(file_path)
            fd = sdk.open(file_path, "rw")
            await fd.write(file_content.encode())
            end_time = time.time()
            write_latency.append(end_time - start_time)

            for i in range(5):
                read_start_time = time.time()
                try:
                    content = await fd.read()
                    read_end_time = time.time()
                    read_latency.append(read_end_time - read_start_time)
                    # print(f"{file_path} read successfully, size: {len(content)} bytes")
                except Exception as e:
                    handle_error(e)

            sorted_read_latency = sorted(read_latency)
            avg_read_latency = sum(sorted_read_latency) / len(read_latency)
            print(
                f"Read {len(read_latency)} files, average time: {avg_read_latency:.6f} seconds, "
                f"max: {max(read_latency):.6f} seconds, min: {min(read_latency):.6f} seconds, "
                f"mid: {sorted_read_latency[len(sorted_read_latency) // 2]:.6f} seconds"
            )
            read_latency = []

            fd.close()
            # print(f"{file_path} created and written successfully")
        except Exception as e:
            handle_error(e)

    # Sort and calculate write latency stats
    sorted_write_latency = sorted(write_latency)
    avg_write_latency = sum(sorted_write_latency) / len(sorted_write_latency)
    print(
        f"Write {len(sorted_write_latency)} files, average time: {avg_write_latency:.6f} seconds, "
        f"max: {max(write_latency):.6f} seconds, min: {min(write_latency):.6f} seconds, "
        f"mid: {sorted_write_latency[len(sorted_write_latency) // 2]:.6f} seconds"
    )

    # Read the first file 5 times and calculate latency
    read_latency = []
    file_path = f"{test_dir}/{file_base}_0.bin"
    fd = sdk.open(file_path, "rw")
    for i in range(5):
        start_time = time.time()
        try:
            fd.seek(0)
            content = await fd.read()
            end_time = time.time()
            read_latency.append(end_time - start_time)
            # print(f"{file_path} read successfully, size: {len(content)} bytes")
        except Exception as e:
            handle_error(e)
    fd.close()

    # Sort and calculate read latency stats
    sorted_read_latency = sorted(read_latency)
    avg_read_latency = sum(sorted_read_latency) / len(read_latency)
    print(
        f"Read {len(read_latency)} files, average time: {avg_read_latency:.6f} seconds, "
        f"max: {max(read_latency):.6f} seconds, min: {min(read_latency):.6f} seconds, "
        f"mid: {sorted_read_latency[len(sorted_read_latency) // 2]:.6f} seconds"
    )

    # Sort and calculate read latency stats
    sorted_read_latency = sorted(read_latency)
    avg_read_latency = sum(sorted_read_latency) / len(read_latency)
    print(
        f"Read {len(read_latency)} files, average time: {avg_read_latency:.6f} seconds, "
        f"max: {max(read_latency):.6f} seconds, min: {min(read_latency):.6f} seconds, "
        f"mid: {sorted_read_latency[len(sorted_read_latency) // 2]:.6f} seconds"
    )

    # # Delete all files
    # for i in range(5):
    #     file_path = f"{test_dir}/{file_base}_{i}.bin"
    #     try:
    #         await sdk.deldir(test_dir, recursive=True)
    #         print(f"{file_path} deleted successfully")
    #     except Exception as e:
    #         handle_error(e)

    # Close SDK
    try:
        is_closed = await sdk.close()
        print(f"SDK closed: {is_closed}")
    except Exception as e:
        handle_error(e)


if __name__ == "__main__":
    asyncio.run(main())
