import sys
import asyncio
import time

from pympler import asizeof

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

    # Delete directory if exists
    if not is_exists:
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

    # file_base = "20mb_file"
    # file_base = "1gb_file"
    file_base = "256mb_file"
    # file_base = "100mb_file"
    write_latency = []
    read_latency = []

    start_read_job_time = time.time()

    # Read the first file 5 times and calculate latency
    read_latency = []
    file_path = f"{test_dir}/{file_base}_0.bin"
    fd = sdk.open(file_path, "rw")
    for i in range(5):
        start_time = time.time()
        try:
            fd.seek(0)
            # content = await fd.read()
            content = fd.read()
            print(f"Read {file_path} file, type: {type(content)}, length: {content.get_len()}")
            # content = fd.alloc(1024 * 1024 * 256)
            end_time = time.time()
            print(f"Read {file_path} file, time: {end_time - start_time:.6f} seconds, type: {type(content)}")
            read_latency.append(end_time - start_time)
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

    close_read_job_time = time.time()

    # Read with close
    read_close_latency = []
    for i in range(5):
        start_time = time.time()
        try:
            fd = sdk.open(file_path, "rw")
            content = fd.read()
            fd.close()
            end_time = time.time()
            read_close_latency.append(end_time - start_time)
        except Exception as e:
            handle_error(e)

    # Sort and calculate read latency stats
    sorted_read_close_latency = sorted(read_close_latency)
    avg_read_close_latency = sum(sorted_read_close_latency) / len(read_close_latency)
    print(
        f"Read with close {len(read_close_latency)} files, average time: {avg_read_close_latency:.6f} seconds, "
        f"max: {max(read_close_latency):.6f} seconds, min: {min(read_close_latency):.6f} seconds, "
        f"mid: {sorted_read_close_latency[len(sorted_read_close_latency) // 2]:.6f} seconds"
    )

    close_read_job_close_time = time.time()

    # Close SDK
    try:
        is_closed = await sdk.close()
        print(f"SDK closed: {is_closed}")
    except Exception as e:
        handle_error(e)

    sdk_close_time = time.time()
    print(f"SDK close time: {sdk_close_time - close_read_job_close_time:.6f} seconds")
    print(f"Read job time(10 files): {close_read_job_close_time - start_read_job_time:.6f} seconds")
    print(f"Read job close time(10 files): {close_read_job_close_time - close_read_job_time:.6f} seconds")
    print(f"Read job open time(10 files): {close_read_job_time - start_read_job_time:.6f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
