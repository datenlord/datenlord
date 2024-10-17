import time
import os
import asyncio
import aiofiles


async def read_file(file_path):
    try:
        async with aiofiles.open(file_path, "rb") as f:
            # start_time = time.time()
            data = await f.read()
            # end_time = time.time()
            # print(f"Read {file_path} in {end_time - start_time:.6f} seconds")
            return data
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None


async def write_file(file_path):
    async with aiofiles.open(file_path, "wb") as f:
        await f.write(b"a" * 256 * 1024 * 1024)


async def main():
    # dir_path = '/home/lvbo/data/local_cache'
    dir_path = "/home/lvbo/data/datenlord_cache"
    # file_base = "20mb_file"
    # file_base = "100mb_file"
    file_base = "256mb_file"

    read_latency = []
    file_path = os.path.join(dir_path, f"{file_base}_0.bin")

    for i in range(10):
        start_time = time.time()
        content = await read_file(file_path)
        if content:
            end_time = time.time()
            read_latency.append(end_time - start_time)
        else:
            print(f"Failed to read {file_path}")

    sorted_read_latency = sorted(read_latency)
    avg_read_latency = sum(sorted_read_latency) / len(read_latency)
    print(
        f"Read {len(read_latency)} files, average time: {avg_read_latency:.6f} seconds, max: {max(read_latency):.6f} seconds, min: {min(read_latency):.6f} seconds, mid: {sorted_read_latency[len(sorted_read_latency)//2]:.6f} seconds"
    )

if __name__ == "__main__":
    asyncio.run(main())
