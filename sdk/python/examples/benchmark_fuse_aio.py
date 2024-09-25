import time
import os
import asyncio
import aiofiles


async def read_file(file_path):
    try:
        async with aiofiles.open(file_path, "rb") as f:
            return await f.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None


async def write_file(file_path):
    async with aiofiles.open(file_path, "wb") as f:
        await f.write(b"a" * 20 * 1024 * 1024)


async def main():
    # dir_path = '/home/lvbo/data/local_cache'
    dir_path = "/home/lvbo/data/datenlord_cache"
    file_base = "20mb_file111"
    write_latency = []

    for i in range(5):
        start_time = time.time()
        file_path = os.path.join(dir_path, f"{file_base}_{i}.bin")
        if not os.path.exists(file_path):
            await write_file(file_path)
        end_time = time.time()
        write_latency.append(end_time - start_time)

    sorted_write_latency = sorted(write_latency)
    avg_write_latency = sum(sorted_write_latency) / len(sorted_write_latency)
    print(
        f"Write {len(sorted_write_latency)} files, average time: {avg_write_latency:.6f} seconds, max: {max(write_latency):.6f} seconds, min: {min(write_latency):.6f} seconds, mid: {sorted_write_latency[len(sorted_write_latency)//2]:.6f} seconds"
    )

    read_latency = []
    file_path = os.path.join(dir_path, f"{file_base}_0.bin")

    for i in range(5):
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

    for i in range(5):
        file_path = os.path.join(dir_path, f"{file_base}_{i}.bin")
        if os.path.exists(file_path):
            os.remove(file_path)


if __name__ == "__main__":
    asyncio.run(main())
