import asyncio
import time
from datenlordsdk import DatenLordSDK

async def benchmark_read():
    # Configurations
    # block_size = 1024 # 1KB
    # block_size = 1024 * 1024 # 1MB
    block_size = 1024 * 1024 * 16  # 16MB

    # data_sizes = [256, 512, 1024]
    # data_sizes = [1024, 2*1024, 4*1024, 8*1024, 16*1024, 32*1024, 64*1024, 128*1024, 256*1024, 512*1024, 1024*1024]
    data_sizes = [512 * 1024, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024]
    num_operations = 200

    # Initialize the SDK
    sdk = DatenLordSDK(
        block_size=block_size,
        kv_engine_address=["127.0.0.1:2379"],
        log_level="error"
    )
    print("SDK initialized successfully")

    for data_size in data_sizes:
        print(f"\nTesting with block_size={block_size}, data_size={data_size}...")

        # # Initialize data to write before reading
        # data_to_write = bytes(data_size)
        keys = [f"key_bs{block_size}_ds{data_size}_id{i}" for i in range(num_operations)]
        # for key in keys:
        #     await sdk.insert(key, data_to_write)

        # Start the benchmark
        start_time = time.time()

        for key in keys:
            await sdk.try_load(key)
            # assert len(data) == data_size, f"Data size mismatch for key {key}: expected {data_size}, got {len(data)}"

        # Calculate elapsed time and throughput
        elapsed_time = time.time() - start_time
        avg_time = elapsed_time / num_operations
        throughput = (data_size * num_operations) / (elapsed_time * 1024**3)  # GB/s

        print(f"Average Read Time: {avg_time:.6f} seconds")
        print(f"Throughput: {throughput:.6f} GB/s")

if __name__ == "__main__":
    asyncio.run(benchmark_read())
