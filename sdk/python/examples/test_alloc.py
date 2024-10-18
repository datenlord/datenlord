import time

def test_alloc_buffer(size_in_mb):
    size_in_bytes = size_in_mb * 1024 * 1024

    start_time = time.time()

    buffer = bytearray(size_in_bytes)  # 或者使用 bytes(size_in_bytes)

    end_time = time.time()

    # print(f"分配 {size_in_mb} MB buffer 的时间: {end_time - start_time:.6f} 秒")
    print(f"allocated {size_in_mb} MB buffer with time: {end_time - start_time:.6f} seconds type: {type(buffer)}")

if __name__ == "__main__":
    for i in range(1, 6):
        test_alloc_buffer(256)
