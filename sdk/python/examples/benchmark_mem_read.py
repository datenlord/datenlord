import time
import io

def read_memory_data(data):
    try:
        # 使用 BytesIO 模拟文件操作
        with io.BytesIO(data) as f:
            start_time = time.time()
            content = f.read()  # 从内存中读取数据
            end_time = time.time()
            print(f"Read data from memory in {end_time - start_time:.6f} seconds")
            return content
    except Exception as e:
        print(f"Error reading data from memory: {e}")
        return None

def main():
    # 创建一个 256MB 的字节对象
    file_size_mb = 256
    file_size_bytes = file_size_mb * 1024 * 1024  # 转换为字节
    data = bytes(file_size_bytes)  # 创建一个全零的 256MB 数据

    read_latency = []

    # 从内存中读取数据 5 次
    for i in range(5):
        start_time = time.time()
        content = read_memory_data(data)
        if content:
            end_time = time.time()
            read_latency.append(end_time - start_time)
        else:
            print(f"Failed to read data from memory")

    sorted_read_latency = sorted(read_latency)
    avg_read_latency = sum(sorted_read_latency) / len(read_latency)
    print(
        f"Read {len(read_latency)} times, average time: {avg_read_latency:.6f} seconds, max: {max(read_latency):.6f} seconds, min: {min(read_latency):.6f} seconds, mid: {sorted_read_latency[len(sorted_read_latency) // 2]:.6f} seconds"
    )

if __name__ == "__main__":
    main()
