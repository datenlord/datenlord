import time
import os


def read_file(file_path):
    try:
        with open(file_path, "rb", buffering=-1) as f:
            start_time = time.time()
            data = f.read()
            end_time = time.time()
            print(f"Read {file_path} in {end_time - start_time:.6f} seconds")
            return data
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None


def main():
    # file_path = '/tmp/20mb_file.bin'
    # dir_path = "/home/lvbo/data/datenlord_cache"
    dir_path = "/home/lvbo/tmp"
    file_base = "256MB_file"
    # file_base = "32mb_file"
    # file_base = "256mb_file"

    read_latency = []
    file_path = os.path.join(dir_path, f"{file_base}.bin")
    # Read the file with 5 times
    for i in range(5):
        start_time = time.time()
        content = read_file(file_path)
        if content:
            # print(f"{file_path} file read successfully, size: {len(content)} bytes")
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
    main()
