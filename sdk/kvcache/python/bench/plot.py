import matplotlib.pyplot as plt

# Data for write benchmark
write_data_sizes = [524288, 1048576, 2097152, 4194304, 8388608, 16777216]  # in bytes
write_throughputs = [0.133028, 0.162465, 0.159861, 0.170111, 0.177012, 0.172859]  # in GB/s

# Data for read benchmark
read_data_sizes = [524288, 1048576, 2097152, 4194304, 8388608, 16777216]  # in bytes
read_throughputs = [0.034788, 0.069951, 0.077923, 0.253875, 0.498208, 0.874864]  # in GB/s

# Convert data sizes to MB for better readability
data_sizes_mb = [size / (1024 * 1024) for size in write_data_sizes]

# Plot the data
plt.figure(figsize=(10, 6))
plt.plot(data_sizes_mb, write_throughputs, marker='o', label="Write Throughput")
plt.plot(data_sizes_mb, read_throughputs, marker='s', label="Read Throughput")

# Chart formatting
plt.title("block_size: 16777216 Read/Write Performance")
plt.xlabel("Data Size (MB)")
plt.ylabel("Throughput (GB/s)")
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend()
plt.tight_layout()

# Show the plot
# plt.show()
plt.savefig("block_size_16777216_read_write_performance.png")
