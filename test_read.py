import threading
import time

# 创建文件并初始化内容
file_path = "/home/lvbo/tmpthread_test.txt"
with open(file_path, "w") as f:
    f.write("Initial content\n")

lock = threading.Lock()
condition = threading.Condition(lock)
turn = 1

# 线程1：写入线程
def write_to_file1():
    with open(file_path, "a") as f:
        for i in range(10):
            time.sleep(0.2)  # 模拟写入延迟
            with condition:  # 获取锁
                while turn != 1:
                    condition.wait()
                f.write(f"Thread1 writing line {i}\n")
                turn = 2
                condition.notify_all()

# 线程2：写入线程
def write_to_file2():
    with open(file_path, "a") as f:
        for i in range(10):
            time.sleep(0.1)  # 模拟写入延迟
            with condition:  # 获取锁
                while turn != 2:
                    condition.wait()
                f.write(f"Thread2 writing line {i}\n")
                turn = 1
                condition.notify_all()

# 读取线程
def read_from_file():
    time.sleep(5)  # 给写入线程一些时间先写入
    with open(file_path, "r") as f:
        content = f.read()
        print(content)

# 创建并启动线程
write_thread1 = threading.Thread(target=write_to_file1)
write_thread2 = threading.Thread(target=write_to_file2)
read_thread = threading.Thread(target=read_from_file)

write_thread1.start()
write_thread2.start()
read_thread.start()

# 等待所有线程执行完毕
write_thread1.join()
write_thread2.join()
read_thread.join()