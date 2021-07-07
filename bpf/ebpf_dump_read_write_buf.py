#!/usr/bin/python3

from __future__ import print_function
from bcc import BPF

# define BPF program
bpf_text = """
BPF_HASH(read_buf_size_map, u64, size_t);
BPF_HASH(read_buf_map, u64, char *);

BPF_HASH(write_buf_size_map, u64, size_t);
BPF_HASH(write_buf_map, u64, const char *);

TRACEPOINT_PROBE(syscalls, sys_enter_read) {
    u64 tpid = bpf_get_current_pid_tgid();

    size_t count = args->count;
    char *buf = args->buf;

    read_buf_size_map.update(&tpid, &count);
    read_buf_map.update(&tpid, &buf);

    return 0;
}

TRACEPOINT_PROBE(syscalls, sys_exit_read) {
    u64 tpid = bpf_get_current_pid_tgid();

    size_t *size_res = read_buf_size_map.lookup(&tpid);
    if (size_res != NULL) {
        size_t buf_size = *size_res;
        read_buf_size_map.delete(&tpid);

        char **buf_res = read_buf_map.lookup(&tpid);
        if (buf_res != NULL) {
            char* buf = *buf_res;
            bpf_trace_printk("read size: %d, buf size: %d, buf content: %s\\n", args->ret, buf_size, buf);
            read_buf_map.delete(&tpid);
        }
    }
    return 0;
}

TRACEPOINT_PROBE(syscalls, sys_enter_write) {
    u64 tpid = bpf_get_current_pid_tgid();
    const char *buf = args->buf;
    size_t count = args->count;

    write_buf_size_map.update(&tpid, &count);
    write_buf_map.update(&tpid, &buf);

    return 0;
}


TRACEPOINT_PROBE(syscalls, sys_exit_write) {
    u64 tpid = bpf_get_current_pid_tgid();

    size_t *size_res = write_buf_size_map.lookup(&tpid);
    if (size_res != NULL) {
        size_t buf_size = *size_res;
        read_buf_size_map.delete(&tpid);

        const char **buf_res = write_buf_map.lookup(&tpid);
        if (buf_res != NULL) {
            const char* buf = *buf_res;
            bpf_trace_printk("write size: %d, buf size: %d, buf content: %s\\n", args->ret, buf_size, buf);
            read_buf_map.delete(&tpid);
        }
    }

    return 0;
}
"""

# load BPF program
b = BPF(text=bpf_text, debug=0x08)

# header
print("%-18s %-16s %-6s %s" % ("TIME(s)", "COMM", "PID", "MSG"))

# format output
while 1:
    try:
        (task, pid, cpu, flags, ts, msg) = b.trace_fields()
    except ValueError:
        continue
    print("%-18.9f %-16s %-6d %s" % (ts, task, pid, msg))
