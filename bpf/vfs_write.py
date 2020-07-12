#!/usr/bin/python3

from __future__ import print_function
from bcc import BPF
import time

# define BPF program
bpf_text = """
#include <uapi/linux/ptrace.h>
#include <linux/fs.h>
#include <linux/sched.h>
enum trace_mode {
    MODE_READ = 0,
    MODE_WRITE,
};
struct val_t {
    u32 sz;
    u32 padding;
    u64 pos;
    u64 ts;
    char *buffer;
    u32 name_len;
    u32 parent_name_len;
    // de->d_name.name may point to de->d_iname so limit len accordingly
    char name[DNAME_INLINE_LEN];
    char parent_name[DNAME_INLINE_LEN];
    char comm[TASK_COMM_LEN];
};
struct data_t {
    u64 mode;
    u32 pid;
    u32 sz;
    u64 pos;
    u64 delta_us;
    u32 name_len;
    u32 parent_name_len;
    char name[DNAME_INLINE_LEN];
    char parent_name[DNAME_INLINE_LEN];
    char comm[TASK_COMM_LEN];
};
const size_t MAX_BUF_SIZE = 128 * 1024; // 128KB
struct event_data_t {
    u64 mode;
    u32 pid;
    u32 tgid;
    u32 sz;
    u32 padding;
    u64 pos;
    u64 delta_us;
    u32 name_len;
    u32 parent_name_len;
    char name[DNAME_INLINE_LEN];
    char parent_name[DNAME_INLINE_LEN];
    char comm[TASK_COMM_LEN];
    char buffer[MAX_BUF_SIZE];
};
const size_t RING_PAGE_COUNT = 2048 * MAX_BUF_SIZE / PAGE_SIZE;
BPF_RINGBUF_OUTPUT(event_ring, RING_PAGE_COUNT);
BPF_HASH(entryinfo, u64, struct val_t);
BPF_PERF_OUTPUT(events);
// store timestamp and size on entry
static int trace_rw_entry(struct pt_regs *ctx, struct file *file,
    char __user *buf, size_t count, loff_t *pos)
{
    u64 ptid = bpf_get_current_pid_tgid();
    u32 tgid = ptid >> 32;
    //if (TGID_FILTER)
    //    return 0;
    u32 pid = ptid;
    // skip I/O lacking a filename
    struct dentry *de = file->f_path.dentry;
    struct dentry *parent_de = de->d_parent;
    int mode = file->f_inode->i_mode;
    //if (de->d_name.len == 0 || TYPE_FILTER)
    if (de->d_name.len == 0 || !S_ISREG(mode))
        return 0;
    // store size and timestamp by pid
    struct val_t val = {};
    val.sz = count;
    val.padding = 0;
    val.pos = *pos;
    val.ts = bpf_ktime_get_ns();
    val.buffer = buf;
    struct qstr d_name = de->d_name;
    val.name_len = d_name.len;
    struct qstr parent_d_name = parent_de->d_name;
    val.parent_name_len = parent_d_name.len;
    //bpf_probe_read_kernel(&val.name, sizeof(val.name), d_name.name);
    bpf_probe_read(&val.name, sizeof(val.name), d_name.name);
    bpf_probe_read(&val.parent_name, sizeof(val.parent_name), parent_d_name.name);
    bpf_get_current_comm(&val.comm, sizeof(val.comm));
    entryinfo.update(&ptid, &val);

    return 0;
}
int trace_read_entry(struct pt_regs *ctx, struct file *file,
    char __user *buf, size_t count, loff_t *pos)
{
    // skip non-sync I/O; see kernel code for __vfs_read()
    if (!(file->f_op->read_iter))
        return 0;
    return trace_rw_entry(ctx, file, buf, count, pos);
}
int trace_write_entry(struct pt_regs *ctx, struct file *file,
    char __user *buf, size_t count, loff_t *pos)
{
    // skip non-sync I/O; see kernel code for __vfs_write()
    if (!(file->f_op->write_iter))
        return 0;
    return trace_rw_entry(ctx, file, buf, count, pos);
}
// output
static int trace_rw_return(struct pt_regs *ctx, int type)
{
    struct val_t *valp;
    u64 ptid = bpf_get_current_pid_tgid();
    u32 tgid = ptid >> 32;
    u32 pid = ptid;
    valp = entryinfo.lookup(&ptid);
    if (valp == NULL) {
        // missed tracing issue or filtered
        //bpf_trace_printk("failed to find ptid=%d", ptid);
        return 0;
    }
    u64 delta_us = (bpf_ktime_get_ns() - valp->ts) / 1000;
    entryinfo.delete(&ptid);

    struct event_data_t *e_data = event_ring.ringbuf_reserve(sizeof(struct event_data_t));
    if (e_data == NULL) { // Failed to reserve space
        bpf_trace_printk("failed to reserve ring buffer space");
        return 1;
    }
    e_data->mode = type;
    e_data->pid = pid;
    e_data->tgid = tgid;
    e_data->sz = valp->sz;
    e_data->padding = 0;
    e_data->pos = valp->pos;
    e_data->delta_us = delta_us;
    e_data->name_len = valp->name_len;
    e_data->parent_name_len = valp->parent_name_len;
    bpf_probe_read_kernel(e_data->name, sizeof(e_data->name), valp->name);
    bpf_probe_read_kernel(e_data->parent_name, sizeof(e_data->parent_name), valp->parent_name);
    bpf_probe_read_kernel(e_data->comm, sizeof(e_data->comm), valp->comm);
    bpf_probe_read_user(e_data->buffer, sizeof(e_data->buffer), valp->buffer);
    event_ring.ringbuf_submit(e_data, BPF_RB_FORCE_WAKEUP);

    //bpf_trace_printk("comm=%s, name=%s/%s", e_data->comm, e_data->parent_name, e_data->name);
    if (valp->sz > MAX_BUF_SIZE) {
        bpf_trace_printk("large buffer size=%d", valp->sz);
    }

    //if (delta_us < MIN_US)
    //    return 0;
    struct data_t data = {};
    data.mode = type;
    data.pid = pid;
    data.sz = valp->sz;
    data.pos = valp->pos;
    data.delta_us = delta_us;
    data.name_len = valp->name_len;
    data.parent_name_len = valp->parent_name_len;
    bpf_probe_read_kernel(&data.name, sizeof(data.name), valp->name);
    bpf_probe_read_kernel(&data.parent_name, sizeof(data.parent_name), valp->parent_name);
    bpf_probe_read_kernel(&data.comm, sizeof(data.comm), valp->comm);
    events.perf_submit(ctx, &data, sizeof(data));

    return 0;
}
int trace_read_return(struct pt_regs *ctx)
{
    return trace_rw_return(ctx, MODE_READ);
}
int trace_write_return(struct pt_regs *ctx)
{
    return trace_rw_return(ctx, MODE_WRITE);
}
"""
#bpf_text = bpf_text.replace('MIN_US', str(min_ms * 1000))
#if args.tgid:
#    bpf_text = bpf_text.replace('TGID_FILTER', 'tgid != %d' % tgid)
#else:
#    bpf_text = bpf_text.replace('TGID_FILTER', '0')
#if args.all_files:
#    bpf_text = bpf_text.replace('TYPE_FILTER', '0')
#else:
#    bpf_text = bpf_text.replace('TYPE_FILTER', '!S_ISREG(mode)')

# initialize BPF
#b = BPF(text=bpf_text, debug=0x08)
b = BPF(text=bpf_text)

# I'd rather trace these via new_sync_read/new_sync_write (which used to be
# do_sync_read/do_sync_write), but those became static. So trace these from
# the parent functions, at the cost of more overhead, instead.
# Ultimately, we should be using [V]FS tracepoints.
#try:
#    b.attach_kprobe(event="__vfs_read", fn_name="trace_read_entry")
#    b.attach_kretprobe(event="__vfs_read", fn_name="trace_read_return")
#except Exception:
#    print('Current kernel does not have __vfs_read, try vfs_read instead')
#    b.attach_kprobe(event="vfs_read", fn_name="trace_read_entry")
#    b.attach_kretprobe(event="vfs_read", fn_name="trace_read_return")
try:
    b.attach_kprobe(event="__vfs_write", fn_name="trace_write_entry")
    b.attach_kretprobe(event="__vfs_write", fn_name="trace_write_return")
except Exception:
    print('Current kernel does not have __vfs_write, try vfs_write instead')
    b.attach_kprobe(event="vfs_write", fn_name="trace_write_entry")
    b.attach_kretprobe(event="vfs_write", fn_name="trace_write_return")

mode_s = {
    0: 'R',
    1: 'W',
}

# header
#print("Tracing sync read/writes slower than %d ms" % min_ms)
print("Tracing sync read/writes...")

start_ts = time.time()
DNAME_INLINE_LEN = 32 
def perf_event(cpu, data, size):
    event = b["events"].event(data)

    ms = float(event.delta_us) / 1000
    name = event.name.decode('utf-8', 'replace')
    if event.name_len > DNAME_INLINE_LEN:
        name = name[:-3] + "..."
    parent_name = event.parent_name.decode('utf-8', 'replace')
    if event.parent_name_len > DNAME_INLINE_LEN:
        parent_name = parent_name[:-3] + "..."

    print("%-8.3f %-14.14s %-6s %1s %-7s %7.2f %-8s %s/%s" % (
        time.time() - start_ts, event.comm.decode('utf-8', 'replace'),
        event.pid, mode_s[event.mode], event.sz, ms, event.pos, parent_name, name))

def ring_event(ctx, data, size):
    e_data = b["event_ring"].event(data)
    name = e_data.name.decode('utf-8', 'replace')
    if e_data.name_len > DNAME_INLINE_LEN:
        name = name[:-3] + "..."
    parent_name = e_data.parent_name.decode('utf-8', 'replace')
    if e_data.parent_name_len > DNAME_INLINE_LEN:
        parent_name = parent_name[:-3] + "..."
    buf = e_data.buffer
    if e_data.sz > 64:
        buf = e_data.buffer[:64]

    print("%-8.3f %-14.14s %-6s %1s %-7s %7.2f %-8s %s/%s %s" % (
        time.time() - start_ts, e_data.comm.decode('utf-8', 'replace'),
        e_data.pid, mode_s[e_data.mode], e_data.sz, e_data.delta_us, e_data.pos, parent_name, name, buf))

#b.trace_print()
if False:
    b["events"].open_perf_buffer(perf_event, page_cnt=64)
    print("%-8s %-14s %-6s %1s %-7s %7s %-8s %-65s" % ("TIME(s)", "COMM", "TID", "D",
        "BYTES", "LAT(ms)", "POS", "FILENAME"))
    while 1:
        try:
            b.perf_buffer_poll()
        except KeyboardInterrupt:
            exit()
else:
    b["event_ring"].open_ring_buffer(ring_event)
    print("%-8s %-14s %-6s %1s %-7s %7s %-8s %-32s %s" % ("TIME(s)", "COMM", "TID", "D",
        "SIZE", "LAT(us)", "POS", "FILENAME", "BUF"))
    while 1:
        try:
            b.ring_buffer_poll()
        except KeyboardInterrupt:
            exit()
