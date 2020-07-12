#!/usr/bin/python
# @lint-avoid-python-3-compatibility-imports
#
# prometheus-ebpf-exporter  Trace slow FUSE operations.
#            For Linux, uses BCC, eBPF.
#
# USAGE: prometheus-ebpf-exporter.py [-h]
#
# This script traces common FUSE file operations: reads, writes, opens, and
# syncs. It measures the time spent in these operations, and prints details
# for each that exceeded a threshold.
#
# WARNING: This adds low-overhead instrumentation to these FUSE operations,
# including reads and writes from the file system cache. Such reads and writes
# can be very frequent (depending on the workload; eg, 1M/sec), at which
# point the overhead of this tool (even if it prints no "slower" events) can
# begin to become significant.
#
# This script is based on the "xfsslower" script with many modifications.
# Copyright 2016 Netflix, Inc.
# Licensed under the Apache License, Version 2.0 (the "License")
#
# 11-Feb-2016   Brendan Gregg   Created this.
# 16-Oct-2016   Dina Goldshtein -p to filter by process ID.
# **-Feb-2018   Olaf Seibert  Reworked: convert to FUSE, add async support, and make it Prometheus exporter.

from __future__ import print_function
from collections import defaultdict
from bcc import BPF
import bcc
import argparse
from time import strftime, sleep
import ctypes as ct
from prometheus_client import start_http_server
from prometheus_client.core import HistogramMetricFamily, CounterMetricFamily, GaugeMetricFamily, REGISTRY
import logging

# arguments
examples = """examples:
    ./prometheus-ebpf-exporter.py             # start a Prometheus exporter
    ./prometheus-ebpf-exporter.py --events    # additionally print the slower events to stdout
"""
parser = argparse.ArgumentParser(
    description="Trace common FUSE file operations slower than a threshold",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog=examples)
parser.add_argument("-l", "--listen-addr", default="localhost",
    help="The Prometheus export server will listen on this address (default localhost)")
parser.add_argument("-p", "--listen-port", default=9500,
    help="The Prometheus export server will listen on this port (default 9500)")
parser.add_argument("--xfs", action="store_true",
    help="instrument xfs instead of fuse (no async I/O supported)")
parser.add_argument("--ext4", action="store_true",
    help="instrument ext4 instead of fuse (no async I/O supported)")
parser.add_argument("--events", action="store_true",
    help="print events in addition to collecting histograms")
parser.add_argument("--minms", action="store", default="10",
    help="only print events that are slower than this number of milliseconds (default 10)")
parser.add_argument("--printhistograms", action="store_true",
    help="print histograms instead of serving them as Prometheus")

args = parser.parse_args()
debug = 0

# define BPF program
bpf_text = """
#include <uapi/linux/ptrace.h>
#include <linux/fs.h>
#include <linux/sched.h>
#include <linux/dcache.h>
#define EIOCBQUEUED     529
struct kref { atomic_t refcount; };
/* from fuse_i.h: */
/** The request IO state (for asynchronous processing) */
struct fuse_io_priv {
        struct kref refcnt;
        int async;
        spinlock_t lock;
        unsigned reqs;
        ssize_t bytes;
        size_t size;
        __u64 offset;
        bool write;
        int err;
        struct kiocb *iocb;
        struct file *file;
        struct completion *done;
};
enum trace_types {
    TRACE_READ = 0,
    TRACE_WRITE,
    TRACE_OPEN,
    TRACE_FSYNC,
};
struct val_t {
    u64 ts;
    u64 offset;
    struct file *fp;
    struct kiocb *iocb;
};
struct data_t {
    // XXX: switch some to u32's when supported
    u64 ts_us;
    u64 type;
    u64 size;
    u64 offset;
    u64 delta_us;
    u64 pid;
    char task[TASK_COMM_LEN];
    char file[DNAME_INLINE_LEN];
};
struct asyncinfo_t {
    struct data_t data;
    struct val_t val;
};
BPF_HASH(entryinfo, u64, struct val_t);
BPF_HASH(asyncinfo, u64, struct asyncinfo_t);
BPF_ARRAY(activerequests, s64, 2);
#define GAUGE_SYNC    0
#define GAUGE_ASYNC   1
#if PRODUCE_EVENTS
BPF_PERF_OUTPUT(events);
#endif
// Histogram keys
struct histo_key_t {
    u32  usec_log2;
    char type;
    char file;
};
enum file_t {
    FILE_REST = 0,
    FILE_DISK,
    FILE_CINDER
};
BPF_HISTOGRAM(distribution, struct histo_key_t);
struct totals_key_t {
    char type;
    char file;
};
BPF_HASH(totals, struct totals_key_t, u64);
///
/// Increment and decrement gauges atomically
///
static inline
void inc_gauge(int which)
{
    s64 *p = activerequests.lookup(&which);
    if (p != 0) {
        lock_xadd(p, 1);
    }
}
static inline
void dec_gauge(int which)
{
    s64 *p = activerequests.lookup(&which);
    if (p != 0) {
        lock_xadd(p, -1);
        if (*p < 0) {
            lock_xadd(p, 1);
        }
    }
}
//
// Store timestamp and offset on entry
//
// fuse_file_read_iter(), fuse_file_write_iter():
int trace_rw_entry(struct pt_regs *ctx, struct kiocb *iocb)
{
    u64 id = bpf_get_current_pid_tgid();
    u32 pid = id >> 32; // PID is higher part
    // store filep and timestamp by id
    struct val_t val = {};
    val.ts = bpf_ktime_get_ns();
    val.fp = iocb->ki_filp;
    val.offset = iocb->ki_pos;
    val.iocb = iocb;
    if (val.fp) {
        // if update returns < 0 there was an error and the value not inserter/updated
        if (entryinfo.update(&id, &val) >= 0) {
            inc_gauge(GAUGE_SYNC);
        }
    }
    return 0;
}
// fuse_file_open():
int trace_open_entry(struct pt_regs *ctx, struct inode *inode,
    struct file *file)
{
    u64 id = bpf_get_current_pid_tgid();
    u32 pid = id >> 32; // PID is higher part
    // store filep and timestamp by id
    struct val_t val = {};
    val.ts = bpf_ktime_get_ns();
    val.fp = file;
    val.offset = 0;
    val.iocb = 0;
    if (val.fp) {
        if (entryinfo.update(&id, &val) >= 0) {
            inc_gauge(GAUGE_SYNC);
        }
    }
    return 0;
}
// fuse_file_fsync():
int trace_fsync_entry(struct pt_regs *ctx, struct file *file)
{
    u64 id = bpf_get_current_pid_tgid();
    u32 pid = id >> 32; // PID is higher part
    // store filep and timestamp by id
    struct val_t val = {};
    val.ts = bpf_ktime_get_ns();
    val.fp = file;
    val.offset = 0;
    val.iocb = 0;
    if (val.fp) {
        if (entryinfo.update(&id, &val) >= 0) {
            inc_gauge(GAUGE_SYNC);
        }
    }
    return 0;
}
// Determine if two string prefixes are equal. Return 1 if so.
static inline
int strneq(char *a, char *b, int maxlen)
{
    while (maxlen > 0 && *a && *a == *b) {
        a++;
        b++;
        maxlen--;
    }
    if (maxlen <= 0)
        return 1;
    return 0;
}
// Correct the bpf_log2l() function. It has a tendency to round up too much:
// it returns the log2 of the next higher power of 2, even when the parameter
// already is a power of 2.
// It uses a method similar to
// https://graphics.stanford.edu/~seander/bithacks.html#IntegerLog "OR (IF YOUR CPU BRANCHES SLOWLY"
// but it always adds 1.
//
// Since we want buckets with observations "less or equal" to powers of 2,
// we have to adjust the result to get (for instance) all of 5, 6, 7, 8 in bucket "<= 8".
//
// We do this by subtracting 1 from the value. For values on the lower edge of the bucket,
// i.e. the powers of 2, this causes the log2 value to change (1 lower). For others,
// it makes no difference.
static inline __attribute__((always_inline))
unsigned int my_log2l(unsigned long v)
{
    if (v <= 1)
        return 0;
    else
        return bpf_log2l(v - 1);
}
//
// Output
//
static int update_histogram(struct pt_regs *ctx, struct data_t *datap)
{
    // If we want to produce events, do it here.
#if PRODUCE_EVENTS
    if (FILTER_US) {
        events.perf_submit(ctx, datap, sizeof(*datap));
    }
#endif
    // Collect histograms keyed by process name (qemu*, rest), file name (disk, cinder*, rest), in log2 slots
    struct histo_key_t histo = {};
    histo.usec_log2 = my_log2l(datap->delta_us);
    // Cap the histogram buckets, so that we can have a fixed number of buckets that we know in advance.
    if (histo.usec_log2 > INFINITY_BUCKET) {
        histo.usec_log2 = INFINITY_BUCKET;
    }
    histo.type = datap->type;
    histo.file = FILE_REST;
    if (strneq(datap->file, "disk", 4)) {
        histo.file = FILE_DISK;
    } else if (strneq(datap->file, "cinder", 6)) {
        histo.file = FILE_CINDER;
    } else {
        histo.file = FILE_REST;
    }
    distribution.increment(histo);
    // Also keep track of the (unrounded) totals of the values in the histogram.
    struct totals_key_t totals_key = { .type = histo.type, .file = histo.file };
    u64 zero = 0;
    u64 *entry = totals.lookup_or_init(&totals_key, &zero);
    if (entry != 0) {
        lock_xadd(entry, datap->delta_us);
    }
    return 0;
}
// Gets a file name from a struct file *.
static int get_file_name(struct data_t *data, struct file *file)
{
    // workaround (rewriter should handle file to d_name in one step):
    struct dentry *de = NULL;
    struct qstr qs = {};
    de = file->f_path.dentry;
    qs = de->d_name;
    if (qs.len == 0)
        return 0;
    bpf_probe_read(&data->file, sizeof(data->file), (void *)qs.name);
    return 1;
}
static int trace_return(struct pt_regs *ctx, int type)
{
    struct val_t *valp;
    u64 id = bpf_get_current_pid_tgid();
    u32 pid = id >> 32; // PID is higher part
    valp = entryinfo.lookup(&id);
    if (valp == 0) {
        // missed tracing issue
        return 0;
    }
    dec_gauge(GAUGE_SYNC);
    // calculate delta
    u64 ts = bpf_ktime_get_ns();
    u64 delta_us = (ts - valp->ts) / 1000;
    struct val_t val = *valp;
    entryinfo.delete(&id); // is *valp actually valid after this?
    // populate output struct
    u64 size = PT_REGS_RC(ctx);
    struct data_t data = {.type = type, .size = size, .delta_us = delta_us,
        .pid = pid};
    data.ts_us = ts / 1000;
    data.offset = val.offset;
    bpf_get_current_comm(&data.task, sizeof(data.task));
    get_file_name(&data, val.fp);
    // output
    update_histogram(ctx, &data);
    if (size == (u64)-EIOCBQUEUED) {
        // Now store the info keyed on the async io request block
        if (val.iocb != 0) {
            u64 id = (u64)val.iocb;
            struct asyncinfo_t ainfo = { .val = val, .data = data };
            asyncinfo.update(&id, &ainfo);
            inc_gauge(GAUGE_ASYNC);
        }
    }
    return 0;
}
// This one is FUSE-specific, because it looks at struct fuse_io_priv.
int trace_fuse_aio_complete(struct pt_regs *ctx, struct fuse_io_priv *io)
{
    struct asyncinfo_t *aip;
    struct kiocb *iocb = io->iocb;
    u64 id = (u64)iocb;
    dec_gauge(GAUGE_ASYNC);
    aip = asyncinfo.lookup(&id);
    if (aip == 0) {
        // Missed tracing issue, or we get here on a second CPU while the first CPU
        // is still adding the data (which happens after the I/O has been queued).
        // This would seem to lead to unbounded memory use, as there are aios stored but never removed again.
        // However this seems not be be a problem so far, as the keys (kiocb) seem to be re-used.
        struct data_t data = {.size = io->size, .offset = io->offset,
            .delta_us = 0,
            .pid = 0 };
        data.type = io->write ? TRACE_WRITE : TRACE_READ;
        get_file_name(&data, io->file);
        update_histogram(ctx, &data);
        return 0;
    }
    // calculate delta
    u64 ts = bpf_ktime_get_ns();
    u64 delta_us = (ts - aip->val.ts) / 1000;
    struct data_t data = aip->data;
    asyncinfo.delete(&id);
    data.ts_us = ts / 1000;
    data.delta_us = delta_us;
    data.size = io->size;
    // output
    update_histogram(ctx, &data);
    return 0;
}
int trace_read_return(struct pt_regs *ctx)
{
    return trace_return(ctx, TRACE_READ);
}
int trace_write_return(struct pt_regs *ctx)
{
    return trace_return(ctx, TRACE_WRITE);
}
int trace_open_return(struct pt_regs *ctx)
{
    return trace_return(ctx, TRACE_OPEN);
}
int trace_fsync_return(struct pt_regs *ctx)
{
    return trace_return(ctx, TRACE_FSYNC);
}
"""

# kernel->user event data: struct data_t
DNAME_INLINE_LEN = 32   # linux/dcache.h
TASK_COMM_LEN = 16      # linux/sched.h
class Data(ct.Structure):
    _fields_ = [
        ("ts_us", ct.c_ulonglong),
        ("type", ct.c_ulonglong),
        ("size", ct.c_longlong),
        ("offset", ct.c_ulonglong),
        ("delta_us", ct.c_ulonglong),
        ("pid", ct.c_ulonglong),
        ("task", ct.c_char * TASK_COMM_LEN),
        ("file", ct.c_char * DNAME_INLINE_LEN)
    ]

class Totals_key(ct.Structure):
    _fields_ = [
        ("type", ct.c_char),
        ("file", ct.c_char),
    ]

mode_s = {
    0: 'read',
    1: 'write',
    2: 'open',
    3: 'fsync',
}

filename_s = {
    0: "rest",
    1: "disk",
    2: "cinder"
}

# For our histogram processing software, which adds histogram values reported
# by all nodes, it makes sense that all nodes report the same number of
# buckets. If nodes grow the number of buckets dynamically as needed, then some
# nodes report "<= 17179869184 ms" and some won't.  The software doesn't know
# to extrapolate between the highest bucket of other nodes and +Infinity.  As a
# result, this bucket is reported too low.
#
# 2^30 suffices for 1073741.824 ms (1073 seconds, 17 min 53 s).
# Infinity is then anything slower than that.

INFINITY_BUCKET = 31

# process event
def print_event(cpu, data, size):
    event = ct.cast(data, ct.POINTER(Data)).contents

    type = mode_s[event.type];

    if event.size == -529:
        size = "QUEUED" # EIOCBQUEUED
    else:
        size = event.size

    LOG.info("%-8s %-14.14s %-6s %-2s %7s %8d %7.2f %s" % (strftime("%H:%M:%S"),
        event.task, event.pid, type, size, event.offset / 1024,
        float(event.delta_us) / 1000, event.file))


def create_histogram_label(value_log2):
    """Create a label for in the histogram. It is used for values that are "less or
    equal (le)" than a certain value.  The various values are powers of two, so
    what we really remember in our internal data is the log2 of the value.
    Depending on how exactly the rounding is done when calculating the log2, the
    upper bound value (4, 8, etc) might nog actually be included.
    The bpf_log2l() function always returns at least 1.
    This function takes the logarithm and recreates the original value, which will be
    rounded to a power of 2: this would often be called pow2(value_log2) or pow(2,
    value_log2).
    It uses the bitshifting shortcut 1 << value_log2, because 1<<0 = 1, 1<<1 = 2,
    1<<2 = 4, 1<<3 = 8, etc, which exactly corresponds to pow2().
    It is recommended to report in base units, e.g. seconds instead of microseconds,
    but for readability of various legends we choose milliseconds.
    """
    return "%0.3f" % ((1 << value_log2) / 1000.0)

class Accumulator(object):
    """A little helper class to accumulate values.
       You always get the sum of all values put in until now.
       When you put in 1, 2, 3, 4, ... you get out 1, 3, 6, 10, ...
    """
    accu = 0

    def next(self, value):
        self.accu += value;
        return self.accu;

class GrowingList(list):
    """Little helper class for creating arrays where we may not assign the elements in order, or all of them.
       defval is the value assigned for the missing intermediate values.
    """
    defval = None

    def __init__(self, defval):
        self.defval = defval

    def __setitem__(self, index, value):
        if index >= len(self):
            self.extend([ self.defval ] * (index + 1 - len(self)))
        list.__setitem__(self, index, value)

class CustomCollector(object):

    def totals_as_dict(self):
        """The BPF_HASH object is not accessible as a Python dictionary.
           Since we need to do multiple lookups in it, do some work ahead of time to create a dict now.
        """
        d = {}

        for item in b["totals"].items():
            tkey = item[0]
            tvalue = item[1].value

            d[ (tkey.type, tkey.file) ] = tvalue

        return d

    def collect(self):
        """Convert the data as we collected it into a form that Prometheus wants it.
           For instance, it wants all buckets for the same file/operation at the same time, sorted, and cumulative.
        """
        h = HistogramMetricFamily("ebpf_fuse_req_latency_ms", "Latency of various FUSE operations", labels=["filename", "operation"])

        # The BPF_HISTOGRAM storage is not the same "shape" as the HistogramMetricFamily.
        # We need to re-arrange the data somewhat.
        # The former is a table of (key, key, key, bucket, count).
        # The latter wants ( (key, key, key), [( bucket_label, count ), ... ], total_of_all_times ) (and sorted)
        # Additionally, the buckets are cumulative, i.e. each bucket counts all the previous ones too.
        # Querying the data gets confusing if buckets "in the middle" are missing, because then there
        # is "no data" instead of "same as the lower bucket".

        distribution = b["distribution"]
        totals = self.totals_as_dict()
        counts = defaultdict(list)

        for item in distribution.items():
            key = item[0]
            count = item[1].value

            counts[ (key.type, key.file) ].append( (key.usec_log2, count) )

        for key, list_of_pairs in counts.items():
            ktype, kfile = key

            # Let's have buckets up to 2^INFINITY_BUCKET
            bucket = [ 0 ] * ( INFINITY_BUCKET + 1)

            # Create a sorted list of pairs of (bucket_label, count). The labels are powers of 2.
            # The counts are cumulative.

            # Collect buckets to prepare for sorting
            for pair in list_of_pairs:
                usec_log2, count = pair

                if usec_log2 >= INFINITY_BUCKET:
                    bucket[INFINITY_BUCKET] += count;
                else:
                    bucket[usec_log2] = count;

            # Put them in sorted order and accumulate (excluding INFINITY_BUCKET)
            accu = Accumulator()
            buckets = [ (create_histogram_label(usec_log2), accu.next(bucket[usec_log2]))
                        for usec_log2 in xrange(0, INFINITY_BUCKET) ]

            # Add the +Inf entry at the end, for all times longer than the previous bucket
            buckets.append(("+Inf", accu.next(bucket[INFINITY_BUCKET])))

            total = totals[ (ktype, kfile) ]

            h.add_metric([ filename_s[ord(kfile)], mode_s[ord(ktype)] ],
                         buckets,
                         total / 1000.0)

        yield h

        activerequests = b["activerequests"]

        # Further statistics: how many outstanding sync / async requests we have in our tables.
        # If this value increases, it points either to a bug in this script, or
        # to non-responsive file systems.
        # In any case, the tables have a maximum size, so there is no risk of unbounded memory use.

        yield GaugeMetricFamily('ebpf_fuse_req_sync_outstanding',
                                'The number of sync I/O requests that did not complete after being registered',
                                value=activerequests[0].value)

        yield GaugeMetricFamily('ebpf_fuse_req_async_outstanding',
                                'The number of async I/O requests that did not complete after being registered',
                                value=activerequests[1].value)



if __name__ == '__main__':
    LOG = logging.getLogger(__name__)
    logging.basicConfig()
    LOG.setLevel(logging.INFO)
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.WARNING)

    REGISTRY.register(CustomCollector())

    # Enable optional code
    if args.events:
	bpf_text = "#define PRODUCE_EVENTS 1\n" + bpf_text
	if (args.minms > 0):
	    bpf_text = "#define FILTER_US (datap->delta_us >= %d)\n" % (float(args.minms) * 1000) + bpf_text
	else:
	    bpf_text = "#define FILTER_US 1\n" + bpf_text

    # Add #define for INFINITY_BUCKET.
    bpf_text = ("#define INFINITY_BUCKET %s\n" % INFINITY_BUCKET) + bpf_text

    # initialize BPF
    b = BPF(text=bpf_text)

    # Instrument common file functions. Do the return parts first, otherwise we may
    # get a call to the entry hook before we have the return set up.
    # This would put data in tables that we never get around to use.
    if args.xfs:
	b.attach_kretprobe(event="xfs_file_read_iter", fn_name="trace_read_return")
	b.attach_kretprobe(event="xfs_file_write_iter", fn_name="trace_write_return")
	b.attach_kretprobe(event="xfs_file_open", fn_name="trace_open_return")
	b.attach_kretprobe(event="xfs_file_fsync", fn_name="trace_fsync_return")

	# no aio supported
	b.attach_kprobe(event="xfs_file_read_iter", fn_name="trace_rw_entry")
	b.attach_kprobe(event="xfs_file_write_iter", fn_name="trace_rw_entry")
	b.attach_kprobe(event="xfs_file_open", fn_name="trace_open_entry")
	b.attach_kprobe(event="xfs_file_fsync", fn_name="trace_fsync_entry")

    elif args.ext4:
	b.attach_kretprobe(event="generic_file_read_iter", fn_name="trace_read_return")
	b.attach_kretprobe(event="ext4_file_write_iter", fn_name="trace_write_return")
	b.attach_kretprobe(event="ext4_file_open", fn_name="trace_open_return")
	b.attach_kretprobe(event="ext4_sync_file", fn_name="trace_fsync_return")

	# no aio supported
	b.attach_kprobe(event="generic_file_read_iter", fn_name="trace_rw_entry")
	b.attach_kprobe(event="ext4_file_write_iter", fn_name="trace_rw_entry")
	b.attach_kprobe(event="ext4_file_open", fn_name="trace_open_entry")
	b.attach_kprobe(event="ext4_sync_file", fn_name="trace_fsync_entry")

    else: # FUSE
	b.attach_kretprobe(event="fuse_file_read_iter", fn_name="trace_read_return")
	b.attach_kretprobe(event="fuse_file_write_iter", fn_name="trace_write_return")
	b.attach_kretprobe(event="fuse_open", fn_name="trace_open_return")
	b.attach_kretprobe(event="fuse_fsync", fn_name="trace_fsync_return")

	b.attach_kprobe(event="fuse_aio_complete", fn_name="trace_fuse_aio_complete")

	b.attach_kprobe(event="fuse_file_read_iter", fn_name="trace_rw_entry")
	b.attach_kprobe(event="fuse_file_write_iter", fn_name="trace_rw_entry")
	b.attach_kprobe(event="fuse_open", fn_name="trace_open_entry")
	b.attach_kprobe(event="fuse_fsync", fn_name="trace_fsync_entry")


    # Start up the server to expose the metrics.
    start_http_server(int(args.listen_port), args.listen_addr)
    #start_http_server(9500)

    if args.events:
	# header
	LOG.info("Tracing FUSE operations")
	LOG.info("%-8s %-14s %-6s %-6s %-7s %-8s %7s %s" % ("TIME", "COMM", "PID", "T ",
	    "BYTES", "OFF_KB", "LAT(ms)", "FILENAME"))

	# read events
	b["events"].open_perf_buffer(print_event, page_cnt=64)
	while 1:
	    b.kprobe_poll()

    elif args.printhistograms:
	# print histograms
	distribution = b["distribution"]
	while True:
	    sleep(5)
	    LOG.info("raw histogram:")
	    LOG.info("-------------")

	    for item in distribution.items():
		key = item[0]
		count = item[1].value

		operation = mode_s[ord(key.type)]
		filename = filename_s[ord(key.file)]
		#LOG.info(task, filename, key.usec_log2, count)
                LOG.info("%6s %5s %5d: %5d", filename, operation, key.usec_log2, count)

    else:
	while True:
	    sleep(1000)
