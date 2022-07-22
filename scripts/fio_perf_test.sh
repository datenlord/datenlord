#/bin/sh

set -xv # enable debug
set -e # exit on error
set -u # unset var as error

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 TEST_DIR" >&2
    exit 1
fi

readonly TEST_DIR="$1"
readonly OUTPUT_DIR="/tmp/output"
readonly TEST_DIR_BASE="$(basename ${TEST_DIR})"
readonly OUTPUT_SUBDIR="${OUTPUT_DIR}/${TEST_DIR_BASE}"

# Test paramaters
readonly BLOCK_SIZES="4k 8k 16k 32k 64k 128k 256k 512k"
readonly BLOCK_SIZES_DIRS="$(
    block_size_dirs=""
    for block_size in ${BLOCK_SIZES}; do
        block_size_dirs="${block_size_dirs} ${OUTPUT_SUBDIR}/${block_size}"
    done
    echo ${block_size_dirs}
)"
readonly THREAD_NUMS="1 2 4 8 16 32"

rm -rf ${OUTPUT_DIR}
mkdir ${OUTPUT_DIR}
# Test different block sizes
bench-fio --target ${TEST_DIR} --type directory --size 10M --output ${OUTPUT_DIR} --iodepth 1 --block-size ${BLOCK_SIZES} --numjobs 1 --engine sync --direct 1 --destructive
fio-plot -i ${BLOCK_SIZES_DIRS} -T "Different Block Size Write" -C --rw randwrite --xlabel-parent 0 -o ${OUTPUT_DIR}/block_size_write.png
fio-plot -i ${BLOCK_SIZES_DIRS} -T "Different Block Size Read" -C --rw randread --xlabel-parent 0 -o ${OUTPUT_DIR}/block_size_read.png

# Test different thread numbers
bench-fio --target ${TEST_DIR} --type directory --size 5M --output ${OUTPUT_DIR} --iodepth 1 --block-size 4k --numjobs ${THREAD_NUMS} --engine sync --direct 1 --destructive
fio-plot -i ${OUTPUT_SUBDIR}/4k/  -T "Different Thread Num Write" -N --rw randwrite -o ${OUTPUT_DIR}/4k_multithread_write.png
fio-plot -i ${OUTPUT_SUBDIR}/4k/  -T "Different Thread Num Read" -N --rw randread -o ${OUTPUT_DIR}/4k_multithread_read.png
