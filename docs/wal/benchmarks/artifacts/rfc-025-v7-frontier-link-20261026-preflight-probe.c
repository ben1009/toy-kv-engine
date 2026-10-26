/* Native scheduling diagnostic only: opaque DATA/marker bytes, not a WAL codec. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <liburing.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#define FRAME 4096U
#define CYCLES 500U

static void fail(const char *message) {
    perror(message);
    /* Process exit retains allocations until outstanding kernel work is gone. */
    exit(1);
}

static uint64_t now_ns(void) {
    struct timespec time;
    if (clock_gettime(CLOCK_MONOTONIC, &time)) fail("clock_gettime");
    return (uint64_t)time.tv_sec * 1000000000ULL + (uint64_t)time.tv_nsec;
}

static uint64_t cpu_us(const struct rusage *usage) {
    return ((uint64_t)usage->ru_utime.tv_sec + (uint64_t)usage->ru_stime.tv_sec) *
               1000000ULL +
           (uint64_t)usage->ru_utime.tv_usec + (uint64_t)usage->ru_stime.tv_usec;
}

static void write_full(int fd, const void *bytes, size_t len, off_t offset) {
    ssize_t written;
    do { written = pwrite(fd, bytes, len, offset); } while (written < 0 && errno == EINTR);
    if (written < 0) fail("pwrite");
    if ((size_t)written != len) { errno = EIO; fail("short pwrite"); }
}

static void sync_full(int fd) {
    int result;
    do { result = fdatasync(fd); } while (result && errno == EINTR);
    if (result) fail("fdatasync");
}

static void linked_commit(struct io_uring *ring, const void *marker, off_t offset) {
    struct io_uring_sqe *write = io_uring_get_sqe(ring);
    if (!write) { errno = ENOSPC; fail("write SQE"); }
    io_uring_prep_write(write, 0, marker, FRAME, (uint64_t)offset);
    write->flags = IOSQE_FIXED_FILE | IOSQE_IO_LINK;
    write->user_data = 1;
    struct io_uring_sqe *sync = io_uring_get_sqe(ring);
    if (!sync) { errno = ENOSPC; fail("sync SQE"); }
    io_uring_prep_fsync(sync, 0, IORING_FSYNC_DATASYNC);
    sync->flags = IOSQE_FIXED_FILE;
    sync->user_data = 2;
    int submitted;
    do { submitted = io_uring_submit_and_wait(ring, 2); } while (submitted == -EINTR);
    if (submitted != 2) {
        errno = submitted < 0 ? -submitted : EIO;
        fail("submit linked pair");
    }
    unsigned seen = 0;
    for (unsigned index = 0; index < 2; ++index) {
        struct io_uring_cqe *cqe;
        int result;
        do { result = io_uring_wait_cqe(ring, &cqe); } while (result == -EINTR);
        if (result) { errno = -result; fail("wait linked CQE"); }
        unsigned tag = (unsigned)cqe->user_data;
        if (cqe->user_data != tag || tag < 1 || tag > 2 || (seen & (1U << tag)) ||
            cqe->res != (tag == 1 ? (int)FRAME : 0)) {
            errno = cqe->res < 0 ? -cqe->res : EIO;
            fail("linked CQE validation");
        }
        seen |= 1U << tag;
        io_uring_cqe_seen(ring, cqe);
    }
    if (seen != 6) { errno = EIO; fail("missing linked CQE"); }
}

static int compare_u64(const void *left, const void *right) {
    uint64_t a = *(const uint64_t *)left, b = *(const uint64_t *)right;
    return (a > b) - (a < b);
}

int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(stderr, "usage: probe POLICY point8|batch64x8 FRESH_PATH\n");
        return 2;
    }
    const char *policy = argv[1], *cell = argv[2];
    unsigned flags = 0;
    int use_ring = strcmp(policy, "syscalls") != 0;
    if (!strcmp(policy, "uring_coop_single"))
        flags = IORING_SETUP_COOP_TASKRUN | IORING_SETUP_SINGLE_ISSUER;
    else if (!strcmp(policy, "uring_defer_single"))
        flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN;
    else if (strcmp(policy, "syscalls") && strcmp(policy, "uring_default")) return 2;
    int point = !strcmp(cell, "point8");
    if (!point && strcmp(cell, "batch64x8")) return 2;
    size_t data_len = point ? 8 * FRAME : 8 * 17 * FRAME;
    size_t stride = data_len + FRAME, image_len = stride * CYCLES;
    void *data, *marker, *zeros;
    if (posix_memalign(&data, FRAME, data_len) ||
        posix_memalign(&marker, FRAME, FRAME) ||
        posix_memalign(&zeros, FRAME, 128 * 1024)) {
        errno = ENOMEM; fail("aligned buffers");
    }
    memset(data, 0xDA, data_len);
    memset(marker, 0xF1, FRAME);
    memset(zeros, 0, 128 * 1024);
    int fd = open(argv[3], O_CREAT | O_EXCL | O_RDWR | O_DIRECT | O_CLOEXEC, 0600);
    if (fd < 0) fail("open fresh file");
    if (fallocate(fd, 0, 0, (off_t)image_len)) fail("fallocate");
    if (point) {
        for (size_t offset = 0; offset < image_len;) {
            size_t len = image_len - offset;
            if (len > 128 * 1024) len = 128 * 1024;
            write_full(fd, zeros, len, (off_t)offset);
            offset += len;
        }
        sync_full(fd);
    }
    struct io_uring ring;
    if (use_ring) {
        struct io_uring_params params = { .flags = flags };
        int result = io_uring_queue_init_params(8, &ring, &params);
        if (result) { errno = -result; fail("io_uring setup"); }
        if (params.flags != flags) { errno = EIO; fail("setup flags"); }
        result = io_uring_register_files(&ring, &fd, 1);
        if (result) { errno = -result; fail("register file"); }
    }
    struct rusage before, after;
    if (getrusage(RUSAGE_SELF, &before)) fail("getrusage");
    uint64_t latencies[CYCLES], frontier_ns = 0, data_ns = 0;
    uint64_t start = now_ns();
    for (unsigned cycle = 0; cycle < CYCLES; ++cycle) {
        off_t offset = (off_t)(cycle * stride);
        uint64_t cycle_start = now_ns();
        write_full(fd, data, data_len, offset);
        uint64_t data_done = now_ns();
        if (use_ring) linked_commit(&ring, marker, offset + (off_t)data_len);
        else { write_full(fd, marker, FRAME, offset + (off_t)data_len); sync_full(fd); }
        uint64_t done = now_ns();
        data_ns += data_done - cycle_start;
        frontier_ns += done - data_done;
        latencies[cycle] = done - cycle_start;
    }
    uint64_t elapsed = now_ns() - start;
    if (getrusage(RUSAGE_SELF, &after)) fail("getrusage");
    if (use_ring) io_uring_queue_exit(&ring);
    if (close(fd)) fail("close direct file");
    fd = open(argv[3], O_RDONLY | O_CLOEXEC);
    if (fd < 0) fail("open verification");
    unsigned char *readback = malloc(stride);
    if (!readback) fail("verification allocation");
    for (unsigned cycle = 0; cycle < CYCLES; ++cycle) {
        ssize_t len = pread(fd, readback, stride, (off_t)(cycle * stride));
        if (len != (ssize_t)stride || memcmp(readback, data, data_len) ||
            memcmp(readback + data_len, marker, FRAME)) {
            errno = EIO; fail("readback verification");
        }
    }
    if (close(fd) || unlink(argv[3])) fail("cleanup");
    free(readback); free(zeros); free(marker); free(data);
    qsort(latencies, CYCLES, sizeof(uint64_t), compare_u64);
    printf("{\"policy\":\"%s\",\"cell\":\"%s\",\"flags\":%u,"
           "\"cycles\":%u,\"data_bytes_per_cycle\":%zu,\"image_bytes\":%zu,"
           "\"prezero\":%s,\"elapsed_ns\":%" PRIu64 ",\"data_ns\":%" PRIu64 ","
           "\"frontier_sync_ns\":%" PRIu64 ",\"cpu_us\":%" PRIu64 ","
           "\"cycle_p50_ns\":%" PRIu64 ",\"cycle_p99_ns\":%" PRIu64 ","
           "\"readback_verified\":true}\n", policy, cell, flags, CYCLES, data_len,
           image_len, point ? "true" : "false", elapsed, data_ns, frontier_ns,
           cpu_us(&after) - cpu_us(&before), latencies[CYCLES / 2],
           latencies[CYCLES * 99 / 100]);
    return 0;
}
