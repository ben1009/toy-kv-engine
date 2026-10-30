/* Diagnostic interposer only. Linux x86-64, not an engine or allocator change. */
#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#define CAPACITY 131072U
#define FD_LIMIT 4096U
struct event {
    uint64_t start, end, offset, length, generation, ticket_end, durable_end;
    long result;
    uint64_t inode;
    int error, tid, fd, operation, kind;
    char name[16];
};
static struct event events[CAPACITY];
static _Atomic unsigned next_event;
static _Atomic unsigned char fd_kind[FD_LIMIT];
static _Atomic uint64_t fd_inode[FD_LIMIT];
static const char *output_path;
static _Thread_local int cached_tid;
static _Thread_local char cached_name[16];

static uint64_t now_ns(void) {
    struct timespec value;
    clock_gettime(CLOCK_MONOTONIC, &value);
    return (uint64_t)value.tv_sec * 1000000000ULL + (uint64_t)value.tv_nsec;
}

static uint64_t be64(const unsigned char *p) {
    uint64_t value = 0;
    for (unsigned i = 0; i < 8; ++i) value = (value << 8) | p[i];
    return value;
}

static int is_wal(int fd) {
    if (fd < 0 || (unsigned)fd >= FD_LIMIT) return 0;
    unsigned char kind = atomic_load_explicit(&fd_kind[fd], memory_order_relaxed);
    if (kind) return kind == 1;
    char path[64], target[4096];
    snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
    ssize_t length = readlink(path, target, sizeof(target) - 1);
    if (length < 0) return 0;
    target[length] = 0;
    int wal = strstr(target, ".wal") != NULL;
    if (wal) {
        struct stat info;
        if (fstat(fd, &info)) return 0;
        atomic_store_explicit(&fd_inode[fd], (uint64_t)info.st_ino, memory_order_relaxed);
    }
    atomic_store_explicit(&fd_kind[fd], wal ? 1 : 2, memory_order_relaxed);
    return wal;
}

static void record(int op, int fd, const void *bytes, size_t length, off64_t offset,
                   uint64_t start, uint64_t end, long result, int error) {
    unsigned index = atomic_fetch_add_explicit(&next_event, 1, memory_order_relaxed);
    if (index >= CAPACITY) return;
    if (!cached_tid) {
        cached_tid = (int)syscall(SYS_gettid);
        prctl(PR_GET_NAME, cached_name, 0, 0, 0);
    }
    struct event *e = &events[index];
    e->start = start; e->end = end; e->offset = (uint64_t)offset;
    e->length = length; e->result = result; e->error = result < 0 ? error : 0;
    e->tid = cached_tid; e->fd = fd; e->operation = op;
    e->inode = atomic_load_explicit(&fd_inode[fd], memory_order_relaxed);
    memcpy(e->name, cached_name, sizeof(e->name));
    if (op == 1 && length == 4096 && bytes) {
        const unsigned char *p = bytes;
        if (!memcmp(p, "TKVW7FR1", 8) && p[8] == 0 && p[9] == 1 &&
            p[12] == 0 && p[13] == 64) {
            e->kind = ((int)p[10] << 8) | p[11];
            if (e->kind == 2) {
                e->generation = be64(p + 64);
                e->ticket_end = be64(p + 72);
                e->durable_end = be64(p + 80);
            }
        }
    }
}

ssize_t pwrite64(int fd, const void *bytes, size_t length, off64_t offset) {
    if (!output_path || !is_wal(fd)) return syscall(SYS_pwrite64, fd, bytes, length, offset);
    uint64_t start = now_ns();
    long result = syscall(SYS_pwrite64, fd, bytes, length, offset);
    int error = errno;
    uint64_t end = now_ns();
    record(1, fd, bytes, length, offset, start, end, result, error);
    errno = error;
    return result;
}

int fdatasync(int fd) {
    if (!output_path || !is_wal(fd)) return (int)syscall(SYS_fdatasync, fd);
    uint64_t start = now_ns();
    long result = syscall(SYS_fdatasync, fd);
    int error = errno;
    uint64_t end = now_ns();
    record(2, fd, NULL, 0, 0, start, end, result, error);
    errno = error;
    return (int)result;
}

int fallocate(int fd, int mode, off_t offset, off_t length) {
    if (!output_path || !is_wal(fd)) return (int)syscall(SYS_fallocate, fd, mode, offset, length);
    uint64_t start = now_ns();
    long result = syscall(SYS_fallocate, fd, mode, offset, length);
    int error = errno;
    uint64_t end = now_ns();
    record(3, fd, NULL, (size_t)length, offset, start, end, result, error);
    errno = error;
    return (int)result;
}

int close(int fd) {
    if (fd >= 0 && (unsigned)fd < FD_LIMIT)
        atomic_store_explicit(&fd_kind[fd], 0, memory_order_relaxed);
    return (int)syscall(SYS_close, fd);
}

__attribute__((constructor)) static void begin_trace(void) {
    output_path = getenv("KV_WAL_IO_TRACE");
    if (!output_path) return;
    /* Pre-touch the bounded array before benchmark setup/timing. */
    volatile unsigned char *p = (volatile unsigned char *)events;
    for (size_t offset = 0; offset < sizeof(events); offset += 4096) p[offset] = 0;
}

__attribute__((destructor)) static void end_trace(void) {
    if (!output_path) return;
    int fd = open(output_path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0) _exit(120);
    FILE *file = fdopen(fd, "w");
    if (!file) _exit(121);
    unsigned count = atomic_load_explicit(&next_event, memory_order_relaxed);
    fprintf(file, "start_ns,end_ns,op,fd,inode,tid,name,offset,length,result,error,kind,generation,ticket_end,durable_end\n");
    for (unsigned i = 0; i < count && i < CAPACITY; ++i) {
        const struct event *e = &events[i];
        fprintf(file, "%" PRIu64 ",%" PRIu64 ",%d,%d,%" PRIu64 ",%d,%s,%" PRIu64
                ",%" PRIu64 ",%ld,%d,%d,%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",
                e->start, e->end, e->operation, e->fd, e->inode, e->tid, e->name,
                e->offset, e->length, e->result, e->error, e->kind, e->generation,
                e->ticket_end, e->durable_end);
    }
    if (fclose(file)) _exit(122);
    if (count >= CAPACITY) _exit(123);
}
