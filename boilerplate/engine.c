#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    32
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define LOG_DIR             "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN   256
#define LOG_CHUNK_SIZE      4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT  (40UL << 20)
#define DEFAULT_HARD_LIMIT  (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* ── helpers ──────────────────────────────────────────────── */

static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage:\n"
        "  %s supervisor <base-rootfs>\n"
        "  %s start <id> <rootfs> <cmd> [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s run   <id> <rootfs> <cmd> [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s ps\n"
        "  %s logs <id>\n"
        "  %s stop <id>\n",
        prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag, const char *value,
                           unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;
    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large\n", flag);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req, int argc,
                                 char *argv[], int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i+1],
                               &req->soft_limit_bytes) != 0) return -1;
        } else if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i+1],
                               &req->hard_limit_bytes) != 0) return -1;
        } else if (strcmp(argv[i], "--nice") == 0) {
            char *end = NULL;
            long v = strtol(argv[i+1], &end, 10);
            if (end == argv[i+1] || *end != '\0' || v < -20 || v > 19) {
                fprintf(stderr, "Invalid --nice value\n");
                return -1;
            }
            req->nice_value = (int)v;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return -1;
        }
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t s)
{
    switch (s) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ── bounded buffer ───────────────────────────────────────── */

static int bounded_buffer_init(bounded_buffer_t *b)
{
    int rc;
    memset(b, 0, sizeof(*b));
    rc = pthread_mutex_init(&b->mutex, NULL);
    if (rc) return rc;
    rc = pthread_cond_init(&b->not_empty, NULL);
    if (rc) { pthread_mutex_destroy(&b->mutex); return rc; }
    rc = pthread_cond_init(&b->not_full, NULL);
    if (rc) {
        pthread_cond_destroy(&b->not_empty);
        pthread_mutex_destroy(&b->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *b)
{
    pthread_cond_destroy(&b->not_full);
    pthread_cond_destroy(&b->not_empty);
    pthread_mutex_destroy(&b->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *b)
{
    pthread_mutex_lock(&b->mutex);
    b->shutting_down = 1;
    pthread_cond_broadcast(&b->not_empty);
    pthread_cond_broadcast(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
}

int bounded_buffer_push(bounded_buffer_t *b, const log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);
    while (b->count == LOG_BUFFER_CAPACITY && !b->shutting_down)
        pthread_cond_wait(&b->not_full, &b->mutex);
    if (b->shutting_down) {
        pthread_mutex_unlock(&b->mutex);
        return -1;
    }
    b->items[b->tail] = *item;
    b->tail = (b->tail + 1) % LOG_BUFFER_CAPACITY;
    b->count++;
    pthread_cond_signal(&b->not_empty);
    pthread_mutex_unlock(&b->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *b, log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);
    while (b->count == 0 && !b->shutting_down)
        pthread_cond_wait(&b->not_empty, &b->mutex);
    if (b->count == 0) {
        pthread_mutex_unlock(&b->mutex);
        return -1;   /* shutdown + empty */
    }
    *item = b->items[b->head];
    b->head = (b->head + 1) % LOG_BUFFER_CAPACITY;
    b->count--;
    pthread_cond_signal(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
    return 0;
}

/* ── logging consumer thread ──────────────────────────────── */

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        /* find log path for this container */
        char log_path[PATH_MAX] = {0};
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c) {
            if (strncmp(c->id, item.container_id, CONTAINER_ID_LEN) == 0) {
                strncpy(log_path, c->log_path, PATH_MAX - 1);
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path[0] == '\0') continue;

        int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) continue;
        write(fd, item.data, item.length);
        close(fd);
    }
    return NULL;
}

/* ── producer: reads pipe from one container ──────────────── */

typedef struct {
    int pipe_fd;
    char container_id[CONTAINER_ID_LEN];
    supervisor_ctx_t *ctx;
} producer_arg_t;

static void *producer_thread(void *arg)
{
    producer_arg_t *pa = (producer_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    while (1) {
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, pa->container_id, CONTAINER_ID_LEN - 1);
        n = read(pa->pipe_fd, item.data, LOG_CHUNK_SIZE);
        if (n <= 0) break;
        item.length = (size_t)n;
        bounded_buffer_push(&pa->ctx->log_buffer, &item);
    }
    close(pa->pipe_fd);
    free(pa);
    return NULL;
}

/* ── child entrypoint (runs inside clone'd namespace) ─────── */

int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* redirect stdout+stderr to supervisor pipe */
    dup2(cfg->log_write_fd, STDOUT_FILENO);
    dup2(cfg->log_write_fd, STDERR_FILENO);
    close(cfg->log_write_fd);

    /* set nice value */
    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    /* chroot into container rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    /* mount /proc so ps works inside container */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        /* non-fatal — /proc might already exist */
    }

    /* set hostname to container id */
    sethostname(cfg->id, strlen(cfg->id));

    /* exec the requested command */
    char *args[] = { cfg->command, NULL };
    execv(cfg->command, args);
    perror("execv");
    return 1;
}

/* ── ioctl helpers ────────────────────────────────────────── */

int register_with_monitor(int monitor_fd, const char *container_id,
                           pid_t host_pid, unsigned long soft_limit_bytes,
                           unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0) return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id,
                             pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0) return -1;
    return 0;
}

/* ── spawn one container ──────────────────────────────────── */

static container_record_t *spawn_container(supervisor_ctx_t *ctx,
                                            const control_request_t *req)
{
    int pipefd[2];
    if (pipe(pipefd) != 0) { perror("pipe"); return NULL; }

    /* build child config on heap (child stack needs it alive) */
    child_config_t *cfg = calloc(1, sizeof(*cfg));
    if (!cfg) { close(pipefd[0]); close(pipefd[1]); return NULL; }
    strncpy(cfg->id,      req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs,  req->rootfs,       PATH_MAX - 1);
    strncpy(cfg->command, req->command,      CHILD_COMMAND_LEN - 1);
    cfg->nice_value    = req->nice_value;
    cfg->log_write_fd  = pipefd[1];

    /* allocate stack for clone */
    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        free(cfg);
        close(pipefd[0]); close(pipefd[1]);
        return NULL;
    }

    int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t pid = clone(child_fn, stack + STACK_SIZE, flags, cfg);
    free(stack);
    close(pipefd[1]);   /* supervisor doesn't write to child's pipe */

    if (pid < 0) {
        perror("clone");
        free(cfg);
        close(pipefd[0]);
        return NULL;
    }
    free(cfg);

    /* create log directory + file */
    mkdir(LOG_DIR, 0755);
    container_record_t *rec = calloc(1, sizeof(*rec));
    if (!rec) { close(pipefd[0]); return NULL; }
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->host_pid          = pid;
    rec->started_at        = time(NULL);
    rec->state             = CONTAINER_RUNNING;
    rec->soft_limit_bytes  = req->soft_limit_bytes;
    rec->hard_limit_bytes  = req->hard_limit_bytes;
    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req->container_id);

    /* register with kernel monitor */
    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd, rec->id, pid,
                              rec->soft_limit_bytes, rec->hard_limit_bytes);

    /* insert into metadata list */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* start producer thread for this container's pipe */
    producer_arg_t *pa = malloc(sizeof(*pa));
    if (pa) {
        pa->pipe_fd = pipefd[0];
        strncpy(pa->container_id, req->container_id, CONTAINER_ID_LEN - 1);
        pa->ctx = ctx;
        pthread_t pt;
        pthread_create(&pt, NULL, producer_thread, pa);
        pthread_detach(pt);
    } else {
        close(pipefd[0]);
    }

    return rec;
}

/* ── SIGCHLD handler (reap zombies) ───────────────────────── */

static supervisor_ctx_t *g_ctx = NULL;

static void sigchld_handler(int sig)
{
    (void)sig;
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (!g_ctx) continue;
        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *c = g_ctx->containers;
        while (c) {
            if (c->host_pid == pid) {
                if (WIFEXITED(status)) {
                    c->exit_code = WEXITSTATUS(status);
                    c->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    c->exit_signal = WTERMSIG(status);
                    if (c->stop_requested)
                        c->state = CONTAINER_STOPPED;
                    else
                        c->state = CONTAINER_KILLED;
                }
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
        /* unregister from kernel monitor */
        if (g_ctx->monitor_fd >= 0 && c)
            unregister_from_monitor(g_ctx->monitor_fd, c->id, pid);
    }
}

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx) g_ctx->should_stop = 1;
}

/* ── handle one client connection ─────────────────────────── */

static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    if (recv(client_fd, &req, sizeof(req), MSG_WAITALL) != sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, CONTROL_MESSAGE_LEN, "bad request");
        send(client_fd, &resp, sizeof(resp), 0);
        return;
    }

    switch (req.kind) {

    case CMD_START: {
        container_record_t *rec = spawn_container(ctx, &req);
        if (rec) {
            resp.status = 0;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "started container %s pid=%d", rec->id, rec->host_pid);
        } else {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN, "failed to start container");
        }
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }

    case CMD_RUN: {
        container_record_t *rec = spawn_container(ctx, &req);
        if (!rec) {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN, "failed to start container");
            send(client_fd, &resp, sizeof(resp), 0);
            break;
        }
        /* send ack first, then wait */
        resp.status = 0;
        snprintf(resp.message, CONTROL_MESSAGE_LEN,
                 "running container %s pid=%d", rec->id, rec->host_pid);
        send(client_fd, &resp, sizeof(resp), 0);

        /* block until container exits */
        int ws;
        waitpid(rec->host_pid, &ws, 0);
        pthread_mutex_lock(&ctx->metadata_lock);
        if (WIFEXITED(ws)) {
            rec->exit_code = WEXITSTATUS(ws);
            rec->state = CONTAINER_EXITED;
        } else if (WIFSIGNALED(ws)) {
            rec->exit_signal = WTERMSIG(ws);
            rec->state = rec->stop_requested ? CONTAINER_STOPPED : CONTAINER_KILLED;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        break;
    }

    case CMD_PS: {
        char buf[4096] = {0};
        int off = 0;
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-16s %-8s %-10s %-20s %s\n",
                        "ID", "PID", "STATE", "STARTED", "LOG");
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c && off < (int)sizeof(buf) - 1) {
            char tstr[32];
            struct tm *tm = localtime(&c->started_at);
            strftime(tstr, sizeof(tstr), "%H:%M:%S", tm);
            off += snprintf(buf + off, sizeof(buf) - off,
                            "%-16s %-8d %-10s %-20s %s\n",
                            c->id, c->host_pid,
                            state_to_string(c->state),
                            tstr, c->log_path);
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp.status = 0;
        strncpy(resp.message, buf, CONTROL_MESSAGE_LEN - 1);
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }

    case CMD_LOGS: {
        char log_path[PATH_MAX] = {0};
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c) {
            if (strncmp(c->id, req.container_id, CONTAINER_ID_LEN) == 0) {
                strncpy(log_path, c->log_path, PATH_MAX - 1);
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (log_path[0] == '\0') {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "container %s not found", req.container_id);
            send(client_fd, &resp, sizeof(resp), 0);
            break;
        }
        resp.status = 0;
        snprintf(resp.message, CONTROL_MESSAGE_LEN,
                 "log: %s", log_path);
        send(client_fd, &resp, sizeof(resp), 0);
        /* stream log file */
        int lfd = open(log_path, O_RDONLY);
        if (lfd >= 0) {
            char lbuf[4096];
            ssize_t n;
            while ((n = read(lfd, lbuf, sizeof(lbuf))) > 0)
                send(client_fd, lbuf, n, 0);
            close(lfd);
        }
        break;
    }

    case CMD_STOP: {
        pid_t target = -1;
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c) {
            if (strncmp(c->id, req.container_id, CONTAINER_ID_LEN) == 0) {
                target = c->host_pid;
                c->stop_requested = 1;
                c->state = CONTAINER_STOPPED;
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (target < 0) {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "container %s not found", req.container_id);
        } else {
            kill(target, SIGTERM);
            resp.status = 0;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "sent SIGTERM to container %s (pid=%d)",
                     req.container_id, target);
        }
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, CONTROL_MESSAGE_LEN, "unknown command");
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }
}

/* ── supervisor main loop ─────────────────────────────────── */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc; perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* open kernel monitor */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] Warning: cannot open /dev/container_monitor: %s\n",
                strerror(errno));

    /* UNIX domain socket */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(ctx.server_fd, 8) < 0) { perror("listen"); return 1; }

    /* signals */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = sigterm_handler;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    /* start logger consumer thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) { errno = rc; perror("pthread_create"); return 1; }

    fprintf(stderr, "[supervisor] Ready. rootfs=%s socket=%s\n",
            rootfs, CONTROL_PATH);

    /* event loop */
    while (!ctx.should_stop) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);
        struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
        int sel = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }
        if (sel == 0) continue;   /* timeout — check should_stop */

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }
        handle_client(&ctx, client_fd);
        close(client_fd);
    }

    fprintf(stderr, "[supervisor] Shutting down...\n");

    /* stop all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING || c->state == CONTAINER_STARTING) {
            c->stop_requested = 1;
            kill(c->host_pid, SIGTERM);
        }
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    sleep(1);   /* give containers a moment to exit */

    /* shut down logging pipeline */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    /* free metadata */
    pthread_mutex_lock(&ctx.metadata_lock);
    c = ctx.containers;
    while (c) {
        container_record_t *next = c->next;
        free(c);
        c = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    fprintf(stderr, "[supervisor] Exited cleanly.\n");
    return 0;
}

/* ── CLI client ───────────────────────────────────────────── */

static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect (is supervisor running?)");
        close(fd);
        return 1;
    }

    if (send(fd, req, sizeof(*req), 0) != sizeof(*req)) {
        perror("send");
        close(fd);
        return 1;
    }

    control_response_t resp;
    if (recv(fd, &resp, sizeof(resp), MSG_WAITALL) == sizeof(resp)) {
        printf("%s\n", resp.message);
        /* for logs command, read extra streamed data */
        if (req->kind == CMD_LOGS) {
            char buf[4096];
            ssize_t n;
            while ((n = recv(fd, buf, sizeof(buf), 0)) > 0)
                fwrite(buf, 1, n, stdout);
        }
    }

    close(fd);
    return (resp.status == 0) ? 0 : 1;
}

/* ── CLI command helpers ──────────────────────────────────── */

static int cmd_start(int argc, char *argv[])
{
    if (argc < 5) { usage(argv[0]); return 1; }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    strncpy(req.rootfs,       argv[3], PATH_MAX - 1);
    strncpy(req.command,      argv[4], CHILD_COMMAND_LEN - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    if (argc < 5) { usage(argv[0]); return 1; }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    strncpy(req.rootfs,       argv[3], PATH_MAX - 1);
    strncpy(req.command,      argv[4], CHILD_COMMAND_LEN - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    if (argc < 3) { usage(argv[0]); return 1; }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    if (argc < 3) { usage(argv[0]); return 1; }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    return send_control_request(&req);
}

/* ── main ─────────────────────────────────────────────────── */

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }
    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) { usage(argv[0]); return 1; }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);
    usage(argv[0]);
    return 1;
}
