Multi-Container Runtime

1. Team Information

Name: shreya mohanty, SRN: PES2UG24CS482
Name: Satvik phadke, SRN: PES2UG24CS450
2. Build, Load, and Run Instructions
Build

cd boilerplate make

Load kernel module

sudo insmod monitor.ko ls /dev/container_monitor

Start supervisor (Terminal 1)

sudo ./engine supervisor ./rootfs

Launch containers (Terminal 2)

sudo ./engine start alpha (pwd)/roof fs/rootfs/cpu_hog

List containers

sudo ./engine ps

View logs

sudo ./engine logs alpha

Stop a container

sudo ./engine stop alpha

Stop supervisor

Press Ctrl+C in Terminal 1

Unload module

sudo rmmod monitor

Demo Screenshots

Screenshots for all tasks are in the all file section.

Engineering Analysis

Isolation Mechanisms

The runtime uses clone() with CLONE_NEWPID, CLONE_NEWUTS, and CLONE_NEWNS flags to give each container its own PID namespace (so container PIDs start at 1), UTS namespace (its own hostname), and mount namespace (isolated filesystem view). chroot() then locks the process into the Alpine rootfs directory. The host kernel itself is still shared — containers use the same kernel, same system calls, and same physical memory as the host. Only the view of resources is isolated, not the kernel itself. This is fundamentally different from a VM.

Supervisor and Process Lifecycle

A long-running supervisor is necessary because containers are child processes that must be reaped when they exit. Without a persistent parent, they become zombies. The supervisor uses SIGCHLD with SA_NOCLDSTOP to get notified when any container exits, then calls waitpid(-1, &status, WNOHANG) in a loop to reap all exited children and update their metadata records.

IPC, Threads, and Synchronization

Two IPC mechanisms are used:

Pipes: between each container and the supervisor for log capture. stdout/stderr of the container are dup2'd into the write end; the supervisor reads from the read end in a producer thread.
UNIX domain socket: between the CLI client and the supervisor for control commands (start, stop, ps, logs).
The bounded buffer uses a mutex + two condition variables (not_full, not_empty). Without the mutex, concurrent producer and consumer threads would corrupt the head/tail indices. Without condition variables, threads would busy-wait instead of sleeping, wasting CPU. A semaphore could also work but condition variables allow the shutdown broadcast to wake all waiting threads at once.

Memory Management and Enforcement

RSS (Resident Set Size) measures the physical memory actually mapped and present in RAM for a process. It does NOT include swap, shared libraries counted only once, or memory that has been allocated but not yet accessed (lazy allocation). Soft limits trigger a warning so operators can see a container is getting close to its budget without disrupting it. Hard limits kill the process because once it exceeds the budget, continued growth could starve other containers or the host. Enforcement must be in kernel space because a user-space monitor could be fooled or killed by the very process it is watching. The kernel timer fires independently and cannot be bypassed by the container.

Scheduling Behavior

[Fill in with your actual experiment numbers] In Experiment 2, high_prio (nice=0) received approximately 99% CPU while low_prio (nice=19) received approximately 99% CPU when running simultaneously. The Linux CFS (Completely Fair Scheduler) uses nice values to assign virtual runtime weights: nice=19 gives roughly 1/20th the weight of nice=0. This matches the observed CPU share difference. Completion time for nice=0 was 59.262s vs 59.723s for nice=19, confirming that lower priority containers are significantly slower when competing for CPU.

5. Design Decisions and Tradeoffs

Namespace Isolation

Choice: clone() with CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS Tradeoff: Network namespace (CLONE_NEWNET) is not isolated, so containers share the host network stack. Justification: Network isolation requires veth pairs and bridge setup which is out of scope; PID/UTS/mount isolation is sufficient for the project goals.

Supervisor Architecture

Choice: Single-threaded event loop with select() for accepting connections. Tradeoff: Cannot handle multiple CLI clients simultaneously. Justification: CLI commands are short-lived and infrequent; a single-threaded loop is simpler and avoids additional synchronization complexity.

IPC / Logging

Choice: UNIX domain socket for control, pipes for log capture, bounded buffer with mutex+condvar for producer-consumer. Tradeoff: Log chunks are limited to LOG_CHUNK_SIZE (4096 bytes); very long lines may be split across two buffer entries. Justification: UNIX sockets are the natural IPC for local supervisor-client communication. The bounded buffer decouples container output speed from log file write speed.

Kernel Monitor

Choice: mutex over spinlock for proc_list protection. Tradeoff: Slightly higher overhead than a spinlock. Justification: The timer callback calls get_task_mm() which can sleep. Spinlocks forbid sleeping in the locked section, so a mutex is required here.

Scheduling Experiments

Choice: nice values via --nice flag rather than CPU affinity. Tradeoff: nice values affect priority but not CPU pinning; on a multi-core machine both processes may run in parallel. Justification: nice values directly demonstrate CFS weight-based scheduling which is the core Linux scheduling concept relevant to this project.








