/*
 * reservoir.h -- Shared-memory reservoir sampler for Linux
 *
 * Maintains a uniform random sample of k items drawn from an unbounded stream
 * (Algorithm R) in fixed memory: each observed item either fills an empty slot
 * (while the reservoir is not yet full) or, once full, replaces a uniformly
 * random slot with probability k/i for the i-th item.  A shared xorshift64 RNG
 * lives in the header so concurrent processes sample one consistent reservoir.
 * A write-preferring futex rwlock with reader-slot dead-process recovery guards
 * mutation.  Items are stored inline, truncated to item_size bytes.
 *
 * A reservoir may instead be created in WEIGHTED mode (Efraimidis-Spirakis
 * A-Res): each item carries a weight and is kept with probability proportional
 * to it, via a shared min-heap keyed by u^(1/weight) over the same items region.
 *
 * Layout (uniform):  Header -> reader_slots[1024] -> items[k * (8 + item_size)]
 * Layout (weighted): Header -> reader_slots[1024] -> heap[k * 16] -> items[...]
 */

#ifndef RSV_H
#define RSV_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <math.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/syscall.h>
#include <linux/futex.h>
#include <pthread.h>

#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#error "reservoir.h: requires little-endian architecture"
#endif


/* ================================================================
 * Constants
 * ================================================================ */

#define RSV_MAGIC        0x52565352U  /* Reservoir */
#define RSV_VERSION      1
#define RSV_ERR_BUFLEN   256
#ifndef RSV_READER_SLOTS
#define RSV_READER_SLOTS 1024         /* max concurrent reader processes for dead-process recovery */
#endif
#define RSV_MIN_K        1
#define RSV_MAX_K        0x40000000ULL   /* 2^30 reservoir slots cap */
#define RSV_MIN_ITEM     1
#define RSV_MAX_ITEM     0x10000ULL      /* 64 KiB max bytes per stored item */

#define RSV_MODE_UNIFORM  0U             /* Algorithm R: uniform sample of the stream */
#define RSV_MODE_WEIGHTED 1U             /* Efraimidis-Spirakis A-Res: weighted sample */

#define RSV_ERR(fmt, ...) do { if (errbuf) snprintf(errbuf, RSV_ERR_BUFLEN, fmt, ##__VA_ARGS__); } while (0)

/* ================================================================
 * Structs
 * ================================================================ */

/* Per-process slot for dead-process recovery.  Each shared rwlock counter
 * (the main rwlock-reader count, rwlock_waiters, rwlock_writers_waiting)
 * is mirrored here so a wrlock timeout can attribute and reverse a dead
 * process's contribution instead of waiting for the slow per-op timeout
 * drain. */
typedef struct {
    uint32_t pid;            /* 0 = unclaimed */
    uint32_t subcount;       /* in-flight rdlock acquisitions for this process */
    uint32_t waiters_parked; /* contribution to hdr->rwlock_waiters         */
    uint32_t writers_parked; /* contribution to hdr->rwlock_writers_waiting */
} RsvReaderSlot;

struct RsvHeader {
    uint32_t magic, version;          /* 0,4 */
    uint32_t mode;                    /* 8   RSV_MODE_UNIFORM | RSV_MODE_WEIGHTED */
    uint32_t _pad1;                   /* 12 */
    uint64_t k;                       /* 16  reservoir capacity (number of slots) */
    uint64_t item_size;               /* 24  max bytes stored per item */
    uint64_t capacity;                /* 32  == k (family stats parity) */
    uint64_t stride;                  /* 40  bytes per slot: 8 (len) + item_size, 8-aligned */
    uint64_t total_size;              /* 48 */
    uint64_t reader_slots_off;        /* 56 */
    uint64_t slots_off;               /* 64 */
    uint32_t rwlock;                  /* 72 */
    uint32_t rwlock_waiters;          /* 76 */
    uint32_t rwlock_writers_waiting;  /* 80 */
    uint32_t slotless_readers;  /* live readers holding the lock with no reader-slot (was padding) */
    uint64_t stat_ops;                /* 88 */
    uint64_t seen;                    /* 96  total items observed (mutable, under wrlock) */
    uint64_t rng_state;               /* 104 xorshift64 RNG state (mutable, under wrlock) */
    uint64_t heap_off;                /* 112 weighted A-Res min-heap region (0 in uniform mode) */
    uint8_t  _pad[136];               /* 120..255 */
};
typedef struct RsvHeader RsvHeader;

_Static_assert(sizeof(RsvHeader) == 256, "RsvHeader must be 256 bytes");

/* Weighted (A-Res) min-heap entry: `key` = u^(1/weight), the smallest at the
 * root so a heavier arrival can evict it.  `item` indexes the items region (a
 * fixed cell that stays put -- only these 16-byte entries move during sift). */
typedef struct { double key; uint64_t item; } RsvHeapEnt;
_Static_assert(sizeof(RsvHeapEnt) == 16, "RsvHeapEnt must be 16 bytes");

/* ---- Process-local handle ---- */

typedef struct RsvHandle {
    RsvHeader     *hdr;
    RsvReaderSlot *reader_slots;  /* RSV_READER_SLOTS entries */
    void         *base;          /* mmap base */
    uint64_t      slots_off;   /* validated geometry, cached: never re-read from the peer-writable header */
    uint64_t      k;           /* reservoir size (cached) */
    uint64_t      item_size;   /* max bytes per item (cached) */
    uint64_t      stride;      /* per-slot byte stride (cached) */
    uint32_t      mode;        /* RSV_MODE_* sampling mode (cached from validated header) */
    uint64_t      heap_off;    /* weighted min-heap region offset (cached), 0 if uniform */
    size_t        mmap_size;
    char         *path;          /* backing file path (strdup'd) */
    int           backing_fd;    /* memfd or reopened-fd to close on destroy, -1 for file/anon */
    uint32_t      my_slot_idx;   /* UINT32_MAX if all slots taken (no recovery for this handle) */
    uint32_t      cached_pid;    /* getpid() cached at last slot claim */
    uint32_t      cached_fork_gen; /* rsv_fork_gen value at last slot claim */
    uint32_t slotless_held; /* rwlock read-locks held with no reader-slot */
} RsvHandle;

/* ================================================================
 * Futex-based write-preferring read-write lock
 * with reader-slot dead-process recovery
 * ================================================================ */

#define RSV_RWLOCK_SPIN_LIMIT 32
#define RSV_LOCK_TIMEOUT_SEC  2  /* FUTEX_WAIT timeout for stale lock detection */

static inline void rsv_rwlock_spin_pause(void) {
#if defined(__x86_64__) || defined(__i386__)
    __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#else
    __asm__ volatile("" ::: "memory");
#endif
}

/* Extract writer PID from rwlock value (lower 31 bits when write-locked). */
#define RSV_RWLOCK_WRITER_BIT 0x80000000U
#define RSV_RWLOCK_PID_MASK   0x7FFFFFFFU
#define RSV_RWLOCK_WR(pid)    (RSV_RWLOCK_WRITER_BIT | ((uint32_t)(pid) & RSV_RWLOCK_PID_MASK))

/* Check if a PID is alive. Returns 1 if alive or unknown, 0 if definitely dead. */
/* Liveness via kill(pid,0). NOTE: cannot detect PID reuse -- if a dead
 * lock-holder's PID is recycled to an unrelated live process before recovery
 * runs, this reports "alive" and that slot's orphaned contribution is not
 * reclaimed until the recycled process exits. Robust detection would require
 * a per-slot process-start-time epoch (a header-layout/version change).
 * Documented under "Crash Safety" in the POD. */
static inline int rsv_pid_alive(uint32_t pid) {
    if (pid == 0) return 1; /* no owner recorded, assume alive */
    return !(kill((pid_t)pid, 0) == -1 && errno == ESRCH);
}

/* Force-recover a stale write lock left by a dead process.
 * CAS to OUR pid to hold the lock while fixing shared state, then release.
 * Using our pid (not a bare WRITER_BIT sentinel) means a subsequent
 * recovering process can detect and re-recover if we crash mid-recovery. */
static inline void rsv_recover_stale_lock(RsvHandle *h, uint32_t observed_rwlock) {
    RsvHeader *hdr = h->hdr;
    uint32_t mypid = RSV_RWLOCK_WR((uint32_t)getpid());
    if (!__atomic_compare_exchange_n(&hdr->rwlock, &observed_rwlock,
            mypid, 0, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
        return;
    /* We now hold the write lock as mypid.  No additional shared state needs
     * repair here (this module has no seqlock); just release the lock. */
    __atomic_store_n(&hdr->rwlock, 0, __ATOMIC_RELEASE);
    if (__atomic_load_n(&hdr->rwlock_waiters, __ATOMIC_RELAXED) > 0)
        syscall(SYS_futex, &hdr->rwlock, FUTEX_WAKE, INT_MAX, NULL, NULL, 0);
}

static const struct timespec rsv_lock_timeout = { RSV_LOCK_TIMEOUT_SEC, 0 };

/* Process-global fork-generation counter.  Incremented in the pthread_atfork
 * child callback so every open handle detects a fork transition on the next
 * lock call without paying a getpid() syscall on the hot path. */
static uint32_t rsv_fork_gen = 1;
static pthread_once_t rsv_atfork_once = PTHREAD_ONCE_INIT;
static void rsv_on_fork_child(void) {
    __atomic_add_fetch(&rsv_fork_gen, 1, __ATOMIC_RELAXED);
}
static void rsv_atfork_init(void) {
    pthread_atfork(NULL, NULL, rsv_on_fork_child);
}

/* Ensure this process owns a reader slot.  Called from the lock helpers so
 * that fork()'d children pick up their own slot lazily instead of sharing
 * the parent's.  Hot-path is a single relaxed load + compare; only on a
 * fork-generation mismatch do we touch getpid() and scan slots. */
static inline void rsv_claim_reader_slot(RsvHandle *h) {
    uint32_t cur_gen = __atomic_load_n(&rsv_fork_gen, __ATOMIC_RELAXED);
    if (__builtin_expect(cur_gen == h->cached_fork_gen && h->my_slot_idx != UINT32_MAX, 1))
        return;
    /* Cold path -- register the atfork hook once per process, then claim. */
    pthread_once(&rsv_atfork_once, rsv_atfork_init);
    /* Re-read after pthread_once: rsv_on_fork_child may have bumped it. */
    cur_gen = __atomic_load_n(&rsv_fork_gen, __ATOMIC_RELAXED);
    uint32_t now_pid = (uint32_t)getpid();
    h->cached_pid = now_pid;
    if (cur_gen != h->cached_fork_gen) h->slotless_held = 0;  /* fork: child holds none of the parent's slotless read locks */
    h->cached_fork_gen = cur_gen;
    h->my_slot_idx = UINT32_MAX;
    uint32_t start = now_pid % RSV_READER_SLOTS;
    for (uint32_t i = 0; i < RSV_READER_SLOTS; i++) {
        uint32_t s = (start + i) % RSV_READER_SLOTS;
        uint32_t expected = 0;
        if (__atomic_compare_exchange_n(&h->reader_slots[s].pid,
                &expected, now_pid, 0,
                __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
            /* Zero all mirror fields, not just subcount: a SIGKILL'd
             * predecessor may have left waiters_parked/writers_parked
             * non-zero, and rsv_recover_dead_readers won't drain them
             * once we own the slot (the CAS expects the dead PID). */
            __atomic_store_n(&h->reader_slots[s].subcount, 0, __ATOMIC_RELAXED);
            __atomic_store_n(&h->reader_slots[s].waiters_parked, 0, __ATOMIC_RELAXED);
            __atomic_store_n(&h->reader_slots[s].writers_parked, 0, __ATOMIC_RELAXED);
            h->my_slot_idx = s;
            return;
        }
    }
    /* Table full -- leave my_slot_idx = UINT32_MAX so we silently skip
     * tracking for this handle (lock still works; just no recovery). */
}

/* Atomically subtract `sub` from a counter, capped at 0 (never underflows). */
static inline void rsv_atomic_sub_cap(uint32_t *p, uint32_t sub) {
    if (!sub) return;
    uint32_t cur = __atomic_load_n(p, __ATOMIC_RELAXED);
    for (;;) {
        uint32_t want = (cur > sub) ? cur - sub : 0;
        if (__atomic_compare_exchange_n(p, &cur, want,
                1, __ATOMIC_RELAXED, __ATOMIC_RELAXED))
            return;
    }
}

/* Try to claim a dead slot (CAS pid -> 0) and drain its parked-waiter
 * contributions back to the global counters.  A no-op if the slot was stolen
 * by another recoverer or had no waiter contribution to drain.
 *
 * Note: subcount/waiters_parked/writers_parked are NOT zeroed here.
 * Between our CAS and a follow-up store, a new process could claim the
 * slot and start populating these fields -- our stores would clobber its
 * state.  rsv_claim_reader_slot zeros all three on every claim, so
 * leaving stale values is harmless. */
static inline void rsv_drain_dead_slot(RsvHandle *h, uint32_t i, uint32_t pid) {
    RsvHeader *hdr = h->hdr;
    uint32_t expected = pid;
    /* ACQ_REL on success: RELEASE publishes pid=0 to other observers;
     * ACQUIRE syncs us with prior writes from the dead process to
     * waiters_parked/writers_parked.  On weakly-ordered archs (aarch64)
     * a plain RELAXED load before the CAS could miss those writes;
     * loading them after the CAS keeps them inside the acquire window. */
    if (!__atomic_compare_exchange_n(&h->reader_slots[i].pid, &expected, 0,
            0, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
        return;
    uint32_t wp    = __atomic_load_n(&h->reader_slots[i].waiters_parked, __ATOMIC_RELAXED);
    uint32_t writp = __atomic_load_n(&h->reader_slots[i].writers_parked, __ATOMIC_RELAXED);
    if (wp)    rsv_atomic_sub_cap(&hdr->rwlock_waiters, wp);
    if (writp) rsv_atomic_sub_cap(&hdr->rwlock_writers_waiting, writp);
}

/* Scan reader slots for dead-process recovery.
 *
 * For each dead PID with non-zero contributions to the shared rwlock,
 * rwlock_waiters, or rwlock_writers_waiting counters, drain its share back
 * out so live processes don't have to wait for the slow per-op timeout
 * decrement to drain it for them.
 *
 * For the main rwlock counter we use the "no live reader holds -> force-
 * reset to 0" trick (precise) because per-process attribution of the
 * subcount is racy across the inc-counter-then-inc-subcount window. */
static inline void rsv_recover_dead_readers(RsvHandle *h) {
    if (!h->reader_slots) return;
    RsvHeader *hdr = h->hdr;
    int any_live_reader = 0;
    int found_dead_reader = 0;

    /* Pass 1: classify slots.  Slots with dead pid and sc == 0 (no rwlock
     * contribution to lose) are wiped immediately to free the slot for
     * future claimants and drain any orphan parked-waiter counters.  Slots
     * with dead pid and sc > 0 are left intact in this pass: if force-
     * reset cannot fire (because a live reader is concurrently present),
     * wiping the dead slot would lose the only record of its orphan
     * rwlock contribution and strand writers permanently once the live
     * reader releases. */
    for (uint32_t i = 0; i < RSV_READER_SLOTS; i++) {
        uint32_t pid = __atomic_load_n(&h->reader_slots[i].pid, __ATOMIC_ACQUIRE);
        if (pid == 0) continue;
        uint32_t sc = __atomic_load_n(&h->reader_slots[i].subcount, __ATOMIC_RELAXED);
        if (rsv_pid_alive(pid)) {
            if (sc > 0) any_live_reader = 1;
            continue;
        }
        if (sc > 0) { found_dead_reader = 1; continue; }
        rsv_drain_dead_slot(h, i, pid);
    }

    /* Pass 2: only if force-reset will fire.  Issue the rwlock force-
     * reset CAS FIRST, while the window since pass 1's last scan is
     * still narrow (a handful of instructions, as in the original
     * single-pass code).  A new reader that started rdlock between
     * pass 1's scan and the CAS will either:
     *   (a) have already CAS'd rwlock from cur to cur+1 -- our CAS then
     *       fails (cur mismatched), recovery yields and a future
     *       cycle retries; or
     *   (b) be still in the subcount-bump phase -- our CAS sees the
     *       stale cur and resets to 0; the new reader's subsequent CAS
     *       rwlock(0 -> 1) succeeds cleanly.
     * Only after the CAS resolves do we wipe the deferred dead slots,
     * keeping that work outside the race-sensitive window. */
    /* A live reader with no slot (table was full) is invisible to the scan
     * above but still holds a +1 in the lock word; never force-reset under it. */
    if (__atomic_load_n(&hdr->slotless_readers, __ATOMIC_RELAXED) > 0)
        any_live_reader = 1;
    if (found_dead_reader && !any_live_reader) {
        /* ACQUIRE: a late reader's subcount++ (before its rwlock CAS) is then visible below. */
        uint32_t cur = __atomic_load_n(&hdr->rwlock, __ATOMIC_ACQUIRE);
        int drain_ok = 1;   /* keep dead slots if the reset doesn't fire */
        if (cur > 0 && cur < RSV_RWLOCK_WRITER_BIT) {
            /* Re-scan for a live reader (fail-safe: only suppresses a reset). */
            int live_now = __atomic_load_n(&hdr->slotless_readers, __ATOMIC_RELAXED) > 0;
            for (uint32_t i = 0; !live_now && i < RSV_READER_SLOTS; i++) {
                uint32_t p = __atomic_load_n(&h->reader_slots[i].pid, __ATOMIC_ACQUIRE);
                if (p && rsv_pid_alive(p) &&
                    __atomic_load_n(&h->reader_slots[i].subcount, __ATOMIC_RELAXED) > 0)
                    live_now = 1;
            }
            if (live_now) {
                drain_ok = 0;
            } else if (__atomic_compare_exchange_n(&hdr->rwlock, &cur, 0,
                    0, __ATOMIC_RELEASE, __ATOMIC_RELAXED)) {
                if (__atomic_load_n(&hdr->rwlock_waiters, __ATOMIC_RELAXED) > 0)
                    syscall(SYS_futex, &hdr->rwlock, FUTEX_WAKE, INT_MAX, NULL, NULL, 0);
            } else {
                drain_ok = 0;   /* rwlock changed under us -- shares may still be live */
            }
        }
        if (drain_ok) {
            for (uint32_t i = 0; i < RSV_READER_SLOTS; i++) {
                uint32_t p = __atomic_load_n(&h->reader_slots[i].pid, __ATOMIC_ACQUIRE);
                if (p == 0 || rsv_pid_alive(p)) continue;
                rsv_drain_dead_slot(h, i, p);
            }
        }
    }
}

/* Inspect the lock word after a futex-wait timeout.  If a dead writer
 * holds it, force-recover the lock.  Otherwise drain dead readers' shares
 * of the rwlock/waiter counters.  Called from rdlock and wrlock ETIMEDOUT
 * branches -- identical recovery logic in both. */
static inline void rsv_recover_after_timeout(RsvHandle *h) {
    RsvHeader *hdr = h->hdr;
    uint32_t val = __atomic_load_n(&hdr->rwlock, __ATOMIC_RELAXED);
    if (val >= RSV_RWLOCK_WRITER_BIT) {
        uint32_t pid = val & RSV_RWLOCK_PID_MASK;
        if (!rsv_pid_alive(pid))
            rsv_recover_stale_lock(h, val);
    } else {
        rsv_recover_dead_readers(h);
    }
}

/* Park/unpark helpers: bump the global waiter counters together with this
 * process's mirrored slot counters so a wrlock-timeout recovery scan can
 * attribute and reverse a dead PID's contribution.  Kept paired to make
 * accidental drift between global and per-slot counts impossible. */
static inline void rsv_park_reader(RsvHandle *h) {
    if (h->my_slot_idx != UINT32_MAX)
        __atomic_add_fetch(&h->reader_slots[h->my_slot_idx].waiters_parked, 1, __ATOMIC_RELAXED);
    __atomic_add_fetch(&h->hdr->rwlock_waiters, 1, __ATOMIC_RELAXED);
}
static inline void rsv_unpark_reader(RsvHandle *h) {
    __atomic_sub_fetch(&h->hdr->rwlock_waiters, 1, __ATOMIC_RELAXED);
    if (h->my_slot_idx != UINT32_MAX)
        __atomic_sub_fetch(&h->reader_slots[h->my_slot_idx].waiters_parked, 1, __ATOMIC_RELAXED);
}
static inline void rsv_park_writer(RsvHandle *h) {
    if (h->my_slot_idx != UINT32_MAX) {
        __atomic_add_fetch(&h->reader_slots[h->my_slot_idx].waiters_parked, 1, __ATOMIC_RELAXED);
        __atomic_add_fetch(&h->reader_slots[h->my_slot_idx].writers_parked, 1, __ATOMIC_RELAXED);
    }
    __atomic_add_fetch(&h->hdr->rwlock_waiters, 1, __ATOMIC_RELAXED);
    __atomic_add_fetch(&h->hdr->rwlock_writers_waiting, 1, __ATOMIC_RELAXED);
}
static inline void rsv_unpark_writer(RsvHandle *h) {
    __atomic_sub_fetch(&h->hdr->rwlock_waiters, 1, __ATOMIC_RELAXED);
    __atomic_sub_fetch(&h->hdr->rwlock_writers_waiting, 1, __ATOMIC_RELAXED);
    if (h->my_slot_idx != UINT32_MAX) {
        __atomic_sub_fetch(&h->reader_slots[h->my_slot_idx].waiters_parked, 1, __ATOMIC_RELAXED);
        __atomic_sub_fetch(&h->reader_slots[h->my_slot_idx].writers_parked, 1, __ATOMIC_RELAXED);
    }
}

/* Reader accounting: a reader mirrors its +1 in the lock word so dead-reader
 * recovery can see it. A slotted reader uses its slot subcount; a reader that
 * could not claim a slot (table full) uses the global hdr->slotless_readers,
 * so recovery's force-reset never fires out from under it. leave() peels
 * slotless first so a later slot claim cannot misattribute the decrement. */
static inline void rsv_reader_enter(RsvHandle *h) {
    if (h->my_slot_idx != UINT32_MAX) {
        __atomic_add_fetch(&h->reader_slots[h->my_slot_idx].subcount, 1, __ATOMIC_RELAXED);
    } else {
        __atomic_add_fetch(&h->hdr->slotless_readers, 1, __ATOMIC_RELAXED);
        h->slotless_held++;
    }
}
static inline void rsv_reader_leave(RsvHandle *h) {
    if (h->slotless_held > 0) {
        h->slotless_held--;
        __atomic_sub_fetch(&h->hdr->slotless_readers, 1, __ATOMIC_RELAXED);
    } else if (h->my_slot_idx != UINT32_MAX) {
        __atomic_sub_fetch(&h->reader_slots[h->my_slot_idx].subcount, 1, __ATOMIC_RELAXED);
    }
}

static inline void rsv_rwlock_rdlock(RsvHandle *h) {
    rsv_claim_reader_slot(h);
    RsvHeader *hdr = h->hdr;
    uint32_t *lock = &hdr->rwlock;
    uint32_t *writers_waiting = &hdr->rwlock_writers_waiting;
    /* Claim subcount BEFORE bumping the shared rwlock counter.  This way
     * a concurrent writer-side recovery scan that sees our PID alive with
     * subcount > 0 will (correctly) defer force-reset, even while we are
     * still spinning trying to win the rwlock CAS.  Without this, a reader
     * killed between rwlock CAS-success and subcount++ would let recovery
     * force-reset rwlock to 0 underneath us, causing a UINT32_MAX wrap on
     * our eventual rdunlock dec. */
    rsv_reader_enter(h);
    for (int spin = 0; ; spin++) {
        uint32_t cur = __atomic_load_n(lock, __ATOMIC_RELAXED);
        /* Write-preferring: when lock is free (cur==0) and writers are
         * waiting, yield to let the writer acquire. When readers are
         * already active (cur>=1), new readers may join freely. */
        if (cur > 0 && cur < RSV_RWLOCK_WRITER_BIT) {
            if (__atomic_compare_exchange_n(lock, &cur, cur + 1,
                    1, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
                return;
        } else if (cur == 0 && !__atomic_load_n(writers_waiting, __ATOMIC_RELAXED)) {
            if (__atomic_compare_exchange_n(lock, &cur, 1,
                    1, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
                return;
        }
        if (__builtin_expect(spin < RSV_RWLOCK_SPIN_LIMIT, 1)) {
            rsv_rwlock_spin_pause();
            continue;
        }
        rsv_park_reader(h);
        cur = __atomic_load_n(lock, __ATOMIC_RELAXED);
        /* Sleep when write-locked OR when yielding to waiting writers */
        if (cur >= RSV_RWLOCK_WRITER_BIT || cur == 0) {
            long rc = syscall(SYS_futex, lock, FUTEX_WAIT, cur,
                              &rsv_lock_timeout, NULL, 0);
            if (rc == -1 && errno == ETIMEDOUT) {
                rsv_unpark_reader(h);
                rsv_recover_after_timeout(h);
                spin = 0;
                continue;
            }
        }
        rsv_unpark_reader(h);
        spin = 0;
    }
}

static inline void rsv_rwlock_rdunlock(RsvHandle *h) {
    RsvHeader *hdr = h->hdr;
    /* Release the shared counter BEFORE dropping our subcount so that
     * "any live PID with subcount > 0" is a reliable in-flight indicator
     * for the writer-side recovery scan.  Inverting these would create a
     * window where we still own a unit of rwlock but our slot subcount is
     * 0, letting recovery force-reset rwlock underneath us. */
    uint32_t after = __atomic_sub_fetch(&hdr->rwlock, 1, __ATOMIC_RELEASE);
    rsv_reader_leave(h);
    if (after == 0 && __atomic_load_n(&hdr->rwlock_waiters, __ATOMIC_RELAXED) > 0)
        syscall(SYS_futex, &hdr->rwlock, FUTEX_WAKE, INT_MAX, NULL, NULL, 0);
}

static inline void rsv_rwlock_wrlock(RsvHandle *h) {
    rsv_claim_reader_slot(h);  /* refresh cached_pid across fork */
    RsvHeader *hdr = h->hdr;
    uint32_t *lock = &hdr->rwlock;
    /* Encode PID in the rwlock word itself (0x80000000 | pid) to eliminate
     * any crash window between acquiring the lock and storing the owner. */
    uint32_t mypid = RSV_RWLOCK_WR(h->cached_pid);
    for (int spin = 0; ; spin++) {
        uint32_t expected = 0;
        if (__atomic_compare_exchange_n(lock, &expected, mypid,
                1, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
            return;
        if (__builtin_expect(spin < RSV_RWLOCK_SPIN_LIMIT, 1)) {
            rsv_rwlock_spin_pause();
            continue;
        }
        rsv_park_writer(h);
        uint32_t cur = __atomic_load_n(lock, __ATOMIC_RELAXED);
        if (cur != 0) {
            long rc = syscall(SYS_futex, lock, FUTEX_WAIT, cur,
                              &rsv_lock_timeout, NULL, 0);
            if (rc == -1 && errno == ETIMEDOUT) {
                rsv_unpark_writer(h);
                rsv_recover_after_timeout(h);
                spin = 0;
                continue;
            }
        }
        rsv_unpark_writer(h);
        spin = 0;
    }
}

static inline void rsv_rwlock_wrunlock(RsvHandle *h) {
    RsvHeader *hdr = h->hdr;
    __atomic_store_n(&hdr->rwlock, 0, __ATOMIC_RELEASE);
    if (__atomic_load_n(&hdr->rwlock_waiters, __ATOMIC_RELAXED) > 0)
        syscall(SYS_futex, &hdr->rwlock, FUTEX_WAKE, INT_MAX, NULL, NULL, 0);
}

/* ================================================================
 * Layout math + create / open / destroy
 *
 * Layout: Header -> reader_slots[1024] -> slots[k*(8+item_size)]
 * ================================================================ */

/* Single source of truth for the mmap region layout offsets. */
typedef struct { uint64_t reader_slots, slots; } RsvLayout;

static inline RsvLayout rsv_layout(void) {
    RsvLayout L;
    L.reader_slots = sizeof(RsvHeader);
    L.slots        = L.reader_slots + (uint64_t)RSV_READER_SLOTS * sizeof(RsvReaderSlot);
    L.slots        = (L.slots + 7) & ~(uint64_t)7;   /* 8-byte align the slots array */
    return L;
}

/* bytes per slot: an 8-byte length followed by item_size data bytes, 8-aligned */
static inline uint64_t rsv_stride(uint64_t item_size) {
    return (8 + item_size + 7) & ~(uint64_t)7;
}

/* Weighted mode inserts a k-entry min-heap between the reader slots and the
 * items region; uniform mode has no heap.  These are the single source of truth
 * for the region offsets, used by both init and validate (never trust a stored
 * derived field -- always re-derive from k + mode). */
static inline uint64_t rsv_heap_region_off(uint32_t mode) {
    return (mode == RSV_MODE_WEIGHTED) ? rsv_layout().slots : 0;
}
static inline uint64_t rsv_items_off(uint64_t k, uint32_t mode) {
    RsvLayout L = rsv_layout();
    if (mode == RSV_MODE_WEIGHTED) {
        uint64_t after_heap = L.slots + k * sizeof(RsvHeapEnt);
        return (after_heap + 7) & ~(uint64_t)7;   /* 8-align the items array */
    }
    return L.slots;
}
static inline uint64_t rsv_total_size(uint64_t k, uint64_t item_size, uint32_t mode) {
    return rsv_items_off(k, mode) + k * rsv_stride(item_size);
}

static inline void rsv_init_header(void *base, uint64_t k, uint64_t item_size, uint32_t mode, uint64_t total) {
    RsvLayout L = rsv_layout();
    RsvHeader *hdr = (RsvHeader *)base;
    /* Zero the header + reader-slot region (lock-recovery state); the slots array
       relies on the fresh mapping being OS zero-filled (all lengths 0 = empty). */
    memset(base, 0, (size_t)L.slots);
    hdr->magic            = RSV_MAGIC;
    hdr->version          = RSV_VERSION;
    hdr->mode             = mode;
    hdr->k                = k;
    hdr->item_size        = item_size;
    hdr->capacity         = k;
    hdr->stride           = rsv_stride(item_size);
    hdr->total_size       = total;
    hdr->reader_slots_off = L.reader_slots;
    hdr->heap_off         = rsv_heap_region_off(mode);
    hdr->slots_off        = rsv_items_off(k, mode);
    hdr->seen             = 0;
    /* seed the shared RNG from process + monotonic-clock entropy (never zero) */
    {
        struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
        uint64_t s = ((uint64_t)getpid() << 32) ^ (uint64_t)ts.tv_nsec ^ ((uint64_t)ts.tv_sec << 20);
        s ^= 0x9E3779B97F4A7C15ULL;
        hdr->rng_state = s ? s : 0x9E3779B97F4A7C15ULL;
    }
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
}

static inline uint8_t *rsv_slots(RsvHandle *h) {
    return (uint8_t *)((char *)h->base + h->slots_off);
}

/* Layer B trusted bound: number of whole slots guaranteed within the real
 * mapping.  Derived from the process-local mmap_size and the SAME slots_off /
 * stride rsv_slots() uses, so a peer that corrupts hdr->k / stride / slots_off
 * after attach-time validation can never drive an access outside the mapping. */
static inline uint64_t rsv_slots_max(RsvHandle *h) {
    uint64_t off    = h->slots_off;
    uint64_t stride = h->stride;            /* cached at attach, not the peer-writable header */
    if (off >= h->mmap_size || stride == 0) return 0;
    return (h->mmap_size - off) / stride;
}

static inline RsvHandle *rsv_setup(void *base, size_t map_size,
                                 const char *path, int backing_fd) {
    RsvHeader *hdr = (RsvHeader *)base;
    RsvHandle *h = (RsvHandle *)calloc(1, sizeof(RsvHandle));
    if (!h) {
        munmap(base, map_size);
        if (backing_fd >= 0) close(backing_fd);
        return NULL;
    }
    h->hdr          = hdr;
    h->base         = base;
    h->reader_slots = (RsvReaderSlot *)((uint8_t *)base + hdr->reader_slots_off);
    /* Cache the geometry from the validated header; the ops use these, never the
       peer-writable hdr->* fields, so a later corruption cannot change a stride /
       item_size / count out from under a bounds check. */
    h->slots_off    = hdr->slots_off;
    h->k            = hdr->k;
    h->item_size    = hdr->item_size;
    h->stride       = hdr->stride;
    h->mode         = hdr->mode;
    h->heap_off     = hdr->heap_off;
    h->mmap_size    = map_size;
    /* Layer B: if the mapping cannot even hold k slots the header lied about its
       size; clamp k to what actually fits (stride is validated 8+item_size). */
    if (h->stride) {
        uint64_t fit = (map_size > h->slots_off) ? (map_size - h->slots_off) / h->stride : 0;
        if (h->k > fit) h->k = fit;
    }
    h->path         = path ? strdup(path) : NULL;
    h->backing_fd   = backing_fd;
    h->my_slot_idx  = UINT32_MAX;
    return h;
}

/* Validate a mapped header (shared by rsv_create reopen and rsv_open_fd). */
static inline int rsv_validate_header(const RsvHeader *hdr, uint64_t file_size) {
    if (hdr->magic != RSV_MAGIC) return 0;
    if (hdr->version != RSV_VERSION) return 0;
    if (hdr->mode != RSV_MODE_UNIFORM && hdr->mode != RSV_MODE_WEIGHTED) return 0;
    if (hdr->k < RSV_MIN_K || hdr->k > RSV_MAX_K) return 0;
    if (hdr->item_size < RSV_MIN_ITEM || hdr->item_size > RSV_MAX_ITEM) return 0;
    if (hdr->capacity != hdr->k) return 0;
    if (hdr->stride != rsv_stride(hdr->item_size)) return 0;
    if (hdr->heap_off != rsv_heap_region_off(hdr->mode)) return 0;
    if (hdr->slots_off != rsv_items_off(hdr->k, hdr->mode)) return 0;
    if (hdr->total_size != file_size) return 0;
    if (hdr->total_size != rsv_total_size(hdr->k, hdr->item_size, hdr->mode)) return 0;
    RsvLayout L = rsv_layout();
    if (hdr->reader_slots_off != L.reader_slots) return 0;
    return 1;
}

/* validate constructor args (k reservoir size, item_size max bytes per item) */
static int rsv_validate_args(uint64_t k, uint64_t item_size, uint32_t mode, char *errbuf) {
    if (errbuf) errbuf[0] = '\0';
    if (k < RSV_MIN_K || k > RSV_MAX_K) { RSV_ERR("reservoir size must be between 1 and 2^30"); return 0; }
    if (item_size < RSV_MIN_ITEM || item_size > RSV_MAX_ITEM) { RSV_ERR("item_size must be between 1 and 65536"); return 0; }
    if (rsv_stride(item_size) > (UINT64_MAX / 4) / k) { RSV_ERR("reservoir_size * item_size too large"); return 0; }
    if (mode == RSV_MODE_WEIGHTED && sizeof(RsvHeapEnt) > (UINT64_MAX / 4) / k) { RSV_ERR("reservoir_size too large for the weighted heap"); return 0; }
    return 1;
}

/* Securely obtain a fd for a path-backed segment: create it exclusively
 * (O_CREAT|O_EXCL|O_NOFOLLOW at `mode`, default 0600 = owner-only), or, if it
 * already exists, attach to it (O_RDWR|O_NOFOLLOW, no O_CREAT). O_EXCL blocks a
 * pre-seeded or hard-linked file and O_NOFOLLOW a symlink swap, so a local
 * attacker can no longer redirect or poison the backing store through the path.
 * Cross-user sharing is opt-in via a wider `mode` (e.g. 0660); the caller still
 * validates the file's contents via rsv_validate_header. */
static int rsv_secure_open(const char *path, mode_t mode, char *errbuf) {
    for (int attempt = 0; attempt < 100; attempt++) {
        int fd = open(path, O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC, mode);
        if (fd >= 0) { (void)fchmod(fd, mode); return fd; }   /* exact mode: umask narrowed the O_EXCL create */
        if (errno != EEXIST) { RSV_ERR("create %s: %s", path, strerror(errno)); return -1; }
        fd = open(path, O_RDWR|O_NOFOLLOW|O_CLOEXEC);
        if (fd >= 0) return fd;
        if (errno == ENOENT) continue;   /* creator unlinked between our two opens; retry */
        RSV_ERR("open %s: %s", path, strerror(errno));  /* ELOOP => symlink rejected */
        return -1;
    }
    RSV_ERR("open %s: create/attach kept racing", path);
    return -1;
}

static RsvHandle *rsv_create(const char *path, uint64_t k, uint64_t item_size, uint32_t weighted, mode_t mode, char *errbuf) {
    if (!rsv_validate_args(k, item_size, weighted, errbuf)) return NULL;

    uint64_t total = rsv_total_size(k, item_size, weighted);
    int anonymous = (path == NULL);
    int fd = -1;
    size_t map_size;
    void *base;

    if (anonymous) {
        map_size = (size_t)total;
        base = mmap(NULL, map_size, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0);
        if (base == MAP_FAILED) { RSV_ERR("mmap: %s", strerror(errno)); return NULL; }
    } else {
        fd = rsv_secure_open(path, mode, errbuf);
        if (fd < 0) return NULL;
        if (flock(fd, LOCK_EX) < 0) { RSV_ERR("flock: %s", strerror(errno)); close(fd); return NULL; }
        struct stat st;
        if (fstat(fd, &st) < 0) { RSV_ERR("fstat: %s", strerror(errno)); flock(fd, LOCK_UN); close(fd); return NULL; }
        int is_new = (st.st_size == 0);
        if (!is_new && (uint64_t)st.st_size < sizeof(RsvHeader)) {
            RSV_ERR("%s: file too small (%lld)", path, (long long)st.st_size);
            flock(fd, LOCK_UN); close(fd); return NULL;
        }
        if (is_new && ftruncate(fd, (off_t)total) < 0) {
            RSV_ERR("ftruncate: %s", strerror(errno)); flock(fd, LOCK_UN); close(fd); return NULL;
        }
        map_size = is_new ? (size_t)total : (size_t)st.st_size;
        base = mmap(NULL, map_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
        if (base == MAP_FAILED) { RSV_ERR("mmap: %s", strerror(errno)); flock(fd, LOCK_UN); close(fd); return NULL; }
        if (!is_new) {
            if (!rsv_validate_header((RsvHeader *)base, (uint64_t)st.st_size)) {
                RSV_ERR("invalid reservoir file"); munmap(base, map_size); flock(fd, LOCK_UN); close(fd); return NULL;
            }
            flock(fd, LOCK_UN); close(fd);
            return rsv_setup(base, map_size, path, -1);
        }
    }
    rsv_init_header(base, k, item_size, weighted, total);
    if (fd >= 0) { flock(fd, LOCK_UN); close(fd); }
    return rsv_setup(base, map_size, path, -1);
}

static RsvHandle *rsv_create_memfd(const char *name, uint64_t k, uint64_t item_size, uint32_t weighted, char *errbuf) {
    if (!rsv_validate_args(k, item_size, weighted, errbuf)) return NULL;

    uint64_t total = rsv_total_size(k, item_size, weighted);
    int fd = memfd_create(name ? name : "reservoir", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd < 0) { RSV_ERR("memfd_create: %s", strerror(errno)); return NULL; }
    if (ftruncate(fd, (off_t)total) < 0) {
        RSV_ERR("ftruncate: %s", strerror(errno)); close(fd); return NULL;
    }
    (void)fcntl(fd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW);
    void *base = mmap(NULL, (size_t)total, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) { RSV_ERR("mmap: %s", strerror(errno)); close(fd); return NULL; }
    rsv_init_header(base, k, item_size, weighted, total);
    return rsv_setup(base, (size_t)total, NULL, fd);
}

static RsvHandle *rsv_open_fd(int fd, char *errbuf) {
    if (errbuf) errbuf[0] = '\0';
    struct stat st;
    if (fstat(fd, &st) < 0) { RSV_ERR("fstat: %s", strerror(errno)); return NULL; }
    if ((uint64_t)st.st_size < sizeof(RsvHeader)) { RSV_ERR("too small"); return NULL; }
    size_t ms = (size_t)st.st_size;
    void *base = mmap(NULL, ms, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) { RSV_ERR("mmap: %s", strerror(errno)); return NULL; }
    if (!rsv_validate_header((RsvHeader *)base, (uint64_t)st.st_size)) {
        RSV_ERR("invalid reservoir table"); munmap(base, ms); return NULL;
    }
    int myfd = fcntl(fd, F_DUPFD_CLOEXEC, 0);
    if (myfd < 0) { RSV_ERR("fcntl: %s", strerror(errno)); munmap(base, ms); return NULL; }
    return rsv_setup(base, ms, NULL, myfd);
}

static void rsv_destroy(RsvHandle *h) {
    if (!h) return;
    /* Release our reader slot on clean teardown (else short-lived-reader churn
     * exhausts the slot table); skip if a lock is still held (subcount>0). */
    if (h->reader_slots && h->my_slot_idx != UINT32_MAX && h->cached_pid &&
        h->cached_fork_gen == __atomic_load_n(&rsv_fork_gen, __ATOMIC_RELAXED) &&
        __atomic_load_n(&h->reader_slots[h->my_slot_idx].subcount, __ATOMIC_ACQUIRE) == 0) {
        uint32_t expected = h->cached_pid;
        __atomic_compare_exchange_n(&h->reader_slots[h->my_slot_idx].pid,
                &expected, 0, 0, __ATOMIC_RELEASE, __ATOMIC_RELAXED);
    }
    if (h->backing_fd >= 0) close(h->backing_fd);
    if (h->base) munmap(h->base, h->mmap_size);
    free(h->path);
    free(h);
}

static inline int rsv_msync(RsvHandle *h) {
    if (!h || !h->base) return 0;
    return msync(h->base, h->mmap_size, MS_SYNC);
}

/* ================================================================
 * Reservoir sampling operations (callers hold the lock) -- Algorithm R with a
 * shared xorshift64 RNG.  Each slot is [uint64 len][item_size data bytes].
 * ================================================================ */

static inline uint64_t rsv_rng_next(RsvHeader *hdr) {
    uint64_t x = hdr->rng_state;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;   /* xorshift64 */
    hdr->rng_state = x;
    return x;
}

/* pointer to slot i's [uint64 len][item_size data] record */
static inline uint8_t *rsv_slot(RsvHandle *h, uint64_t i) {
    return rsv_slots(h) + i * h->stride;
}

/* store an item into slot idx (truncated to item_size) */
static inline void rsv_store(RsvHandle *h, uint64_t idx, const void *item, uint64_t len) {
    uint8_t *slot = rsv_slot(h, idx);
    uint64_t cap = h->item_size;            /* cached: <= stride-8, so the write stays in-slot */
    if (len > cap) len = cap;                   /* truncate over-long items */
    memcpy(slot, &len, sizeof(uint64_t));        /* stored length */
    if (len) memcpy(slot + 8, item, (size_t)len);
}

/* number of items currently held = min(seen, k) */
static inline uint64_t rsv_count_locked(RsvHandle *h) {
    uint64_t k = h->k, seen = h->hdr->seen;    /* k cached; seen is live mutable state */
    return seen < k ? seen : k;
}

/* observe one item.  Returns 1 if stored in the reservoir, 0 if discarded.
 * Algorithm R: the i-th item fills slot i-1 while the reservoir is not full;
 * once full, it replaces a uniformly random slot with probability k/i. */
static int rsv_add_locked(RsvHandle *h, const void *item, uint64_t len) {
    RsvHeader *hdr = h->hdr;
    uint64_t k = h->k;                          /* cached geometry */
    uint64_t smax = rsv_slots_max(h);
    if (k > smax) k = smax;                     /* Layer B: never index past the mapping */
    if (k == 0) return 0;
    uint64_t s = ++hdr->seen;                   /* 1-based index of this item */
    uint64_t idx;
    if (s <= k) {
        idx = s - 1;                            /* still filling the reservoir */
    } else {
        uint64_t j = rsv_rng_next(hdr) % s;     /* uniform in [0, s) */
        if (j >= k) return 0;                   /* discard (prob (s-k)/s) */
        idx = j;                                /* replace slot j (prob k/s) */
    }
    rsv_store(h, idx, item, len);
    return 1;
}

/* ---- weighted (A-Res) min-heap over the items region (weighted mode only) ----
 * One 16-byte {key,item} entry per kept item; entry.item indexes a fixed items
 * cell that stays put -- sifting moves only these entries, never the item bytes. */
static inline RsvHeapEnt *rsv_heap(RsvHandle *h) {
    return (RsvHeapEnt *)((char *)h->base + h->heap_off);
}
/* Layer B trusted bound: heap entries guaranteed within the real mapping. */
static inline uint64_t rsv_heap_max(RsvHandle *h) {
    uint64_t off = h->heap_off;
    if (off == 0 || off >= h->mmap_size) return 0;
    return (h->mmap_size - off) / sizeof(RsvHeapEnt);
}
static inline void rsv_heap_swap(RsvHeapEnt *hp, uint64_t a, uint64_t b) {
    RsvHeapEnt t = hp[a]; hp[a] = hp[b]; hp[b] = t;
}
static inline void rsv_heap_sift_up(RsvHeapEnt *hp, uint64_t i) {
    while (i > 0) {
        uint64_t p = (i - 1) / 2;
        if (hp[i].key < hp[p].key) { rsv_heap_swap(hp, i, p); i = p; } else break;
    }
}
static inline void rsv_heap_sift_down(RsvHeapEnt *hp, uint64_t i, uint64_t size) {
    for (;;) {
        uint64_t l = 2*i + 1, r = 2*i + 2, m = i;
        if (l < size && hp[l].key < hp[m].key) m = l;
        if (r < size && hp[r].key < hp[m].key) m = r;
        if (m == i) break;
        rsv_heap_swap(hp, i, m); i = m;
    }
}

/* observe one weighted item.  A-Res: assign it key = u^(1/weight), u ~ U(0,1];
 * keep the k items with the largest keys via a min-heap (root = smallest kept
 * key).  Returns 1 if kept, 0 if discarded.  `weight` is validated finite > 0 in
 * XS before the lock.  Caller holds the write lock. */
static int rsv_add_weighted_locked(RsvHandle *h, const void *item, uint64_t len, double weight) {
    RsvHeader *hdr = h->hdr;
    uint64_t k = h->k;                          /* cached geometry */
    uint64_t smax = rsv_slots_max(h);
    uint64_t hmax = rsv_heap_max(h);
    if (k > smax) k = smax;
    if (k > hmax) k = hmax;                     /* Layer B: never index past either region */
    if (k == 0) return 0;
    uint64_t x = rsv_rng_next(hdr);
    double u = (double)((x >> 11) + 1) * (1.0 / 9007199254740992.0);   /* (0, 1] */
    double key = pow(u, 1.0 / weight);
    RsvHeapEnt *hp = rsv_heap(h);
    uint64_t size = hdr->seen < k ? hdr->seen : k;   /* items currently kept */
    if (size < k) {                             /* still filling: append + sift up */
        rsv_store(h, size, item, len);
        hp[size].key = key; hp[size].item = size;
        rsv_heap_sift_up(hp, size);
        hdr->seen++;
        return 1;
    }
    hdr->seen++;                                /* full: this item was observed regardless */
    if (key > hp[0].key) {                       /* heavier than the current minimum: evict it */
        uint64_t cell = hp[0].item;
        if (cell >= k) return 0;                /* Layer B: corrupt heap entry -> refuse */
        rsv_store(h, cell, item, len);
        hp[0].key = key;
        rsv_heap_sift_down(hp, 0, k);
        return 1;
    }
    return 0;                                    /* lighter: discard */
}

/* point *out_ptr/*out_len at slot i's item inside the mapping (caller holds the
 * read lock).  Returns 1 if i is a filled slot, else 0. */
static int rsv_get_locked(RsvHandle *h, uint64_t i, const uint8_t **out_ptr, uint64_t *out_len) {
    if (i >= rsv_count_locked(h) || i >= rsv_slots_max(h)) return 0;
    uint8_t *slot = rsv_slot(h, i);
    uint64_t len; memcpy(&len, slot, sizeof(uint64_t));
    if (len > h->item_size) len = h->item_size;   /* Layer B clamp (cached item_size) */
    *out_ptr = slot + 8;
    *out_len = len;
    return 1;
}

/* reset the reservoir to empty (caller holds the write lock) */
static inline void rsv_clear_locked(RsvHandle *h) {
    uint64_t k = h->k;                          /* cached geometry */
    uint64_t smax = rsv_slots_max(h);
    if (k > smax) k = smax;
    for (uint64_t i = 0; i < k; i++) {           /* zero each slot's length word */
        uint64_t z = 0; memcpy(rsv_slot(h, i), &z, sizeof(uint64_t));
    }
    h->hdr->seen = 0;
}

#endif /* RSV_H */
