/*
 * Copyright (c) 2026 Martin Harvey <martin_c_harvey@hotmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

/*
 * engine_baseline.c - mdb2bench reference engine: the same two indexes as
 * plain C data structures, with no transactions, journal or versioning.
 *
 *   - Each record is one malloc'd block (header + payload).
 *   - ID index:     AVL tree keyed on id.
 *   - Expiry index: AVL tree keyed on (expiry, id).
 *   - Concurrency:  one SRWLOCK; readers shared, writers exclusive.
 *
 * It answers "what do the indexed structures themselves cost?", so that
 * MemDB2's overhead (transactions, MVCC buffers, handle checks, journal)
 * can be read off as the difference. It is a lower bound, not a
 * like-for-like competitor: it is neither durable nor isolated.
 */
#include "bench.h"
#include <stdlib.h>
#include <string.h>

typedef struct avl_node {
    struct avl_node *l, *r;
    int h;
} avl_node;

typedef int (*avl_cmp)(const avl_node *a, const avl_node *b);

typedef struct brec {
    avl_node by_id;
    avl_node by_exp;
    uint64_t id;
    int64_t  expiry;
    int32_t  state;
    uint32_t len;
    uint32_t cap;
    uint8_t  payload[1];      /* cap bytes */
} brec;

#define ID_REC(n)  ((brec *)((char *)(n) - offsetof(brec, by_id)))
#define EXP_REC(n) ((brec *)((char *)(n) - offsetof(brec, by_exp)))

typedef struct base_eng {
    SRWLOCK lock;
    avl_node *id_root;
    avl_node *exp_root;
    volatile LONG64 allocs, frees;
    volatile LONG count_allocs;
} base_eng;

typedef struct base_ts {
    base_eng *e;
} base_ts;

/* ---------------------------------------------------------------------------
 * AVL tree (recursive; depth is ~1.44 log2 n, about 32 at 5 million).
 * ------------------------------------------------------------------------- */
static __inline int avl_h(const avl_node *n) { return n ? n->h : 0; }

static __inline void avl_fix(avl_node *n)
{
    int hl = avl_h(n->l), hr = avl_h(n->r);
    n->h = (hl > hr ? hl : hr) + 1;
}

static avl_node *avl_rot_r(avl_node *n)
{
    avl_node *l = n->l;
    n->l = l->r;
    l->r = n;
    avl_fix(n);
    avl_fix(l);
    return l;
}

static avl_node *avl_rot_l(avl_node *n)
{
    avl_node *r = n->r;
    n->r = r->l;
    r->l = n;
    avl_fix(n);
    avl_fix(r);
    return r;
}

static avl_node *avl_balance(avl_node *n)
{
    int bf;
    avl_fix(n);
    bf = avl_h(n->l) - avl_h(n->r);
    if (bf > 1) {
        if (avl_h(n->l->l) < avl_h(n->l->r))
            n->l = avl_rot_l(n->l);
        return avl_rot_r(n);
    }
    if (bf < -1) {
        if (avl_h(n->r->r) < avl_h(n->r->l))
            n->r = avl_rot_r(n->r);
        return avl_rot_l(n);
    }
    return n;
}

static avl_node *avl_insert(avl_node *t, avl_node *n, avl_cmp cmp)
{
    if (!t) {
        n->l = n->r = NULL;
        n->h = 1;
        return n;
    }
    if (cmp(n, t) < 0)
        t->l = avl_insert(t->l, n, cmp);
    else
        t->r = avl_insert(t->r, n, cmp);
    return avl_balance(t);
}

static avl_node *avl_remove_min(avl_node *t, avl_node **out_min)
{
    if (!t->l) {
        *out_min = t;
        return t->r;
    }
    t->l = avl_remove_min(t->l, out_min);
    return avl_balance(t);
}

/* Remove node x (which must be in the tree; keys are unique). */
static avl_node *avl_remove(avl_node *t, avl_node *x, avl_cmp cmp)
{
    int c;
    if (!t)
        return NULL;
    c = cmp(x, t);
    if (c < 0)
        t->l = avl_remove(t->l, x, cmp);
    else if (c > 0)
        t->r = avl_remove(t->r, x, cmp);
    else {
        avl_node *m;
        if (!t->l) return t->r;
        if (!t->r) return t->l;
        t->r = avl_remove_min(t->r, &m);
        m->l = t->l;
        m->r = t->r;
        t = m;
    }
    return avl_balance(t);
}

static avl_node *avl_find(avl_node *t, const avl_node *key, avl_cmp cmp)
{
    while (t) {
        int c = cmp(key, t);
        if (!c) return t;
        t = c < 0 ? t->l : t->r;
    }
    return NULL;
}

static __inline avl_node *avl_first(avl_node *t)
{
    if (!t) return NULL;
    while (t->l) t = t->l;
    return t;
}

static int cmp_id(const avl_node *a, const avl_node *b)
{
    uint64_t x = ID_REC(a)->id, y = ID_REC(b)->id;
    return x < y ? -1 : x > y;
}

static int cmp_exp(const avl_node *a, const avl_node *b)
{
    const brec *x = EXP_REC(a), *y = EXP_REC(b);
    if (x->expiry != y->expiry)
        return x->expiry < y->expiry ? -1 : 1;
    return x->id < y->id ? -1 : x->id > y->id;
}

/* ---------------------------------------------------------------------------
 * Records
 * ------------------------------------------------------------------------- */
static brec *rec_alloc(base_eng *e, const rec *r)
{
    brec *b = (brec *)malloc(offsetof(brec, payload) + (r->payload_len ? r->payload_len : 1));
    if (!b)
        return NULL;
    if (e->count_allocs)
        InterlockedIncrement64(&e->allocs);
    b->id = r->id;
    b->expiry = r->expiry;
    b->state = r->state;
    b->len = b->cap = r->payload_len;
    memcpy(b->payload, r->payload, r->payload_len);
    return b;
}

static void rec_free(base_eng *e, brec *b)
{
    if (e->count_allocs)
        InterlockedIncrement64(&e->frees);
    free(b);
}

static brec *find_id_locked(base_eng *e, uint64_t id)
{
    brec key;
    avl_node *n;
    key.id = id;
    n = avl_find(e->id_root, &key.by_id, cmp_id);
    return n ? ID_REC(n) : NULL;
}

static void link_rec(base_eng *e, brec *b)
{
    e->id_root = avl_insert(e->id_root, &b->by_id, cmp_id);
    e->exp_root = avl_insert(e->exp_root, &b->by_exp, cmp_exp);
}

static void unlink_rec(base_eng *e, brec *b)
{
    e->id_root = avl_remove(e->id_root, &b->by_id, cmp_id);
    e->exp_root = avl_remove(e->exp_root, &b->by_exp, cmp_exp);
}

static void copy_out(const brec *b, rec_out *o)
{
    o->id = b->id;
    o->expiry = b->expiry;
    o->state = b->state;
    o->payload_len = b->len;
    memcpy(o->payload, b->payload, b->len);
}

/* ---------------------------------------------------------------------------
 * Engine
 * ------------------------------------------------------------------------- */
static int b_open(const bench_cfg *cfg, void **out_eng)
{
    base_eng *e = (base_eng *)calloc(1, sizeof *e);
    (void)cfg;
    if (!e)
        return R_FAIL;
    InitializeSRWLock(&e->lock);
    *out_eng = e;
    return R_OK;
}

static void free_tree(base_eng *e, avl_node *n)
{
    /* Free via the ID tree; iterative over the right spine to bound depth. */
    while (n) {
        avl_node *r = n->r;
        free_tree(e, n->l);
        free(ID_REC(n));
        n = r;
    }
}

static void b_close(void *eng)
{
    base_eng *e = (base_eng *)eng;
    if (!e)
        return;
    free_tree(e, e->id_root);
    free(e);
}

static int b_thread_init(void *eng, void **out_ts)
{
    base_ts *t = (base_ts *)calloc(1, sizeof *t);
    if (!t)
        return R_FAIL;
    t->e = (base_eng *)eng;
    *out_ts = t;
    return R_OK;
}

static void b_thread_done(void *ts)
{
    free(ts);
}

static int b_insert(void *ts, const rec *r)
{
    base_eng *e = ((base_ts *)ts)->e;
    brec *b = rec_alloc(e, r);           /* allocate outside the lock */
    if (!b)
        return R_FAIL;
    AcquireSRWLockExclusive(&e->lock);
    link_rec(e, b);
    ReleaseSRWLockExclusive(&e->lock);
    return R_OK;
}

static int b_preload_batch(void *ts, const rec *recs, uint32_t n)
{
    uint32_t i;
    for (i = 0; i < n; i++)
        if (b_insert(ts, &recs[i]) != R_OK)
            return R_FAIL;
    return R_OK;
}

static int b_after_preload(void *eng)
{
    (void)eng;
    return R_OK;
}

static int b_find_id(void *ts, uint64_t id, rec_out *out)
{
    base_eng *e = ((base_ts *)ts)->e;
    brec *b;
    AcquireSRWLockShared(&e->lock);
    b = find_id_locked(e, id);
    if (b)
        copy_out(b, out);
    ReleaseSRWLockShared(&e->lock);
    return b ? R_OK : R_MISS;
}

static int b_find_earliest(void *ts, rec_out *out)
{
    base_eng *e = ((base_ts *)ts)->e;
    avl_node *n;
    AcquireSRWLockShared(&e->lock);
    n = avl_first(e->exp_root);
    if (n) {
        out->id = EXP_REC(n)->id;
        out->expiry = EXP_REC(n)->expiry;
    }
    ReleaseSRWLockShared(&e->lock);
    return n ? R_OK : R_MISS;
}

static int b_delete_id(void *ts, uint64_t id)
{
    base_eng *e = ((base_ts *)ts)->e;
    brec *b;
    AcquireSRWLockExclusive(&e->lock);
    b = find_id_locked(e, id);
    if (b)
        unlink_rec(e, b);
    ReleaseSRWLockExclusive(&e->lock);
    if (!b)
        return R_MISS;
    rec_free(e, b);                      /* free outside the lock */
    return R_OK;
}

static int b_delete_expired(void *ts, int64_t now)
{
    base_eng *e = ((base_ts *)ts)->e;
    avl_node *n;
    brec *b = NULL;
    AcquireSRWLockExclusive(&e->lock);
    n = avl_first(e->exp_root);
    if (n && EXP_REC(n)->expiry <= now) {
        b = EXP_REC(n);
        unlink_rec(e, b);
    }
    ReleaseSRWLockExclusive(&e->lock);
    if (!b)
        return R_MISS;
    rec_free(e, b);
    return R_OK;
}

static int b_transition(void *ts, uint64_t id, int32_t *out_new_state)
{
    base_eng *e = ((base_ts *)ts)->e;
    brec *b;
    int32_t st = 0;
    AcquireSRWLockExclusive(&e->lock);
    b = find_id_locked(e, id);
    if (b) {
        st = b->state = (b->state + 1) % ST_STATES;
    }
    ReleaseSRWLockExclusive(&e->lock);
    if (!b)
        return R_MISS;
    if (out_new_state)
        *out_new_state = st;
    return R_OK;
}

static int b_recycle(void *ts, int64_t now, const rec *r)
{
    base_eng *e = ((base_ts *)ts)->e;
    avl_node *n;
    brec *b = NULL, *nb = NULL, *old = NULL;
    AcquireSRWLockExclusive(&e->lock);
    n = avl_first(e->exp_root);
    if (n && EXP_REC(n)->expiry <= now) {
        b = EXP_REC(n);
        unlink_rec(e, b);
        if (r->payload_len <= b->cap) {
            /* Reuse the block in place. */
            b->id = r->id;
            b->expiry = r->expiry;
            b->state = r->state;
            b->len = r->payload_len;
            memcpy(b->payload, r->payload, r->payload_len);
            link_rec(e, b);
        } else {
            old = b;
        }
    }
    ReleaseSRWLockExclusive(&e->lock);
    if (!b)
        return R_MISS;
    if (old) {
        /* Too small to reuse: replace it (rare: only larger payloads). */
        rec_free(e, old);
        nb = rec_alloc(e, r);
        if (!nb)
            return R_FAIL;
        AcquireSRWLockExclusive(&e->lock);
        link_rec(e, nb);
        ReleaseSRWLockExclusive(&e->lock);
    }
    return R_OK;
}

static int b_alloc_stats(void *eng, uint64_t *allocs, uint64_t *frees)
{
    base_eng *e = (base_eng *)eng;
    *allocs = (uint64_t)e->allocs;
    *frees = (uint64_t)e->frees;
    return 1;
}

static void b_alloc_enable(void *eng, int on)
{
    ((base_eng *)eng)->count_allocs = on;
}

static int b_checkpoint(void *eng, int async)
{
    (void)eng; (void)async;
    fprintf(stderr, "baseline engine has no checkpoints\n");
    return R_FAIL;
}

const engine_vt g_engine_baseline = {
    "baseline",
    b_open, b_close, b_thread_init, b_thread_done,
    b_preload_batch, b_after_preload,
    b_insert, b_find_id, b_find_earliest, b_delete_id,
    b_delete_expired, b_transition, b_recycle,
    b_alloc_stats, b_alloc_enable,
    b_checkpoint,
};
