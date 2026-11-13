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
 * engine_memdb2.c - mdb2bench engine over the MemDB2 C API (memdb2.h).
 *
 * Every benchmark operation is one complete MemDB2 transaction:
 *   begin -> open table data -> navigate / edit -> post -> commit.
 * Nothing is cached across transactions except the session and a search
 * key per thread, which is how a real C client would use the API.
 */
#include "bench.h"
#include "memdb2.h"
#include <stdlib.h>
#include <string.h>

#define TBL        "Bench"
#define F_ID       "Id"
#define F_EXPIRY   "Expiry"
#define F_STATE    "State"
#define F_PAYLOAD  "Payload"
#define IX_ID      "ById"
#define IX_EXPIRY  "ByExpiry"

typedef int32_t (MDB_CALL *pfn_alloc_stats)(uint64_t *allocs, uint64_t *frees, uint64_t *reallocs);
typedef int32_t (MDB_CALL *pfn_alloc_enable)(int32_t on);

typedef struct m2_eng {
    bench_cfg cfg;
    mdb_handle db;
    int32_t sync;
    int32_t wmode, rmode, iso;
    pfn_alloc_stats  p_stats;
    pfn_alloc_enable p_enable;
} m2_eng;

typedef struct m2_ts {
    m2_eng *e;
    mdb_handle session;
    mdb_handle key;
} m2_ts;

/* Report a failure. Concurrency clashes are expected and silent. */
static int m2_classify(int32_t rc, const char *what)
{
    char msg[1024];
    size_t len = 0;
    if (rc == MDB_ERR_CONCURRENCY)
        return R_CONFLICT;
    msg[0] = 0;
    mdb_last_error_message(msg, sizeof msg, &len);
    fprintf(stderr, "MemDB2: %s failed (%d): %s\n", what, (int)rc, msg);
    return R_FAIL;
}

/* Evaluate an API call; on failure classify it, roll back, and return. */
#define M2(call)                                             \
    do {                                                     \
        int32_t rc_ = (call);                                \
        if (rc_ != MDB_OK) {                                 \
            int res_ = m2_classify(rc_, #call);              \
            if (txn) mdb_txn_rollback(txn);                  \
            return res_;                                     \
        }                                                    \
    } while (0)

/* ---------------------------------------------------------------------------
 * Open / schema
 * ------------------------------------------------------------------------- */

/* Delete files matching 'pattern' (e.g. "*.dbincr") directly inside 'dir'.
   Not recursive. Adds to *count and *bytes. Returns 0 if any delete failed. */
static int delete_matching(const char *dir, const char *pattern,
                           unsigned *count, uint64_t *bytes)
{
    char spec[MAX_PATH], path[MAX_PATH];
    WIN32_FIND_DATAA fd;
    HANDLE h;
    int ok = 1;

    if (_snprintf_s(spec, sizeof spec, _TRUNCATE, "%s\\%s", dir, pattern) < 0)
        return 0;
    h = FindFirstFileA(spec, &fd);
    if (h == INVALID_HANDLE_VALUE)
        return 1;                      /* nothing there */
    do {
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
            continue;
        if (_snprintf_s(path, sizeof path, _TRUNCATE, "%s\\%s", dir, fd.cFileName) < 0) {
            ok = 0;
            continue;
        }
        if (DeleteFileA(path)) {
            (*count)++;
            *bytes += ((uint64_t)fd.nFileSizeHigh << 32) | fd.nFileSizeLow;
        } else {
            fprintf(stderr, "Could not delete %s (error %lu)\n", path, GetLastError());
            ok = 0;
        }
    } while (FindNextFileA(h, &fd));
    FindClose(h);
    return ok;
}

/* Start from an empty database folder, so MemDB2 does not replay the last
   run's journal on open. Only MemDB2's own files are removed. */
static int clean_db_folder(const bench_cfg *cfg)
{
    const char *p = cfg->db_path, *name = p, *q;
    char lower[MAX_PATH];
    size_t i;
    unsigned count = 0;
    uint64_t bytes = 0;
    int ok;

    if (cfg->db_clean < 0)
        return 1;
    /* last path component, ignoring a trailing slash */
    for (q = p; *q; q++)
        if ((*q == '\\' || *q == '/') && q[1])
            name = q + 1;
    for (i = 0; name[i] && name[i] != '\\' && name[i] != '/' && i + 1 < sizeof lower; i++)
        lower[i] = (char)((name[i] >= 'A' && name[i] <= 'Z') ? name[i] + 32 : name[i]);
    lower[i] = 0;
    if (cfg->db_clean == 0 && !strstr(lower, "mdb2bench")) {
        printf("Keeping existing files in %s (folder name does not contain \"mdb2bench\";\n"
               "use --fresh to delete its .dbincr/.dbinit files anyway)\n", p);
        return 1;
    }
    if (GetFileAttributesA(p) == INVALID_FILE_ATTRIBUTES)
        return 1;                      /* no folder yet */
    ok = delete_matching(p, "*.dbincr", &count, &bytes);
    ok = delete_matching(p, "*.dbinit", &count, &bytes) && ok;
    if (count)
        printf("Deleted %u journal/checkpoint files (%.1f MB) from %s\n",
               count, (double)bytes / 1048576.0, p);
    return ok;
}

static int m2_open(const bench_cfg *cfg, void **out_eng)
{
    m2_eng *e = (m2_eng *)calloc(1, sizeof *e);
    mdb_handle session = 0, txn = 0, meta = 0;
    HMODULE dll;
    int32_t rc;

    if (!e)
        return R_FAIL;
    e->cfg = *cfg;
    e->sync = cfg->sync_flush ? MDB_SYNC_FLUSH_BUFFERS : MDB_SYNC_LAZY_WRITE;
    e->wmode = cfg->writer_exclusive ? MDB_ACCESS_WRITE_EXCLUSIVE : MDB_ACCESS_READ_WRITE_SHARED;
    e->rmode = cfg->reader_rw ? MDB_ACCESS_READ_WRITE_SHARED : MDB_ACCESS_READ_SHARED;
    e->iso = cfg->isolation;

    if (mdb_api_version() < 3)
        fprintf(stderr, "Warning: DLL C API version %d (header %d)\n",
                (int)mdb_api_version(), MDB_API_VERSION);

    dll = GetModuleHandleA("MemDB2Dll.dll");
    if (dll) {
        e->p_stats  = (pfn_alloc_stats)GetProcAddress(dll, "mdb_bench_alloc_stats");
        e->p_enable = (pfn_alloc_enable)GetProcAddress(dll, "mdb_bench_alloc_enable");
    }

    if (!clean_db_folder(cfg)) {
        fprintf(stderr, "Could not clear %s; use --keep-db to start without clearing it\n",
                cfg->db_path);
        free(e);
        return R_FAIL;
    }
    CreateDirectoryA(cfg->db_path, NULL);   /* MemDB2 wants the folder */
    rc = mdb_db_open_ex(cfg->db_path, MDB_OPEN_NO_AUTO_CHECKPOINT, &e->db);
    if (rc != MDB_OK) {
        m2_classify(rc, "mdb_db_open_ex");
        if (e->db) mdb_db_close(e->db);
        free(e);
        return R_FAIL;
    }
    *out_eng = e;   /* so close() cleans up on failure below */

    rc = mdb_session_open(e->db, &session);
    if (rc != MDB_OK)
        return m2_classify(rc, "mdb_session_open");

    if (cfg->reuse_db) {
        /* Use the table an earlier run left: it must exist. */
        M2(mdb_txn_begin(session, MDB_ACCESS_READ_SHARED, MDB_SYNC_LAZY_WRITE,
                         MDB_ISO_REPEATABLE_READ, &txn));
        rc = mdb_txn_open_table_meta(txn, TBL, &meta);
        mdb_txn_rollback(txn);
        txn = 0;
        mdb_session_close(session);
        if (rc != MDB_OK) {
            fprintf(stderr, "--reuse-db: no %s table in %s. Load one first, e.g.\n"
                    "  mdb2bench --db %s --records N --load-only\n",
                    TBL, cfg->db_path, cfg->db_path);
            return R_FAIL;
        }
        return R_OK;
    }

    /* Drop any table left by an earlier run. */
    M2(mdb_txn_begin(session, MDB_ACCESS_WRITE_EXCLUSIVE, MDB_SYNC_LAZY_WRITE,
                     MDB_ISO_REPEATABLE_READ, &txn));
    if (mdb_txn_open_table_meta(txn, TBL, &meta) == MDB_OK) {
        M2(mdb_close(meta));
        meta = 0;
        M2(mdb_txn_delete_entity(txn, TBL));
        M2(mdb_txn_commit(txn));
        txn = 0;
        /* Compact the journal so old data does not slow the next load. */
        rc = mdb_db_checkpoint(e->db);
        if (rc != MDB_OK)
            m2_classify(rc, "mdb_db_checkpoint (after drop)");
    } else {
        M2(mdb_txn_rollback(txn));
        txn = 0;
    }

    M2(mdb_txn_begin(session, MDB_ACCESS_WRITE_EXCLUSIVE, MDB_SYNC_LAZY_WRITE,
                     MDB_ISO_REPEATABLE_READ, &txn));
    M2(mdb_txn_create_table(txn, TBL));
    M2(mdb_txn_open_table_meta(txn, TBL, &meta));
    M2(mdb_meta_create_field(meta, F_ID, MDB_FT_UINT64));
    M2(mdb_meta_create_field(meta, F_EXPIRY, MDB_FT_INT64));
    M2(mdb_meta_create_field(meta, F_STATE, MDB_FT_INT32));
    M2(mdb_meta_create_field(meta, F_PAYLOAD, MDB_FT_BLOB));
    M2(mdb_meta_create_index(meta, IX_ID, F_ID, MDB_INDEX_UNIQUE | MDB_INDEX_NOT_EMPTY));
    M2(mdb_meta_create_index(meta, IX_EXPIRY, F_EXPIRY, MDB_INDEX_NOT_EMPTY));
    M2(mdb_txn_commit(txn));
    txn = 0;
    mdb_session_close(session);
    return R_OK;
}

static void m2_close(void *eng)
{
    m2_eng *e = (m2_eng *)eng;
    if (!e)
        return;
    if (e->db)
        mdb_db_close(e->db);
    mdb_shutdown();
    free(e);
}

static int m2_thread_init(void *eng, void **out_ts)
{
    m2_ts *t = (m2_ts *)calloc(1, sizeof *t);
    int32_t rc;
    if (!t)
        return R_FAIL;
    t->e = (m2_eng *)eng;
    rc = mdb_session_open(t->e->db, &t->session);
    if (rc != MDB_OK) {
        free(t);
        return m2_classify(rc, "mdb_session_open");
    }
    rc = mdb_key_create(&t->key);
    if (rc != MDB_OK) {
        mdb_session_close(t->session);
        free(t);
        return m2_classify(rc, "mdb_key_create");
    }
    *out_ts = t;
    return R_OK;
}

static void m2_thread_done(void *ts)
{
    m2_ts *t = (m2_ts *)ts;
    if (!t)
        return;
    if (t->key) mdb_close(t->key);
    if (t->session) mdb_session_close(t->session);
    free(t);
}

/* ---------------------------------------------------------------------------
 * Helpers
 * ------------------------------------------------------------------------- */
static __inline int32_t write_rec(mdb_handle data, const rec *r)
{
    int32_t rc;
    if ((rc = mdb_data_write_uint64(data, F_ID, r->id)) != MDB_OK) return rc;
    if ((rc = mdb_data_write_int64(data, F_EXPIRY, r->expiry)) != MDB_OK) return rc;
    if ((rc = mdb_data_write_int32(data, F_STATE, r->state)) != MDB_OK) return rc;
    return mdb_data_write_blob(data, F_PAYLOAD, r->payload, r->payload_len);
}

static __inline int32_t read_rec(mdb_handle data, rec_out *o)
{
    int32_t rc;
    size_t len = 0;
    if ((rc = mdb_data_read_uint64(data, F_ID, &o->id)) != MDB_OK) return rc;
    if ((rc = mdb_data_read_int64(data, F_EXPIRY, &o->expiry)) != MDB_OK) return rc;
    if ((rc = mdb_data_read_int32(data, F_STATE, &o->state)) != MDB_OK) return rc;
    rc = mdb_data_read_blob(data, F_PAYLOAD, o->payload, sizeof o->payload, &len);
    o->payload_len = (uint32_t)len;
    return rc;
}

/* ---------------------------------------------------------------------------
 * Operations
 * ------------------------------------------------------------------------- */
static int m2_preload_batch(void *ts, const rec *recs, uint32_t n)
{
    m2_ts *t = (m2_ts *)ts;
    mdb_handle txn = 0, data = 0;
    uint32_t i;
    /* One loading thread: exclusive, as before. Several: shared, so their
       appends and posts overlap and only the commits are serialised. */
    int32_t mode = t->e->cfg.load_threads > 1 ? MDB_ACCESS_READ_WRITE_SHARED
                                              : MDB_ACCESS_WRITE_EXCLUSIVE;
    M2(mdb_txn_begin(t->session, mode, MDB_SYNC_LAZY_WRITE,
                     MDB_ISO_REPEATABLE_READ, &txn));
    M2(mdb_txn_open_table_data(txn, TBL, &data));
    for (i = 0; i < n; i++) {
        M2(mdb_data_append(data));
        M2(write_rec(data, &recs[i]));
        M2(mdb_data_post(data));
    }
    M2(mdb_txn_commit(txn));
    return R_OK;
}

static int m2_after_preload(void *eng)
{
    m2_eng *e = (m2_eng *)eng;
    int32_t rc;
    if (!e->cfg.checkpoint)
        return R_OK;
    rc = mdb_db_checkpoint(e->db);
    return rc == MDB_OK ? R_OK : m2_classify(rc, "mdb_db_checkpoint");
}

static int m2_insert(void *ts, const rec *r)
{
    m2_ts *t = (m2_ts *)ts;
    mdb_handle txn = 0, data = 0;
    M2(mdb_txn_begin(t->session, t->e->wmode, t->e->sync, t->e->iso, &txn));
    M2(mdb_txn_open_table_data(txn, TBL, &data));
    M2(mdb_data_append(data));
    M2(write_rec(data, r));
    M2(mdb_data_post(data));
    M2(mdb_txn_commit(txn));
    return R_OK;
}

/* Readers change nothing, so they end with a rollback unless --reader-end
   commit asks for the commit path (to measure its cost). */
static int32_t end_read(m2_ts *t, mdb_handle txn)
{
    return t->e->cfg.reader_commit ? mdb_txn_commit(txn) : mdb_txn_rollback(txn);
}

static int m2_find_id(void *ts, uint64_t id, rec_out *out)
{
    m2_ts *t = (m2_ts *)ts;
    mdb_handle txn = 0, data = 0;
    int32_t found = 0;
    M2(mdb_txn_begin(t->session, t->e->rmode, MDB_SYNC_LAZY_WRITE, t->e->iso, &txn));
    M2(mdb_txn_open_table_data(txn, TBL, &data));
    M2(mdb_key_clear(t->key));
    M2(mdb_key_add_uint64(t->key, id));
    M2(mdb_data_find(data, IX_ID, t->key, &found));
    if (found)
        M2(read_rec(data, out));
    M2(end_read(t, txn));
    return found ? R_OK : R_MISS;
}

static int m2_find_earliest(void *ts, rec_out *out)
{
    m2_ts *t = (m2_ts *)ts;
    mdb_handle txn = 0, data = 0;
    int32_t found = 0;
    M2(mdb_txn_begin(t->session, t->e->rmode, MDB_SYNC_LAZY_WRITE, t->e->iso, &txn));
    M2(mdb_txn_open_table_data(txn, TBL, &data));
    M2(mdb_data_locate(data, MDB_POS_FIRST, IX_EXPIRY, &found));
    if (found) {
        M2(mdb_data_read_uint64(data, F_ID, &out->id));
        M2(mdb_data_read_int64(data, F_EXPIRY, &out->expiry));
    }
    M2(end_read(t, txn));
    return found ? R_OK : R_MISS;
}

static int m2_delete_id(void *ts, uint64_t id)
{
    m2_ts *t = (m2_ts *)ts;
    mdb_handle txn = 0, data = 0;
    int32_t found = 0;
    M2(mdb_txn_begin(t->session, t->e->wmode, t->e->sync, t->e->iso, &txn));
    M2(mdb_txn_open_table_data(txn, TBL, &data));
    M2(mdb_key_clear(t->key));
    M2(mdb_key_add_uint64(t->key, id));
    M2(mdb_data_find(data, IX_ID, t->key, &found));
    if (!found) {
        M2(mdb_txn_rollback(txn));
        return R_MISS;
    }
    M2(mdb_data_delete(data, 0));
    M2(mdb_txn_commit(txn));
    return R_OK;
}

static int m2_delete_expired(void *ts, int64_t now)
{
    m2_ts *t = (m2_ts *)ts;
    mdb_handle txn = 0, data = 0;
    int32_t found = 0;
    int64_t exp = 0;
    M2(mdb_txn_begin(t->session, t->e->wmode, t->e->sync, t->e->iso, &txn));
    M2(mdb_txn_open_table_data(txn, TBL, &data));
    M2(mdb_data_locate(data, MDB_POS_FIRST, IX_EXPIRY, &found));
    if (found)
        M2(mdb_data_read_int64(data, F_EXPIRY, &exp));
    if (!found || exp > now) {
        M2(mdb_txn_rollback(txn));
        return R_MISS;
    }
    M2(mdb_data_delete(data, 0));
    M2(mdb_txn_commit(txn));
    return R_OK;
}

static int m2_transition(void *ts, uint64_t id, int32_t *out_new_state)
{
    m2_ts *t = (m2_ts *)ts;
    mdb_handle txn = 0, data = 0;
    int32_t found = 0, st = 0;
    M2(mdb_txn_begin(t->session, t->e->wmode, t->e->sync, t->e->iso, &txn));
    M2(mdb_txn_open_table_data(txn, TBL, &data));
    M2(mdb_key_clear(t->key));
    M2(mdb_key_add_uint64(t->key, id));
    M2(mdb_data_find(data, IX_ID, t->key, &found));
    if (!found) {
        M2(mdb_txn_rollback(txn));
        return R_MISS;
    }
    M2(mdb_data_read_int32(data, F_STATE, &st));
    st = (st + 1) % ST_STATES;          /* NEW -> RESERVED -> FILLED -> NEW */
    M2(mdb_data_write_int32(data, F_STATE, st));
    M2(mdb_data_post(data));
    M2(mdb_txn_commit(txn));
    if (out_new_state)
        *out_new_state = st;
    return R_OK;
}

static int m2_recycle(void *ts, int64_t now, const rec *r)
{
    m2_ts *t = (m2_ts *)ts;
    mdb_handle txn = 0, data = 0;
    int32_t found = 0;
    int64_t exp = 0;
    M2(mdb_txn_begin(t->session, t->e->wmode, t->e->sync, t->e->iso, &txn));
    M2(mdb_txn_open_table_data(txn, TBL, &data));
    M2(mdb_data_locate(data, MDB_POS_FIRST, IX_EXPIRY, &found));
    if (found)
        M2(mdb_data_read_int64(data, F_EXPIRY, &exp));
    if (!found || exp > now) {
        M2(mdb_txn_rollback(txn));
        return R_MISS;
    }
    /* Overwrite the expired row in place: new identity, expiry and body. */
    M2(write_rec(data, r));
    M2(mdb_data_post(data));
    M2(mdb_txn_commit(txn));
    return R_OK;
}

static int m2_alloc_stats(void *eng, uint64_t *allocs, uint64_t *frees)
{
    m2_eng *e = (m2_eng *)eng;
    uint64_t re = 0;
    if (!e->p_stats)
        return 0;
    return e->p_stats(allocs, frees, &re) == 0;
}

static void m2_alloc_enable(void *eng, int on)
{
    m2_eng *e = (m2_eng *)eng;
    if (e->p_enable)
        e->p_enable(on);
}

static int m2_checkpoint(void *eng, int async)
{
    m2_eng *e = (m2_eng *)eng;
    int32_t rc = mdb_db_checkpoint_ex(e->db, async ? MDB_CHECKPOINT_ASYNC : 0);
    return rc == MDB_OK ? R_OK : m2_classify(rc, "mdb_db_checkpoint_ex");
}

/* --reuse-db: highest id (by the id index) and row count of the table. */
static int m2_inspect(void *eng, uint64_t *max_id, uint64_t *rows)
{
    m2_eng *e = (m2_eng *)eng;
    mdb_handle session = 0, txn = 0, data = 0;
    int32_t found = 0, rc;
    uint64_t n = 0;

    *max_id = 0;
    *rows = 0;
    rc = mdb_session_open(e->db, &session);
    if (rc != MDB_OK)
        return m2_classify(rc, "mdb_session_open");
    rc = mdb_txn_begin(session, MDB_ACCESS_READ_SHARED, MDB_SYNC_LAZY_WRITE,
                       MDB_ISO_REPEATABLE_READ, &txn);
    if (rc == MDB_OK)
        rc = mdb_txn_open_table_data(txn, TBL, &data);
    if (rc == MDB_OK)
        rc = mdb_data_locate(data, MDB_POS_LAST, IX_ID, &found);
    if (rc == MDB_OK && found)
        rc = mdb_data_read_uint64(data, F_ID, max_id);
    /* Count in storage order: no index walk needed. */
    if (rc == MDB_OK)
        rc = mdb_data_locate(data, MDB_POS_FIRST, NULL, &found);
    while (rc == MDB_OK && found) {
        n++;
        rc = mdb_data_locate(data, MDB_POS_NEXT, NULL, &found);
    }
    if (rc != MDB_OK) {
        int res = m2_classify(rc, "inspecting the existing table");
        if (txn) mdb_txn_rollback(txn);
        mdb_session_close(session);
        return res;
    }
    mdb_txn_rollback(txn);
    mdb_session_close(session);
    *rows = n;
    return R_OK;
}

const engine_vt g_engine_memdb2 = {
    "memdb2",
    m2_open, m2_close, m2_thread_init, m2_thread_done,
    m2_preload_batch, m2_after_preload,
    m2_insert, m2_find_id, m2_find_earliest, m2_delete_id,
    m2_delete_expired, m2_transition, m2_recycle,
    m2_alloc_stats, m2_alloc_enable,
    m2_checkpoint,
    m2_inspect,
};
