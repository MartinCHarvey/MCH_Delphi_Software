/*
 * example.c - minimal use of the MemDB2 C API.
 *
 * Creates (or opens) a database in the folder given on the command line,
 * creates a "People" table with a unique index on Id, adds a row if it is
 * not there yet, then reads it back by index.
 *
 * Build (MSVC, from a Developer Command Prompt; see C_DLL\make_importlib.bat):
 *   cl /I..\C_API example.c ..\C_DLL\Win32\Debug\MemDB2Dll.lib
 * Then put MemDB2Dll.dll next to example.exe (or on the PATH) to run it.
 */
#include <stdio.h>
#include <string.h>
#include "memdb2.h"

/* Print the last error and jump to cleanup on failure. */
#define CHECK(call)                                                     \
    do {                                                                \
        int32_t rc_ = (call);                                           \
        if (rc_ != MDB_OK) {                                            \
            char msg_[1024];                                            \
            size_t len_ = 0;                                            \
            mdb_last_error_message(msg_, sizeof msg_, &len_);           \
            fprintf(stderr, "%s failed (%d): %s\n", #call, (int)rc_, msg_); \
            goto cleanup;                                               \
        }                                                               \
    } while (0)

int main(int argc, char **argv)
{
    mdb_handle db = 0, session = 0, txn = 0, meta = 0, data = 0, key = 0, names = 0;
    int32_t found = 0, count = 0, i;
    char name[256];
    size_t len = 0;
    int ok = 0;

    if (argc != 2) {
        fprintf(stderr, "Usage: example <database folder>\n");
        return 2;
    }
    printf("MemDB2 C API version %d\n", (int)mdb_api_version());

    CHECK(mdb_db_open(argv[1], &db));
    CHECK(mdb_session_open(db, &session));

    /* Schema: create the table and index if they don't exist. */
    CHECK(mdb_txn_begin(session, MDB_ACCESS_WRITE_EXCLUSIVE, MDB_SYNC_LAZY_WRITE,
                        MDB_ISO_REPEATABLE_READ, &txn));
    if (mdb_txn_open_table_meta(txn, "People", &meta) == MDB_ERR_NOT_FOUND) {
        CHECK(mdb_txn_create_table(txn, "People"));
        CHECK(mdb_txn_open_table_meta(txn, "People", &meta));
        CHECK(mdb_meta_create_field(meta, "Id", MDB_FT_INT32));
        CHECK(mdb_meta_create_field(meta, "Name", MDB_FT_STRING));
        CHECK(mdb_meta_create_index(meta, "PeopleById", "Id",
                                    MDB_INDEX_UNIQUE | MDB_INDEX_NOT_EMPTY));
    }
    CHECK(mdb_txn_commit(txn));   /* also closes meta */
    txn = meta = 0;

    /* Data: add Id 1 if missing. */
    CHECK(mdb_key_create(&key));
    CHECK(mdb_key_add_int32(key, 1));
    CHECK(mdb_txn_begin(session, MDB_ACCESS_WRITE_EXCLUSIVE, MDB_SYNC_LAZY_WRITE,
                        MDB_ISO_REPEATABLE_READ, &txn));
    CHECK(mdb_txn_open_table_data(txn, "People", &data));
    CHECK(mdb_data_find(data, "PeopleById", key, &found));
    if (!found) {
        CHECK(mdb_data_append(data));
        CHECK(mdb_data_write_int32(data, "Id", 1));
        CHECK(mdb_data_write_string(data, "Name", "Zo\xC3\xAB"));  /* UTF-8 */
        CHECK(mdb_data_post(data));
    }
    CHECK(mdb_txn_commit(txn));
    txn = data = 0;

    /* Read it back. */
    CHECK(mdb_txn_begin(session, MDB_ACCESS_READ_SHARED, MDB_SYNC_LAZY_WRITE,
                        MDB_ISO_REPEATABLE_READ, &txn));
    CHECK(mdb_txn_open_table_data(txn, "People", &data));
    CHECK(mdb_data_find(data, "PeopleById", key, &found));
    if (found) {
        CHECK(mdb_data_read_string(data, "Name", name, sizeof name, &len));
        printf("Id 1: %s (%u bytes)\n", name, (unsigned)len);
    }
    CHECK(mdb_txn_entity_names(txn, &names));
    CHECK(mdb_strlist_count(names, &count));
    for (i = 0; i < count; i++) {
        CHECK(mdb_strlist_get(names, i, name, sizeof name, &len));
        printf("Entity: %s\n", name);
    }
    CHECK(mdb_txn_commit(txn));
    txn = data = 0;
    ok = 1;

cleanup:
    /* Closing the database closes its session and any open transaction
       (rolling it back). Keys and string lists are closed separately. */
    if (names) mdb_close(names);
    if (key) mdb_close(key);
    if (db) mdb_db_close(db);
    mdb_shutdown();   /* closes anything still open; do this before FreeLibrary */
    return ok ? 0 : 1;
}
