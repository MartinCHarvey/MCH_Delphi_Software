/*
 * example.c - minimal use of the MemDB2 C API.
 *
 * Creates (or opens) a database in the folder given on the command line,
 * creates a "People" table with a unique index on Id, adds a row if it is
 * not there yet, then reads it back by index.
 * Blobs: a "Files" table keyed by a blob "Hash" field (unique index) with a
 * blob "Content" field, read back whole and as a prefix.
 *
 * Build (MSVC, from a Developer Command Prompt; see C_DLL\make_importlib.bat):
 *   cl /I..\C_API example.c ..\C_DLL\Win32\Debug\MemDB2Dll.lib
 * Then put MemDB2Dll.dll next to example.exe (or on the PATH) to run it.
 */
#include <stdio.h>
#include <string.h>
#include <stdint.h>
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
    mdb_handle hkey = 0, fdata = 0;
    int32_t found = 0, count = 0, i;
    char name[256];
    size_t len = 0, copied = 0;
    uint64_t total = 0;
    int ok = 0;
    /* Blob test data: a 4 byte "hash" key and 300 bytes of content. */
    static const unsigned char hash[4] = { 0xDE, 0xAD, 0xBE, 0xEF };
    unsigned char content[300], readback[300], prefix[16];

    if (argc != 2) {
        fprintf(stderr, "Usage: example <database folder>\n");
        return 2;
    }
    printf("MemDB2 C API version %d\n", (int)mdb_api_version());
    if (mdb_api_version() < 2) {
        fprintf(stderr, "Need C API version 2 or later for blob keys.\n");
        return 1;
    }
    for (i = 0; i < (int32_t)sizeof content; i++)
        content[i] = (unsigned char)(i * 7);

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
    if (meta) {               /* done with People's meta; Files needs its own */
        CHECK(mdb_close(meta));
        meta = 0;
    }
    if (mdb_txn_open_table_meta(txn, "Files", &meta) == MDB_ERR_NOT_FOUND) {
        CHECK(mdb_txn_create_table(txn, "Files"));
        CHECK(mdb_txn_open_table_meta(txn, "Files", &meta));
        CHECK(mdb_meta_create_field(meta, "Hash", MDB_FT_BLOB));
        CHECK(mdb_meta_create_field(meta, "Content", MDB_FT_BLOB));
        CHECK(mdb_meta_create_index(meta, "FilesByHash", "Hash",
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

    /* Blob data: add the file if its hash is not there yet. */
    CHECK(mdb_key_create(&hkey));
    CHECK(mdb_key_add_blob(hkey, hash, sizeof hash));  /* key keeps a copy */
    CHECK(mdb_txn_begin(session, MDB_ACCESS_WRITE_EXCLUSIVE, MDB_SYNC_LAZY_WRITE,
                        MDB_ISO_REPEATABLE_READ, &txn));
    CHECK(mdb_txn_open_table_data(txn, "Files", &data));
    CHECK(mdb_data_find(data, "FilesByHash", hkey, &found));
    if (!found) {
        CHECK(mdb_data_append(data));
        CHECK(mdb_data_write_blob(data, "Hash", hash, sizeof hash));
        CHECK(mdb_data_write_blob(data, "Content", content, sizeof content));
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
    CHECK(mdb_txn_open_table_data(txn, "Files", &fdata));
    CHECK(mdb_data_find(fdata, "FilesByHash", hkey, &found));
    if (found) {
        CHECK(mdb_data_read_blob(fdata, "Content", NULL, 0, &len));  /* size query */
        printf("Files: Content is %u bytes\n", (unsigned)len);
        CHECK(mdb_data_read_blob(fdata, "Content", readback, sizeof readback, &len));
        printf("Files: Content %s\n",
               (len == sizeof content && memcmp(readback, content, len) == 0)
                   ? "matches" : "DOES NOT MATCH");
        CHECK(mdb_data_read_blob_prefix(fdata, "Content", prefix, sizeof prefix,
                                        &copied, &total));
        printf("Files: prefix %u of %u bytes, byte[15] = %u\n",
               (unsigned)copied, (unsigned)total, (unsigned)prefix[15]);
    } else {
        printf("Files: blob key not found\n");
    }
    CHECK(mdb_txn_entity_names(txn, &names));
    CHECK(mdb_strlist_count(names, &count));
    for (i = 0; i < count; i++) {
        CHECK(mdb_strlist_get(names, i, name, sizeof name, &len));
        printf("Entity: %s\n", name);
    }
    CHECK(mdb_txn_commit(txn));
    txn = data = fdata = 0;
    ok = 1;

cleanup:
    /* Closing the database closes its session and any open transaction
       (rolling it back). Keys and string lists are closed separately. */
    if (names) mdb_close(names);
    if (key) mdb_close(key);
    if (hkey) mdb_close(hkey);
    if (db) mdb_db_close(db);
    mdb_shutdown();   /* closes anything still open; do this before FreeLibrary */
    return ok ? 0 : 1;
}
