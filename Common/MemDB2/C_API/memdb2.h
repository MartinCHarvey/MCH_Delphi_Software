/*
 * memdb2.h - C API for MemDB2.
 *
 * Implemented in Delphi by MemDB2CApi.pas and exported from the MemDB2 DLL.
 * API version: MDB_API_VERSION (check mdb_api_version() at run time).
 *
 * ---------------------------------------------------------------------------
 * Rules
 * ---------------------------------------------------------------------------
 *
 * Calling convention
 *   Every function is cdecl (MDB_CALL).
 *
 * Errors
 *   Every function except mdb_api_version, mdb_last_error_code and
 *   mdb_last_error_message returns an int32_t status: MDB_OK (0) on success,
 *   or a negative MDB_ERR_* code. The library never lets an exception escape.
 *   After a failure, mdb_last_error_message() gives a UTF-8 description of the
 *   most recent failure on the calling thread (stored per thread, so threads
 *   do not overwrite each other's messages). A successful call clears it.
 *   Messages longer than 1023 bytes are truncated.
 *
 *   MDB_ERR_CONCURRENCY means the operation clashed with another transaction;
 *   roll back and retry. MDB_ERR_API means the call was not valid in the
 *   current state (for example reading a field with no row selected).
 *
 * Handles
 *   Every object is an opaque 64 bit handle (mdb_handle). 0 is never valid.
 *   A closed, stale or wrong type handle is rejected with MDB_ERR_BAD_HANDLE
 *   or MDB_ERR_WRONG_HANDLE_TYPE; it is never dereferenced.
 *
 *   Ownership tree. Closing a handle closes everything beneath it first:
 *
 *       database
 *         session
 *           transaction
 *             table metadata / table data / foreign key objects
 *
 *   Closing (or rolling back) a transaction that has not been committed rolls
 *   it back. mdb_txn_rollback always finishes the transaction: its handle
 *   and its child handles are closed.
 *
 *   mdb_txn_commit first closes the transaction's table / foreign key
 *   handles (MemDB2 will not commit while they are open), then commits:
 *     - success: the transaction handle is closed.
 *     - failure: as in MemDB2, the transaction stays open and its handle
 *       stays valid. Roll it back with mdb_txn_rollback, or retry.
 *   While a commit is running, any other use of that transaction handle
 *   (including closing it, or a parent, from another thread) fails with
 *   MDB_ERR_HANDLE_BUSY, and the parent session / database stay open.
 *
 *   Search keys and string lists are independent: they belong to nobody and
 *   stay valid until you mdb_close them, even after the transaction that
 *   produced a string list has finished.
 *
 *   mdb_close(h) closes any kind of handle. The specific close functions
 *   (mdb_db_close, mdb_session_close) also check the handle type.
 *
 * Strings
 *   All strings are UTF-8, NUL terminated, in both directions. Names and
 *   values are copied on the way in; the caller keeps ownership.
 *   Input strings must be valid UTF-8.
 *
 *   Output strings use a caller supplied buffer:
 *     - buf == NULL: size query. *out_len is set to the length in bytes,
 *       excluding the terminating NUL. Returns MDB_OK.
 *     - buf too small (buf_size < *out_len + 1): returns
 *       MDB_ERR_BUFFER_TOO_SMALL, sets *out_len, copies nothing.
 *     - otherwise the string and a terminating NUL are copied, and *out_len
 *       is set to the length excluding the NUL.
 *
 * Blobs
 *   Same buffer pattern as strings, but with no NUL: buf == NULL queries the
 *   size; a buffer smaller than the blob gives MDB_ERR_BUFFER_TOO_SMALL.
 *   Writes copy the bytes; the caller keeps ownership of its buffer.
 *
 * Booleans
 *   Returned as int32_t: 0 = false, 1 = true.
 *
 * Threading
 *   A database handle may be shared between threads, and each thread may open
 *   its own sessions. A session, and the transactions and objects beneath it,
 *   must only be used by one thread at a time. Do not close a database while
 *   other threads are still using its sessions.
 *
 * Shutdown
 *   Call mdb_shutdown() (or close every database yourself) before unloading
 *   the DLL with FreeLibrary. Each open database runs a journal thread that
 *   must be stopped while the DLL is still loaded; this cannot be done safely
 *   during unload itself (Windows holds the loader lock), so the DLL does not
 *   try. At process exit Windows ends those threads, so no call is needed then.
 */

#ifndef MEMDB2_H
#define MEMDB2_H

#include <stddef.h>
#include <stdint.h>

#ifdef _WIN32
  #define MDB_CALL __cdecl
  #ifdef MDB_BUILDING_DLL
    #define MDB_API __declspec(dllexport)
  #else
    #define MDB_API __declspec(dllimport)
  #endif
#else
  #define MDB_CALL
  #define MDB_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define MDB_API_VERSION 1

typedef uint64_t mdb_handle;

/* Same layout as the Windows GUID / Delphi TGUID. */
typedef struct mdb_guid {
    uint32_t data1;
    uint16_t data2;
    uint16_t data3;
    uint8_t  data4[8];
} mdb_guid;

/* Status codes. */
#define MDB_OK                      0
#define MDB_ERR_INVALID_ARG        -1   /* NULL pointer, bad enum value, out of range */
#define MDB_ERR_BAD_HANDLE         -2   /* not a live handle */
#define MDB_ERR_WRONG_HANDLE_TYPE  -3   /* live handle of another kind */
#define MDB_ERR_NOT_FOUND          -4   /* no such table, foreign key, field or index */
#define MDB_ERR_BUFFER_TOO_SMALL   -5   /* see Strings / Blobs above */
#define MDB_ERR_TYPE_MISMATCH      -6   /* field is not the type requested */
#define MDB_ERR_DB_STATE           -7   /* database could not open, or is in an error state */
#define MDB_ERR_HANDLE_BUSY        -8   /* transaction is mid-commit in another call */
#define MDB_ERR_API               -10   /* call not valid in the current state */
#define MDB_ERR_CONCURRENCY       -11   /* clash with another transaction: roll back, retry */
#define MDB_ERR_CONSISTENCY       -12   /* consistency check failed */
#define MDB_ERR_INTERNAL          -13   /* internal error */
#define MDB_ERR_DB                -14   /* other database error */
#define MDB_ERR_OUT_OF_MEMORY     -15
#define MDB_ERR_UNEXPECTED        -99   /* any other failure */

/* Field types. */
#define MDB_FT_INT32   0
#define MDB_FT_UINT32  1
#define MDB_FT_INT64   2
#define MDB_FT_UINT64  3
#define MDB_FT_STRING  4
#define MDB_FT_DOUBLE  5
#define MDB_FT_BLOB    6
#define MDB_FT_GUID    7

/* Transaction access modes. */
#define MDB_ACCESS_READ_SHARED        0
#define MDB_ACCESS_WRITE_EXCLUSIVE    1
#define MDB_ACCESS_READ_WRITE_SHARED  2

/* Transaction sync modes. */
#define MDB_SYNC_LAZY_WRITE     0
#define MDB_SYNC_FLUSH_BUFFERS  1

/* Transaction isolation levels. */
#define MDB_ISO_READ_COMMITTED   0
#define MDB_ISO_REPEATABLE_READ  1   /* MemDB2's default */
#define MDB_ISO_SNAPSHOT         2
#define MDB_ISO_SERIALISABLE     3

/* Navigation positions. Edge searches accept only FIRST and LAST. */
#define MDB_POS_FIRST     0
#define MDB_POS_LAST      1
#define MDB_POS_NEXT      2
#define MDB_POS_PREVIOUS  3

/* Index attribute flags (combine with |). */
#define MDB_INDEX_UNIQUE     1
#define MDB_INDEX_NOT_EMPTY  2

/* Session temporary storage. */
#define MDB_TEMP_MEMORY  0
#define MDB_TEMP_DISK    1

/* Database phases (mdb_db_state). */
#define MDB_PHASE_NULL                        0
#define MDB_PHASE_INIT                        1
#define MDB_PHASE_INIT_CHECKPOINT             2
#define MDB_PHASE_RUNNING                     3
#define MDB_PHASE_RUNNING_ERROR               4   /* open, but commits fail */
#define MDB_PHASE_CLOSING_WAIT_CLIENTS        5
#define MDB_PHASE_CLOSING_WAIT_CLIENTS_ERROR  6
#define MDB_PHASE_CLOSING_WAIT_PERSIST        7
#define MDB_PHASE_CLOSED                      8
#define MDB_PHASE_CLOSED_ERROR                9

/* ---------------------------------------------------------------------------
 * General
 * ------------------------------------------------------------------------- */

/* Returns MDB_API_VERSION of the DLL. */
MDB_API int32_t MDB_CALL mdb_api_version(void);

/* Status of the most recent failure on this thread (MDB_OK if the last call
   succeeded). */
MDB_API int32_t MDB_CALL mdb_last_error_code(void);

/* Message for the most recent failure on this thread. Does not clear it. */
MDB_API int32_t MDB_CALL mdb_last_error_message(char *buf, size_t buf_size, size_t *out_len);

/* Close any handle, and everything beneath it. */
MDB_API int32_t MDB_CALL mdb_close(mdb_handle handle);

/* Close every open handle (databases, rolling back open transactions; keys;
   string lists). Call before FreeLibrary. Safe to call when nothing is open. */
MDB_API int32_t MDB_CALL mdb_shutdown(void);

/* ---------------------------------------------------------------------------
 * Database
 * ------------------------------------------------------------------------- */

/* Open (or create) the database stored in folder 'path'. Synchronous.
   On MDB_ERR_DB_STATE with *out_db != 0, the database opened in an error
   state: it can be read, but commits will fail. Close it when done. */
MDB_API int32_t MDB_CALL mdb_db_open(const char *path, mdb_handle *out_db);
MDB_API int32_t MDB_CALL mdb_db_close(mdb_handle db);
MDB_API int32_t MDB_CALL mdb_db_checkpoint(mdb_handle db);
MDB_API int32_t MDB_CALL mdb_db_state(mdb_handle db, int32_t *out_phase);
/* Reason for the database's most recent move to an error phase ("" if none). */
MDB_API int32_t MDB_CALL mdb_db_last_error(mdb_handle db, char *buf, size_t buf_size, size_t *out_len);

/* ---------------------------------------------------------------------------
 * Session
 * ------------------------------------------------------------------------- */

MDB_API int32_t MDB_CALL mdb_session_open(mdb_handle db, mdb_handle *out_session);
MDB_API int32_t MDB_CALL mdb_session_close(mdb_handle session);
MDB_API int32_t MDB_CALL mdb_session_get_temp_storage(mdb_handle session, int32_t *out_mode);
MDB_API int32_t MDB_CALL mdb_session_set_temp_storage(mdb_handle session, int32_t mode);

/* ---------------------------------------------------------------------------
 * Transaction, and database level operations within it
 * ------------------------------------------------------------------------- */

MDB_API int32_t MDB_CALL mdb_txn_begin(mdb_handle session, int32_t access, int32_t sync,
                                       int32_t isolation, mdb_handle *out_txn);
/* On failure the transaction stays open: call mdb_txn_rollback (or retry). */
MDB_API int32_t MDB_CALL mdb_txn_commit(mdb_handle txn);
MDB_API int32_t MDB_CALL mdb_txn_rollback(mdb_handle txn);

MDB_API int32_t MDB_CALL mdb_txn_create_table(mdb_handle txn, const char *name);
MDB_API int32_t MDB_CALL mdb_txn_create_foreign_key(mdb_handle txn, const char *name);
MDB_API int32_t MDB_CALL mdb_txn_rename_entity(mdb_handle txn, const char *old_name, const char *new_name);
MDB_API int32_t MDB_CALL mdb_txn_delete_entity(mdb_handle txn, const char *name);
/* Names of all tables and foreign keys, as a string list handle. */
MDB_API int32_t MDB_CALL mdb_txn_entity_names(mdb_handle txn, mdb_handle *out_list);

/* Open an object on a table or foreign key. MDB_ERR_NOT_FOUND if there is no
   entity of that kind with that name. The handle belongs to the transaction. */
MDB_API int32_t MDB_CALL mdb_txn_open_table_meta(mdb_handle txn, const char *name, mdb_handle *out_meta);
MDB_API int32_t MDB_CALL mdb_txn_open_table_data(mdb_handle txn, const char *name, mdb_handle *out_data);
MDB_API int32_t MDB_CALL mdb_txn_open_foreign_key(mdb_handle txn, const char *name, mdb_handle *out_fk);

/* ---------------------------------------------------------------------------
 * Table metadata
 * ------------------------------------------------------------------------- */

MDB_API int32_t MDB_CALL mdb_meta_create_field(mdb_handle meta, const char *name, int32_t field_type);
MDB_API int32_t MDB_CALL mdb_meta_delete_field(mdb_handle meta, const char *name);
MDB_API int32_t MDB_CALL mdb_meta_rename_field(mdb_handle meta, const char *old_name, const char *new_name);
/* MDB_ERR_NOT_FOUND if there is no such field. */
MDB_API int32_t MDB_CALL mdb_meta_field_type(mdb_handle meta, const char *name, int32_t *out_type);
MDB_API int32_t MDB_CALL mdb_meta_field_names(mdb_handle meta, mdb_handle *out_list);

/* attrs: MDB_INDEX_* flags. */
MDB_API int32_t MDB_CALL mdb_meta_create_index(mdb_handle meta, const char *name,
                                               const char *field_name, int32_t attrs);
MDB_API int32_t MDB_CALL mdb_meta_create_multi_index(mdb_handle meta, const char *name,
                                                     const char *const *field_names,
                                                     int32_t field_count, int32_t attrs);
MDB_API int32_t MDB_CALL mdb_meta_delete_index(mdb_handle meta, const char *name);
MDB_API int32_t MDB_CALL mdb_meta_rename_index(mdb_handle meta, const char *old_name, const char *new_name);
/* Indexed field names (in order) as a string list handle, plus MDB_INDEX_*
   flags. MDB_ERR_NOT_FOUND if there is no such index. */
MDB_API int32_t MDB_CALL mdb_meta_index_info(mdb_handle meta, const char *name,
                                             mdb_handle *out_field_list, int32_t *out_attrs);
MDB_API int32_t MDB_CALL mdb_meta_index_names(mdb_handle meta, mdb_handle *out_list);

/* ---------------------------------------------------------------------------
 * Table data: navigation and editing
 *
 * A table data handle is a cursor. Navigation selects a row; *out_found is 0
 * when there is no (further) row. Field reads and writes act on the selected
 * row, or on the new row after mdb_data_append. Writes must be posted
 * (mdb_data_post) or discarded (mdb_data_discard) before navigating again.
 * ------------------------------------------------------------------------- */

/* index_name NULL or "" walks rows in storage order; otherwise in index order. */
MDB_API int32_t MDB_CALL mdb_data_locate(mdb_handle data, int32_t pos, const char *index_name,
                                         int32_t *out_found);
/* Find a row whose index fields equal the key (one value per indexed field). */
MDB_API int32_t MDB_CALL mdb_data_find(mdb_handle data, const char *index_name, mdb_handle key,
                                       int32_t *out_found);
/* Find the first (MDB_POS_FIRST) or last (MDB_POS_LAST) row equal to the key.
   Continue with mdb_data_locate(MDB_POS_NEXT / MDB_POS_PREVIOUS, same index),
   checking the index fields, to visit all matching rows. */
MDB_API int32_t MDB_CALL mdb_data_find_edge(mdb_handle data, int32_t pos, const char *index_name,
                                            mdb_handle key, int32_t *out_found);
MDB_API int32_t MDB_CALL mdb_data_row_selected(mdb_handle data, int32_t *out_selected);
MDB_API int32_t MDB_CALL mdb_data_append(mdb_handle data);
MDB_API int32_t MDB_CALL mdb_data_post(mdb_handle data);
MDB_API int32_t MDB_CALL mdb_data_discard(mdb_handle data);
/* Delete the selected row. auto_inc != 0: the cursor moves on to the next row
   in the direction of travel (check mdb_data_row_selected); 0: no row is
   selected afterwards. */
MDB_API int32_t MDB_CALL mdb_data_delete(mdb_handle data, int32_t auto_inc);

/* ---------------------------------------------------------------------------
 * Table data: field values. Reads check the field's type (MDB_ERR_TYPE_MISMATCH).
 * ------------------------------------------------------------------------- */

MDB_API int32_t MDB_CALL mdb_data_read_int32(mdb_handle data, const char *field, int32_t *out_value);
MDB_API int32_t MDB_CALL mdb_data_read_uint32(mdb_handle data, const char *field, uint32_t *out_value);
MDB_API int32_t MDB_CALL mdb_data_read_int64(mdb_handle data, const char *field, int64_t *out_value);
MDB_API int32_t MDB_CALL mdb_data_read_uint64(mdb_handle data, const char *field, uint64_t *out_value);
MDB_API int32_t MDB_CALL mdb_data_read_double(mdb_handle data, const char *field, double *out_value);
MDB_API int32_t MDB_CALL mdb_data_read_guid(mdb_handle data, const char *field, mdb_guid *out_value);
MDB_API int32_t MDB_CALL mdb_data_read_string(mdb_handle data, const char *field,
                                              char *buf, size_t buf_size, size_t *out_len);
MDB_API int32_t MDB_CALL mdb_data_read_blob(mdb_handle data, const char *field,
                                            void *buf, size_t buf_size, size_t *out_len);

MDB_API int32_t MDB_CALL mdb_data_write_int32(mdb_handle data, const char *field, int32_t value);
MDB_API int32_t MDB_CALL mdb_data_write_uint32(mdb_handle data, const char *field, uint32_t value);
MDB_API int32_t MDB_CALL mdb_data_write_int64(mdb_handle data, const char *field, int64_t value);
MDB_API int32_t MDB_CALL mdb_data_write_uint64(mdb_handle data, const char *field, uint64_t value);
MDB_API int32_t MDB_CALL mdb_data_write_double(mdb_handle data, const char *field, double value);
MDB_API int32_t MDB_CALL mdb_data_write_guid(mdb_handle data, const char *field, const mdb_guid *value);
MDB_API int32_t MDB_CALL mdb_data_write_string(mdb_handle data, const char *field, const char *value);
/* value may be NULL only when len == 0 (empty blob). */
MDB_API int32_t MDB_CALL mdb_data_write_blob(mdb_handle data, const char *field,
                                             const void *value, size_t len);

/* ---------------------------------------------------------------------------
 * Search keys: one value per indexed field, in index order.
 * Blob values cannot be used in keys.
 * ------------------------------------------------------------------------- */

MDB_API int32_t MDB_CALL mdb_key_create(mdb_handle *out_key);
MDB_API int32_t MDB_CALL mdb_key_clear(mdb_handle key);
MDB_API int32_t MDB_CALL mdb_key_count(mdb_handle key, int32_t *out_count);
MDB_API int32_t MDB_CALL mdb_key_add_int32(mdb_handle key, int32_t value);
MDB_API int32_t MDB_CALL mdb_key_add_uint32(mdb_handle key, uint32_t value);
MDB_API int32_t MDB_CALL mdb_key_add_int64(mdb_handle key, int64_t value);
MDB_API int32_t MDB_CALL mdb_key_add_uint64(mdb_handle key, uint64_t value);
MDB_API int32_t MDB_CALL mdb_key_add_double(mdb_handle key, double value);
MDB_API int32_t MDB_CALL mdb_key_add_guid(mdb_handle key, const mdb_guid *value);
MDB_API int32_t MDB_CALL mdb_key_add_string(mdb_handle key, const char *value);

/* ---------------------------------------------------------------------------
 * Foreign keys: the child table's index references the parent table's index.
 * ------------------------------------------------------------------------- */

MDB_API int32_t MDB_CALL mdb_fk_set_referencing_child(mdb_handle fk, const char *table_name,
                                                      const char *index_name);
MDB_API int32_t MDB_CALL mdb_fk_set_referenced_parent(mdb_handle fk, const char *table_name,
                                                      const char *index_name);
MDB_API int32_t MDB_CALL mdb_fk_get_child_table(mdb_handle fk, char *buf, size_t buf_size, size_t *out_len);
MDB_API int32_t MDB_CALL mdb_fk_get_child_index(mdb_handle fk, char *buf, size_t buf_size, size_t *out_len);
MDB_API int32_t MDB_CALL mdb_fk_get_parent_table(mdb_handle fk, char *buf, size_t buf_size, size_t *out_len);
MDB_API int32_t MDB_CALL mdb_fk_get_parent_index(mdb_handle fk, char *buf, size_t buf_size, size_t *out_len);

/* ---------------------------------------------------------------------------
 * String lists (returned by the *_names and index_info functions).
 * Close with mdb_close when done.
 * ------------------------------------------------------------------------- */

MDB_API int32_t MDB_CALL mdb_strlist_count(mdb_handle list, int32_t *out_count);
MDB_API int32_t MDB_CALL mdb_strlist_get(mdb_handle list, int32_t index,
                                         char *buf, size_t buf_size, size_t *out_len);

#ifdef __cplusplus
}
#endif

#endif /* MEMDB2_H */
