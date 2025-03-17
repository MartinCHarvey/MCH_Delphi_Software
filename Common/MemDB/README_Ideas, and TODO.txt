
MemDB: TODO and notes.

TODO - More optimizations.

- Indexes. Don't blow away at the start. Have a delete pending state,
  and blow them away at the beginning of the commit (not the precommit).
  Delete pending -> Always looks at current.

- Threading opts.
  - Decide when to use threads based on # of tables actually changed.
  - Think a bit harder about foreign key case.

TODO - Composite ops to allow us to do SQL ALTER etc etc ..

- Change index attrs.
- Change field types.

TODO - Mod API to do implicit mini commits for ops which are retryable.
     - review rename/delete conflicts.

TODO - Need a case insensitive attr for string indexes.

TODO - SQL query engine.
     - Parser - UTF8ToUnicode on LexString.

TODO - Later. Further optimisation possible at initial load time:
  - Stream directly into current buffer, no A/B commit stage.
  - Need to check indexing and FK still works.


Later:

Have two distinct buffering models:

- 1. Current A/B model with daisychained xactions.
- Heavyweight locking.
- Allows full SQL compliance (just a matter of codeing).
- But doesn't allow multi-write at same time.

2. Lighterweight locking with multiple writes in progress at same time.
   (write shared vs write exclusive).

- OK to disallow some sorts of operation (table restructure / delete / fkeys?).
- Would use for higher throughput / availability.

- Drop back to "heavyweight" mode on DB restart for admin operations.




---- Transaction handling can be improved: nested calls to start transaction - solving that is easy(ish),
   but more complicatedly, allowing multiple write transactions in progress at the same time,
   which requires a rethink to the buffering model.

Actually, it's pretty doable, and with finer grained locking as well.

Instead of current / next we have:

current (tbl/rowid/etc) + generation_count (not streamed).
        - possible next (ref transaction/minicommit) (gen_count_cloned_from)
        - possible next (ref transaction/minicommit) (gen_count_cloned_from)
        - possible next (ref transaction/minicommit) (gen_count_cloned_from)

Rollbacks do not decrement gen-counts? Mini-commit multi-transaction rollbacks
db is not necessarily the same as it was in a previous generation count,
"unrelated" (or seemingly unrelated) changes have occured in the DB since,

so you'd have to rollback a/b changes, and then attempt to immediately
apply *and commit* "inverse" changesets, before anyone else changed the
generation counts. That might still require a big MRSW lock, which would
then be the "commit-rollback lock". No-one else would be allowed
to commit changes whilst you wanted to do a rollback.

In pre-commit, we check the gen_counts, and if wrong, abort the transaction.

Can then have per-entity and per-row locking.

Leave this for later after composite API, and SQL engine.

It *is* doable, but it's tricky. Might require retries at higher levels.

