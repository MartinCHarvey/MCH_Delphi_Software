A quick overview of the various conditionals in MemDB code.

Conditional:                    Suggested Debug value.      Suggested Release Value.

MAGIC_CHECKS                    FALSE                       FALSE
Conditional in debug object tracking used to detect wholsale memory corruption.

USE_TRACKABLES                  TRUE (if speed not vital).  FALSE
Used to detect memory leaks specifically in MemDB and related objects.

AUTOREFCOUNT                    LEAVE UNCHANGED             LEAVE UNCHANGED.
Platform definition pulled in from headers.

DEBUG_DATABASE                  FALSE                       FALSE
Detailed database logging for debug purposes.

DEBUG_DATABASE_DELETE           FALSE                       FALSE
Detailed database logging for debug purposes.

DEBUG_DATABASE_NAVIGATE         FALSE                       FALSE
Detailed database logging for debug purposes.

DEBUG_INDEXING_CHECK            FALSE                       FALSE
Detailed database logging for debug purposes.

MSWINDOWS                       LEAVE UNCHANGED             LEAVE UNCHANGED.
Platform definition pulled in from headers.

JOURNAL_WRITE_FLOW_CONTROL      APP SPECIFIC.               APP SPECIFIC.
Optional flow control if slow I/O and many transactions.

USE_WINDOWS_LOCKS               APP SPECIFIC.               APP SPECIFIC.
If true, uses Win32 synchronization primitives, else uses platform agnostic
Delphi definitions. Slight performance gain, might be useful for lock
debugging.

WIN32                           LEAVE UNCHANGED             LEAVE UNCHANGED.
Platform definition pulled in from headers.

