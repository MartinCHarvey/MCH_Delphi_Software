# MCH_Delphi_Software
GitHub repo for open sourced MCH Software.

Most recent (2026) development is in Common/MemDB2, an in-memory database engine, now with a C API, Android support and asynchronous checkpoints. See below for details.

*MemDB (the original) is now legacy code, and will be removed at the start of Dec 2026.*

Also:
- TP Lex / Yacc port for Delphi now at: https://github.com/MartinCHarvey/MCH-TPLY41
  - Now has an OO mode "-oo" for generating object orientated code.
  - Support via additional declarations "%classname" "%classdecl" "%classfunc" "%classvar" for OO encapsulated lexers and parsers.

If any of you folks get some of this to compile / run under the FPC compiler, I'd be very interested to know.

martin_c_harvey@hotmail.com

# Summary of contents.

Files / Folders:

- Common/MemDB2: In-memory database engine, successor to MemDB.
  - Now suitable for general use, with many bugfixes from AI review. *Significantly more robust*.
  - Asynchronous checkpoints: checkpoint without blocking shared read/write transactions.
  - C API, and pre-built binaries for Visual Studio jockeys.
  - Compiles & runs on both **Win32/64 and Android 32/64**.
    - If you want to build for Android (running Delphi > XE4), remove "USE_WINDOWS_LOCKS" from project conditional defines, that bugfix is no longer needed.
  - Docs\ folder contains Claude code review for those of you worried about code quality.
  - Upgrade tool (MemDB to MemDB2 format) on the MemDB_Upgrade_Tool branch.
  - SQL support in progress.

- KnowComment: A Delphi client for Instagram (and eventually Twitter too, but needs a few fixes). Needs port to MemDB2 in the next month.

- CoCoDM: A Delphi port of the CoCo/R parser generator.

- ProjectXPlant: A handy utility which parses delphi project files and copies projects from one location to another.

- CheckIn: Simple hard-coded COVID-19 inspired web application.
  - Requires users to check-in online regularly.
  - Sends notifications to nominated contacts if they don't.

- Sudoku: A sudoku solver.
  - Simple testapp for Exact cover DLX algorithm. Allows you to load, save, and solve sudoku problems fast.

- Sudoku / Symmetries.
  - Some ways of finding isomorphic board configurations.
    - Graph method has errors, but included for interest.
    - Fastest is "CellCountMT", still being optimised.

- NQueens: Solves the N queens problem.
  - Another, perhaps simpler, example of the Exact cover algorithm.

- MFract.
  - A simple fractal generator (Mandelbrot / Julia). Install MCHComponents bpl first.

Common: Common utility code, including:

- MCHComponents visual component library (required for MFract - TSizeableImage).
  - Open project, right click MCHComponents.bpl, select "install" to add to IDE palette.

- Raw AVL tree.
- Copy on write AVL tree (CowTree).
- Read and Write cached memory streams.
- Doubly linked lists.
- HTML / JSON / CSS / Javascript parsers.
- HTTP fetcher.
- Combined HTTP fetch and document parse.
- Indexed store (see http://www.martincharvey.net/ for more info).
- Logging code.
- MemDB2 in memory database engine (see above), and legacy MemDB.
- Ordinal set handling.
- Reconfigurable lexer.
- Custom object streaming system.
- Debug object tracking.
- Custom threadpool implementation.
- Sparse Matrices.
- DLX like "Algorithm X" implementation for the exact cover problem.
- Parallelizer: Spawn threads to run method invocations in parallel.

Additional testapps and utility code.

- CoWTree testapp: Common/CowTree/Test
- MemDB2 testapp: Common/MemDB2/Test/
- MemDB2 C API example: Common/MemDB2/C_API/example.c
- MemDB testapp (legacy): Common/MemDB/Test/
- Lexer testapp: Common/SoftLexer/Test/
- Streaming system testapp: Common/StreamingSystem/Test/
- Threadpool testapp: Common/WorkItems/Test/
- Threadpool testapp: Common/WorkItems/Test2/
- Threadpool testapp: Common/WorkItems/Test3/
- Instagram / Twitter importer testapp MMapper/
- FetchParse library testapp: PageMapper/
- Sparse Matrix testapp Common/SparseMatrix/Test
- ExactCover testapp Common/ExactCover/Test
- Batch Sudoku solver (Sudoku/BatchThreadedSolver)

Future development plans:

- Clean up parser error recovery code with a "standard" lexer.
- MemDB2: Query engine (SQL).
- Investigate compilability under FreePascal compiler.

Comments / queries to martin_c_harvey@hotmail.com
