# MCH_Delphi_Software
GitHub repo for open sourced MCH Software.

Most recent (2026) development is in Common/MemDB2:

- Builds on MemDB code, quick, and developed up to the end of 2025.
- They share the same file format so you can swap back and forth between them, and of course 32/64 bit builds can r/w the same format.
- They also share very similar API's.
- MemDB best for single/lightly threaded straight-line workloads, and will be fastest for that use case.
- MemDB2 hopefully will be better for more deeply parallel / async workloads.
- MemDB2 is still work in progress.
  - Caching / locking / etc is still being worked/reworked.
  - _I'll let you know here when it's done and of acceptable quality, which it isn't yet._
- SQL support for both MemDB and MemDB2 to follow.

If any of you folks get some of this to compile / run under the FPC compiler, I'd be very interested to know.

martin_c_harvey@hotmail.com

# Summary of contents.

Files / Folders:

- KnowComment: A Delphi client for Instagram (and eventually Twitter too, but needs a few fixes).

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
- Lightweight in memory database engine. Currently suitable for relatively small (in modern terms) datasets. E-mail me for an explanation of limitations. 
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
- MemDB testapp: Common/MemDB/Test/
- Lexer testapp: Common/SoftLexer/Test/
- Streaming system testapp: Common/StreamingSystem/Test/
- Threadpool testapp: Common/WorkItems/Test/
- Threadpool testapp: Common/WorkItems/Test2/
- Threadpool testapp: Common/WorkItems/Test3/
- Instagram / Twitter importer testapp MMapper/
- FetchParse library testapp: PageMapper/
- Sparse Matrix testapp Commmon/SparseMatrix/Test
- ExactCover testapp Common/ExactCover/Test
- Batch Sudoku solver (Sudoku/BatchThreadedSolver)

Future development plans:

- Clean up parser error recovery code with a "standard" lexer.
- MemDB: Query engine.
- MemDB (long term): Have a "pessimistic" buffering and locking model (as currently) for full SQL compatiblity, and a "lightweight parallel" mode which does not allow composite ops, and/or big restructuring, but does allow lightweght parallel write operations.
- Investigate compilability under FreePascal compiler.
- Investigate portability to mobile apps.

Comments / queries to martin_c_harvey@hotmail.com
