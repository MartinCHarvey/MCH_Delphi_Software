library MemDB2Dll;
{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

{
  MemDB2 as a self-contained DLL with a C API (see ..\C_API\memdb2.h).

  Memory
    No Delphi strings, objects or dynamic arrays cross the DLL boundary:
    strings are UTF-8 char* copied in, or copied out into caller buffers, and
    objects are opaque handles. So ShareMem / BORLNDMM.DLL are not needed;
    the DLL has its own private memory manager.

  Loading (DLL_PROCESS_ATTACH)
    Unit initialization sections run here, under the Windows loader lock.
    None of the linked units starts or waits for a thread during
    initialization (checked: Trackables, MemDB2Misc, StripedLock, CRC32 only
    create objects, set up tables and patch this DLL's own SysFreeMem).

  Threads
    IsMultiThread is set below. The Delphi memory manager only takes its
    locks when IsMultiThread is true; C callers may call in from several
    threads before MemDB2 has started a Delphi thread of its own.

  Unloading (DLL_PROCESS_DETACH)
    Each open database runs a journal thread. Stopping it must happen while
    the DLL is loaded and outside the loader lock, so the host must call
    mdb_shutdown() (or close every database) before FreeLibrary.
    Finalization does not try to stop databases. Parallel work threads are
    created and joined per operation, so none outlive a call.
    At process exit Windows ends all threads first, so no call is needed.
}

uses
  System.SysUtils,
  System.Classes,
  MemDB2CApi in '..\C_API\MemDB2CApi.pas',
  Trackables in '..\..\Tracking\Trackables.pas',
  MemDB2 in '..\MemDB2.pas',
  MemDB2Buffered in '..\MemDB2Buffered.pas',
  MemDB2Misc in '..\MemDB2Misc.pas',
  IndexedStore in '..\..\Indexed Store\IndexedStore.pas',
  BinaryTree in '..\..\Balanced Tree\BinaryTree.pas',
  DLList in '..\..\DLList\DLList.pas',
  MemDB2Indexing in '..\MemDB2Indexing.pas',
  MemDB2Streamable in '..\MemDB2Streamable.pas',
  MemDB2Journal in '..\MemDB2Journal.pas',
  DLThreadQueue in '..\..\WorkItems\DLThreadQueue.pas',
  MemDB2Api in '..\MemDB2Api.pas',
  BufferedFileStream in '..\..\CachedStream\BufferedFileStream.pas',
  Parallelizer in '..\..\Parallelizer\Parallelizer.pas',
  NullStream in '..\..\NullStream\NullStream.pas',
  LockAbstractions in '..\..\LockAbstractions\LockAbstractions.pas',
  SRWLockWrapper in '..\..\SRWLockWrapper\SRWLockWrapper.pas',
  TinyLock in '..\..\LockAbstractions\TinyLock.pas',
  RWWLock in '..\..\LockAbstractions\RWWLock.pas',
  MemDB2BufBase in '..\MemDB2BufBase.pas',
  Reffed in '..\..\Reffed\Reffed.pas',
  CoWTree in '..\..\CoWTree\CoWTree.pas',
  StripedLock in '..\..\LockAbstractions\StripedLock.pas',
  CRC32 in '..\..\CRC\CRC32.pas',
  PatchFreeMem in '..\..\PatchFreeMem\PatchFreeMem.pas',
  PComp in '..\..\PComp\PComp.pas';

{$R *.res}

{$I ..\C_API\MemDB2CApi_Exports.inc}

begin
  //Must be set before any C thread calls in: see "Threads" above.
  IsMultiThread := true;
end.
