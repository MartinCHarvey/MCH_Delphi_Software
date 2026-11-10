unit MemDB2BenchHooks;

{
  Optional benchmark hook for MemDB2Dll: counts calls to the DLL's Delphi
  memory manager so that mdb2bench can report allocations per transaction.

  To use it, add this unit to the uses clause of C_DLL\MemDB2Dll.dpr:

      MemDB2BenchHooks in '..\Bench\MemDB2BenchHooks.pas',

  and rebuild. It exports two extra cdecl functions, found by mdb2bench at
  run time with GetProcAddress (they are deliberately not in memdb2.h or
  MemDB2Dll.def, so the public API and import library are unchanged):

      int32_t mdb_bench_alloc_enable(int32_t on);
      int32_t mdb_bench_alloc_stats(uint64_t *allocs, uint64_t *frees,
                                    uint64_t *reallocs);

  Cost: while counting is off, one extra indirect call and a compare per
  memory manager call. While on, an interlocked increment as well, which is
  contended when several threads allocate at once. Leave it out of builds
  used for headline latency figures.

  The hook chains to whatever memory manager was installed before it
  (including PatchFreeMem's patched SysFreeMem), so blocks allocated before
  it was installed are freed correctly.
}

interface

implementation

var
  OldMM: TMemoryManagerEx;
  NewMM: TMemoryManagerEx;
  CountOn: Integer = 0;
  Allocs: Int64 = 0;
  Frees: Int64 = 0;
  Reallocs: Int64 = 0;

function HookGetMem(Size: NativeInt): Pointer;
begin
  if CountOn <> 0 then
    AtomicIncrement(Allocs);
  Result := OldMM.GetMem(Size);
end;

function HookFreeMem(P: Pointer): Integer;
begin
  if CountOn <> 0 then
    AtomicIncrement(Frees);
  Result := OldMM.FreeMem(P);
end;

function HookReallocMem(P: Pointer; Size: NativeInt): Pointer;
begin
  if CountOn <> 0 then
    AtomicIncrement(Reallocs);
  Result := OldMM.ReallocMem(P, Size);
end;

function HookAllocMem(Size: NativeInt): Pointer;
begin
  if CountOn <> 0 then
    AtomicIncrement(Allocs);
  Result := OldMM.AllocMem(Size);
end;

function HookRegisterLeak(P: Pointer): Boolean;
begin
  Result := OldMM.RegisterExpectedMemoryLeak(P);
end;

function HookUnregisterLeak(P: Pointer): Boolean;
begin
  Result := OldMM.UnregisterExpectedMemoryLeak(P);
end;

function mdb_bench_alloc_enable(Enable: Int32): Int32; cdecl;
begin
  AtomicExchange(CountOn, Ord(Enable <> 0));
  Result := 0;
end;

function mdb_bench_alloc_stats(OutAllocs, OutFrees, OutReallocs: PUInt64): Int32; cdecl;
begin
  //AtomicCmpExchange(X, 0, 0) is an atomic 64 bit read, also on Win32.
  if OutAllocs <> nil then
    OutAllocs^ := UInt64(AtomicCmpExchange(Allocs, 0, 0));
  if OutFrees <> nil then
    OutFrees^ := UInt64(AtomicCmpExchange(Frees, 0, 0));
  if OutReallocs <> nil then
    OutReallocs^ := UInt64(AtomicCmpExchange(Reallocs, 0, 0));
  Result := 0;
end;

exports
  mdb_bench_alloc_enable,
  mdb_bench_alloc_stats;

initialization
  GetMemoryManager(OldMM);
  NewMM.GetMem := HookGetMem;
  NewMM.FreeMem := HookFreeMem;
  NewMM.ReallocMem := HookReallocMem;
  NewMM.AllocMem := HookAllocMem;
  NewMM.RegisterExpectedMemoryLeak := HookRegisterLeak;
  NewMM.UnregisterExpectedMemoryLeak := HookUnregisterLeak;
  SetMemoryManager(NewMM);

finalization
  SetMemoryManager(OldMM);

end.
