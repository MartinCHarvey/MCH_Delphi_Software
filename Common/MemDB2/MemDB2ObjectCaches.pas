unit MemDB2ObjectCaches;

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

interface

{ Cache a bunch of things which get allocated and freed frequently. }

uses
  LSLCache, MemDB2Misc;

type
  TMemDBCacheBlock = class;

  TMemDBObjectCache = class(TLSLCache)
  private
    FBlock: TMemDBCacheBlock;
  public
    property Block: TMemDBCacheBlock read FBlock;
  end;

  TTidLocalCache = class(TMemDBObjectCache);
  TMemDBNodeCacheCache = class(TMemDBObjectCache);

  TMemDBCacheBlock = class
  private
    FTidLocalCache: TTidLocalCache;
    FNodeCacheCache: TMemDBNodeCacheCache;
    FInternalNodeCacheCache: TMemDBNodeCacheCache;
  public
    constructor Create;
    destructor Destroy; override;
    property TidLocalCache: TTidLocalCache read FTidLocalCache;
    property NodeCacheCache: TMemDBNodeCacheCache read FNodeCacheCache;
    property InternalNodeCacheCache: TMemDBNodeCacheCache read FInternalNodeCacheCache;
  end;

  function GetCacheBlockForTid(const Tid: TTransactionID): TMemDbCacheBlock;

implementation

const
  CACHE_STRIPE_BITS = 3;                       // 8 stripes; 2 = 4 stripes
  CACHE_STRIPES = 1 shl CACHE_STRIPE_BITS;

var
  CacheBlocks: array[0..CACHE_STRIPES - 1] of TMemDBCacheBlock;
  i: integer;

{ Objects are cached in CACHE_STRIPES blocks, so that concurrent transactions
  mostly use different list heads. The block is chosen by a hash of the
  transaction ID, not the thread ID: we don't want to create TThread objects
  for threads we didn't create (e.g. C callers of the DLL), and a Tid collision
  only lasts for one transaction.
  An object always goes back to the block it came from (TLSLObject.MyCache). }

{$IFOPT Q+}
  {$DEFINE MEMDB_Q_WAS_ON}
  {$Q-}
{$ENDIF}
function GetCacheBlockForTid(const Tid: TTransactionID): TMemDBCacheBlock;
var
  h: UInt64;
  L1, L2, L3, L4: LongWord;
begin
  L1 := Tid.G.D1;
  L2 := LongWord(Tid.G.D2) or ((LongWord(Tid.G.D3) shl 16));
  L3 := PLongWord(@Tid.G.D4[0])^;
  L4 := PLongWord(@Tid.G.D4[4])^;
  h := L1 xor L2 xor L3 xor L4;
  //Fibonacci hashing: the top bits of the product are well mixed.
  h := h * UInt64($9E3779B97F4A7C15);
  result := CacheBlocks[h shr (64 - CACHE_STRIPE_BITS)];
end;
{$IFDEF MEMDB_Q_WAS_ON}
  {$Q+}
  {$UNDEF MEMDB_Q_WAS_ON}
{$ENDIF}

constructor TMemDBCacheBlock.Create;
begin
  FTidLocalCache := TTidLocalCache.Create;
  FTidLocalCache.FBlock := self;
  FNodeCacheCache := TMemDbNodeCacheCache.Create;
  FNodeCacheCache.FBlock := self;
  FInternalNodeCacheCache := TMemDbNodeCacheCache.Create;
  FInternalNodeCacheCache.FBlock := self;
  inherited;
end;

destructor TMemDBCacheBlock.Destroy;
begin
  FTidLocalCache.Free;
  FNodeCacheCache.Free;
  FInternalNodeCacheCache.Free;
  inherited;
end;


initialization
  for i := 0 to CACHE_STRIPES - 1 do
    CacheBlocks[i] := TMemDBCacheBlock.Create;
finalization
  for i := 0 to CACHE_STRIPES - 1 do
    CacheBlocks[i].Free;
end.
