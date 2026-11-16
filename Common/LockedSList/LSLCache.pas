unit LSLCache;

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

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  LockedSList,
  Reffed;

type
  TLSLCache = class;
  TLSLReffedCache = class;

  //Plain implementation.

{$IFDEF USE_TRACKABLES}
  TLSLObject = class(TTrackable)
{$ELSE}
  TLSLObject = class
{$ENDIF}
  private
    FSiblingEntry: TSLEntry;
  protected
    FMyCache: TLSLCache;
  public
    constructor Create;
    destructor Destroy; override;
    procedure CreateFromCache(Cache: TLSLCache); virtual;
    procedure Reset(Cache: TLSLCache); virtual;
    property MyCache: TLSLCache read FMyCache;
  end;

{$IFDEF USE_TRACKABLES}
  TLSLCache = class(TTrackable)
{$ELSE}
  TLSLCache = class
{$ENDIF}
  private
    FListHead: TSLListHead;
  public
    constructor Create;
    destructor Destroy; override;
    function GetFromCache: TLSLObject;
    procedure PutToCache(Obj: TLSLObject);
  end;

  // TReffed implementation.

  //Duplicate implementation because we want speed here,
  //and doing metaclass / reflection stuff will just slow things down.
  //A quick diff / claude to ensure identical not too much
  //hassle.

  TLSLReffed = class(TReffed)
  private
    FSiblingEntry: TSLEntry;
    FMyCache: TLSLReffedCache;
  public
    constructor Create;
    destructor Destroy; override;
    procedure CreateFromCache(Cache: TLSLReffedCache); virtual;
    procedure Reset(Cache: TReffedCache); override;
    property MyCache: TLSLReffedCache read FMyCache;
  end;

  //Most application defined reffed caches are purely thread local.
  //When added to cache, removed from datatstructures, cache access
  //is single threaded.

  //However, this one is a lock free stack shared between threads,
  //so slightly different requirements w.r.t object reset etc.
  TLSLReffedCache = class(TReffedCache)
  private
    FListHead: TSLListHead;
  protected
    function MakeObj: TLSLReffed; virtual;
  public
    constructor Create;
    destructor Destroy; override;
    function GetFromCache: TReffed; override;
    function PutToCache(Reffed: TReffed): boolean; override;
  end;

implementation

{ TLSLObject }

constructor TLSLObject.Create;
begin
  inherited;
  SLItemInitObj(@FSiblingEntry, self);
end;

destructor TLSLObject.Destroy;
begin
  Assert(SlItemIsObj(@FSiblingEntry));
  inherited;
end;

procedure TLSLObject.CreateFromCache(Cache: TLSLCache);
begin
end;

procedure TLSLObject.Reset(Cache: TLSLCache);
begin
end;

{ TSLSCache }

constructor TLSLCache.Create;
begin
  inherited;
  SLItemInitList(@FListHead);
end;

destructor TLSLCache.Destroy;
var
  Entry: PSLEntry;
begin
  Entry := SLListLockedPopItem(@FListHead);
  while Assigned(Entry) do
  begin
    Entry.Owner.Free;
    Entry := SLListLockedPopItem(@FListHead);
  end;
  Assert(SLItemIsList(@FListHead.Entry));
  Assert(not Assigned(FListHead.Entry.FLink));
  inherited;
end;

function TLSLCache.GetFromCache: TLSLObject;
var
  Entry: PSLEntry;
begin
  Entry := SLListLockedPopItem(@FListHead);
  if Assigned(Entry) then
  begin
{$IFOPT C+}
    result := Entry.Owner as TLSLObject;
{$ELSE}
    result := TLSLObject(Entry.Owner);
{$ENDIF}
  end
  else
    result := nil;
  if Assigned(Result) then
  begin
    result.FMyCache := self;
    result.CreateFromCache(self);
  end;
end;

procedure TLSLCache.PutToCache(Obj: TLSLObject);
begin
  if Assigned(Obj) then
  begin
    Obj.Reset(self);
    Obj.FMyCache := nil;
    //Reset before adding to cache, where shared, no exceptions to worry about
    //compared to thread local TList Caches.
    SLListLockedPushItem(@FListHead, @Obj.FSiblingEntry);
  end;
end;

//===============================================

{ TLSLReffed }

constructor TLSLReffed.Create;
begin
  inherited;
  SLItemInitObj(@FSiblingEntry, self);
end;

destructor TLSLReffed.Destroy;
begin
  Assert(SlItemIsObj(@FSiblingEntry));
  inherited;
end;

procedure TLSLReffed.CreateFromCache(Cache: TLSLReffedCache);
begin
end;

procedure TLSLReffed.Reset(Cache: TReffedCache);
begin
  Assert(Cache is TLSLReffedCache);
end;

{ TLSLReffedCache }

function TLSLReffedCache.MakeObj: TLSLReffed;
begin
  result := nil;
end;

constructor TLSLReffedCache.Create;
begin
  inherited;
  SLItemInitList(@FListHead);
end;

destructor TLSLReffedCache.Destroy;
var
  Entry: PSLEntry;
begin
  Entry := SLListLockedPopItem(@FListHead);
  //Reminder. Reffed.Reset sets the refcount back to 1 whilst it's in
  //the cache.
  while Assigned(Entry) do
  begin
{$IFOPT C+}
    (Entry.Owner as TReffed).Release;
{$ELSE}
    TReffed(Entry.Owner).Release;
{$ENDIF}
    Entry := SLListLockedPopItem(@FListHead);
  end;
  Assert(SLItemIsList(@FListHead.Entry));
  Assert(not Assigned(FListHead.Entry.FLink));
  inherited;
end;

function TLSLReffedCache.GetFromCache: TReffed;
var
  Entry: PSLEntry;
begin
  Entry := SLListLockedPopItem(@FListHead);
  if Assigned(Entry) then
  begin
{$IFOPT C+}
    result := Entry.Owner as TLSLReffed;
{$ELSE}
    result := TLSLReffed(Entry.Owner);
{$ENDIF}
  end
  else
    result := MakeObj;
  if Assigned(Result) then
  begin
{$IFOPT C+}
    (result as TLSLReffed).FMyCache := self;
{$ELSE}
    TLSLReffed(result).FMyCache := self;
{$ENDIF}
    if Assigned(Entry) then //got from cache, not made fresh.
    begin
  {$IFOPT C+}
      (result as TLSLReffed).CreateFromCache(self);
  {$ELSE}
      TLSLReffed(result).CreateFromCache(self);
  {$ENDIF}
    end;
  end;
end;

function TLSLReffedCache.PutToCache(Reffed: TReffed): boolean;
begin
  if Assigned(Reffed) then
  begin
    //Reset before adding to cache, where shared, no exceptions to worry about
    //compared to thread local TList Caches.

    //Reminder. Reffed.Reset sets the refcount back to 1 whilst it's in
    //the cache.
    Reffed.Reset(self);
{$IFOPT C+}
    (Reffed as TLSLReffed).FMyCache := nil;
    SLListLockedPushItem(@FListHead, @(Reffed as TLSLReffed).FSiblingEntry);
{$ELSE}
    TLSLReffed(Reffed).FMyCache := nil;
    SLListLockedPushItem(@FListHead, @TLSLReffed(Reffed).FSiblingEntry);
{$ENDIF}
  end;
  result := true;
end;

end.
