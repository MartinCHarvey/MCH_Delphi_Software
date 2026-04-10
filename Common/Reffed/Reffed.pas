unit Reffed;

{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the �Software�), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED �AS IS�, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
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
  SysUtils, Classes;

type
{$IFDEF USE_TRACKABLES}
  TReffed = class(TTrackable)
{$ELSE}
  TReffed = class
{$ENDIF}
  private
    FRef: integer;
  public
    constructor Create;

{$IFOPT C+}
    function Unitary: boolean;
    function AddRef: TReffed;  //Where you know there shouldn't be a race conditon.
    function TryAddRef: TReffed; //Where you know there will be.
    function TryRelease: boolean;
    procedure Release;
{$ELSE}
    function Unitary: boolean; inline;
    function AddRef: TReffed; inline;
    function TryAddRef: TReffed;
    function TryRelease: boolean; inline;
    procedure Release; inline;
{$ENDIF}

    procedure Free;
  end;

  TReffedList = class(TReffed)
  private
    FList: TList;
  protected
    function GetCount: integer;
{$IFOPT C+}
    function GetItem(Idx: integer): TReffed;
    procedure SetItem(Idx: integer; Item: TReffed);
{$ELSE}
    function GetItem(Idx: integer): TReffed; inline;
    procedure SetItem(Idx: integer; Item: TReffed); inline;
{$ENDIF}
  public
    constructor Create;
    destructor Destroy; override;
    procedure ReleaseAndClear;
    //No delete sentinels in these lists - just allowing for
    //dbl buffered, atomic, ref counted switchover between two sets of stuff.
    function AddNoRef(Item: TReffed): integer;
    procedure Clear;
    procedure Pack;
    property Count: Integer read GetCount;
    property Items[idx:integer]: TReffed read GetItem write SetItem; default;
  end;

  TReffedProxy = class(TReffed)
  private
    FProxy: TObject;
  public
    destructor Destroy; override;
    property Proxy: TObject read FProxy write FProxy;
  end;


  EReffedError = class (Exception);

const
  S_REFFED_TEARDOWN_RACE_ADDING_REF = 'Error: Teardown race with AddRef.';
  S_REFFED_TEARDOWN_RACE_RELEASING_REF = 'Error: Teardown race with Release.';

implementation

uses
  LockAbstractions;

constructor TReffed.Create;
begin
  inherited;
  FRef := 1;
end;

function TReffed.AddRef: TReffed;
begin
  if Assigned(Self) then
  begin
    result := TryAddRef;
    if not Assigned(Result) then
      raise EReffedError.Create(S_REFFED_TEARDOWN_RACE_ADDING_REF);
    Assert(result = self);
  end
  else
    result := nil;
end;

function TReffed.TryAddRef: TReffed;
var
  Existing: integer;
  GoodExchange: boolean;
begin
  if Assigned(Self) then
  begin
    //In some nasty race cases, we can try to increment the ref
    //after the final Dec Ref has been perfomed, but before destructors
    //have managed to remove the item from shared datastructures. In such
    //cases we want to fail the AddRef, and make sure it doesn't increment
    //the ref count.
    repeat
      Existing := FRef;
      if Existing <= 0 then
      begin
        Assert(Existing = 0);
        result := nil;
        exit;
      end;
      GoodExchange := InterlockedCompareExchange(FRef,
                        Succ(Existing), Existing) = Existing;
    until GoodExchange;
  end;
  result := Self;
end;

procedure TReffed.Release;
begin
  if Assigned(Self) then
  begin
    if not TryRelease then
      raise EReffedError.Create(S_REFFED_TEARDOWN_RACE_RELEASING_REF);
  end;
end;

function TReffed.TryRelease: boolean;
var
  Existing: integer;
  GoodExchange: boolean;
begin
  result := false;
  if Assigned(self) then
  begin
    repeat
      Existing := FRef;
      if Existing <= 0 then
      begin
        Assert(Existing = 0);
        exit;
      end;
      GoodExchange := InterlockedCompareExchange(FRef,
                        Pred(Existing), Existing) = Existing;
    until GoodExchange;
    result := true;
    if Existing = 1 then
      inherited Free;
  end;
end;

function TReffed.Unitary: boolean;
begin
  result := FRef = 1;
end;

procedure TReffed.Free;
begin
  Assert(false);    //Use release instead.
  Assert(FRef = 1);
  Release;
end;

{ TReffedList }

constructor TReffedList.Create;
begin
  inherited;
  FList := TList.Create;
end;

destructor TReffedList.Destroy;
begin
  ReleaseAndClear;
  FList.Free;
  inherited;
end;

function TReffedList.GetCount: integer;
begin
  result := FList.Count;
end;

function TReffedList.GetItem(Idx: integer): TReffed;
begin
  result := FList.Items[idx];
end;

procedure TReffedList.SetItem(Idx: integer; Item: TReffed);
begin
  FList.Items[idx] := Item;
end;

function TReffedList.AddNoRef(Item: TReffed): integer;
begin
  result := FList.Add(Item);
end;

procedure TReffedList.Clear;
begin
  FList.Clear;
end;

procedure TReffedList.Pack;
begin
  FList.Pack;
end;

procedure TReffedList.ReleaseAndClear;
var
  i: integer;
begin
  for i := 0 to Pred(Count) do
    Items[i].Release;
  Clear;
end;

{ TReffedProxy }

//Generally the proxies will be used to allow atomic /
//protected access to sets of things via pinned lists,
//so we don't expect direct assignment, only referencing
//via lists.

destructor TReffedProxy.Destroy;
begin
  FProxy.Free;
  inherited;
end;

end.
