unit SharedSymmetries;

{

Copyright © 2021 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the “Software”), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}


interface

{$IFDEF USE_TRACKABLES}
uses Trackables;
{$ENDIF}

const
{$IFDEF ORDER2}
  ORDER = 2;
  FACT_ORDER = 2;
{$ELSE}
{$IFDEF ORDER3}
  ORDER = 3;
  FACT_ORDER = 6;
{$ELSE}
ERROR Need to define board order.
{$ENDIF}
{$ENDIF}
  RowsCols = ORDER * ORDER;
  Squares = RowsCols * RowsCols;
  BitsPerU64 = 64;

{$IF ((Squares mod BitsPerU64) = 0)}
  U64sPerBoard = (Squares div BitsPerU64);
{$ELSE}
  U64sPerBoard = Succ(Squares div BitsPerU64);
{$ENDIF}

  //All this is for the purpose of finding the number of uniquely different
  //(taking into account isomorphisms)

  //Not that we are (for the moment) only considering the symmetry of givens
  //not any other symmetry involving the 9! renumberings

type
  TRowColIdx = 0 .. Pred(RowsCols);
  TRowColSet = set of TRowColIdx;
  TOrderIdx = 0 .. Pred(ORDER);
  TOrderSet = set of TOrderIdx;

  TPermIdx = 0..Pred(FACT_ORDER);
  TPermutation = array[TOrderIdx] of TOrderIdx;
  TPermList = array[TPermIdx] of TPermutation;
  TPermIdxList = array[TPermIdx] of TPermIdx;

  TSymRow = array [TRowColIdx] of integer;

  TSymBoardState = array [TRowColIdx] of TSymRow;
  TSymBoardPackedIdx = 0.. Pred(U64sPerBoard);
  TSymBoardPackedState = array[TSymBoardPackedIdx] of UInt64;

  TLogFunction = procedure(S:string);

{$IFDEF USE_TRACKABLES}
  TSymBoardAbstract = class (TTrackable)
{$ELSE}
  TSymBoardAbstract = class
{$ENDIF}
  protected
    function GetBoardEntry(Row, Col: TRowColIdx): integer; virtual; abstract;
    procedure SetBoardEntry(Row, Col: TRowColIdx; Entry: integer); virtual; abstract;
  public
    constructor Create; virtual;
    function AmEqualTo(Other:TSymBoardAbstract): boolean; virtual;
    procedure Assign(Src: TSymBoardAbstract); virtual;
    function AmIsomorphicTo(Other: TSymBoardAbstract): boolean; virtual; abstract;
    property Entries[Row, Col: TRowColIdx]: integer read GetBoardEntry write SetBoardEntry;
    procedure LogBoard(Fn: TLogFunction);
  end;

  TSymBoardClass = class of TSymBoardAbstract;

  TSymBoardUnpacked = class (TSymBoardAbstract)
  protected
    FSymBoardState: TSymBoardState;
    function GetBoardEntry(Row, Col: TRowColIdx): integer; override;
    procedure SetBoardEntry(Row, Col: TRowColIdx; Entry: integer); override;
  end;

  TSymBoardPacked = class(TSymBoardAbstract)
  protected
    FSymBoardPackedState: TSymBoardPackedState;
    function GetBoardEntry(Row, Col: TRowColIdx): integer; override;
    procedure SetBoardEntry(Row, Col: TRowColIdx; Entry: integer); override;
  end;

procedure AbsToRelIndexed(AbsRowCol: TRowColIdx; var StackBand: TOrderIdx; var RowCol: TOrderIdx); inline;
procedure RelToAbsIndexed(StackBand, RowCol: TOrderIdx; var AbsRowCol: TRowColIdx); inline;

function GetPackedEntry(const PackedRep: TSymBoardPackedState; Row, Col: TOrderIdx): boolean; inline;
procedure SetPackedEntry(var PackedRep: TSymBoardPackedState; Row, Col: TOrderIdx; Val: boolean); inline;
function IsZeroRep(const Existing: TSymBoardPackedState): boolean; inline;
function IsEqualRep(const A: TSymBoardPackedState; const B: TSymBoardPackedState): boolean; inline;

var
  PermList: TPermList;

implementation

{ TSymBoardAbstract }

procedure TSymBoardAbstract.LogBoard(Fn: TLogFunction);
var
  j,k: TRowColIdx;
  S: string;
begin
  for j := Low(j) to High(j) do
  begin
    S := '';
    for k := Low(k) to High(k) do
      if Entries[j,k] <> 0 then
        S := S + 'X'
      else
        S := S + '0';
    Fn(S);
  end;
  Fn('');
end;

procedure TSymBoardAbstract.Assign(Src: TSymBoardAbstract);
var
  i,j : TRowColIdx;
begin
  if Assigned(Src) then
    for i := Low(i) to High(i) do
      for j := Low(j) to High(j) do
        Entries[i,j] := Src.Entries[i,j];
end;

function TSymBoardAbstract.AmEqualTo(Other:TSymBoardAbstract): boolean;
var
  Row, Col: TRowColIdx;
begin
  result := true;
  for Row := Low(Row) to High(Row) do
    for Col := Low(Col) to High(Col) do
      begin
        if Entries[Row, Col] <> Other.Entries[Row, Col] then
        begin
          result := false;
          exit;
        end;
      end;
end;

constructor TSymBoardAbstract.Create;
begin
  inherited; //Yes, this is required....
end;

{ TSymBoardUnpacked }

function TSymBoardUnpacked.GetBoardEntry(Row, Col: TRowColIdx): integer;
begin
  result := FSymBoardState[Row, Col];
end;

procedure TSymBoardUnpacked.SetBoardEntry(Row, Col: TRowColIdx; Entry: integer);
begin
  FSymBoardState[Row, Col] := Entry;
end;

{ TSymBoardPacked }

function TSymBoardPacked.GetBoardEntry(Row, Col: TRowColIdx): integer;
begin
  result := Ord(GetPackedEntry(FSymBoardPackedState, Row, Col));
end;

procedure TSymBoardPacked.SetBoardEntry(Row, Col: TRowColIdx; Entry: integer);
begin
  SetPackedEntry(FSymBoardPackedState, Row, Col, Entry <> 0);
end;

{ Misc functions }

function GetPackedEntry(const PackedRep: TSymBoardPackedState; Row, Col: TOrderIdx): boolean;
var
  WNo: Cardinal;
  BitNo: Cardinal;
begin
  BitNo := (Row * RowsCols) + Col;
  WNo := BitNo shr 6;
  //WNo := BitNo div BitsPerU64;
  BitNo := BitNo and $3F;
  //BitNo := BitNo mod BitsPerU64;
  result := (PackedRep[WNo] and (UInt64(1) shl BitNo)) <> 0;
end;

procedure SetPackedEntry(var PackedRep: TSymBoardPackedState; Row, Col: TOrderIdx; Val: boolean);
var
  WNo: Cardinal;
  BitNo: Cardinal;
begin
  BitNo := (Row * RowsCols) + Col;
  WNo := BitNo shr 6;
  //WNo := BitNo div BitsPerU64;
  BitNo := BitNo and $3F;
  //BitNo := BitNo mod BitsPerU64;
  if Val then
    PackedRep[WNo] := PackedRep[WNo] or (UInt64(1) shl BitNo)
  else
    PackedRep[WNo] := PackedRep[WNo] and not (UInt64(1) shl BitNo)
end;

function IsZeroRep(const Existing: TSymBoardPackedState): boolean;
var
  i: TSymBoardPackedIdx;
begin
  result := true;
  for i := Low(i) to High(i) do
  begin
    if Existing[i] <> 0 then
    begin
      result := false;
      exit;
    end;
  end;
end;

function IsEqualRep(const A: TSymBoardPackedState; const B: TSymBoardPackedState): boolean;
var
  i: TSymBoardPackedIdx;
begin
  result := true;
  for i := Low(i) to High(i) do
  begin
    if A[i] <> B[i] then
    begin
      result := false;
      exit;
    end;
  end;
end;

procedure InitPermList;
var
  ListIdx: integer;
  PickedSet: TOrderSet;
  CurPerm: TPermutation;

  procedure AddPerms(Pos: integer);
  var
    Candidate: TOrderIdx;
  begin
    if Pos = ORDER then
    begin
      //Record permutation
      PermList[ListIdx] := CurPerm;
      Inc(ListIdx);
    end
    else
    begin
      for Candidate := Low(Candidate) to High(Candidate) do
      begin
        if not (Candidate in PickedSet) then
        begin
          CurPerm[Pos] := Candidate;
          PickedSet := PickedSet + [Candidate];
          AddPerms(Succ(Pos));
          PickedSet := PickedSet - [Candidate];
        end;
      end;
    end;
  end;

begin
  ListIdx := 0;
  PickedSet := [];
  AddPerms(0);
  Assert(ListIdx = Length(PermList));
end;

procedure AbsToRelIndexed(AbsRowCol: TRowColIdx; var StackBand: TOrderIdx; var RowCol: TOrderIdx);
begin
{$IFDEF ORDER3}
  if Order = 3 then
  begin
    case AbsRowCol of
      0,1,2: StackBand := 0;
      3,4,5: StackBand := 1;
      6,7,8: StackBand := 2;
    else
      Assert(false);
    end;
    RowCol := AbsRowCol - (StackBand * ORDER);
  end
  else
{$ENDIF}
  begin
    StackBand := AbsRowCol div ORDER;
    RowCol := AbsRowCol mod ORDER;
  end;
end;

procedure RelToAbsIndexed(StackBand, RowCol: TOrderIdx; var AbsRowCol: TRowColIdx);
begin
  AbsRowCol := (StackBand * ORDER) + RowCol;
end;

initialization
  InitPermlist;
end.
