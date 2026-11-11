unit KnownSymmetriesMT;
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
  CellCountSymmetriesMT;

procedure KnownSymmetries(BoardClass: TCellCountBoardClass; InputFile: string);

implementation

uses
  WorkItems, KnownSymUtils, SharedSymmetries, SysUtils, LockAbstractions;

type
  TCellCountKnownWI = class(TWorkItem)
  private
    FIsoList: TMTIsoList;
    FBoardClass: TCellCountBoardClass;
    FSpecList: TSudokuSpecList;
  protected
    procedure SetBoardFromSpec(Board: TSymBoardAbstract; Spec: string);
    function DoWork: integer; override;
    procedure DoNormalCompletion; override;
    procedure DoCancelledCompletion; override;
  end;

var
  WICountOutstanding: integer;
  WIDoneEvent: TEvent;
  WorkFarm: TWorkFarm;
  ErrFlag: boolean;

procedure Log(S:string);
begin
  WriteLn(S);
//  if LogToFile then
//    System.WriteLn(LogFile, S);
end;

procedure TCellCountKnownWI.SetBoardFromSpec(Board: TSymBoardAbstract; Spec: string);
var
  StrIdx: integer;
  Row, Col: TRowColIdx;
  C: Char;
  StrOrd: integer;
begin
  if Length(Spec) <> Squares then
    raise Exception.Create('Bad puzzle spec');
  StrIdx := 1;
  for Row := Low(Row) to High(Row) do
  begin
    for Col := Low(Col) to High(Col) do
    begin
      C := Spec[StrIdx];
      StrOrd := Ord(C) - Ord('0');
      if (StrOrd < 0) or (StrOrd > 9) then
        raise Exception.Create('Bad puzzle spec');
      if StrOrd = 0 then
        Board.Entries[Row, Col] := 0
      else
        Board.Entries[Row,Col] := 1;
      Inc(StrIdx);
    end;
  end;
end;

function TCellCountKnownWI.DoWork: integer;
var
  TestBoard, ParentBoard: TCellCountMTBoard;
  x,y: TRowColIdx;
  SV: TPackedRepSearchVal;
  SpecStr: string;
begin
  try
    TestBoard := FBoardClass.Create;
    SV := TPackedRepSearchVal.Create;
    try
      SpecStr := FSpecList.InterlockedGetString;
      while not ErrFlag and (Length(SpecStr) > 0) do
      begin
        SetBoardFromSpec(TestBoard, SpecStr);
        if FIsoList.AddBoard(TestBoard, SV) then
          TestBoard := FBoardClass.Create;
        SpecStr := FSpecList.InterlockedGetString;
      end;
    finally
      TestBoard.Free;
      SV.Free;
    end;
  except
    on Exception do
    begin
      ErrFlag := true;
      Cancel;
    end;
  end;
  result := 0;
end;

procedure TCellCountKnownWI.DoNormalCompletion;
begin
  if LockAbstractions.InterlockedDecrement(WICountOutstanding) = 0 then
    WIDoneEvent.SetEvent;
end;

procedure TCellCountKnownWI.DoCancelledCompletion;
begin
  if LockAbstractions.InterlockedDecrement(WICountOutstanding) = 0 then
    WIDoneEvent.SetEvent;
end;

procedure DumpIsoListMT(IsoList: TMTIsoList);
var
  Board: TSymBoardAbstract;
begin
  Log('-----');
  Log('There are ' + IntToStr(IsoList.Count) + ' isomorphisms');
  Log('');
{
  Board := IsoList.GetInMinlexOrder;
  while Assigned(Board) do
  begin
    Board.LogBoard(Log);
    Board := IsoList.GetInMinlexOrder;
  end;
}
  Log('');
  if ErrFlag then
    Log('There was an exception calculating set of isomorphisms.');
end;


procedure KnownSymmetries(BoardClass: TCellCountBoardClass; InputFile: string);
var
  IsoList: TMTIsoList;
  SpecList: TSudokuSpecList;
  i, LOutstanding: integer;
  WI: TCellCountKnownWI;

begin
  SpecList:= TSudokuSpecList.Create;
  try
    SpecList.ReadFromFile(InputFile);
    IsoList := TMTIsoList.Create;
    try
      WIDoneEvent.ResetEvent;
      WICountOutstanding := WorkFarm.ThreadCount;
      LOutstanding := WICountOutstanding;
      for i := 0 to Pred(LOutstanding) do
      begin
        WI := TCellCountKnownWI.Create;
        WI.CanAutoReset := false;
        WI.CanAutoFree := true;
        WI.FBoardClass := BoardClass;
        WI.FIsoList := IsoList;
        WI.FSpecList := SpecList;
        WorkFarm.AddWorkItem(WI);
      end;
      WIDoneEvent.WaitFor;
      DumpIsoListMT(IsoList);
    finally
      IsoList.Free;
    end;
  finally
    SpecList.Free;
  end;
end;

initialization
  WorkFarm := TWorkFarm.Create;
  WiDoneEvent := TEvent.Create(nil, true, false, '');
  ErrFlag := false;
finalization
  WiDoneEvent.Free;
  WorkFarm.Free;
end.
