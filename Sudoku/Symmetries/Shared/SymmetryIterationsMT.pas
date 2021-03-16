unit SymmetryIterationsMT;

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

{
  TODO - Initially, have brute force algorithm which places givens in all poss
  locations, and checks whether isomorphic.

  Later on, need a smarter algorithm which generates N+1 given isomorphics from
  N given isomorphisms.

  There is both a naive (translate back to board, try all poss)
  and a smart (growing connectivity graphs subject to row/col/block max counts)
  way of generating N+1 isomorphisms.
}

procedure TestCellCountMT;
procedure SizeCheck;

//{$DEFINE OMIT_OUTPUT}

implementation

uses
  SharedSymmetries, Contnrs, SysUtils, CellCountSymmetriesMT,
  Classes, SyncObjs, WorkItems, IndexedStore, CellCountShared;

type
  TCellCountMTWI = class(TWorkItem)
  private
    FOldIsoList, FNewIsoList: TMTIsoList;
    FBoardClass: TCellCountBoardClass;
  protected
    function DoWork: integer; override;
    procedure DoNormalCompletion; override;
    procedure DoCancelledCompletion; override;
  end;

var
  LogFile: TextFile;
  LogToFile: boolean;
  WorkFarm: TWorkFarm;
  ErrFlag: boolean;

procedure Log(S:string);
begin
  WriteLn(S);
  if LogToFile then
    System.WriteLn(LogFile, S);
end;

var
  WICountOutstanding: integer;
  WIDoneEvent: TEvent;

function TCellCountMTWI.DoWork: integer;
var
  TestBoard, ParentBoard: TCellCountMTBoard;
  x,y: TRowColIdx;
  SV: TPackedRepSearchVal;
begin
  try
    TestBoard := FBoardClass.Create;
    SV := TPackedRepSearchVal.Create;
    try
      ParentBoard := FOldIsoList.GetInMinlexOrder;
      while Assigned(ParentBoard) and not ErrFlag do
      begin
        TestBoard.Assign(ParentBoard);
        for x := Low(x) to High(x) do
        begin
          for y := Low(y) to High(y) do
          begin
            if TestBoard.Entries[x,y] = 0 then
            begin
              TestBoard.Entries[x,y] := 1;
              if FNewIsoList.AddBoard(TestBoard, SV) then
              begin
                TestBoard := FBoardClass.Create;
                TestBoard.Assign(ParentBoard);
              end
              else
                TestBoard.Entries[x,y] := 0;
            end;
          end;
        end;
        ParentBoard := FOldIsoList.GetInMinlexOrder;
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

procedure TCellCountMTWI.DoNormalCompletion;
begin
  if TInterlocked.Decrement(WICountOutstanding) = 0 then
    WIDoneEvent.SetEvent;
end;

procedure TCellCountMTWI.DoCancelledCompletion;
begin
  if TInterlocked.Decrement(WICountOutstanding) = 0 then
    WIDoneEvent.SetEvent;
end;

procedure DumpIsoListMT(IsoList: TMTIsoList; Givens: integer);
var
  Board: TSymBoardAbstract;
begin
  Log('-----');
  Log('There are ' + IntToStr(IsoList.Count) +
      ' isomorphisms with ' + IntToStr(Givens) + ' givens, order' +
      IntToStr(ORDER));
  Log('');
  Board := IsoList.GetInMinlexOrder;
  while Assigned(Board) do
  begin
    Board.LogBoard(Log);
    Board := IsoList.GetInMinlexOrder;
  end;
  Log('');
  if ErrFlag then
    Log('There was an exception calculating last set of isomorphisms (out of memory?)');
end;


procedure IsomorphismsGenerativeMT(MaxGivens: integer; BoardClass: TCellCountBoardClass);
var
  IsoList, Tmp, PartialNextIsoList: TMTIsoList;
  GivenCount: integer;
  LOutstanding: integer;
  i: integer;
  WI: TCellCountMTWI;
  SV: TPackedRepSearchVal;
begin
  GivenCount := 0;
  IsoList := TMTIsoList.Create;
  PartialNextIsoList := TMTIsoList.Create;
  try
    while (GivenCount <= MaxGivens) and not ErrFlag do
    begin
      if GivenCount = 0 then
      begin
        SV := TPackedRepSearchVal.Create;
        try
          IsoList.AddBoard(BoardClass.Create, SV);
        finally
          SV.Free;
        end;
      end
      else
      begin
        WIDoneEvent.ResetEvent;
        WICountOutstanding := WorkFarm.ThreadCount;
        LOutstanding := WICountOutstanding;
        for i := 0 to Pred(LOutstanding) do
        begin
          WI := TCellCountMTWI.Create;
          WI.CanAutoReset := false;
          WI.CanAutoFree := true;
          WI.FOldIsoList := IsoList;
          WI.FNewIsoList := PartialNextIsoList;
          WI.FBoardClass := BoardClass;
          WorkFarm.AddWorkItem(WI);
        end;
        WIDoneEvent.WaitFor;
        Tmp := IsoList;
        IsoList := PartialNextIsoList;
        Tmp.Clear;
        PartialNextIsoList := Tmp;
      end;
{$IFNDEF OMIT_OUTPUT}
      DumpIsoListMT(IsoList, GivenCount);
{$ELSE}
      IsoList.SetTraversed;
{$ENDIF}
      IsoList.ResetTraverse;
      Inc(GivenCount);
    end;
  finally
    IsoList.Free;
    PartialNextIsoList.Free;
  end;
end;


const
  MaxGivens = (ORDER * ORDER * ORDER * ORDER);
//  MaxGivens = 8;

var
  StartTime: TDateTime;

procedure StartTiming();
begin
  StartTime := Now;
end;

procedure StopTiming;
var
  StopTime: TDateTime;
  ElapsedSecs: double;
begin
  StopTime := Now;
  ElapsedSecs := (StopTime - StartTime) * (3600 * 24);
  WriteLn('ElapsedTime: ' + FloatToStr(ElapsedSecs));
end;

procedure TestCellCountMT;
begin
  StartTiming();
  IsomorphismsGenerativeMT(MaxGivens, TCellCountMTBoard);
  StopTiming();
end;

procedure SizeCheck;
begin
  WriteLn('Sizeof TIndexNode ', TIndexNode.InstanceSize);
  WriteLn('Sizeof TIndexNodeLink ', TIndexNodeLink.InstanceSize);
  WriteLn('Sizeof TTItemRec ', TItemRec.InstanceSize);
  WriteLn('Sizeof TCellCountMTBoard ', TCellCountMTBoard.InstanceSize);
  WriteLn('Sizeof TSymBoardPackedState ', sizeof(TSymBoardPackedState));
  WriteLn('Sizeof TCountInfo ', sizeof(TCountInfo));
  WriteLn('Sizeof TCountInfoMT ', sizeof(TCountInfoMT));
  WriteLn('Sizeof TAllowedPerms ', sizeof(TAllowedPerms));
  WriteLn('Sizeof TSelectedPerms ', sizeof(TSelectedPerms));
  WriteLn('U64sPerBoard ', U64sPerBoard);
end;

initialization
  WorkFarm := TWorkFarm.Create;
  WiDoneEvent := TEvent.Create(nil, true, false, '');
  ErrFlag := false;
finalization
  WiDoneEvent.Free;
  WorkFarm.Free;
end.
