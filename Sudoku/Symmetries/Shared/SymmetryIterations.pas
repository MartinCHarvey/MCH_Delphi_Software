unit SymmetryIterations;

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

procedure TestNaive;
procedure TestCellCount;
procedure TestGraph;

//{$DEFINE OMIT_OUTPUT}

implementation

uses
  SharedSymmetries, NaiveSymmetries, Contnrs, SysUtils, CellCountSymmetries,
  GraphSymmetries;

var
  LogFile: TextFile;
  LogToFile: boolean;

procedure Log(S:string);
begin
  WriteLn(S);
  if LogToFile then
    System.WriteLn(LogFile, S);
end;

procedure DumpIsoList(IsoList: TObjectList; Givens: integer);
var
  i: integer;
  j, k: TRowColIdx;
  S: string;
  Board: TSymBoard;
begin
  Log('-----');
  Log('There are ' + IntToStr(IsoList.Count) +
      ' isomorphisms with ' + IntToStr(Givens) + ' givens, order' +
      IntToStr(ORDER));
  Log('');
  for i := 0 to Pred(IsoList.Count) do
  begin
    Board := TSymBoard(IsoList[i]);
    for j := Low(j) to High(j) do
    begin
      S := '';
      for k := Low(k) to High(k) do
        if Board.Entries[j,k] <> 0 then
          S := S + 'X'
        else
          S := S + '0';
      WriteLn(S);
    end;
    WriteLn('');
  end;
  WriteLn('');
end;

procedure IsomorphismsGenerative(MaxGivens: integer; BoardClass: TSymBoardClass);
var
  IsoList, Tmp, PartialNxtIsoList: TObjectList;
  GivenCount: integer;
  Idx, Idx2: integer;
  TestBoard, NewBoard: TSymBoard;
  x,y: TRowColIdx;
  CanAdd: boolean;
begin
  GivenCount := 0;
  IsoList := TObjectList.Create;
  IsoList.OwnsObjects := true;
  PartialNxtIsoList := TObjectList.Create;
  PartialNxtIsoList.OwnsObjects := true;
  TestBoard := BoardClass.Create;
  try
    while GivenCount <= MaxGivens do
    begin
      if GivenCount = 0 then
        IsoList.Add(BoardClass.Create)
      else
      begin
        for Idx := 0 to Pred(IsoList.Count) do
        begin
          TestBoard.Assign(TSymBoard(IsoList.Items[Idx]));
          Assert(TestBoard.AmEqualTo(TSymBoard(IsoList.Items[Idx])));

          for x := Low(x) to High(x) do
          begin
            for y := Low(y) to High(y) do
            begin
              if TestBoard.Entries[x,y] = 0 then
              begin
                TestBoard.Entries[x,y] := 1;

                Idx2 := 0;
                CanAdd := true;
                while Idx2 < PartialNxtIsoList.Count do
                begin
                  Assert(PartialNxtIsoList.Items[Idx2] is TSymBoard);
                  if TestBoard.AmIsomorphicTo(TSymBoard(PartialNxtIsoList.Items[Idx2])) then
                  begin
                    CanAdd := false;
                    break;
                  end;
                  Inc(Idx2);
                end;
                if CanAdd then
                begin
                  NewBoard := BoardClass.Create;
                  NewBoard.Assign(TestBoard);
                  PartialNxtIsoList.Add(NewBoard);
                end;
                TestBoard.Entries[x,y] := 0;
              end;
            end;
          end;
        end;
        Tmp := IsoList;
        IsoList := PartialNxtIsoList;
        Tmp.Clear;
        PartialNxtIsoList := Tmp;
      end;
{$IFNDEF OMIT_OUTPUT}
      DumpIsoList(IsoList, GivenCount);
{$ENDIF}
      Inc(GivenCount);
    end;
  finally
    IsoList.Free;
    PartialNxtIsoList.Free;
    TestBoard.Free;
  end;
end;

const
  MaxGivens = 6;
//  MaxGivens = (ORDER * ORDER * ORDER * ORDER);

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

procedure TestNaive;
begin
  StartTiming();
  IsomorphismsGenerative(MaxGivens, TNaiveSymBoard);
  StopTiming();
end;

procedure TestCellCount;
begin
  StartTiming();
  IsomorphismsGenerative(MaxGivens, TCellCountBoard);
  StopTiming();
end;

procedure TestGraph;
begin
  StartTiming();
  IsomorphismsGenerative(MaxGivens, TGraphSymBoard);
  StopTiming();
end;

end.
