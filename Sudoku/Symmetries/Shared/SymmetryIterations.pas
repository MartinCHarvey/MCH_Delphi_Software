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

procedure BruteForceIsomorphismsPermutations(Givens: integer);
procedure Test1;
procedure Test2;
procedure Test3;

implementation

uses
  SharedSymmetries, NaiveSymmetries, Contnrs, SysUtils, GraphSymmetries;

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

procedure BruteForceIsomorphismsPermutations(Givens: integer);
var
  SymBoard: TNaiveSymBoard;
  IsoList: TObjectList;

  procedure Note;
  var
    BoardCopy: TNaiveSymBoard;
    i: integer;

  begin
    for i := 0 to Pred(IsoList.Count) do
    begin
      if SymBoard.AmIsomorphicTo (IsoList[i] as TNaiveSymBoard) then
        exit;
    end;
    BoardCopy := TNaiveSymBoard.Create;
    BoardCopy.Assign(SymBoard);
    IsoList.Add(BoardCopy);
  end;

  procedure Place(StartIdx, GivensLeft: integer);
  var
    NxtIdx: integer;
    Row, Col: TRowColIdx;
  begin
    if GivensLeft > 0 then
    begin
      NxtIdx := StartIdx;
      while NxtIdx < (Succ(High(Row)) * Succ(High(Col))) do
      begin
        Row := NxtIdx div (Succ(High(Col)));
        Col := NxtIdx mod (Succ(High(Col)));
        SymBoard.Entries[Row, Col] := 1;
        Place(Succ(NxtIdx), Pred(GivensLeft));
        SymBoard.Entries[Row, Col] := 0;
        Inc(NxtIdx);
      end;
    end
    else
      Note;
  end;

begin
  SymBoard := TNaiveSymBoard.Create;
  try
    IsoList := TObjectList.Create;
    IsoList.OwnsObjects := true;
    try
      Place(0, Givens);
      DumpIsoList(IsoList, Givens);
    finally
      IsoList.Free;
    end;
  finally
    SymBoard.Free;
  end;
end;

procedure BruteForceIsomorphismsGraph(Givens: integer);
var
  SymBoard: TGraphSymBoard;
  IsoList: TObjectList;

  procedure Note;
  var
    BoardCopy: TGraphSymBoard;
    i: integer;

  begin
    for i := 0 to Pred(IsoList.Count) do
    begin
      if SymBoard.AmIsomorphicTo (IsoList[i] as TGraphSymBoard) then
        exit;
    end;
    BoardCopy := TGraphSymBoard.Create;
    BoardCopy.Assign(SymBoard);
    IsoList.Add(BoardCopy);
  end;

  procedure Place(StartIdx, GivensLeft: integer);
  var
    NxtIdx: integer;
    Row, Col: TRowColIdx;
  begin
    if GivensLeft > 0 then
    begin
      NxtIdx := StartIdx;
      while NxtIdx < (Succ(High(Row)) * Succ(High(Col))) do
      begin
        Row := NxtIdx div (Succ(High(Col)));
        Col := NxtIdx mod (Succ(High(Col)));
        SymBoard.Entries[Row, Col] := 1;
        Place(Succ(NxtIdx), Pred(GivensLeft));
        SymBoard.Entries[Row, Col] := 0;
        Inc(NxtIdx);
      end;
    end
    else
      Note;
  end;

begin
  SymBoard := TGraphSymBoard.Create;
  try
    IsoList := TObjectList.Create;
    IsoList.OwnsObjects := true;
    try
      Place(0, Givens);
      DumpIsoList(IsoList, Givens);
    finally
      IsoList.Free;
    end;
  finally
    SymBoard.Free;
  end;
end;

//TODO - This is broken.
procedure IsomorphismsGraphGenerative(MaxGivens: integer);
var
  IsoList, Tmp, PartialNxtIsoList: TObjectList;
  GivenCount: integer;
  Idx, Idx2: integer;
  TestBoard, NewBoard: TGraphSymBoard;
  x,y: TRowColIdx;
  CanAdd: boolean;
begin
  GivenCount := 0;
  IsoList := TObjectList.Create;
  IsoList.OwnsObjects := true;
  PartialNxtIsoList := TObjectList.Create;
  PartialNxtIsoList.OwnsObjects := true;
  TestBoard := TGraphSymBoard.Create;
  try
    while GivenCount < MaxGivens do
    begin
      if GivenCount = 0 then
        IsoList.Add(TGraphSymBoard.Create)
      else
      begin
        for Idx := 0 to Pred(IsoList.Count) do
        begin
          TestBoard.Assign(TGraphSymBoard(IsoList.Items[Idx]));
          for x := Low(x) to High(x) do
            for y := Low(y) to High(y) do
            begin
              if TestBoard.Entries[x,y] = 0 then
              begin
                TestBoard.Entries[x,y] := 1;
                Idx2 := 0;
                CanAdd := true;
                while Idx2 < PartialNxtIsoList.Count do
                begin
                  if TestBoard.AmIsomorphicTo(PartialNxtIsoList.Items[idx] as TSymBoard) then
                  begin
                    CanAdd := false;
                    break;
                  end;
                  Inc(Idx2);
                end;
                if CanAdd then
                begin
                  NewBoard := TGraphSymBoard.Create;
                  NewBoard.Assign(TestBoard);
                  PartialNxtIsoList.Add(NewBoard);
                end;
                TestBoard.Entries[x,y] := 0;
              end;
            end;
        end;
        Tmp := IsoList;
        IsoList := PartialNxtIsoList;
        Tmp.Clear;
        PartialNxtIsoList := Tmp;
      end;
      DumpIsoList(IsoList, GivenCount);
      Inc(GivenCount);
    end;
  finally
    IsoList.Free;
    PartialNxtIsoList.Free;
    TestBoard.Free;
  end;
end;

const
//  MaxGivens = 4;
  MaxGivens = (ORDER * ORDER * ORDER * ORDER);

procedure Test1;
var
  N: integer;
begin
  for N := 0 to MaxGivens do
    BruteForceIsomorphismsPermutations(N);
end;

procedure Test2;
var
  N: integer;
begin
  for N := 0 to MaxGivens do
    BruteForceIsomorphismsGraph(N);
end;

procedure Test3;
var
  NB1, NB2: TNaiveSymBoard;
  GB1, GB2: TGraphSymBoard;

begin
{
  We have a bug. The code thinks these two are not isomorphic, when in fact they
  are.
  X0X0
  0000
  0000
  0000

  X00X
  0000
  0000
  0000
}
  NB1 := TNaiveSymBoard.Create;
  NB2 := TNaiveSymBoard.Create;
  GB1 := TGraphSymBoard.Create;
  GB2 := TGraphSymBoard.Create;
  try
    //TODO - Reinstate some more interesting configs in a moment.
    GB1.Entries[0,0] := 1;
    GB1.Entries[0,2] := 1;

    GB2.Entries[0,0] := 1;
    GB2.Entries[0,3] := 1;

//    GB1.Assign(NB1);
//    GB2.Assign(NB2);

    if NB1.AmIsomorphicTo(NB2) then
      WriteLn('NB1 iso NB2')
    else
      WriteLn('NB1 diff NB2');
    if NB2.AmIsomorphicTo(NB1) then
      WriteLn('NB2 iso NB1')
    else
      WriteLn('NB2 diff NB1');

    if GB1.AmIsomorphicTo(GB2) then
      WriteLn('GB1 iso GB2')
    else
      WriteLn('GB1 diff GB2');
    if GB2.AmIsomorphicTo(GB1) then
      WriteLn('GB2 iso GB1')
    else
      WriteLn('GB2 diff GB1');

  finally
    NB1.Free;
    NB2.Free;
    GB1.Free;
    GB2.Free;
  end;
end;

{ Not yet, but compare for speed eventually.
procedure Test3;
var
  N: integer;
begin
  for N := 0 to MaxGivens do
    IsomorphismsGraphBruteForce(N);
end;
}

end.
