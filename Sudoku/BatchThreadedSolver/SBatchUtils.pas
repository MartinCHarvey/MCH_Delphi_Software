unit SBatchUtils;
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
  SysUtils, Classes, LockAbstractions, WorkItems;

type
  EBatchSudokuError = class(Exception);

  TSudokuSpecList = class
  private
    FStrings: TStringList;
    FIndex: integer;
  protected
  public
    constructor Create;
    destructor Destroy; override;
    procedure ReadFromFile(Filename: string); //Single threaded setup.
    function InterlockedGetString: string;
  end;

  TSudokuWorkItem = class(TWorkItem)
  public
    function DoWork: integer; override;
  end;


var
  WorkItemsDone: TEvent;
  SpecList: TSudokuSpecList;
  Solved, Unsolved, Error: integer;

  procedure RunCalculation;

implementation

uses
  SudokuExactCover, BufferedFileStream, SudokuAbstracts, SudokuBoard, IdGlobal;

var
  WorkFarm: TWorkFarm;
  WorkItemsOutstanding: integer;

{ TSudokuSpecList }

constructor TSudokuSpecList.Create;
begin
  FStrings := TStringList.Create;
end;

destructor TSudokuSpecList.Destroy;
begin
  FStrings.Free;
end;

procedure TSudokuSpecList.ReadFromFile(Filename: string); //Single threaded setup.
var
  FS: TReadOnlyCachedFileStream;
  Line: string;
  PuzzleCount: integer;
  Idx: integer;

begin
  FS := TReadOnlyCachedFileStream.Create(Filename, 1024 * 1024);
  try
    Line := ReadLnFromStream(FS);
    Line := Line.Trim;
    PuzzleCount := StrToInt(Line); //Allow failures to fall out.
    WriteLn('Initial puzzle count: ' + IntToStr(PuzzleCount));
    for Idx := 0 to Pred(PuzzleCount) do
    begin
      Line := ReadLnFromStream(FS);
      Line := Line.Trim;
      //Check that we have 81 numbers 0..9
      if Line.Length <> 81 then
        raise Exception.Create('Line bad length');
      //Apart from that let MT solvers determine whether input good.
      FStrings.Add(Line);
    end;
  finally
    FS.Free;
  end;
end;

function TSudokuSpecList.InterlockedGetString: string;
var
  Idx: integer;
begin
  Idx := Pred(LockAbstractions.InterlockedIncrement(FIndex));
  if Idx < FStrings.Count then
    result := FStrings[idx]
  else
    result := '';
end;

{ TSudokuWorkItem }

function TSudokuWorkItem.DoWork: integer;
var
  PuzzleDesc, ErrStr: string;
  Solver: TSExactCoverSolver;
  Row, Col: TSNumber;
  StrIdx, StrOrd: integer;
  TmpBoardState: TSAbstractBoardState;
  SolversRunning: integer;
  C: char;

  procedure DoError;
  begin
    if LockAbstractions.InterlockedIncrement(Error) < 10 then
    begin
      ErrStr := 'Error on puzzle (roughly) ' + IntToStr(SpecList.FIndex) +
      ' : ' + PuzzleDesc;
      WriteLn(ErrStr);
    end;
  end;

label
  next;
begin
  Solver := TSExactCoverSolver.Create;
  Solver.SolveMode := ssmFindOne;
  TmpBoardState := TSBoardState.Create;
  try
    PuzzleDesc := SpecList.InterlockedGetString;
    while Length(PuzzleDesc) > 0 do
    begin
      if Length(PuzzleDesc) <> 81 then
      begin
        DoError;
        goto next;
      end;

      StrIdx := 1;
      TmpBoardState.Clear;
      for Row := Low(Row) to High(Row) do
      begin
        for Col := Low(Col) to High(Col) do
        begin
          C := PuzzleDesc[StrIdx];
          StrOrd := Ord(C) - Ord('0');
          if (StrOrd < Low(TSOptNumber)) or (StrOrd > High(TSOptNumber)) then
          begin
            DoError;
            goto next;
          end;
          if StrOrd = Low(TSOptNumber) then
            TmpBoardState.Entries[Row, Col] := StrOrd
          else
          begin
            if not TmpBoardState.SetEntry(Row, Col, StrOrd) then
            begin
              DoError;
              goto next;
            end;
          end;
          Inc(StrIdx);
        end;
      end;
      Solver.BoardState := TmpBoardState;
      Solver.Solve;
      case Solver.Stats.Classification of
        scInvalid, scMany:
          DoError;
        scNone: LockAbstractions.InterlockedIncrement(Unsolved);
        scOne: LockAbstractions.InterlockedIncrement(Solved);
      end;
next:
      PuzzleDesc := SpecList.InterlockedGetString;
    end;
  finally
    Solver.Free;
    TmpBoardState.Free;
    SolversRunning := LockAbstractions.InterlockedDecrement(WorkItemsOutstanding);
    if SolversRunning = 0 then
      WorkItemsDone.SetEvent;
  end;
  result := 0;
end;

{ Misc functions }

procedure RunCalculation;
var
  CpuCount: integer;
  WorkItems:  array of TSudokuWorkItem;
  i: integer;

begin
  CpuCount := WorkFarm.ThreadCount;
  SetLength(WorkItems, CpuCount);
  WorkItemsOutstanding := CpuCount;
  for i := 0 to Pred(CpuCount) do
  begin
    WorkItems[i] := TSudokuWorkItem.Create;
    WorkItems[i].CanAutoFree := true;
    WorkItems[i].CanAutoReset := false;
  end;
  WorkFarm.AddWorkItemBatch(@WorkItems[0], CpuCount);
  WorkItemsDone.WaitFor(INFINITE);
end;

initialization
  WorkFarm := TWorkFarm.Create;
  WorkItemsDone := TEvent.Create;
  WorkItemsDone.ResetEvent;
  SpecList := TSudokuSpecList.Create;
  Solved := 0;
  Unsolved := 0;
  Error := 0;
finalization
  WorkFarm.Free;
  SpecList.Free;
  WorkItemsDone.Free;
end.
