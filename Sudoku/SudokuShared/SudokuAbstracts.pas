unit SudokuAbstracts;
{

Copyright © 2020 Martin Harvey <martin_c_harvey@hotmail.com>

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

{$DEFINE OPTIMISE_SET_HANDLING}

//{$DEFINE OPTIMISE_BLOCK_SET_MAP} Nope, makes it slower!
//Testament to good branch prediction on modern CPU's.

uses
  SSStreamables, StreamingSystem;

type
  TSNumber = 1..9;
{$IFDEF OPTIMISE_SET_HANDLING}
  TSNumberSet = type WORD;
{$ELSE}
  TSNumberSet = set of TSNumber;
{$ENDIF}
  TSConstraint = array[TSNumber] of TSNumberSet;

  TSOptNumber = 0..9;
  TSRow = array[TSNumber] of TSOptNumber;
  TSBoard = array[TSNumber] of TSRow;

  TSAbstractSolver = class;

  TSAbstractBoardState = class(TObjStreamable)
  protected
    procedure CustomMarshal(Sender: TDefaultSSController); override;
    procedure CustomUnmarshal(Sender: TDefaultSSController); override;

    function GetOptEntry(Row, Col: TSNumber): TSOptNumber; virtual; abstract;
    procedure SetOptEntry(Row, Col: TSNumber; Entry: TSOptNumber);
    procedure SetOptEntryInt(Row, Col: TSNumber; Entry: TSOptNumber; Check: boolean = false);

    function SetEntry(Row, Col, Entry: TSNumber): boolean; virtual; abstract;
    procedure ClearEntry(Row, Col: TSNumber); virtual; abstract;

    function GetComplete: boolean; virtual; abstract;
    function GetProceedable: boolean;virtual; abstract;
  public
    procedure Assign(Source: TObjStreamable); override;

    function FindNextMove(var Row, Col: TSNumber): TSNumberSet; virtual; abstract;
    function EvolveIfSingular(Row, Col: TSNumber; NSet: TSNumberSet): boolean; virtual; abstract;

    procedure Clear; virtual; abstract;

    property Entries[Row, Col: TSNumber]: TSOptNumber read GetOptEntry write SetOptEntry; default;
    property Complete: boolean read GetComplete;
    property Proceedable: boolean read GetProceedable;
  end;

  TSSolverStats = class
  public
    DecisionPoints: int64;
    DecisionPointsWithSolution: int64;
    DecisionPointsAllDeadEnd: int64;
    CorrectSolutions: int64;
    DeadEnds: int64;
    StartTime: TDateTime;
    ElapsedTime: double;
    procedure Zero;
    procedure Sum(OtherStats: TSSolverStats);
  end;

  TSSolveMode = (ssmFindOne,
                 ssmFindAll);

{$IFDEF USE_TRACKABLES}
  TSAbstractSolver = class(TTrackable)
{$ELSE}
  TSAbstractSolver = class
{$ENDIF}
  private
  protected
    FSolveMode: TSSolveMode;
    FBoardState: TSAbstractBoardState;
    FUniqueSolution: TSAbstractBoardState;
    FStats: TSSolverStats;
    procedure SetInitialConfig(InitialConfig: TSAbstractBoardState); virtual;
    function CreateBoardState: TSAbstractBoardState; virtual; abstract;
  public
    destructor Destroy; override;
    property BoardState: TSAbstractBoardState read FBoardState write SetInitialConfig;
    property UniqueSolution: TSAbstractBoardState read FUniqueSolution;
    property Stats: TSSolverStats read FStats;

    function Solve: boolean; virtual; abstract;
    property SolveMode: TSSolveMode read FSolveMode write FSolveMode;
  end;

{ Misc util functions }

function TSNumberIn(NSet: TSNumberSet; Number: integer): boolean; inline;
function TSSetSub(X, Y: TSNumberSet): TSNumberSet; inline;
function TSSetAdd(X, Y: TSNumberSet): TSNumberSet; inline;
function TSSetIndividual(X: integer): TSNumberSet; inline;
function CountBits(NSet: TSNumberSet): integer; inline;
function FindSetBit(NSet: TSNumberSet): TSNumber;inline;
function MapToBlockSetIdx(Row, Col: TSNumber): TSNumber; inline;

const
{$IFDEF OPTIMISE_SET_HANDLING}
  AllAllowed: TSNumberSet = $3FE;
  EmptySet: TSNumberSet = 0;
{$ELSE}
  AllAllowed: TSNumberSet = [1,2,3,4,5,6,7,8,9];
  EmptySet: TSNumberSet = [];
{$ENDIF}

implementation

uses
 SysUtils, SSAbstracts;

const
  S_INVALID_CONFIGURATION = 'Unstream board: Invalid configuration.';


{ TSAbstractBoardState }

procedure TSAbstractBoardState.CustomMarshal(Sender: TDefaultSSController);
var
  Row, Col: TSNumber;
  ArrayNum: integer;
  BoardEntry: TSOptNumber;
begin
  Sender.StreamArrayStart('Rows');
  ArrayNum := 0;
  for Row := Low(TSNumber) to High(TSNumber) do
  begin
    Sender.StreamArrayStart('');
    for Col := Low(TSNumber) to High(TSNumber) do
    begin
      BoardEntry := GetOptEntry(Row, Col);
      Sender.StreamUByte('', BoardEntry);
    end;
    Sender.StreamArrayEnd(IntToStr(ArrayNum));
    Inc(ArrayNum);
  end;
  Sender.StreamArrayEnd('Rows');
end;

procedure TSAbstractBoardState.CustomUnmarshal(Sender: TDefaultSSController);
var
  Row, Col: TSNumber;
  BVal: Byte;
  Count: integer;
  ArrayNum: integer;
begin
  Clear;

  Sender.UnStreamArrayStart('Rows', Count);
  if Count <> Succ(High(TSNumber) - Low(TSNumber)) then
    raise EStreamSystemError.Create(S_INVALID_CONFIGURATION);
  ArrayNum := 0;
  for Row := Low(TSNumber) to High(TSNumber) do
  begin
    Sender.UnStreamArrayStart('', Count);
    if Count <> Succ(High(TSNumber) - Low(TSNumber)) then
      raise EStreamSystemError.Create(S_INVALID_CONFIGURATION);
    for Col := Low(TSNumber) to High(TSNumber) do
    begin
      if not Sender.UnStreamUByte('', BVal) then
        raise EStreamSystemError.Create(S_INVALID_CONFIGURATION);
      if (BVal > High(TSOptNumber)) then
        raise EStreamSystemError.Create(S_INVALID_CONFIGURATION);
      if BVal <> 0 then
      begin
        if not SetEntry(Row, Col, BVal) then
          raise EStreamSystemError.Create(S_INVALID_CONFIGURATION);
      end;
    end;
    Sender.UnStreamArrayEnd(IntToStr(ArrayNum));
    Inc(ArrayNum);
  end;
  Sender.UnStreamArrayEnd('Rows');
end;

procedure TSAbstractBoardState.SetOptEntry(Row, Col: TSNumber; Entry: TSOptNumber);
begin
  SetOptEntryInt(Row, Col, Entry);
end;

procedure TSAbstractBoardState.SetOptEntryInt(Row, Col: TSNumber; Entry: TSOptNumber; Check: boolean = false);
var
  SetOK: boolean;
begin
  if Entry = 0 then
    ClearEntry(Row, Col)
  else
  begin
    SetOK := SetEntry(Row, Col, Entry);
    Assert((not Check) or SetOk);
  end;
end;

procedure TSAbstractBoardState.Assign(Source: TObjStreamable);
var
  Src: TSAbstractBoardState;
  R,C: TSNumber;
begin
  Assert(Assigned(Source) and (Source is TSAbstractBoardState));
  Src := TSAbstractBoardState(Source);
  Clear;
  for R := Low(R) to High(R) do
    for C := Low(C) to High(C) do
      self.Entries[R,C] := Src.Entries[R,C];
end;


{ TSSolverStats }

procedure TSSolverStats.Zero;
begin
  DecisionPoints := 0;
  DecisionPointsWithSolution := 0;
  DecisionPointsAllDeadEnd := 0;
  CorrectSolutions := 0;
  DeadEnds := 0;
end;

procedure TSSolverStats.Sum(OtherStats: TSSolverStats);
begin
  Inc(DecisionPoints, OtherStats.DecisionPoints);
  Inc(DecisionPointsWithSolution, OtherStats.DecisionPointsWithSolution);
  Inc(DecisionPointsAllDeadEnd, OtherStats.DecisionPointsAllDeadEnd);
  Inc(CorrectSolutions, OtherStats.CorrectSolutions);
  Inc(DeadEnds, OtherStats.DeadEnds);
end;


{ TSAbstractSolver }

destructor TSAbstractSolver.Destroy;
begin
  FBoardState.Free;
  FUniqueSolution.Free;
  FStats.Free;
  inherited;
end;

procedure TSAbstractSolver.SetInitialConfig(InitialConfig: TSAbstractBoardState);
begin
  if not Assigned(FBoardState) then
    FBoardState := CreateBoardState;
  FBoardState.Assign(InitialConfig);
  FUniqueSolution.Free;
  FUniqueSolution := nil;
  if not Assigned(FStats) then
    FStats := TSSolverStats.Create;
  FStats.Zero;
end;

{ Misc functions }

{$IFDEF OPTIMISE_BLOCK_SET_MAP}
function MapToBlockSetIdx(Row, Col: TSNumber): TSNumber;
begin
  result := PrecalcMap[Row][Col];
end;

function MapToBlockSetIdxSlow(Row, Col: TSNumber): TSNumber;
{$ELSE}
function MapToBlockSetIdx(Row, Col: TSNumber): TSNumber;
{$ENDIF}
label error;
begin
  case Row of
    1,2,3:
      case Col of
        1,2,3: result := 1;
        4,5,6: result := 2;
        7,8,9: result := 3;
      else
        goto error;
      end;
    4,5,6:
      case Col of
        1,2,3: result := 4;
        4,5,6: result := 5;
        7,8,9: result := 6;
      else
        goto error;
      end;
    7,8,9:
      case Col of
        1,2,3: result := 7;
        4,5,6: result := 8;
        7,8,9: result := 9;
      else
        goto error;
      end;
    else
      goto error;
  end;
  exit;
error:
  Assert(false);
  result := 1;
end;

{$IFDEF OPTIMISE_BLOCK_SET_MAP}
procedure Precalc;
var
  Row, Col: TSNumber;
begin
  for Row := Low(Row) to High(Row) do
    for Col := Low(Col) to High(Col) do
      PrecalcMap[Row][Col] := MapToBlockSetIdxSlow(Row, Col);
end;
{$ENDIF}


{ Misc util functions }

function TSNumberIn(NSet: TSNumberSet; Number: integer): boolean; inline;
begin
{$IFNDEF OPTIMISE_SET_HANDLING}
  result := Number in NSet;
{$ELSE}
  result := (Word(1 shl Number) and NSet) <> 0;
{$ENDIF}
end;

function TSSetSub(X, Y: TSNumberSet): TSNumberSet; inline;
begin
{$IFNDEF OPTIMISE_SET_HANDLING}
  result := X - Y;
{$ELSE}
  result := X and not Y;
{$ENDIF}
end;

function TSSetAdd(X, Y: TSNumberSet): TSNumberSet; inline;
begin
{$IFNDEF OPTIMISE_SET_HANDLING}
  result := X + Y;
{$ELSE}
  result := X or Y;
{$ENDIF}
end;

function TSSetIndividual(X: integer): TSNumberSet; inline;
begin
{$IFNDEF OPTIMISE_SET_HANDLING}
  result := [X];
{$ELSE}
  result := 1 shl X;
{$ENDIF}
end;

function CountBits(NSet: TSNumberSet): integer; inline;
var
{$IFDEF OPTIMISE_COUNT_BITS}
  C: WORD;
{$ELSE}
  i: TSNumber;
{$ENDIF}
begin
  result := 0;
{$IFDEF OPTIMISE_COUNT_BITS}
  Assert(Sizeof(NSet) = sizeof(Word));
{$IFDEF OPTIMISE_SET_HANDLING}
  C := NSet;
{$ELSE}
  C := PWord(@NSet)^;
{$ENDIF}
  while C <> 0 do
  begin
    C := C and (C-1);
    Inc(Result);
  end;
{$ELSE}
  if NSet <> EmptySet then
  begin
    for i := Low(i) to High(i) do
    begin
      if TSNumberIn(NSet,i) then
        Inc(result);
    end;
  end;
{$ENDIF}
end;

function FindSetBit(NSet: TSNumberSet): TSNumber;
var
  N: TSNumber;
begin
  result := Low(N);
  Assert(CountBits(NSet) = 1);
  for N := Low(TSNumber) to High(TSNumber) do
    if TSNumberIn(NSet, N) then
    begin
      result := N;
      exit;
    end;
end;


initialization
{$IFDEF OPTIMISE_BLOCK_SET_MAP}
  Precalc;
{$ENDIF}
end.
