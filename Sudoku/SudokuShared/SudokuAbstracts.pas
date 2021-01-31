unit SudokuAbstracts;

interface

{$DEFINE OPTIMISE_SET_HANDLING}

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
    //TODO - Write these in terms of standard get/set routines for individual cells.
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
//  TODO - What way round to do this?
//    function GetSolver: TSAbstractSolver; virtual; abstract;

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

{$IFDEF USE_TRACKABLES}
  TSAbstractSolver = class(TTrackable)
{$ELSE}
  TSAbstractSolver = class
{$ENDIF}
  private
  protected
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
  end;

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

end.
