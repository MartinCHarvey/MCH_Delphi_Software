unit CellCountSymmetries;

interface

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SharedSymmetries;

type
  TCellCountBoard = class(TSymBoard)
  protected
  public
    function CountCellsLine(Row: boolean; Idx: TRowColIdx): integer; inline;
    function AmIsomorphicTo(Other: TSymBoard): boolean; override;
  end;

  //////////////// Counts.

  TRowColCellCount = 0.. (ORDER * ORDER);
  TRowColCellCountSet = set of TRowColCellCount;

  TStackBandCellCount = 0 .. (ORDER * ORDER * ORDER);
  TStackBandCellCountSet = set of TStackBandCellCount;

  //Rows or cols in a stack or band.
  TRowColCountInfo = record
    CellCounts: array[TOrderIdx] of TRowColCellCount;
    CellCountSet: TRowColCellCountSet;
  end;

  TStackBandCountInfo = record
    CellCounts: array[TOrderIdx] of TStackBandCellCount;
    CellCountSet: TStackBandCellCountSet;
    UnpermutedLineInfo: array[TOrderIdx] of TRowColCountInfo;
  end;

  TCountInfo = array [boolean] of TStackBandCountInfo;
  PCountInfo = ^TCountInfo;

  /////////////// Permutations.

{$IFDEF USE_TRACKABLES}
  TCellCountIsomorphismChecker = class(TTrackable)
{$ELSE}
  TCellCountIsomorphismChecker = class
{$ENDIF}
  private
    //Counts all unpermuted.
    FCountsA, FCountsB: TCountInfo;
  protected
    procedure SetupCounts(A,B: TCellCountBoard);
    function IsomorphicStackBand(A,B: TCellCountBoard):boolean;
  public
    function AreIsomorphic(A,B: TCellCountBoard):boolean;
  end;

implementation

{ TCellCountBoard }

function TCellCountBoard.CountCellsLine(Row: boolean; Idx: TRowColIdx): integer;
var
  VarIdx: TRowColIdx;
  Contents: integer;
begin
  result := 0;
  for VarIdx := Low(VarIdx) to High(VarIdx) do
  begin
    if Row then
      Contents := Entries[Idx, VarIdx]
    else
      Contents := Entries[VarIdx, Idx];
    if Contents <> 0 then
      Inc(Result);
  end;
end;

function TCellCountBoard.AmIsomorphicTo(Other: TSymBoard): boolean;
var
  C: TCellCountIsomorphismChecker;
begin
  C := TCellCountIsomorphismChecker.Create;
  try
    result := C.AreIsomorphic(self, Other as TCellCountBoard);
  finally
    C.Free;
  end;
end;

{ TCellCountIsomorphismChecker }

procedure TCellCountIsomorphismChecker.SetupCounts(A,B: TCellCountBoard);
var
  Row, Board: boolean;
  StackBandIdx, LineIdx: TOrderIdx;
  AbsLineIdx: TRowColIdx;
  PCounts: PCountInfo;
  BoardSrc: TCellCountBoard;
begin
  for Board := Low(Board) to High(Board) do
  begin
    if Board = Low(Board) then
    begin
      PCounts := @FCountsA;
      BoardSrc := A;
    end
    else
    begin
      PCounts := @FCountsB;
      BoardSrc := B;
    end;
    for Row := Low(Row) to High(Row) do
    begin
      for StackBandIdx := Low(StackBandIdx) to High(StackBandIdx) do
      begin
        with PCounts[Row] do
        begin
          for LineIdx := Low(LineIdx) to High(LineIdx) do
          begin
            RelToAbsIndexed(StackBandIdx, LineIdx, AbsLineIdx);
            with UnpermutedLineInfo[StackBandIdx] do
            begin
              CellCounts[LineIdx] := BoardSrc.CountCellsLine(Row, AbsLineIdx);
              CellCountSet := CellCountSet + [CellCounts[LineIdx]];
            end;
            CellCounts[StackBandIdx] := CellCounts[StackBandIdx] +
              UnpermutedLineInfo[StackBandIdx].CellCounts[LineIdx];
          end;
          CellCountSet := CellCountSet+ [CellCounts[StackBandIdx]];
        end;
      end;
    end;
  end;
end;

function TCellCountIsomorphismChecker.IsomorphicStackBand(A,B: TCellCountBoard):boolean;
var
  Reflect: boolean;
begin
  result := false;
  for Reflect := Low(Reflect) to High(Reflect) do
  begin

  end;
end;

function TCellCountIsomorphismChecker.AreIsomorphic(A,B: TCellCountBoard):boolean;
begin
  SetupCounts(A, B);
  result := IsomorphicStackBand(A,B);
end;

end.
