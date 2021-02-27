unit GraphSymmetries;

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

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SharedSymmetries, DLList, ExactCover;

type
{$IFDEF USE_TRACKABLES}
  TNode = class(TTrackable)
{$ELSE}
  TNode = class
{$ENDIF}
  protected
    function GetConnected: boolean; virtual; abstract;
    //Function to sort list of nodes for
    //quick graph equiv comparison (not exhaustive), but removes
    //most possibs
{$IFOPT C+}
    function LogStr: string; virtual; abstract;
{$ENDIF}
  public
    property Connected: boolean read GetConnected;
  end;

  TConsType = (tctRowCol, tctBlock);

  TConsNode = class(TNode)
  protected
    FIndex: TRowColIdx;
    FConnectedCells: TDLEntry;
    FConnectedCellCount: TRowColIdx;
    function GetConnected: boolean; override;
{$IFOPT C+}
    function LogStr:string; override;
{$ENDIF}
  public
    function GetConsType: TConsType; virtual; abstract;
    function LocalTopologyEqual(Other: TConsNode): boolean;
    constructor Create;
    destructor Destroy; override;
    property Idx: TRowColIdx read FIndex;
  end;

  function CompareConsNode(Item1, Item2: Pointer): integer;

type
  TLineNode = class(TConsNode)
  protected
    FIsRow: boolean;
{$IFOPT C+}
    function LogStr:string; override;
{$ENDIF}
  public
    function GetConsType: TConsType; override;
    property IsRow: boolean read FIsRow;
  end;

  TBlockNode = class(TConsNode)
  protected
{$IFOPT C+}
    function LogStr:string; override;
{$ENDIF}
  public
    function GetConsType: TConsType; override;
  end;

  TCellNode = class(TNode)
  protected
    FRow, FCol: TRowColIdx;
    FRowLink: TDLEntry;
    FColLink: TDLEntry;
    FBlockLink: TDLEntry;
    FRowNode, FColNode: TLineNode;
    FBlockNode: TBlockNode;
    FConRow, FConCol, FConBlock: TConsNode;
    function GetConnected: boolean; override;
{$IFOPT C+}
    function LogStr:string; override;
{$ENDIF}
  public
    constructor Create;
    destructor Destroy; override;
  end;

  TLineNodes = array [TRowColIdx] of TLineNode;
  TRowColNodes = array [boolean] of TLineNodes;
  TBlockNodes = array [TRowColIdx] of TBlockNode;
  TCellNodes = array[TRowColIdx, TRowColIdx] of TCellNode;

  TGraphSymBoard = class(TSymBoard)
  protected
    FRowColNodes: TRowColNodes;
    FBlockNodes: TBlockNodes;
    FCellNodes: TCellNodes;

    procedure SetBoardEntry(Row, Col: TRowColIdx; Entry: integer); override;
    function GetBoardEntry(Row, Col: TRowColIdx): integer; override;
  public
    function AmIsomorphicTo(Other: TSymBoard): boolean; override;
    constructor Create; override;
    destructor Destroy; override;
    property Entries[Row, Col: TRowColIdx]: integer read GetBoardEntry write SetBoardEntry;
  end;

  TInclConstraintNodes = record
    //Twice RowColIdx nodes at max.
    Nodes: array[0 .. Pred(2 * ORDER * ORDER)] of TConsNode;
    Count: integer;
  end;

  TInclCellNodes = record
    //RowColIdx * RowColIdx nodes at max.
    Nodes: array[0.. Pred(ORDER * ORDER * ORDER * ORDER)] of TCellNode;
    Count: integer;
  end;

  TInclEdge = record
    FromCell: TCellNode;
    ToCons: TConsNode;
  end;

  PInclEdge = ^TInclEdge;

  TInclEdges = record
    Edges: array [0..Pred( 3 * ORDER * ORDER * ORDER * ORDER)] of TInclEdge;
    Count: integer;
  end;

  TCompareConstraintNodes = array [boolean] of TInclConstraintNodes;
  TCompareCellNodes = array [boolean] of TInclCellNodes;
  TCompareEdges = array [boolean] of TInclEdges;

{$IFDEF USE_TRACKABLES}
  TIsomorphicChecker = class(TTrackable)
{$ELSE}
  TIsomorphicChecker = class
{$ENDIF}
  private
    //TODO - One use only at the moment.
    FUsed: boolean;
    FSolvedOnce,
{$IFOPT C+}
    FSolvedMulti,
{$ENDIF}
    FConstraintErr: boolean;
    //No free on destroy.
    FCompareRowColNodes: TCompareConstraintNodes;
    FCompareBlockNodes: TCompareConstraintNodes;
    FCompareCellNodes: TCompareCellNodes;
    FCompareEdges: TCompareEdges;
    //Free on destroy;
    FCover: TExactCoverProblem;
  protected
{$IFOPT C+}
    procedure IsomorphismSanityCheck;
{$ENDIF}
    function HandleSatisfies(Possibility: TPossibility; Constraint: TConstraint): boolean;
    function AllocPossibility(Sender:TObject): TPossibility;
    function AllocConstraint(Sender: TObject): TConstraint;
    procedure HandleNotify (Sender: TObject; TerminationType: TCoverTerminationType; var Stop: boolean);

{$IFDEF EXACT_COVER_LOGGING}
    procedure HandleLogPoss(Sender: TObject; Poss: TPossibility; EvtType: TEvtType);
    procedure HandleLogCons(Sender: TObject; Cons: TConstraint; EvtType: TEvtType);
    procedure HandleLogOther(Sender: TObject; EvtType: TEvtType);
    procedure LogNodes;
    procedure LogEdges;
    procedure LogPossibilities;
    procedure LogConstraints;
{$ENDIF}

    //TODO - List all isomoprhisms?
    //Can get n! more symmetries from graph self-isomorphism.
    function CheckIsomorphicExactCover(A, B: TGraphSymBoard): boolean;
  public
    function CheckIsomorphic(A,B: TGraphSymBoard): boolean;
    destructor Destroy; override;
  end;

  TIsoPossType = (iptNodeMapPossibility,
                  iptNegatedFirstVertexEdgeMap,
                  iptNegatedSecondVertexEdgeMap,
                  iptNegatedBothVertexEdgeMap,
                  iptCombinedEdgePoss);

  TIsoPossTypeStrs = array[TIsoPossType] of string;

  TPossVarRec = record
                  case PossType: TIsoPossType of
                   iptNodeMapPossibility: (NMapsFrom, NMapsTo: TNode);
                   iptNegatedFirstVertexEdgeMap,
                   iptNegatedSecondVertexEdgeMap,
                   iptNegatedBothVertexEdgeMap,
                   iptCombinedEdgePoss:
                    (EMapsFrom, EMapsTo: PInclEdge);
                end;

  TIsomorphicPossibility = class(TPossibility)
  private
    FVarRec: TPossVarRec;
  protected
{$IFOPT C+}
    function LogStr: string; virtual;
{$ENDIF}
  public
    property PossType: TIsoPossType read FVarRec.PossType;
  end;

  TIsoConsType = (ictAllNodesMappedOnce,
                  ictAllEdgesMappedOnce, //For first and second edges...
                  ictNegatedFirstVertexEdgeMap,
                  ictNegatedSecondVertexEdgeMap,
                  ictCombinedEdgeCons);

  TIsoConsTypeStrs = array[TIsoConsType] of string;

  TConstraintVarRec = record
                        case ConsType: TIsoConsType of
                          ictAllNodesMappedOnce: (MappedNode: TNode);
                          ictAllEdgesMappedOnce: (MappedEdge: PInclEdge);
                          //Need two separate constraints for the edges,
                          //one for the connects from part, one for the connects
                          //to part.
                          ictNegatedFirstVertexEdgeMap,
                          ictNegatedSecondVertexEdgeMap,
                          ictCombinedEdgeCons:
                            (EMapsFrom, EMapsTo: PInclEdge);
                      end;

  TIsomorphicConstraint = class(TConstraint)
  private
    FVarRec: TConstraintVarRec;
  protected
{$IFOPT C+}
    function LogStr: string; virtual;
{$ENDIF}
  public
    property ConsType: TIsoConsType read FVarRec.ConsType;
  end;

  function MapToBlockSetIdx(Row, Col: TRowColIdx): TRowColIdx;

{$IFOPT C+}
  procedure Log(Msg:string);
  function EdgeLogStr(Edge: PInclEdge): string;
{$ENDIF}

const
  IsoPossTypeStrs: TIsoPossTypeStrs = ('iptNodeMapPossibility',
                                       'iptNegatedFirstVertexEdgeMap',
                                       'iptNegatedSecondVertexEdgeMap',
                                       'iptNegatedBothVertexEdgeMap',
                                       'iptCombinedEdgePoss');

  IsoConsTypeStrs: TIsoConsTypeStrs = ('ictAllNodesMappedOnce',
                                       'ictAllEdgesMappedOnce',
                                       'ictNegatedFirstVertexEdgeMap',
                                       'ictNegatedSecondVertexEdgeMap',
                                       'ictCombinedEdgeCons');

implementation

uses
  Classes, SysUtils;

{ TNode }

{ TConsNode }

constructor TConsNode.Create;
begin
  inherited;
  DLItemInitList(@FConnectedCells);
end;

destructor TConsNode.Destroy;
begin
  Assert(DLItemIsEmpty(@FConnectedCells));
  inherited;
end;

function TConsNode.GetConnected;
begin
  result := not DLItemIsEmpty(@FConnectedCells);
end;

function TConsNode.LocalTopologyEqual(Other: TConsNode): boolean;
begin
  result := Self.FConnectedCellCount = Other.FConnectedCellCount;
end;

{$IFOPT C+}
function TConsNode.LogStr: string;
begin
  result := 'Index: ' + IntToStr(FIndex);
end;
{$ENDIF}

{ TLineNode }

{$IFOPT C+}
function TLineNode.LogStr:string;
begin
  if FIsRow then
    result := 'RowCons: ' + inherited
  else
    result := 'ColCons: ' + inherited;
end;
{$ENDIF}

function TLineNode.GetConsType: TConsType;
begin
  result := tctRowCol;
end;

{ TBlockNode }

{$IFOPT C+}
function TBlockNode.LogStr:string;
begin
  result := 'BlockCons: ' + inherited
end;
{$ENDIF}

function TBlockNode.GetConsType: TConsType;
begin
  result := tctBlock;
end;

{ TCellNode }

{$IFOPT C+}
function TCellNode.LogStr:string;
begin
  result := 'Cell: R: ' + IntToStr(FRow) + ' C: ' + IntToStr(FCol);
end;
{$ENDIF}

function TCellNode.GetConnected: boolean;
begin
  result := not (DLItemIsEmpty(@FRowLink)
    and DLItemIsEmpty(@FColLink)
    and DlItemIsEmpty(@FBlockLink));
end;

constructor TCellNode.Create;
begin
  inherited;
  DLItemInitObj(self, @FRowLink);
  DlItemInitObj(self, @FColLink);
  DlItemInitObj(self, @FBlockLink);
end;

destructor TCellNode.Destroy;
begin
  if not DLItemIsEmpty(@FRowLink) then
    DLListRemoveObj(@FRowLink);
  if not DLItemIsEmpty(@FColLink) then
    DLListRemoveObj(@FColLink);
  if not DlItemIsEmpty(@FBlockLink) then
    DLListRemoveObj(@FBlockLink);
  FRowNode := nil;
  FColNode := nil;
  FBlockNode := nil;
  inherited;
end;

{ TGraphSymBoard }

function TGraphSymBoard.GetBoardEntry(Row, Col: TRowColIdx): integer;
var
  Cell: TCellNode;
begin
  result := inherited;
  Cell := FCellNodes[Row, Col];
  Assert(Assigned(Cell));
  Assert((result <> 0) = not DLItemIsEmpty(@Cell.FRowLink));
  Assert((result <> 0) = not DLItemIsEmpty(@Cell.FColLink));
  Assert((result <> 0) = not DLItemIsEmpty(@Cell.FBlockLink));

  Assert((result <> 0) = Assigned(Cell.FRowNode));
  Assert((result <> 0) = Assigned(Cell.FColNode));
  Assert((result <> 0) = Assigned(Cell.FBlockNode));
  Assert((result <> 0) = Cell.Connected);
end;

procedure TGraphSymBoard.SetBoardEntry(Row, Col: TRowColIdx; Entry: integer);
var
  Cell: TCellNode;
  Connected, NewConnected: boolean;
  RowNode, ColNode: TLineNode;
  BlockNode: TBlockNode;
begin
  Cell := FCellNodes[Row, Col];
  Assert(Assigned(Cell));
  Assert(Cell.FRow = Row);
  Assert(Cell.FCol = Col);
  Connected := GetBoardEntry(Row, Col) <> 0;
  NewConnected := Entry <> 0;
  if NewConnected <> Connected then
  begin
    RowNode := self.FRowColNodes[true][Row];
    Assert(RowNode.IsRow);
    Assert(RowNode.Idx = Row);
    //
    ColNode := self.FRowColNodes[false][Col];
    Assert(not ColNode.IsRow);
    Assert(ColNode.Idx = Col);
    //
    BlockNode := self.FBlockNodes[MapToBlockSetIdx(Row, Col)];
    Assert(BlockNode.Idx = MapToBlockSetIdx(Row, Col));
    if NewConnected then
    begin
      Inc(RowNode.FConnectedCellCount);
      Inc(ColNode.FConnectedCellCount);
      Inc(BlockNode.FConnectedCellCount);

      DLListInsertHead(@RowNode.FConnectedCells, @Cell.FRowLink);
      DLListInsertHead(@ColNode.FConnectedCells, @Cell.FColLink);
      DLListInsertHead(@BlockNode.FConnectedCells, @Cell.FBlockLink);
      Cell.FRowNode := RowNode;
      Cell.FColNode := ColNode;
      Cell.FBlockNode := BlockNode;
    end
    else
    begin
      Dec(RowNode.FConnectedCellCount);
      Dec(ColNode.FConnectedCellCount);
      Dec(BlockNode.FConnectedCellCount);

      DLListRemoveObj(@Cell.FRowLink);
      DLListRemoveObj(@Cell.FColLink);
      DLListRemoveObj(@Cell.FBlockLink);

      Assert(Cell.FRowNode = RowNode);
      Assert(Cell.FColNode = ColNode);
      Assert(Cell.FBlockNode = BlockNode);

      Cell.FRowNode := nil;
      Cell.FColNode := nil;
      Cell.FBlockNode := nil;
    end;
  end;
  inherited;
end;

function TGraphSymBoard.AmIsomorphicTo(Other: TSymBoard): boolean;
var
  Checker: TIsomorphicChecker;
begin
  Checker := TIsomorphicChecker.Create;
  try
    Assert(self is TGraphSymBoard);
    Assert(Other is TGraphSymBoard);
    result := Checker.CheckIsomorphic(TGraphSymBoard(self), TGraphSymBoard(Other));
  finally
    Checker.Free;
  end;
end;

constructor TGraphSymBoard.Create;
var
  i, j: TRowColIdx;
  b: boolean;
begin
  inherited;
  for i := Low(i) to High(i) do
  begin
    for b := Low(b) to High(b) do
    begin
      FRowColNodes[b,i] := TLineNode.Create;
      FRowColNodes[b,i].FIsRow := b;
      FRowColNodes[b,i].FIndex := i;
    end;
    FBlockNodes[i] := TBlockNode.Create;
    FBlockNodes[i].FIndex := i;
  end;
  for i := Low(i) to High(i) do
  begin
    for j := Low(j) to High(j) do
    begin
      FCellNodes[i,j] := TCellNode.Create;
      FCellNodes[i,j].FRow := i;
      FCellNodes[i,j].FCol := j;
    end;
  end;
end;

destructor TGraphSymBoard.Destroy;
var
  i, j: TRowColIdx;
  b: boolean;
begin
  for i := Low(i) to High(i) do
    for j := Low(j) to High(j) do
      FCellNodes[i,j].Free;
  for i := Low(i) to High(i) do
  begin
    for b := Low(b) to High(b) do
      FRowColNodes[b,i].Free;
    FBlockNodes[i].Free;
  end;
  inherited;
end;

{ TIsomorphicChecker }

{$IFDEF EXACT_COVER_LOGGING}
procedure TIsomorphicChecker.HandleLogPoss(Sender: TObject; Poss: TPossibility; EvtType: TEvtType);
begin
  Log(ExactCover.EvtTypeStrs[EvtType] + ' ' + (Poss as TIsomorphicPossibility).LogStr);
end;

procedure TIsomorphicChecker.HandleLogCons(Sender: TObject; Cons: TConstraint; EvtType: TEvtType);
begin
  Log(ExactCover.EvtTypeStrs[EvtType] + ' ' + (Cons as TIsomorphicConstraint).LogStr);
end;

procedure TIsomorphicChecker.HandleLogOther(Sender: TObject; EvtType: TEvtType);
begin
  Log(ExactCover.EvtTypeStrs[EvtType]);
end;

procedure TIsomorphicChecker.LogNodes;
var
  B: boolean;
  Idx: integer;
begin
  Log('Nodes: ');
  for B := Low(B) to High(B) do
  begin
    if not B then
      Log('First:')
    else
      Log('Second:');
    for Idx := 0 to Pred(FCompareRowColNodes[B].Count) do
      Log(FCompareRowColNodes[B].Nodes[Idx].LogStr);
    for Idx := 0 to Pred(FCompareBlockNodes[B].Count) do
      Log(FCompareBlockNodes[B].Nodes[Idx].LogStr);
    for Idx := 0 to Pred(FCompareCellNodes[B].Count) do
      Log(FCompareCellNodes[B].Nodes[Idx].LogStr);
  end;
end;

procedure TIsomorphicChecker.LogEdges;
var
  B: boolean;
  Idx: integer;
begin
  Log('Edges: ');
  for B := Low(B) to High(B) do
  begin
    if not B then
      Log('First:')
    else
      Log('Second:');
    for Idx := 0 to Pred(FCompareEdges[B].Count) do
      Log(EdgeLogStr(@FCompareEdges[B].Edges[Idx]));
  end;
end;

procedure TIsomorphicChecker.LogPossibilities;
var
  Poss: TIsomorphicPossibility;
begin
  Log('Possibilities:');
  Poss := FCover.FirstPossibility as TIsomorphicPossibility;
  while Assigned(Poss) do
  begin
    Log(Poss.LogStr);
    Poss := FCover.NextPossibility(Poss) as TIsomorphicPossibility;
  end;
end;

procedure TIsomorphicChecker.LogConstraints;
var
  Cons: TIsomorphicConstraint;
begin
  Log('Constraints:');
  Cons := FCover.FirstConstraint as TIsomorphicConstraint;
  while Assigned(Cons) do
  begin
    Log(Cons.LogStr);
    Cons := FCover.NextConstraint(Cons) as TIsomorphicConstraint;
  end;
end;
{$ENDIF}

destructor TIsomorphicChecker.Destroy;
begin
  FCover.Free;
  inherited;
end;

{$IFOPT C+}
procedure TIsomorphicChecker.IsomorphismSanityCheck;
var
  Limit, idx, Counted, NodeType: integer;
  Node: TNode;
  Edge: PInclEdge;
  Poss, Poss2: TIsomorphicPossibility;
  Dest: boolean;
begin
{$IFDEF EXACT_COVER_LOGGING}
  //Dump the final mapping.
  Log('');
  Log('Final solution mapping:');
  Poss := self.FCover.TopPartialSolutionPossibility as TIsomorphicPossibility;
  while Assigned(Poss) do
  begin
    Log(Poss.LogStr);
    Poss := self.FCover.NextPartialSolutionPossibility(Poss) as TIsomorphicPossibility;
  end;
{$ENDIF}

  //Check each node in A mentioned once in maps from, and never in maps to
  //Check each node in B mentioned once in maps to, and never in maps from
  for Dest := Low(Dest) to High(Dest) do
  begin
    for NodeType := 0 to 2 do
    begin
      case NodeType of
        0: Limit := self.FCompareRowColNodes[Dest].Count;
        1: Limit := self.FCompareBlockNodes[Dest].Count;
        2: Limit := self.FCompareCellNodes[Dest].Count;
      else
        Assert(false);
        Limit := 0;
      end;
      for Idx := 0 to Pred(Limit) do
      begin
        case NodeType of
          0: Node := self.FCompareRowColNodes[Dest].Nodes[idx];
          1: Node := self.FCompareBlockNodes[Dest].Nodes[idx];
          2: Node := self.FCompareCellNodes[Dest].Nodes[idx];
        else
          Assert(false);
          Node := nil;
        end;
        Counted := 0;
        Poss := self.FCover.TopPartialSolutionPossibility as TIsomorphicPossibility;
        while Assigned(Poss) do
        begin
          if Poss.FVarRec.PossType = iptNodeMapPossibility then
          begin
            if not Dest then
            begin
              if Poss.FVarRec.NMapsFrom = Node then
                Inc(Counted);
              if Poss.FVarRec.NMapsTo = Node then
                Assert(false);
            end
            else
            begin
              if Poss.FVarRec.NMapsTo = Node then
                Inc(Counted);
              if Poss.FVarRec.NMapsFrom = Node then
                Assert(false);
            end;
          end;
          Poss := self.FCover.NextPartialSolutionPossibility(Poss) as TIsomorphicPossibility;
        end;
        Assert(Counted = 1);
      end;
    end;
  end;
  //Check each edge in A mentioned once in maps from, and never in maps to
  //Check each edge in B mentioned once in maps to, and never in maps from
  for Dest := Low(Dest) to High(Dest) do
  begin
    Limit:= Self.FCompareEdges[Dest].Count;
    for Idx := 0 to Pred(Limit) do
    begin
      Edge := @self.FCompareEdges[Dest].Edges[Idx];
      Counted := 0;
      Poss := self.FCover.TopPartialSolutionPossibility as TIsomorphicPossibility;
      while Assigned(Poss) do
      begin
        if Poss.FVarRec.PossType = iptCombinedEdgePoss then
        begin
          if not Dest then
          begin
            if Poss.FVarRec.EMapsFrom = Edge then
              Inc(Counted);
            if Poss.FVarRec.EMapsTo = Edge then
              Assert(false);
          end
          else
          begin
            if Poss.FVarRec.EMapsTo = Edge then
              Inc(Counted);
            if Poss.FVarRec.EMapsFrom = Edge then
              Assert(false);
          end;
        end;
        Poss := self.FCover.NextPartialSolutionPossibility(Poss) as TIsomorphicPossibility;
      end;
      Assert(Counted = 1);
    end;
  end;

  //For each edge mapping (a -> b) maps (a' -> b')
  Poss := self.FCover.TopPartialSolutionPossibility as TIsomorphicPossibility;
  while Assigned(Poss) do
  begin
    if Poss.FVarRec.PossType = iptCombinedEdgePoss then
    begin
      //Check node a -> node a'
      Poss2 := self.FCover.TopPartialSolutionPossibility as TIsomorphicPossibility;
      while Assigned(Poss2) do
      begin
        if Poss2.FVarRec.PossType = iptNodeMapPossibility then
        begin
          if Poss2.FVarRec.NMapsFrom = Poss.FVarRec.EMapsFrom.FromCell then
          begin
            Assert(Poss2.FVarRec.NMapsTo = Poss.FVarRec.EMapsTo.FromCell);
            break;
          end;
        end;
        Poss2 := self.FCover.NextPartialSolutionPossibility(Poss2) as TIsomorphicPossibility;
      end;
      //Check node b -> node b'
      Poss2 := self.FCover.TopPartialSolutionPossibility as TIsomorphicPossibility;
      while Assigned(Poss2) do
      begin
        if Poss2.FVarRec.PossType = iptNodeMapPossibility then
        begin
          if Poss2.FVarRec.NMapsFrom = Poss.FVarRec.EMapsFrom.ToCons then
          begin
            Assert(Poss2.FVarRec.NMapsTo = Poss.FVarRec.EMapsTo.ToCons);
            break;
          end;
        end;
        Poss2 := self.FCover.NextPartialSolutionPossibility(Poss2) as TIsomorphicPossibility;
      end;
      //Check there is just one edge a -> b
      //Check there is just one edge a' -> b'
      for Dest := Low(Dest) to High(Dest) do
      begin
        Counted := 0;
        Limit := self.FCompareEdges[Dest].Count;
        for Idx := 0 to Pred(Limit) do
        begin
          if not Dest then
          begin
            if (self.FCompareEdges[Dest].Edges[Idx].FromCell = Poss.FVarRec.EMapsFrom.FromCell)
              and (self.FCompareEdges[Dest].Edges[Idx].ToCons = Poss.FVarRec.EMapsFrom.ToCons) then
              Inc(Counted);
          end
          else
          begin
            if (self.FCompareEdges[Dest].Edges[Idx].FromCell = Poss.FVarRec.EMapsTo.FromCell)
              and (self.FCompareEdges[Dest].Edges[Idx].ToCons = Poss.FVarRec.EMapsTo.ToCons) then
              Inc(Counted);
          end;
        end;
        Assert(Counted = 1);
      end;
    end;
    Poss := self.FCover.NextPartialSolutionPossibility(Poss) as TIsomorphicPossibility;
  end;
end;
{$ENDIF}


function TIsomorphicChecker.HandleSatisfies(Possibility: TPossibility; Constraint: TConstraint): boolean;
var
  Poss: TIsomorphicPossibility;
  Cons: TIsomorphicConstraint;
begin
{$IFOPT C+}
  Cons := Constraint as TIsomorphicConstraint;
  Poss := Possibility as TIsomorphicPossibility;
{$ELSE}
  Cons := TIsomorphicConstraint(Constraint);
  Poss := TIsomorphicPossibility(Possibility);
{$ENDIF}
  case Cons.FVarRec.ConsType of
    ictAllNodesMappedOnce:
    begin
      if Poss.PossType = iptNodeMapPossibility then
      begin
        result :=
        (Cons.FVarRec.MappedNode = Poss.FVarRec.NMapsFrom) or
        (Cons.FVarRec.MappedNode = Poss.FVarRec.NMapsTo);
      end
      else
        result := false;
    end;
    ictAllEdgesMappedOnce:
    begin
      if Poss.PossType = iptCombinedEdgePoss then
      begin
        result := (Cons.FVarRec.MappedEdge = Poss.FVarRec.EMapsFrom)
          or (Cons.FVarRec.MappedEdge = Poss.FVarRec.EMapsTo);
      end
      else
        result := false;
    end;
    ictNegatedFirstVertexEdgeMap:
    begin
      result := false;
      if Poss.FVarRec.PossType = iptNodeMapPossibility then
      begin
        result := (Poss.FVarRec.NMapsFrom = Cons.FVarRec.EMapsFrom.FromCell)
              and (Poss.FVarRec.NMapsTo = Cons.FVarRec.EMapsTo.FromCell);
      end
      else if (Poss.PossType = iptNegatedFirstVertexEdgeMap)
        or (Poss.PossType = iptNegatedBothVertexEdgeMap) then
      begin
        result := (Poss.FVarRec.EMapsFrom = Cons.FVarRec.EMapsFrom)
          and (Poss.FVarRec.EMapsTo = Cons.FVarRec.EMapsTo);
      end;
    end;
    ictNegatedSecondVertexEdgeMap:
    begin
      result := false;
      if Poss.FVarRec.PossType = iptNodeMapPossibility then
      begin
        result := (Poss.FVarRec.NMapsFrom = Cons.FVarRec.EMapsFrom.ToCons)
              and (Poss.FVarRec.NMapsTo = Cons.FVarRec.EMapsTo.ToCons);
      end
      else if (Poss.PossType = iptNegatedSecondVertexEdgeMap)
        or (Poss.PossType = iptNegatedBothVertexEdgeMap) then
      begin
        result := (Poss.FVarRec.EMapsFrom = Cons.FVarRec.EMapsFrom)
          and (Poss.FVarRec.EMapsTo = Cons.FVarRec.EMapsTo);
      end;
    end;
    ictCombinedEdgeCons:
    begin
      case Poss.FVarRec.PossType of
        iptNegatedFirstVertexEdgeMap,
        iptNegatedSecondVertexEdgeMap,
        iptNegatedBothVertexEdgeMap,
        iptCombinedEdgePoss:
        begin
          result := (Poss.FVarRec.EMapsFrom = Cons.FVarRec.EMapsFrom)
            and (Poss.FVarRec.EMapsTo = Cons.FVarRec.EMapsTo);
        end
      else
        result := false;
      end;
    end
  else
    Assert(false);
    result := false;
  end;
{$IFDEF EXACT_COVER_LOGGING}
  if result then
  begin
    Log('POSS: ' + Poss.LogStr);
    Log('CONS: ' + Cons.LogStr);
    Log('');
  end;
{$ENDIF}

end;

function TIsomorphicChecker.AllocPossibility(Sender:TObject): TPossibility;
begin
  result := TIsomorphicPossibility.Create;
end;

function TIsomorphicChecker.AllocConstraint(Sender: TObject): TConstraint;
begin
  result := TIsomorphicConstraint.Create;
end;

procedure TIsomorphicChecker.HandleNotify (Sender: TObject; TerminationType: TCoverTerminationType; var Stop: boolean);
begin
  case TerminationType of
    cttInvalid: Assert(false);
    cttOKExactCover:
    begin
{$IFOPT C+}
      IsomorphismSanityCheck;
{$ENDIF}
      if not FSolvedOnce then
      begin
        FSolvedOnce := true;
{$IFOPT C-}
        Stop := true;
{$ENDIF}
      end
{$IFOPT C+}
      else if not FSolvedMulti then
      begin
        FSolvedMulti := true;
        Stop := true;
      end;
{$ENDIF}
    end;
    cttOKUnderconstrained, cttFailGenerallyOverconstrained,
    cttFailSpecificallyOverconstrained:
    begin
      FConstraintErr := true;
      Stop := true;
    end;
  else
    Assert(false);
  end;
end;

function TIsomorphicChecker.CheckIsomorphicExactCover(A, B: TGraphSymBoard): boolean;

  procedure SetupMapNodePossibilities;
  var
    Limit, Idx1, Idx2: integer;
    Poss: TIsomorphicPossibility;
  begin
    //All possible mappings of rowcol, block, and cell mappings,
    //three subsets of mappings, can't jumble them all together!
    Limit := FCompareRowColNodes[false].Count;
    Assert(Limit = FCompareRowColNodes[true].Count);
    for Idx1 := 0 to Pred(Limit) do
      for Idx2 := 0 to Pred(Limit) do
      begin
        Poss := FCover.AddPossibility as TIsomorphicPossibility;
        Poss.FVarRec.PossType := iptNodeMapPossibility;
        Poss.FVarRec.NMapsFrom := FCompareRowColNodes[false].Nodes[Idx1];
        Poss.FVarRec.NMapsTo := FCompareRowColNodes[true].Nodes[Idx2];
      end;
    Limit := FCompareBlockNodes[false].Count;
    Assert(Limit = FCompareBlockNodes[true].Count);
    for Idx1 := 0 to Pred(Limit) do
      for Idx2 := 0 to Pred(Limit) do
      begin
        Poss := FCover.AddPossibility as TIsomorphicPossibility;
        Poss.FVarRec.PossType := iptNodeMapPossibility;
        Poss.FVarRec.NMapsFrom := FCompareBlockNodes[false].Nodes[Idx1];
        Poss.FVarRec.NMapsTo := FCompareBlockNodes[true].Nodes[Idx2];
      end;
    Limit := FCompareCellNodes[false].Count;
    Assert(Limit = FCompareCellNodes[true].Count);
    for Idx1 := 0 to Pred(Limit) do
      for Idx2 := 0 to Pred(Limit) do
      begin
        Poss := FCover.AddPossibility as TIsomorphicPossibility;
        Poss.FVarRec.PossType := iptNodeMapPossibility;
        Poss.FVarRec.NMapsFrom := FCompareCellNodes[false].Nodes[Idx1];
        Poss.FVarRec.NMapsTo := FCompareCellNodes[true].Nodes[Idx2];
      end;
  end;

  procedure SetupMapNodeConstraints;
  var
    NodeSrc: boolean;
    Idx, Limit: integer;
    Cons: TIsomorphicConstraint;
  begin
    //Constrain everything to "each node is mapped once".
    for NodeSrc := Low(NodeSrc) to High(NodeSrc) do
    begin
      Limit := FCompareRowColNodes[NodeSrc].Count;
      for Idx := 0 to Pred(Limit) do
      begin
        Cons := FCover.AddConstraint as TIsomorphicConstraint;
        Cons.FVarRec.ConsType := ictAllNodesMappedOnce;
        Cons.FVarRec.MappedNode := FCompareRowColNodes[NodeSrc].Nodes[Idx];
      end;
      Limit := FCompareBlockNodes[NodeSrc].Count;
      for Idx := 0 to Pred(Limit) do
      begin
        Cons := FCover.AddConstraint as TIsomorphicConstraint;
        Cons.FVarRec.ConsType := ictAllNodesMappedOnce;
        Cons.FVarRec.MappedNode := FCompareBlockNodes[NodeSrc].Nodes[Idx];
      end;
      Limit := FCompareCellNodes[NodeSrc].Count;
      for Idx := 0 to Pred(Limit) do
      begin
        Cons := FCover.AddConstraint as TIsomorphicConstraint;
        Cons.FVarRec.ConsType := ictAllNodesMappedOnce;
        Cons.FVarRec.MappedNode := FCompareCellNodes[NodeSrc].Nodes[Idx];
      end;
    end;
  end;

  procedure SetupMapEdgePossibilities;
  var
    Limit, Idx1, Idx2: integer;
    Poss: TIsomorphicPossibility;
    PossType: TIsoPossType;
  begin
    Limit := FCompareEdges[false].Count;
    Assert(Limit = FCompareEdges[true].Count);
    for Idx1 := 0 to Pred(Limit) do
      for Idx2 := 0 to Pred(Limit) do
      begin
        //Edge [src][idx1] maps to Edge [dst][idx2]
        //Remember rows/cols can't map to blocks.
        if FCompareEdges[false].Edges[Idx1].ToCons.GetConsType
          = FCompareEdges[true].Edges[Idx2].ToCons.GetConsType then
        begin
          for PossType := iptNegatedFirstVertexEdgeMap to iptCombinedEdgePoss do
          begin
            Poss := FCover.AddPossibility as TIsomorphicPossibility;
            Poss.FVarRec.PossType := PossType;
            Poss.FVarRec.EMapsFrom := @FCompareEdges[false].Edges[Idx1];
            Poss.FVarRec.EMapsTo := @FCompareEdges[true].Edges[Idx2];
          end;
        end;
      end;
  end;

  procedure SetupMapEdgeVertexConstraints;
  var
    Limit, Idx1, Idx2: integer;
    Cons: TIsomorphicConstraint;
    ConsType: TIsoConsType;
  begin
    Limit := FCompareEdges[false].Count;
    Assert(Limit = FCompareEdges[true].Count);
    for Idx1 := 0 to Pred(Limit) do
      for Idx2 := 0 to Pred(Limit) do
      begin
        //Remember rows/cols can't map to blocks.
        if FCompareEdges[false].Edges[Idx1].ToCons.GetConsType
          = FCompareEdges[true].Edges[Idx2].ToCons.GetConsType then
        begin
          for ConsType := ictNegatedFirstVertexEdgeMap to ictCombinedEdgeCons do
          begin
            Cons := FCover.AddConstraint as TIsomorphicConstraint;
            Cons.FVarRec.ConsType := ConsType;
            Cons.FVarRec.EMapsFrom := @FCompareEdges[false].Edges[Idx1];
            Cons.FVarRec.EMapsTo := @FCompareEdges[true].Edges[Idx2];
          end;
        end;
      end;
  end;

  procedure SetupEdgeFinalConstraints;
  var
    Src: boolean;
    Limit, Idx: integer;
    Cons: TIsomorphicConstraint;
  begin
    for Src := Low(Src) to High(Src) do
    begin
      Limit := FCompareEdges[false].Count;
      Assert(Limit = FCompareEdges[true].Count);
      for Idx := 0 to Pred(Limit) do
      begin
        Cons := FCover.AddConstraint as TIsomorphicConstraint;
        Cons.FVarRec.ConsType := ictAllEdgesMappedOnce;
        Cons.FVarRec.MappedEdge := @FCompareEdges[src].Edges[idx];
      end;
    end;
  end;


begin
  Assert(not Assigned(FCover));
  FCover := TExactCoverProblem.Create;
  FCover.OnPossibilitySatisfiesConstraint := HandleSatisfies;
  FCover.OnCoverNotify := HandleNotify;
  FCover.OnAllocPossibility := AllocPossibility;
  FCover.OnAllocConstraint := AllocConstraint;
{$IFDEF EXACT_COVER_LOGGING}
  FCover.OnLogPossibilityEvent := self.HandleLogPoss;
  FCover.OnLogConstraintEvent := self.HandleLogCons;
  FCover.OnLogOtherEvent := self.HandleLogOther;
{$ENDIF}
  //TODO - OnLog events for exact cover problem.
  SetupMapNodePossibilities;
  SetupMapNodeConstraints;
  SetupMapEdgePossibilities;
  SetupMapEdgeVertexConstraints;
  SetupEdgeFinalConstraints;
{$IFDEF EXACT_COVER_LOGGING}
  LogPossibilities;
  LogConstraints;
{$ENDIF}
{$IFDEF EXACT_COVER_LOGGING}
  Log('');
{$ENDIF}
  FCover.SetupConnectivity;
{$IFDEF EXACT_COVER_LOGGING}
  Log('');
{$ENDIF}
  FCover.AlgorithmX;
  result := FSolvedOnce;
  //FSolvedMulti TBD.
  Assert(not FConstraintErr);
end;

function TIsomorphicChecker.CheckIsomorphic(A,B: TGraphSymBoard): boolean;
var
  Dest, Tmp: boolean;
  Idx, Idx2: TRowColIdx;
  TmpIntIdx: integer;
  Src: TGraphSymBoard;
  FirstList, SecondList, TmpList: TList;
begin
  Assert(not FUsed);
  FUsed := true;

  //Start out with gross checks for number of nodes connected,
  //and edge counts for each vertex - hopefully less effort
  //than doing an entire exact cover problem.

  //Collate things into some arrays to make life easier.
  for Dest := Low(Dest) to High(Dest) do
  begin
    if not Dest then
      Src := A
    else
      Src := B;
    for Idx := Low(Idx) to High(Idx) do
    begin
      for Tmp := Low(Tmp) to High(Tmp) do
        if (Src.FRowColNodes[tmp][idx].Connected) then
        begin
          FCompareRowColNodes[Dest]
            .Nodes[FCompareRowColNodes[Dest].Count] := Src.FRowColNodes[tmp][idx];
          Inc(FCompareRowColNodes[Dest].Count);
        end;
      if Src.FBlockNodes[idx].Connected then
      begin
        FCompareBlockNodes[Dest].Nodes[FCompareBlockNodes[Dest].Count]
          := Src.FBlockNodes[idx];
        Inc(FCompareBlockNodes[Dest].Count);
      end;
      for Idx2 := Low(Idx2) to High(Idx2) do
      begin
        if Src.FCellNodes[Idx, Idx2].Connected then
        begin
          FCompareCellNodes[Dest].Nodes[FCompareCellNodes[Dest].Count]
            := Src.FCellNodes[Idx, Idx2];
            Inc(FCompareCellNodes[Dest].Count);
        end;
      end;
    end;
  end;
{$IFDEF EXACT_COVER_LOGGING}
  LogNodes;
{$ENDIF}

  //Okay, now check basic vertex counts the same.
  if (FCompareRowColNodes[false].Count <> FCompareRowColNodes[true].Count)
    or (FCompareBlockNodes[false].Count <> FCompareBlockNodes[true].Count)
    or (FCompareCellNodes[false].Count <> FCompareCellNodes[true].Count) then
  begin
    result := false;
    exit;
  end;
  //Now, for RowColNodes and BlockNodes, check they have the same
  //edge count for each vertex. Do a bit of sorting.
  FirstList := TList.Create;
  SecondList := TList.Create;
  try
    for Tmp := Low(Tmp) to High(Tmp) do //RowCol or Block nodes.
    begin
      for Dest := Low(Dest) to High(Dest) do //First or second list / array
      begin
        if not Dest then
          TmpList := FirstList
        else
          TmpList := SecondList;
        if Tmp then
          for TmpIntIdx := 0 to Pred(FCompareRowColNodes[Dest].Count) do
            TmpList.Add(FCompareRowColNodes[Dest].Nodes[TmpIntIdx])
        else
          for TmpIntIdx := 0 to Pred(FCompareBlockNodes[Dest].Count) do
            TmpList.Add(FCompareBlockNodes[Dest].Nodes[TmpIntIdx]);
      end;
      //Now have two lists of same types of nodes from each matrix.
      //Sort the lists of nodes by edge count.
      FirstList.Sort(CompareConsNode);
      SecondList.Sort(CompareConsNode);
      Assert(FirstList.Count = SecondList.Count);
      for TmpIntIdx := 0 to Pred(FirstList.Count) do
      begin
        Assert(TObject(FirstList[TmpintIdx]) is TConsNode);
        Assert(TObject(SecondList[TmpintIdx]) is TConsNode);
        //And check that the edge counts for the vertices are the same.
        //If not, then graphs cannot be isomorphic.
        if not TConsNode(FirstList[TmpIntIdx])
          .LocalTopologyEqual(TConsNode(SecondList[TmpIntIdx])) then
        begin
          result := false;
          exit;
        end;
      end;
      FirstList.Clear;
      SecondList.Clear;
    end;
  finally
    FirstList.Free;
    SecondList.Free;
  end;
  //Not able to eliminate based on simple checks? Try exact cover.

  //First we also need to build a list of the edges for later use
  //in exact cover algorithm.
  for Dest := Low(Dest) to High(Dest) do
  begin
    if not Dest then
      Src := A
    else
      Src := B;
    for Idx := Low(Idx) to High(Idx) do
      for Idx2 := Low(Idx2) to High(Idx2) do
        if Src.FCellNodes[Idx, Idx2].Connected then
        begin
          FCompareEdges[Dest].Edges[FCompareEdges[Dest].Count].FromCell
            := Src.FCellNodes[Idx,Idx2];
          Assert(Assigned(Src.FCellNodes[Idx, Idx2].FRowNode));
          FCompareEdges[Dest].Edges[FCompareEdges[Dest].Count].ToCons
            := Src.FCellNodes[Idx, Idx2].FRowNode;
          Inc(FCompareEdges[Dest].Count);

          FCompareEdges[Dest].Edges[FCompareEdges[Dest].Count].FromCell
            := Src.FCellNodes[Idx,Idx2];
          Assert(Assigned(Src.FCellNodes[Idx, Idx2].FColNode));
          FCompareEdges[Dest].Edges[FCompareEdges[Dest].Count].ToCons
            := Src.FCellNodes[Idx, Idx2].FColNode;
          Inc(FCompareEdges[Dest].Count);

          FCompareEdges[Dest].Edges[FCompareEdges[Dest].Count].FromCell
            := Src.FCellNodes[Idx,Idx2];
          Assert(Assigned(Src.FCellNodes[Idx, Idx2].FBlockNode));
          FCompareEdges[Dest].Edges[FCompareEdges[Dest].Count].ToCons
            := Src.FCellNodes[Idx, Idx2].FBlockNode;
          Inc(FCompareEdges[Dest].Count);
        end;
  end;
{$IFDEF EXACT_COVER_LOGGING}
  LogEdges;
{$ENDIF}

  result := CheckIsomorphicExactCover(A, B);
end;

{ Misc Functions }

//TODO - Remove logging when you no longer need it.

{$IFOPT C+}
procedure Log(Msg: string);
begin
  WriteLn(Msg);
end;

function EdgeLogStr(Edge: PInclEdge): string;
begin
  result := 'Edge: [' + Edge.FromCell.LogStr + '] --> [' + Edge.ToCons.LogStr + '] ';
end;
{$ENDIF}

function CompareConsNode(Item1, Item2: Pointer): integer;
var
  Node1, Node2: TConsNode;
begin
  Node1 := TConsNode(Item1);
  Node2 := TConsNode(Item2);
  result := Node2.FConnectedCellCount - Node1.FConnectedCellCount;
end;


//This map is one particular mapping for the order specified.
//Note of course there are many other isomorphic mappings,
//if you wish to relabel the block constraints.
function MapToBlockSetIdx(Row, Col: TRowColIdx): TRowColIdx;
label error;
begin
{$IFDEF ORDER2}
  case Row of
    0,1:
      case Col of
        0,1: result := 0;
        2,3: result := 1;
      else
        goto error;
      end;
    2,3:
      case Col of
        0,1: result := 2;
        2,3: result := 3;
      else
        goto error;
      end;
    else
      goto error;
  end;
  exit;
{$ENDIF}
{$IFDEF ORDER3}
  case Row of
    0,1,2:
      case Col of
        0,1,2: result := 0;
        3,4,5: result := 1;
        6,7,8: result := 2;
      else
        goto error;
      end;
    3,4,5:
      case Col of
        0,1,2: result := 3;
        3,4,5: result := 4;
        6,7,8: result := 5;
      else
        goto error;
      end;
    6,7,8:
      case Col of
        0,1,2: result := 6;
        3,4,5: result := 7;
        6,7,8: result := 8;
      else
        goto error;
      end;
    else
      goto error;
  end;
  exit;
{$ENDIF}
  //TODO - Generalised mapping function for higher orders
  //if necessary.
error:
  Assert(false);
  result := 0;
end;

{ TIsomorphicPossibility }

{$IFOPT C+}
function TIsomorphicPossibility.LogStr: string;
begin
  result := IsoPossTypeStrs[FVarRec.PossType] + ' ';
  case FVarRec.PossType of
    iptNodeMapPossibility: result :=
      result + '(' + FVarRec.NMapsFrom.LogStr + ') -Maps-> (' + FVarRec.NMapsTo.LogStr + ')';
    iptNegatedFirstVertexEdgeMap,
    iptNegatedSecondVertexEdgeMap,
    iptNegatedBothVertexEdgeMap,
    iptCombinedEdgePoss: result :=
      result + '(' + EdgeLogStr(FVarRec.EMapsFrom) + ') -Maps-> (' + EdgeLogStr(FVarRec.EMapsTo) + ')';
  else
    Assert(false);
  end;
end;
{$ENDIF}

{ TIsomorphicConstraint }

 //TODO. Oooops? - Cross check logs don't distinguish between src / dest being mapped.
{$IFOPT C+}
function TIsomorphicConstraint.LogStr: string;
begin
  result := IsoConsTypeStrs[FVarRec.ConsType] + ' ';
  case FVarRec.ConsType of
    ictAllNodesMappedOnce: result :=
      result + FVarRec.MappedNode.LogStr + ' is mapped from or to, once.';
    ictAllEdgesMappedOnce: result :=
      result + EdgeLogStr(FVarRec.MappedEdge) + ' is mapped from or to, once.';
    ictNegatedFirstVertexEdgeMap,
    ictNegatedSecondVertexEdgeMap,
    ictCombinedEdgeCons:
      result := result + '(' + EdgeLogStr(FVarRec.EMapsFrom) + ') -Maps-> (' + EdgeLogStr(FVarRec.EMapsTo)+ ') ';
  end;
end;
{$ENDIF}

end.
