unit SparseMatrix;

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

//TODO - Potentially optimiseable with respect to classes / records,
//       allocation, lookaside lists etc. Not yet tho!

uses
  DLList,
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  Classes;

type
  TSparseMatrix = class;
  TMatrixCell = class;

{$IFDEF USE_TRACKABLES}
  THeader = class(TTrackable)
{$ELSE}
  THeader = class
{$ENDIF}
  private
    FHeaderLinks: TDLEntry;
    FDataCells: TDLEntry;
    FMatrix: TSparseMatrix;
    FIndex: integer;
    FTag: TObject;
    FIsRow: boolean;
    FEntryCount: integer;
  protected
    procedure GenericUnlinkAndFree; virtual;
  public
    constructor Create;
    destructor Destroy; override;

    function FirstItem: TMatrixCell; inline;
    function LastItem: TMatrixCell; inline;
    function NextHeader: THeader; inline;
    function PrevHeader: THeader; inline;

    property Matrix: TSparseMatrix read FMatrix;
    property Idx: integer read FIndex;
    property Tag: TObject read FTag write FTag;
    property IsRow: boolean read FIsRow;
    property EntryCount: integer read FEntryCount;
  end;

{$IFDEF USE_TRACKABLES}
  TMatrixCell = class(TTrackable)
{$ELSE}
  TMatrixCell = class
{$ENDIF}
  private
    FRowLinks: TDLEntry;
    FColLinks: TDLEntry;
    FRowHeader: THeader;
    FColHeader: THeader;
    FItem: TObject;
  protected
    function GetRow: integer; inline;
    function GetCol: integer; inline;
    procedure GenericUnlinkAndFree; virtual; //Without invoking destructor.
  public
    constructor Create;
    destructor Destroy; override;

    function NextCellAlongRow: TMatrixCell; inline;
    function NextCellDownColumn:TMatrixCell; inline;
    function PrevCellAlongRow: TMatrixCell; inline;
    function PrevCellDownColumn: TMatrixCell; inline;

    property Item: TObject read FItem write FItem;
    property Row: integer read GetRow;
    property Col: integer read GetCol;
  end;

{$IFDEF USE_TRACKABLES}
  TSparseMatrix = class(TTrackable)
{$ELSE}
  TSparseMatrix = class
{$ENDIF}
  private
    //Row and col headers also sparse.
    FRowHeaders, FColHeaders: TDLEntry;
    FRowCount, FColCount: integer;
    FOwnsItems: boolean;
    FSearchAlongRows: boolean; //The alternative being searching down columns
  protected
    function AddHeaderGeneric(HeaderList: PDLEntry): THeader;
    function InsertHeaderGeneric(Idx: integer; HeaderList: PDLEntry; Reindex: boolean; var IncCount: boolean): THeader;
    function GetHeaderGeneric(Idx: integer; HeaderList: PDLEntry): THeader;
    function NextHeaderGeneric(Header:Theader):THeader; inline;
    function PrevHeaderGeneric(Header:THeader):THeader; inline;
    function FirstHeaderGeneric(HeaderList: PDLEntry): THeader; inline;
    function LastHeaderGeneric(HeaderList: PDLEntry): THEader; inline;
    procedure ClearHeaderDataGeneric(Header: THeader);
    function DeleteHeaderGeneric(Header: THeader; Reindex:boolean): boolean;
  public
    constructor Create;
    destructor Destroy; override;

    //NB. For debug usage, do ops "both ways round" if possible.
    function AddRow: THeader;
    function AddColumn: THeader;

    function InsertRowAt(Idx: integer; Reindex: boolean): THeader;
    function InsertColumnAt(Idx: integer; Reindex: boolean): Theader;

    function GetRow(Index: integer): THeader; inline;
    function GetColumn(Index: integer): THeader; inline;
    //Quick functions to avoid N^2 nastiness...
    function NextRow(Row: THeader): THeader; inline;
    function PrevRow(Row: THeader): THeader; inline;
    function NextColumn(Col: THeader): THeader; inline;
    function PrevColumn(Col: THeader): THeader; inline;
    function FirstRow: THeader; inline;
    function LastRow: THeader; inline;
    function FirstColumn: THeader; inline;
    function LastColumn: THeader; inline;

    function DeleteRow(Header: THeader; Reindex:boolean): boolean; inline;
    function DeleteColumn(Header: THeader; Reindex: boolean): boolean; inline;

    function ClearRow(Header: THeader): boolean; inline;
    function ClearColumn(Header: THeader): boolean; inline;
    procedure Clear;

    function GetCell(Row, Column: integer): TMatrixCell;overload;
    function GetCell(Row, Column: THeader): TMatrixCell;overload;

    function InsertCell(Row, Column: integer): TMatrixCell;overload;
    function InsertCell(Row, Column: THeader): TMatrixCell;overload;

    function DeleteCell(Row, Column: integer): boolean; overload;
    function DeleteCell(Row, Column: THeader): boolean;overload;
    function DeleteCell(Cell: TMatrixCell): boolean;overload;

    //Alloc and free functions to enable lookaside lists if you want.
    function AllocCell: TMatrixCell; virtual;
    procedure FreeCell(Cell: TMatrixCell); virtual;
    function AllocHeader: THeader; virtual;
    procedure FreeHeader(Header:THeader);virtual;

    property OwnsItems: boolean read FOwnsItems write FOwnsItems;
    property SearchAlongRows: boolean read FSearchAlongRows write FSearchAlongRows;
  end;

implementation

{ Misc functions }

{ THeader }

procedure THeader.GenericUnlinkAndFree;
var
  Cell: TMatrixCell;
begin
  //Same as clear HeaderDataGeneric.
  Cell := FirstItem;
  while Assigned(Cell) do
  begin
    FMatrix.FreeCell(Cell);
    Cell := FirstItem;
  end;
  Assert(DLItemIsEmpty(@FDataCells));
  Assert(FEntryCount = 0);
  DLListRemoveObj(@FHeaderLinks);
  if FIsRow then
    Dec(FMatrix.FRowCount)
  else
    Dec(FMatrix.FColCount);
  if FMatrix.OwnsItems then
    FTag.Free;
  FMatrix := nil;
end;

constructor THeader.Create;
begin
  inherited;
  DLItemInitObj(self, @FHeaderLinks);
  DLItemInitList(@FDataCells);
end;

destructor THeader.Destroy;
begin
  GenericUnlinkAndFree;
  inherited;
end;

function THeader.FirstItem: TMatrixCell;
begin
{$IFOPT C+}
  result := FDataCells.FLink.Owner as TMatrixCell;
{$ELSE}
  result := TMatrixCell(FDataCells.FLink.Owner);
{$ENDIF}
end;

function THeader.LastItem: TMatrixCell;
begin
{$IFOPT C+}
  result := FDataCells.BLink.Owner as TMatrixCell;
{$ELSE}
  result := TMatrixCell(FDataCells.BLink.Owner);
{$ENDIF}
end;

function THeader.NextHeader: THeader;
begin
{$IFOPT C+}
  result := FHeaderLinks.FLink.Owner as THeader;
{$ELSE}
  result := THeader(FHeaderLinks.FLink.Owner);
{$ENDIF}
end;

function THeader.PrevHeader: THeader;
begin
{$IFOPT C+}
  result := FHeaderLinks.BLink.Owner as THeader;
{$ELSE}
  result := THeader(FHeaderLinks.BLink.Owner);
{$ENDIF}
end;

{ TMatrixCell }

function TMatrixCell.GetRow: integer;
begin
  result := FRowHeader.Idx;
end;

function TMatrixCell.GetCol: integer;
begin
  result := FColHeader.Idx;
end;

procedure TMatrixCell.GenericUnlinkAndFree;
begin
  Assert(FRowHeader.FEntryCount > 0);
  Assert(FColHeader.FEntryCount > 0);
  Dec(FRowHeader.FEntryCount);
  Dec(FColHeader.FEntryCount);
  DLListRemoveObj(@FRowLinks);
  DLListRemoveObj(@FColLinks);
  if FRowHeader.FMatrix.FOwnsItems then
    FItem.Free;
  FRowHeader := nil;
  FColHeader := nil;
  FItem := nil;
end;

constructor TMatrixCell.Create;
begin
  inherited;
  DLItemInitObj(self, @FRowLinks);
  DLItemInitObj(self, @FColLinks);
end;

destructor TMatrixCell.Destroy;
begin
  GenericUnlinkAndFree;
  inherited;
end;

function TMatrixCell.NextCellAlongRow: TMatrixCell;
begin
{$IFOPT C+}
  result := FRowLinks.FLink.Owner as TMatrixCell;
{$ELSE}
  result := TMatrixCell(FRowLinks.FLink.Owner);
{$ENDIF}
end;

function TMatrixCell.NextCellDownColumn:TMatrixCell;
begin
{$IFOPT C+}
  result := FColLinks.FLink.Owner as TMatrixCell;
{$ELSE}
  result := TMatrixCell(FColLinks.FLink.Owner);
{$ENDIF}
end;

function TMatrixCell.PrevCellAlongRow: TMatrixCell;
begin
{$IFOPT C+}
  result := FRowLinks.BLink.Owner as TMatrixCell;
{$ELSE}
  result := TMatrixCell(FRowLinks.BLink.Owner);
{$ENDIF}
end;

function TMatrixCell.PrevCellDownColumn: TMatrixCell;
begin
{$IFOPT C+}
  result := FColLinks.BLink.Owner as TMatrixCell;
{$ELSE}
  result := TMatrixCell(FColLinks.BLink.Owner);
{$ENDIF}
end;

{ TSparseMatrix }

constructor TSparseMatrix.Create;
begin
  DLItemInitList(@FRowHeaders);
  DLItemInitList(@FColHeaders);
  inherited;
end;

destructor TSparseMatrix.Destroy;
begin
  while not DlItemIsEmpty(@FRowHeaders) do
  begin
{$IFOPT C+}
    DeleteRow(FRowHeaders.FLink.Owner as THeader, false);
{$ELSE}
    DeleteRow(THeader(FRowHeaders.FLink.Owner), false);
{$ENDIF}
  end;
  Assert(FRowCount = 0);
  while not DLItemIsEmpty(@FColHeaders) do
  begin
{$IFOPT C+}
    DeleteColumn(FColHeaders.FLink.Owner as THeader, false);
{$ELSE}
    DeleteColumn(THeader(FColHeaders.FLink.Owner), false);
{$ENDIF}
  end;
  Assert(FColCount = 0);
  inherited;
end;

function TSparseMatrix.AddHeaderGeneric(HeaderList: PDLEntry): THeader;
var
  PrevIndex: integer;
begin
  PrevIndex := Pred(0);
  if not DLItemIsEmpty(HeaderList) then
  begin
{$IFOPT C+}
    PrevIndex := (HeaderList.BLink.Owner as THeader).FIndex;
{$ELSE}
    PrevIndex := THeader(HeaderList.BLink.Owner).FIndex;
{$ENDIF}
  end;
  result := AllocHeader;
  with result do
  begin
    FMatrix := self;
    FIndex := Succ(PrevIndex);
    DLListInsertTail(HeaderList, @FHeaderLinks);
  end;
end;

function TSparseMatrix.AddRow: THeader;
begin
  result := AddHeaderGeneric(@FRowHeaders);
  if Assigned(result) then
  begin
    result.FIsRow := true;
    Inc(FRowCount);
  end;
end;

function TSparseMatrix.AddColumn: THeader;
begin
  result := AddHeaderGeneric(@FColHeaders);
  if Assigned(result) then
  begin
    result.FIsRow := false;
    Inc(FColCount);
  end;
end;

function TSparseMatrix.InsertHeaderGeneric(Idx: integer; HeaderList: PDLEntry; Reindex: boolean;  var IncCount: boolean): THeader;
var
  PrevEntry, NextEntry: PDLEntry;
  PrevHeader, NextHeader: THeader;
  DistToStart, DistToEnd: integer;
  GoDown, GoUp: boolean;
{$IFOPT C+}
  GoDownPrev, GoUpPrev: PDlEntry;
{$ENDIF}
begin
  if DLItemIsEmpty(HeaderList) then
    PrevEntry := HeaderList
  else if Idx < ((HeaderList.FLink.Owner as THeader).FIndex) then
    PrevEntry := HeaderList
  else if Idx > ((HeaderList.BLink.Owner as THeader).FIndex) then
    PrevEntry := HeaderList.BLink
  else
  begin
    PrevEntry := nil;
{$IFOPT C+}
    DistToStart := Idx - (HeaderList.FLink.Owner as THeader).FIndex;
    DistToEnd := (HeaderList.BLink.Owner as THeader).FIndex - Idx;
{$ELSE}
    DistToStart := Idx - THeader(HeaderList.FLink.Owner).FIndex;
    DistToEnd := THeader(HeaderList.BLink.Owner).FIndex - Idx;
{$ENDIF}
    Assert(DistToStart >= 0);
    Assert(DistToEnd >= 0);

{$IFOPT C+}
    GoDown := true;
    GoUp := true;
    GoDownPrev := nil;
    GoUpPrev := nil;
{$ELSE}
    GoUp := DistToStart < DistToEnd;
    GoDown := not GoUp;
{$ENDIF}

    if GoUp then
    begin
      //Iterate from start towards end
      PrevEntry := HeaderList;

      NextEntry := PrevEntry.FLink;
{$IFOPT C+}
      NextHeader := NextEntry.Owner as THeader;
{$ELSE}
      NextHeader := THeader(NextEntry.Owner);
{$ENDIF}
      while Assigned(NextHeader) and (NextHeader.FIndex < Idx) do
      begin
        PrevEntry := NextEntry;
        NextEntry := PrevEntry.FLink;
{$IFOPT C+}
        NextHeader := NextEntry.Owner as THeader;
{$ELSE}
        NextHeader := THeader(NextEntry.Owner);
{$ENDIF}
      end;
      //PrevEntry now last entry with index below what we want.
{$IFOPT C+}
      GoDownPrev := PrevEntry;
{$ENDIF}
    end;

    if GoDown then
    begin
      PrevEntry := HeaderList.BLink;
{$IFOPT C+}
      PrevHeader := PrevEntry.Owner as THeader;
{$ELSE}
      PrevHeader := THeader(PrevEntry.Owner);
{$ENDIF}
      while Assigned(PrevHeader) and (PrevHeader.FIndex >= Idx) do
      begin
        PrevEntry := PrevEntry.BLink;
{$IFOPT C+}
        PrevHeader := PrevEntry.Owner as THeader;
{$ELSE}
        PrevHeader := THeader(PrevEntry.Owner);
{$ENDIF}
      end;
      //PrevEntry now last entry with index below what we want.
{$IFOPT C+}
      GoUpPrev := PrevEntry;
{$ENDIF}
    end;

{$IFOPT C+}
    Assert(GoDownPrev = GoUpPrev);
{$ENDIF}
    //Result is still PrevEntry.
  end;
  result := nil;
  IncCount := false;
  if not Reindex then
  begin
    NextEntry := PrevEntry.FLink;
{$IFOPT C+}
    NextHeader := NextEntry.Owner as THeader;
{$ELSE}
    NextHeader := THeader(NextEntry.Owner);
{$ENDIF}
    if Assigned(NextHeader) then
    begin
      Assert(NextHeader.FIndex >= Idx);
      if NextHeader.FIndex = Idx then
        result := NextHeader;
    end;
  end;
  if not Assigned(result) then
  begin
    //OK, insert new header just after PrevEntry.
    IncCount := true;
    result := AllocHeader;
    result.FMatrix := self;
    result.FIndex := Idx;
    DLItemInsertAfter(PrevEntry, @result.FHeaderLinks);
    if Reindex then
    begin
      //And now increment index values of all headers after PrevEntry.
{$IFOPT C+}
      NextHeader := result.FHeaderLinks.FLink.Owner as THeader;
{$ELSE}
      NextHeader := THeader(result.FHeaderLinks.FLink.Owner);
{$ENDIF}
      while Assigned(NextHeader) do
      begin
        Inc(NextHeader.FIndex);
{$IFOPT C+}
        NextHeader := NextHeader.FHeaderLinks.FLink.Owner as THeader;
{$ELSE}
        NextHeader := THeader(NextHeader.FHeaderLinks.FLink.Owner);
{$ENDIF}
      end;
    end;
  end;
end;

function TSparseMatrix.InsertRowAt(Idx: integer; Reindex: boolean): THeader;
var
  IncCount: boolean;
begin
  result := InsertHeaderGeneric(Idx, @FRowHeaders, Reindex, IncCount);
  if Assigned(result) then
    result.FIsRow := true;
  if IncCount then  
    Inc(FRowCount);
end;

function TSparseMatrix.InsertColumnAt(Idx: integer; Reindex: boolean): THeader;
var
  IncCount: boolean;
begin
  result := InsertHeaderGeneric(Idx, @FColHeaders, Reindex, IncCount);
  if Assigned(result) then
    result.FIsRow := false;
  if IncCount then  
    Inc(FColCount);
end;

function TSparseMatrix.GetHeaderGeneric(Idx: integer; HeaderList: PDLEntry): THeader;
var
  DistToStart, DistToEnd: integer;
  GoUp, GoDown: boolean;
  HeaderUp, HeaderDown: THeader;
  Entry: PDLEntry;
begin
  if DLItemIsEmpty(HeaderList)
  or (Idx < ((HeaderList.FLink.Owner as THeader).FIndex))
  or (Idx > ((HeaderList.BLink.Owner as THeader).FIndex)) then
    result := nil
  else
  begin
{$IFOPT C+}
    DistToStart := Idx - (HeaderList.FLink.Owner as THeader).FIndex;
    DistToEnd := (HeaderList.BLink.Owner as THeader).FIndex - Idx;
{$ELSE}
    DistToStart := Idx - THeader(HeaderList.FLink.Owner).FIndex;
    DistToEnd := THeader(HeaderList.BLink.Owner).FIndex - Idx;
{$ENDIF}
    Assert(DistToStart >= 0);
    Assert(DistToEnd >= 0);
    result := nil;
{$IFOPT C+}
    GoUp := true;
    GoDown := true;
    HeaderUp := nil;
    HeaderDown := nil;
{$ELSE}
    GoUp := DistToStart < DistToEnd;
    GoDown := not GoUp;
{$ENDIF}

    if GoUp then
    begin
      //Iterate from start towards end
      Entry := HeaderList.FLink;
{$IFOPT C+}
      HeaderUp := Entry.Owner as THeader;
{$ELSE}
      HeaderUp := THeader(Entry.Owner);
{$ENDIF}
      while Assigned(HeaderUp) and (HeaderUp.Idx < Idx) do
      begin
        Entry := Entry.FLink;
{$IFOPT C+}
        HeaderUp := Entry.Owner as THeader;
{$ELSE}
        HeaderUp := THeader(Entry.Owner);
{$ENDIF}
      end;
      if Assigned(HeaderUp) and (HeaderUp.Idx <> Idx) then
        HeaderUp := nil;
      result := HeaderUp;
    end;

    if GoDown then
    begin
      //Iterate from end toward start.
      Entry := HeaderList.BLink;
{$IFOPT C+}
      HeaderDown := Entry.Owner as THeader;
{$ELSE}
      HeaderDown := THeader(Entry.Owner);
{$ENDIF}
      while Assigned(HeaderDown) and (HeaderDown.Idx > Idx) do
      begin
        Entry := Entry.BLink;
{$IFOPT C+}
        HeaderDown := Entry.Owner as THeader;
{$ELSE}
        HeaderDown := THeader(Entry.Owner);
{$ENDIF}
      end;
      if Assigned(HeaderDown) and (HeaderDown.Idx <> Idx) then
        HeaderDown := nil;
      result := HeaderDown;
    end;
{$IFOPT C+}
    Assert(HeaderUp = HeaderDown);
{$ENDIF}
  end;
end;

function TSparseMatrix.GetRow(Index: integer): THeader;
begin
  result := GetHeaderGeneric(Index, @FRowHeaders);
end;

function TSparseMatrix.GetColumn(Index: integer): THeader;
begin
  result := GetHeaderGeneric(Index, @FColHeaders);
end;

function TSparseMatrix.NextHeaderGeneric(Header:Theader):THeader;
begin
  result := Header.NextHeader;
end;

function TSparseMatrix.PrevHeaderGeneric(Header:THeader):THeader;
begin
  result := Header.PrevHeader;
end;

function TSparseMatrix.FirstHeaderGeneric(HeaderList: PDLEntry): THeader;
begin
{$IFOPT C+}
  result := HeaderList.FLink.Owner as THeader;
{$ELSE}
  result := THeader(HeaderList.FLink.Owner);
{$ENDIF}
end;

function TSparseMatrix.LastHeaderGeneric(HeaderList: PDLEntry): THEader;
begin
{$IFOPT C+}
  result := HeaderList.BLink.Owner as THeader;
{$ELSE}
  result := THeader(HeaderList.BLink.Owner);
{$ENDIF}
end;

function TSparseMatrix.NextRow(Row: THeader): THeader;
begin
  Assert(Row.FIsRow);
  result := NextHeaderGeneric(Row);
  if Assigned(result) then
    Assert(result.FIsRow);
end;

function TSparseMatrix.PrevRow(Row: THeader): THeader;
begin
  Assert(Row.FIsRow);
  result := PrevHeaderGeneric(Row);
  if Assigned(result) then
    Assert(result.FIsRow);
end;

function TSparseMatrix.NextColumn(Col: THeader): THeader;
begin
  Assert(not Col.FIsRow);
  result := NextHeaderGeneric(Col);
  if Assigned(result) then
    Assert(not result.FIsRow);
end;

function TSparseMatrix.PrevColumn(Col: THeader): THeader;
begin
  Assert(not Col.FIsRow);
  result := PrevHeaderGeneric(Col);
  if Assigned(result) then
    Assert(not result.FIsRow);
end;

function TSparseMatrix.FirstRow: THeader;
begin
  result := FirstHeaderGeneric(@FRowHeaders);
  if Assigned(result) then
    Assert(result.FIsRow);
end;

function TSparseMatrix.LastRow: THeader;
begin
  result := LastHeaderGeneric(@FRowHeaders);
  if Assigned(result) then
    Assert(result.FIsRow);
end;

function TSparseMatrix.FirstColumn: THeader;
begin
  result := FirstHeaderGeneric(@FColHeaders);
  if Assigned(result) then
    Assert(not result.FIsRow);
end;

function TSparseMatrix.LastColumn: THeader;
begin
  result := LastHeaderGeneric(@FColHeaders);
  if Assigned(result) then
    Assert(not result.FIsRow);
end;

function TSparseMatrix.DeleteHeaderGeneric(Header: THeader; Reindex:boolean): boolean;
var
  NextHeader: THeader;
begin
  //Don't call the clear functions here, because it checks
  //that our secondary method of removing items
  //from the header works.
  result := Assigned(Header);
  if result then
  begin
    if Reindex then
    begin
{$IFOPT C+}
      NextHeader := Header.FHeaderLinks.FLink.Owner as THeader;
{$ELSE}
      NextHeader := THeader(Header.FHeaderLinks.FLink.Owner);
{$ENDIF}
      while Assigned(NextHeader) do
      begin
        Dec(NextHeader.FIndex);
{$IFOPT C+}
        NextHeader := NextHeader.FHeaderLinks.FLink.Owner as THeader;
{$ELSE}
        NextHeader := THeader(NextHeader.FHeaderLinks.FLink.Owner);
{$ENDIF}
      end;
    end;
    FreeHeader(Header);
  end;
end;

function TSparseMatrix.DeleteRow(Header: THeader; Reindex:boolean): boolean;
begin
  if Assigned(Header) then
    Assert(Header.IsRow);
  result := DeleteHeaderGeneric(Header, Reindex);
end;

function TSparseMatrix.DeleteColumn(Header: THeader; Reindex: boolean): boolean;
begin
  if Assigned(Header) then
    Assert(not Header.IsRow);
  result := DeleteHeaderGeneric(Header, Reindex);
end;

procedure TSparseMatrix.ClearHeaderDataGeneric(Header: THeader);
var
  Cell: TMatrixCell;
begin
  //Should be partial duplicate of THeader.GenericUnlinkAndFree
  Cell := Header.FirstItem;
  while Assigned(Cell) do
  begin
    FreeCell(Cell);
    Cell := Header.FirstItem;
  end;
  Assert(DLItemIsEmpty(@Header.FDataCells));
  Assert(Header.FEntryCount = 0);
end;

function TSparseMatrix.ClearRow(Header: THeader): boolean;
begin
  result := Assigned(Header);
  if result then
    ClearHeaderDataGeneric(Header);
end;

function TSparseMatrix.ClearColumn(Header: THeader): boolean;
begin
  result := Assigned(Header);
  if result then
    ClearHeaderDataGeneric(Header);
end;

procedure TSparseMatrix.Clear;
var
  Row: THeader;
{$IFOPT C+}
  Col: THeader;
{$ENDIF}
begin
  Row := FirstRow;
  while Assigned(Row) do
  begin
    ClearRow(Row);
    Row := NextRow(Row);
  end;
{$IFOPT C+}
  Col := FirstColumn;
  while Assigned(Col) do
  begin
    Assert(Col.FEntryCount = 0);
    Col := NextHeaderGeneric(Col);
  end;
{$ENDIF}
end;

function TSparseMatrix.GetCell(Row, Column: integer): TMatrixCell;
var
  RowHeader, ColHeader: THeader;
begin
  RowHeader := GetRow(Row);
  ColHeader := GetColumn(Column);
  if Assigned(RowHeader) and Assigned(ColHeader) then
    result := GetCell(RowHeader, ColHeader)
  else
    result := nil;
end;

function TSparseMatrix.GetCell(Row, Column: THeader): TMatrixCell;

  function GetCellInternal(SearchAlongRow, SearchDown: boolean): TMatrixCell;
  begin
    if SearchAlongRow then
    begin
      if SearchDown then
        result := Row.LastItem
      else
        result := Row.FirstItem;
    end
    else
    begin
      if SearchDown then
        result := Column.LastItem
      else
        result := Column.FirstItem;
    end;
    while Assigned(result)
      and
        ((result.FRowHeader <> Row) or (result.FColHeader <> Column)) do
    begin
      if SearchAlongRow then
      begin
        if SearchDown then
          result := result.PrevCellAlongRow
        else
          result := result.NextCellAlongRow;
      end
      else
      begin
        if SearchDown then
          result := result.PrevCellDownColumn
        else
          result := result.NextCellDownColumn;
      end;
    end;
  end;

var
  FirstHeaderIdx, LastHeaderIdx, DistToFirst, DistToLast: integer;
  SearchDown: boolean;

begin
  //Not always clear whether quickest to get by row or column
  //the matrix could be sparse more one way than the other.
  //Also, could go either direction, up or down...

  //Do the combination we expect to be quickest,
  //and then debug check the other combinations produce the same result.
  if FSearchAlongRows then
  begin
    FirstHeaderIdx := FirstColumn.FIndex;
    LastHeaderIdx := LastColumn.FIndex;
    DistToFirst := Column.FIndex - FirstHeaderIdx;
    DistToLast := LastHeaderIdx - Column.FIndex;
    SearchDown := DistToFirst > DistToLast;
  end
  else
  begin
    FirstHeaderIdx := FirstRow.FIndex;
    LastHeaderIdx := LastRow.FIndex;
    DistToFirst := Row.FIndex - FirstHeaderIdx;
    DistToLast := LastHEaderIdx - Row.FIndex;
    SearchDown := DistToFirst > DistToLast;
  end;
  Assert(DistToFirst >= 0);
  Assert(DistToLast >= 0);
  result := GetCellInternal(FSearchAlongRows, SearchDown);
  Assert(result = GetCellInternal(not FSearchAlongRows, SearchDown));
  Assert(result = GetCellInternal(FSearchAlongRows, not SearchDown));
  Assert(result = GetCellInternal(not FSearchAlongRows, not SearchDown));
end;


function TSparseMatrix.InsertCell(Row, Column: integer): TMatrixCell;
var
  RowHeader, ColHeader: THeader;
begin
  RowHeader := GetRow(Row);
  ColHeader := GetColumn(Column);
  if Assigned(RowHeader) and Assigned(ColHeader) then
    result := InsertCell(RowHeader, ColHeader)
  else
    result := nil;
end;

function TSparseMatrix.InsertCell(Row, Column: THeader): TMatrixCell;
var
  PrevEntryInRow, PrevEntryInCol: PDLEntry;
  Candidate, NextInRow,NextInCol: TMatrixCell;
  FirstIdx, LastIdx, DistToFirst, DistToLast: integer;
  SearchUp, SearchDown: boolean;
{$IFOPT C+}
  DbgPrevUp, DbgPrevDown: PDLEntry;
{$ENDIF}
begin
{
  Previous entry is always absolute previous regardless of
  search direction. "Positional previous"
  Candidate is "next highest" or "next lowest" thru search,
  depending on search direction. "Temporal next"

  All this code probably won't hurt - faster than something
  more abstract.
}
  //Do rows first.
  if DLItemIsEmpty(@Row.FDataCells) then
    PrevEntryInRow := @Row.FDataCells
  else
  begin
    FirstIdx := Row.FirstItem.FColHeader.FIndex;
    LastIdx := Row.LastItem.FColHeader.FIndex;
    if Column.FIndex < FirstIdx then
      PrevEntryInRow := @Row.FDataCells
    else if Column.FIndex > LastIdx then
      PrevEntryInRow := Row.FDataCells.BLink
    else
    begin
      DistToFirst := Column.FIndex - FirstIdx;
      DistToLast := LastIdx - Column.FIndex;
      Assert(DistToFirst >= 0);
      Assert(DistToLast >= 0);

      PrevEntryInRow := nil;
{$IFOPT C+}
      SearchUp := true;
      SearchDown := true;
      DbgPrevUp := nil;
      DbgPrevDown := nil;
{$ELSE}
      SearchDown := DistToLast < DistToFirst;
      SearchUp := not SearchDown;
{$ENDIF}

      if SearchUp then
      begin
        PrevEntryInRow := @Row.FDataCells;
        //Next candidate up from lowest.
{$IFOPT C+}
        Candidate := PrevEntryInRow.FLink.Owner as TMatrixCell;
{$ELSE}
        Candidate := TMatrixCell(PrevEntryInRow.FLink.Owner);
{$ENDIF}
        while Assigned(Candidate)
              and (Candidate.FColHeader.FIndex < Column.FIndex) do
        begin
          PrevEntryInRow := PrevEntryInRow.FLink;
{$IFOPT C+}
          Candidate := PrevEntryInRow.FLink.Owner as TMatrixCell;
{$ELSE}
          Candidate := TMatrixCell(PrevEntryInRow.FLink.Owner);
{$ENDIF}
        end;
        //OK, found prev entry.
{$IFOPT C+}
        DbgPrevUp := PrevEntryInRow;
{$ENDIF}
      end;

      if SearchDown then
      begin
        PrevEntryInRow := Row.FDataCells.BLink;
{$IFOPT C+}
        Candidate := PrevEntryInRow.Owner as TMatrixCell;
{$ELSE}
        Candidate := TMatrixCell(PrevEntryInRow.Owner);
{$ENDIF}
        while Assigned(Candidate)
          and (Candidate.FColHeader.FIndex >= Column.FIndex) do
        begin
          PrevEntryInRow := PrevEntryInRow.BLink;
{$IFOPT C+}
          Candidate := PrevEntryInRow.Owner as TMatrixCell;
{$ELSE}
          Candidate := TMatrixCell(PrevEntryInRow.Owner);
{$ENDIF}
        end;
{$IFOPT C+}
        DbgPrevDown := PrevEntryInRow;
{$ENDIF}
      end;
{$IFOPT C+}
      Assert(DbgPrevUp = DbgPrevDown);
{$ENDIF}
    end;
  end;

  //Now do columns.
  if DLItemIsEmpty(@Column.FDataCells) then
    PrevEntryInCol := @Column.FDataCells
  else
  begin
    FirstIdx := Column.FirstItem.FRowHeader.FIndex;
    LastIdx := Column.LastItem.FRowHeader.FIndex;
    if Row.FIndex < FirstIdx then
      PrevEntryInCol := @Column.FDataCells
    else if Row.FIndex > LastIdx then
      PrevEntryInCol := Column.FDataCells.BLink
    else
    begin
      DistToFirst := Row.FIndex - FirstIdx;
      DistToLast := LastIdx - Row.FIndex;
      Assert(DistToFirst >= 0);
      Assert(DistToLast >= 0);

      PrevEntryInCol := nil;
{$IFOPT C+}
      SearchUp := true;
      SearchDown := true;
      DbgPrevUp := nil;
      DbgPrevDown := nil;
{$ELSE}
      SearchDown := DistToLast < DistToFirst;
      SearchUp := not SearchDown;
{$ENDIF}

      if SearchUp then
      begin
        PrevEntryInCol := @Column.FDataCells;
        //Next candidate up from lowest.
{$IFOPT C+}
        Candidate := PrevEntryInCol.FLink.Owner as TMatrixCell;
{$ELSE}
        Candidate := TMatrixCell(PrevEntryInCol.FLink.Owner);
{$ENDIF}
        while Assigned(Candidate)
              and (Candidate.FRowHeader.FIndex < Row.FIndex) do
        begin
          PrevEntryInCol := PrevEntryInCol.FLink;
{$IFOPT C+}
          Candidate := PrevEntryInCol.FLink.Owner as TMatrixCell;
{$ELSE}
          Candidate := TMatrixCell(PrevEntryInCol.FLink.Owner);
{$ENDIF}
        end;
        //OK, found prev entry.
{$IFOPT C+}
        DbgPrevUp := PrevEntryInCol;
{$ENDIF}
      end;

      if SearchDown then
      begin
        PrevEntryInCol := Column.FDataCells.BLink;
{$IFOPT C+}
        Candidate := PrevEntryInCol.Owner as TMatrixCell;
{$ELSE}
        Candidate := TMatrixCell(PrevEntryInCol.Owner);
{$ENDIF}
        while Assigned(Candidate)
          and (Candidate.FRowHeader.FIndex >= Row.FIndex) do
        begin
          PrevEntryInCol := PrevEntryInCol.BLink;
{$IFOPT C+}
          Candidate := PrevEntryInCol.Owner as TMatrixCell;
{$ELSE}
          Candidate := TMatrixCell(PrevEntryInCol.Owner);
{$ENDIF}
        end;
{$IFOPT C+}
        DbgPrevDown := PrevEntryInCol;
{$ENDIF}
      end;
{$IFOPT C+}
      Assert(DbgPrevUp = DbgPrevDown);
{$ENDIF}
    end;
  end;

  //Check cell not already inserted.
{$IFOPT C+}
  NextInRow := PrevEntryInRow.FLink.Owner as TMatrixCell;
  NextInCol := PrevEntryInCol.FLink.Owner as TMatrixCell;
{$ELSE}
  NextInRow := TMatrixCell(PrevEntryInRow.FLink.Owner);
  NextInCol := TMatrixCell(PrevEntryInCol.FLink.Owner);
{$ENDIF}
  if Assigned(NextInRow) and (NextInRow = NextInCol) then
  begin
    //The cell already exists (we think).
    Assert(NextInRow.FRowHeader = Row);
    Assert(NextInRow.FColHeader = Column);
    result := NextInRow;
    Assert(GetCell(Row.Idx, Column.Idx) = NextInRow);
  end
  else
  begin
    //OK, need to create a brand new cell.
    result := AllocCell;
    DLItemInsertAfter(PrevEntryInRow, @result.FRowLinks);
    DLItemInsertAfter(PrevEntryInCol, @result.FColLinks);
    result.FRowHeader := Row;
    Inc(Row.FEntryCount);
    result.FColHeader := Column;
    Inc(Column.FEntryCount);
    Assert(GetCell(Row.Idx, Column.Idx) = result);
  end;
end;

function TSparseMatrix.DeleteCell(Row, Column: integer): boolean;
var
  RowHeader, ColHeader: THeader;
begin
  RowHeader := GetRow(Row);
  ColHeader := GetColumn(Column);
  result := Assigned(RowHeader) and Assigned(ColHeader)
    and DeleteCell(RowHeader, ColHeader);
end;

function TSparseMatrix.DeleteCell(Row, Column: THeader): boolean;
var
  Cell: TMatrixCell;
begin
  Cell := GetCell(Row, Column);
  result := Assigned(Cell);
  FreeCell(Cell);
end;

function TSparseMatrix.DeleteCell(Cell: TMatrixCell): boolean;
begin
  result := Assigned(Cell);
  FreeCell(Cell);
end;


function TSparseMatrix.AllocCell: TMatrixCell;
begin
  result := TMatrixCell.Create;
end;

procedure TSparseMatrix.FreeCell(Cell: TMatrixCell);
begin
  //Overrides not calling destructor should call generic unlink.
  Cell.Free;
end;

function TSparseMatrix.AllocHeader: THeader;
begin
  result := THeader.Create;
end;

procedure TSparseMatrix.FreeHeader(Header:THeader);
begin
  //Overrides not calling destructor should call generic unlink.
  Header.Free;
end;

end.
