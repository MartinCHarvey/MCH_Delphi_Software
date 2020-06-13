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
    procedure ClearData;
  public
    constructor Create;
    destructor Destroy; override;

    function FirstItem: TMatrixCell; inline;
    function NextHeader: THeader; inline;

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

  TSparseFunc = function(Item:TObject): boolean;

{$IFDEF USE_TRACKABLES}
  TSparseMatrix = class(TTrackable)
{$ELSE}
  TSparseMatrix = class
{$ENDIF}
  private
    //Row and col headers also sparse.
    FRowHeaders, FColHeaders: TDLEntry;
    FOwnsItems: boolean;
  protected
    function AddHeaderGeneric(HeaderList: PDLEntry): THeader;
    function InsertHeaderGeneric(Idx: integer; HeaderList: PDLEntry): THeader;
  public
    constructor Create;
    destructor Destroy; override;

    //NB. For debug usage, do ops "both ways round" if possible.
    function AddRow: THeader;
    function AddCol: THeader;

    function InsertRowAt(Idx: integer): THeader;
    function InsertColumnAt(Idx: integer): Theader;

    function GetRow(Index: integer): THeader;
    function GetColumn(Index: integer): THeader;

    function DeleteRow(Index: integer): boolean;
    function DeleteColumn(Index: integer): boolean;

    function GetCell(Row, Column: integer): TMatrixCell;
    function InsertCell(Row, Column: integer): TMatrixCell;
    function DeleteCell(Row, Column: integer): boolean;

    function ClearRow(Idx: integer): boolean;
    function ClearColumn(Idx: integer): boolean;
    procedure Clear;

    function AllocCell: TMatrixCell; virtual;
    procedure FreeCell(Cell: TMatrixCell); virtual;

    property OwnsItems: boolean read FOwnsItems write FOwnsItems;
  end;

implementation

{ Misc functions }

{ THeader }

procedure THeader.ClearData;
var
  HeadEntry: PDLEntry;
  HeadCell: TMatrixCell;
begin
  while not DlItemIsEmpty(@FDataCells) do
  begin
    HeadEntry := DLListRemoveHead(@FDataCells);
{$IFOPT C+}
    HeadCell := HeadEntry.Owner as TMatrixCell;
{$ELSE}
    HeadCell := TMatrixCell(HeadEntry.Owner);
{$ENDIF}
    FMatrix.FreeCell(HeadCell);
  end;
end;

constructor THeader.Create;
begin
  inherited;
  DLItemInitList(@FHeaderLinks);
  DLItemInitList(@FDataCells);
end;

destructor THeader.Destroy;
begin
  ClearData;
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

function THeader.NextHeader: THeader;
begin
{$IFOPT C+}
  result := FHeaderLinks.FLink.Owner as THeader;
{$ELSE}
  result := THeader(FHeaderLinks.FLink.Owner);
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

{
    //Row and col headers also sparse.
    FRowHeaders, FColHeaders: TDLEntry;
    FOwnsItems: boolean;
}

constructor TSparseMatrix.Create;
begin
  DLItemInitList(@FRowHeaders);
  DLItemInitList(@FColHeaders);
  inherited;
end;

destructor TSparseMatrix.Destroy;
begin
  while not DlItemIsEmpty(@FRowHeaders) do
    DeleteRow((FRowHeaders.FLink.Owner as THeader).FIndex);
  while not DLItemIsEMpty(@FColHeaders) do
    DeleteColumn((FColHeaders.FLink.Owner as THeader).FIndex);
  inherited;
end;

function TSparseMatrix.AddHeaderGeneric(HeaderList: PDLEntry): THeader;
var
  PrevIndex: integer;
  NewHeader: THeader;
begin
  PrevIndex := Pred(0);
  if not DLItemIsEmpty(HeaderList) then
    PrevIndex := (HeaderList.BLink.Owner as THeader).FIndex;
  NewHeader := THeader.Create;
  with NewHeader do
  begin
    FMatrix := self;
    FIndex := Succ(PrevIndex);
    DLListInsertTail(HeaderList, FHeaderLinks);
  end;
  result := NewHeader;
end;

function TSparseMatrix.AddRow: THeader;
begin
  result := AddHeaderGeneric(@FRowHeaders);
  result.FIsRow := true;
end;

function TSparseMatrix.AddCol: THeader;
begin
  result := AddHeaderGeneric(@FColHeaders);
  result.FIsRow := false;
end;

function TSparseMatrix.InsertHeaderGeneric(Idx: integer; HeaderList: PDLEntry): THeader;
var
  PrevEntry: PDLEntry;
  DistToStart, DistToEnd: integer;
begin
  if DLItemIsEmpty(HeaderList) then
    PrevEntry := HeaderList
  else if Idx < ((HeaderList.FLink.Owner as THeader).FIndex) then
    PrevEntry := HeaderList
  else if Idx > ((HeaderList.BLink.Owner as THeader).FIndex) then
    PrevEntry := HeaderList.BLink
  else
  begin
    DistToStart := Idx - (HeaderList.FLink.Owner as THeader).FIndex;
    DistToEnd := (HeaderList.BLink.Owner as THeader).FIndex - Idx;
    Assert(DistToStart >= 0);
    Assert(DistToEnd >= 0);
    if DistToStart > DistToEnd then
    begin

    end
    else
    begin

    end;
  end;
end;

function TSparseMatrix.InsertRowAt(Idx: integer): THeader;
begin
  result := InsertHeaderGeneric(Idx, @FRowHeaders);
end;

function TSparseMatrix.InsertColumnAt(Idx: integer): Theader;
begin
  result := InsertHeaderGeneric(Idx, @FColHeaders);
end;



function TSparseMatrix.GetRow(Index: integer): THeader;
begin

end;

function TSparseMatrix.GetColumn(Index: integer): THeader;
begin

end;


function TSparseMatrix.DeleteRow(Index: integer): boolean;
begin

end;

function TSparseMatrix.DeleteColumn(Index: integer): boolean;
begin

end;


function TSparseMatrix.GetCell(Row, Column: integer): TMatrixCell;
begin

end;

function TSparseMatrix.InsertCell(Row, Column: integer): TMatrixCell;
begin

end;

function TSparseMatrix.DeleteCell(Row, Column: integer): boolean;
begin

end;

function ClearRow(Idx: integer): boolean;
begin

end;

function ClearColumn(Idx: integer): boolean;
begin

end;

procedure Clear;
begin

end;


function AllocCell: TMatrixCell;
begin
  result := TMatrixCell.Create;
end;

procedure FreeCell(Cell: TMatrixCell);
begin
  Cell.Free;
end;

end.
