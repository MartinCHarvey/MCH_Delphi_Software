unit CocoComment;
{$INCLUDE CocoCD.inc}

interface

uses
  CRT, CocoBase, Classes;

const MaxCommentLen = 8;

type
  TCommentString = ShortString;
  TCommentItem = class(TCollectionItem)
  private
    fNested: boolean;
    fStart: TCommentString;
    fStop: TCommentString;
  public
    property Start : TCommentString read fStart write fStart;
    property Stop : TCommentString read fStop write fStop;
    property Nested : boolean read fNested write fNested;
  end; {TCommentItem}

  TCommentList = class(TCollection)
  private
    fOnScannerError: TErrorProc;
    fTableHandler: TTableHandler;
    fOnGetCurrentSymbol: TFunctionSymbolPosition;
    function GetItems(const Index : Integer) : TCommentItem;
    procedure SetItems(const Index : Integer; const Value : TCommentItem);
    function MakeCommentString(gp: integer): TCommentString;
    function GetHasCommentWithOneEndChar: boolean;
  public
    constructor Create;

    function InsertAtTop(const Start : integer; const Stop : integer;
        const Nested : boolean) : TCommentItem;
    function Insert(const Index : integer) : TCommentItem;
    procedure SemError(const ErrorNum : integer);

    property HasCommentWithOneEndChar: boolean read GetHasCommentWithOneEndChar;
    property Items[const Index : Integer] : TCommentItem read GetItems write SetItems;
    property OnScannerError : TErrorProc read fOnScannerError write fOnScannerError;
    property OnGetCurrentSymbol : TFunctionSymbolPosition read fOnGetCurrentSymbol
        write fOnGetCurrentSymbol;
    property TableHandler : TTableHandler read fTableHandler write fTableHandler;
  end; {TCommentList}

implementation

uses
  Sets, CRTypes;

{ TCommentList }

procedure TCommentList.SemError(const ErrorNum: integer);
begin
  Assert(Assigned(fOnScannerError) AND Assigned(fOnGetCurrentSymbol),'TCommentList.SemError: events not assigned');
  if Assigned(fOnScannerError) AND Assigned(fOnGetCurrentSymbol) then
    fOnScannerError(ErrorNum + 100, fOnGetCurrentSymbol, '', etSymantic)
end; {SemError}

function TCommentList.MakeCommentString(gp : integer) : TCommentString;
var
  i : integer;
  n : integer;
  gn : CRTGraphNode;
  sset : CRTSet;
begin
  i := 1;
  while gp <> 0 do
  begin
    fTableHandler.GetNode(gp, gn);
    if gn.typ = CRTchart then
    begin
      if i < Succ(MaxCommentLen) then
        Result[i] := AnsiChar(gn.p1);
      inc(i)
    end
    else if gn.typ = CRTchrclass then
    begin
      fTableHandler.GetClass(gn.p1, sset);
      if (Sets.Elements(sset, n) <> 1) then
        SemError(26);
      if i < Succ(MaxCommentLen) then
        Result[i] := AnsiChar(n);
      inc(i)
    end
    else
      SemError(22);
    gp := gn.next
  end;
  if (i = 1) or (i > Succ(MaxCommentLen)) then
    SemError(25)
  else
    Result[0] := AnsiChar(i - 1)
end; {MakeCommentString}

function TCommentList.InsertAtTop(const Start : integer;
    const Stop : integer; const Nested : boolean) : TCommentItem;
var
  CommentItem : TCommentItem;
begin
  CommentItem := Insert(0);
  CommentItem.Start := MakeCommentString(start);
  CommentItem.Stop := MakeCommentString(stop);
  CommentItem.Nested := nested;
  Result := CommentItem;
end; {Add}

constructor TCommentList.Create;
begin
  inherited Create(TCommentItem);
end; {Create}

function TCommentList.GetItems(const Index: Integer): TCommentItem;
begin
  Result := TCommentItem(inherited GetItem(Index));
end; {GetItems}

function TCommentList.Insert(const Index: integer): TCommentItem;
begin
  Result := TCommentItem(Inherited Insert(Index));
end; {Insert}

procedure TCommentList.SetItems(const Index: Integer; const Value: TCommentItem);
begin
  inherited setItem(Index, Value);
end; {SetItems}

function TCommentList.GetHasCommentWithOneEndChar: boolean;
var
  i : integer;
begin
  Result := FALSE;
  for i := 0 to Count - 1 do
    if length(Items[i].Stop) = 1 then
      Result := TRUE;
end; {GetHasCommentWithOneEndChar}

end.

