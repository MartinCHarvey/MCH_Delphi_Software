unit SQL92Nodes;

{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

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

uses CommonNodes, SysUtils, Trackables;

{ N.B All this written with "bare minimum necessary" at the moment.
  I don't yet know how the various subsystems / structures will pan out,
  so, I'm just trying to keep things with a minimum of compexity to start
  with. i.e. we have enumerated types for "what sort of", when we
  expect class overrides / specialisation may handle that later on. }

type

  TSQLSynNode = class(TCommonNode)
  public
    constructor Create; virtual;
    constructor CreateWithTracker(Tracker: TTracker); virtual;
    destructor Destroy; override;

    function FirstChild: TSQLSynNode; inline;
    function LastChild: TSQLSynNode; inline;
    function NextSibling: TSQLSynNode; inline;
    function PrevSibling: TSQLSynNode; inline;

    procedure InsertHeadChild(Dest: TSQLSynNode);
    procedure InsertTailChild(Dest: TSQLSynNode);

    procedure InsertAfterSibling(Dest: TSQLSynNode);
    procedure InsertBeforeSibling(Dest: TSQLSynNode);

    procedure RemoveFromTree;

    procedure ValidateAST;
    procedure ValidateSelf; virtual;
  end;

  TSQLSynNodeClass = class of TSQLSynNode;

  //Just a temporary thing for the parser where ordering is required.
  //Rely on tracking and assertions to make sure this is not misused.
  TTempTpl = class(TSqlSynNode)
  public
    T1, T2, T3, T4: TSQLSynNode;
  end;

  ESQLSynException = class(Exception)
  private
    FN1, FN2: TSQLSynNode;
  public
    constructor CreateCtxt(const S:string; N1, N2: TSQLSynNode);
    property N1: TSQLSynNode read FN1;
    property N2: TSQLSynNode read FN2;
  end;

  TSQLSynExpr = class(TSqlSynNode)
    //Todo - lots of things on expr return type, evaluatability
    //folding, who knows what else.
  end;

  TSQLSynLiteralType = (
    //Basic, simple string represented literals.
    sltUnsInt,
    sltInt,
    sltExactNumeric,
    sltApproxNumeric,
    sltSignedExactNumeric,
    sltSignedApproxNumeric,
    sltNatString,
    sltString,
    sltBitString,
    sltHexString,
    sltIntervalString,
    sltDate,
    sltTime,
    sltTimestamp,
    //Compound literals
    sltInterval
  );

  TSQLSynLiteral = class(TSQlSynExpr)
  private
    FText: string;
    FLitType: TSQLSynLiteralType;
  public
    property Text: string read FText write FText;
    property LitType: TSQLSynLiteralType read FLitType write FLitType;
  end;

  TSQLSynIntervalStringType = (
    istPlainInt,
    istYearMonth,
    istDayTime1,
    istDayTime2,
    istDayTime3,
    istTime1,
    istTime2,
    istTime3);

  TSQLSynIntervalStringLiteral = class(TSQLSynLiteral)
  private
    FIntervalStringType: TSQLSynIntervalStringType;
  public
    property IntervalStringType: TSQLSynIntervalStringType
      read FIntervalStringType write FIntervalStringType;
  end;

  TSQLSynIntervalQualifier = class;

  TSQLSynIntervalLiteral = class(TSQLSynLiteral)
  private
    FInterval: TSQLSynIntervalStringLiteral;
    FQualifier: TSQLSynIntervalQualifier;
    FNegated: boolean;
  public
    property Negated: boolean read FNegated write FNegated;
    property Interval: TSQLSynIntervalStringLiteral read FInterval write FInterval;
    property Qualifier: TSQLSynIntervalQualifier read FQualifier write FQualifier;
  end;

  TSQLSynIdent = class(TSqlSynNode)
  private
    FIdentName: string;
    FWildcard: boolean;
  public
    property IdentName: string read FIdentName write FIdentName;
    property Wildcard: boolean read FWildcard write FWildCard;
  end;

  //Expect some level of coercion allowed between literal types, and
  //general types, but we'll get there in a bit.
  TSQLGeneralType = (
    sgtExactNumeric,
    sgtApproxNumeric,
    sgtNatString,
    sgtString,
    sgtBitString,
    sgtHexString,
    sgtDate,
    sgtTime,
    sgtTimestamp,
    sgtInterval,
    sgtRelation { Returns something relation-like }
  );

  TSQLSynType = class(TSqlSynNode)
  private
    FGeneralType: TSQLGeneralType;
  public
    property GeneralType: TSQLGeneralType read FGeneralType write FGeneralType;
  end;

  TSQLSynIntervalType = class(TSQLSynType)
  private
    FQualifier: TSQLSynIntervalQualifier;
  public
    property Qualifier: TSQLSynIntervalQualifier read FQualifier write FQualifier;
  end;

  TSQLSynQualField = (
    sqfSecond,
    sqfMinute,
    sqfHour,
    sqfDay,
    sqfMonth,
    sqfYear
  );

  TSQLSynIntervalQualifier = class(TSQLSynNode)
  private
    FStart, FEnd: TSQLSynQualField;
  public
    property Start: TSQLSynQualField read FStart write FStart;
    property _End: TSQLSynQualField read FEnd write FEnd;
  end;

  { Built in functions / constants which are effectively exprs. }
  TSQLSynBuiltInType = (
    sftCurrentDate,
    sftCurrentTime,
    sftCurrentTimestamp,
    sftUser,
    sftCurrentUser,
    sftSessionUser,
    sftSystemUser,
    sftNull
  );

  TSqlSynBuiltin = class(TSQlSynExpr)
  private
    FBuiltInType: TSqlSynBuiltinType;
  public
    property BuiltInType: TSQLSynBuiltinType read FBuiltInType write FBuiltInType;
  end;

  //TODO - I'm going to worry about identifier resolution
  //and namespaces a bit later, because there are many and varied ways of introducing them.
  TSQLSynStructuralType = (
    sstModule
  );

  TSQLSynStructural = class(TSQLSynNode)
  private
    FStructuralType: TSQLSynStructuralType;
  public
    property StructuralType:TSQLSynStructuralType read FStructuralType write FStructuralType;
  end;

  TSQLSynNamedStructural = class(TSQLSynStructural)
  private
    FName: TSqlSynIdent;
    //TODO - First and last pointers for contained list of stuff?
  public
    property Name:TSqlSynIdent read FName write FName;
  end;

  TSQLSynModule = class(TSQLSynNamedStructural)
  private
    FLanguage, FSchema, FAuthorization: TSQLSynIdent;
    FFirstContents: TSQLSynNode;
  public
    property Language: TSqlSynIdent read FLanguage write FLanguage;
    property Schema: TSqlSynIdent read FSchema write FSchema;
    property Authorization: TSqlSynIdent read FAuthorization write FAuthorization;
    //Includes both temp tbl decls and contents.
    property FirstContents: TSqlSynNode read FFirstContents write FFirstContents;
  end;

implementation

uses
  DLList;

const
  S_INSERTING_NIL_CHILD = 'Trying to insert a NIL node into tree structure.';
  S_INTERNAL_NODE = 'AST contains compiler internal nodes. Should have been removed by now.';

{ ESmarterNodeException }


constructor ESQLSynException.CreateCtxt(const S:string; N1, N2: TSQLSynNode);
begin
  inherited Create(S);
  FN1 := N1;
  FN2 := N2;
end;

{ TSQLSynNode }

constructor TSQLSynNode.Create;
begin
  inherited;
end;

constructor TSQLSynNode.CreateWithTracker(Tracker: TTracker);
begin
  inherited;
end;

destructor TSQLSynNode.Destroy;
begin
  if not DLItemIsEmpty(@FSiblingListEntry) then
    DLListRemoveObj(@FSiblingListEntry);
  inherited;
end;

function TSQLSynNode.FirstChild: TSQLSynNode;
begin
  result := FContainedListHead.Flink.Owner as TSQLSynNode;
end;

function TSQLSynNode.LastChild: TSQLSynNode;
begin
  result := FContainedListHead.Blink.Owner as TSQLSynNode;
end;

function TSQLSynNode.NextSibling: TSQLSynNode;
begin
  result := FSiblingListEntry.Flink.Owner as TSQLSynNode;
end;

function TSQLSynNode.PrevSibling: TSQLSynNode;
begin
  result := FSiblingListEntry.Blink.Owner as TSQLSynNode;
end;

procedure TSQLSynNode.InsertHeadChild(Dest: TSQLSynNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ESQLSynException.Create(S_INSERTING_NIL_CHILD);
  Assert(DLItemIsEmpty(@Dest.FSiblingListEntry));
  Assert(not Assigned(Dest.FContainerNode));
  DLListInsertHead(@self.FContainedListHead, @Dest.FSiblingListEntry);
  Dest.FContainerNode := self;
end;

procedure TSQLSynNode.InsertTailChild(Dest: TSQLSynNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ESQLSynException.Create(S_INSERTING_NIL_CHILD);
  Assert(DLItemIsEmpty(@Dest.FSiblingListEntry));
  Assert(not Assigned(Dest.FContainerNode));
  DLListInsertTail(@self.FContainedListHead, @Dest.FSiblingListEntry);
  Dest.FContainerNode := self;
end;

procedure TSQLSynNode.InsertAfterSibling(Dest: TSQLSynNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ESQLSynException.Create(S_INSERTING_NIL_CHILD);
  Assert(DLItemIsEmpty(@Dest.FSiblingListEntry));
  Assert(not Assigned(Dest.FContainerNode));
  DLItemInsertAfter(@self.FSiblingListEntry, @dest.FSiblingListEntry);
  Dest.FContainerNode := self.FContainerNode;
end;

procedure TSQLSynNode.InsertBeforeSibling(Dest: TSQLSynNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ESQLSynException.Create(S_INSERTING_NIL_CHILD);
  Assert(DLItemIsEmpty(@Dest.FSiblingListEntry));
  Assert(not Assigned(Dest.FContainerNode));
  DLItemInsertBefore(@self.FSiblingListEntry, @dest.FSiblingListEntry);
  Dest.FContainerNode := self.FContainerNode;
end;

procedure TSQLSynNode.RemoveFromTree;
begin
  Assert(DLItemIsEmpty(@FSiblingListEntry) = not Assigned(FContainerNode));
  if Assigned(FContainerNode) then
  begin
    FContainerNode := nil;
    DLListRemoveObj(@FSiblingListEntry);
  end;
end;

procedure TSQLSynNode.ValidateAST;
var
  N: TSQLSynNode;
begin
  ValidateSelf;
  N := FirstChild;
  while Assigned(N) do
  begin
    N.ValidateAST;
    N := N.NextSibling;
  end;
end;

procedure TSQLSynNode.ValidateSelf;
begin
end;


end.
