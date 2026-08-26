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

uses CommonNodes, SysUtils;

type
  TSQLSynNode = class(TCommonNode)
  public
    constructor Create; virtual;
    destructor Destroy; override;

    function FirstChild: TSQLSynNode; inline;
    function LastChild: TSQLSynNode; inline;
    function NextSibling: TSQLSynNode; inline;
    function PrevSibling: TSQLSynNode; inline;

    procedure InsertHeadChild(Dest: TSQLSynNode);
    procedure InsertTailChild(Dest: TSQLSynNode);

    procedure InsertAfterSibling(Dest: TSQLSynNode);
    procedure InsertBeforeSibling(Dest: TSQLSynNode);

    procedure ValidateAST;
    procedure ValidateSelf; virtual;
  end;

  TSQLSynNodeClass = class of TSQLSynNode;

  ESQLSynException = class(Exception)
  private
    FN1, FN2: TSQLSynNode;
  public
    constructor CreateCtxt(const S:string; N1, N2: TSQLSynNode);
    property N1: TSQLSynNode read FN1;
    property N2: TSQLSynNode read FN2;
  end;

  TSQLSynLiteralType = (
    sltUnsInt,
    sltInt,
    sltExactNumeric,
    sltApproxNumeric,
    sltNatString,
    sltString,
    sltBitString,
    sltHexString
  );

  TSQLSynLiteral = class(TSQLSynNode)
  private
    FText: string;
    FLitType: TSQLSynLiteralType;
  public
    property Text: string read FText write FText;
    property LitType: TSQLSynLiteralType read FLitType write FLitType;
  end;

  TSQLIdentifier = class(TSqlSynNode)
  private
    FName: string;
  public
    property Name: string read FName write FName;
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
end;

procedure TSQLSynNode.InsertTailChild(Dest: TSQLSynNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ESQLSynException.Create(S_INSERTING_NIL_CHILD);
end;

procedure TSQLSynNode.InsertAfterSibling(Dest: TSQLSynNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ESQLSynException.Create(S_INSERTING_NIL_CHILD);
end;

procedure TSQLSynNode.InsertBeforeSibling(Dest: TSQLSynNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ESQLSynException.Create(S_INSERTING_NIL_CHILD);
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
