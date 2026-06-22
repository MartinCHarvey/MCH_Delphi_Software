unit SQL92Nodes;

{

Copyright © 2024 Martin Harvey <martin_c_harvey@hotmail.com>

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
  TSmarterCommonNode = class(TCommonNode)
  public
    destructor Destroy; override;

    function FirstChild: TSmarterCommonNode; inline;
    function LastChild: TSmarterCommonNode; inline;
    function NextSibling: TSmarterCommonNode; inline;
    function PrevSibling: TSmarterCommonNode; inline;

    procedure InsertHeadChild(Dest: TSmarterCommonNode);
    procedure InsertTailChild(Dest: TSmarterCommonNode);

    procedure InsertAfterSibling(Dest: TSmarterCommonNode);
    procedure InsertBeforeSibling(Dest: TSmarterCommonNode);

    procedure ValidateAST;
    procedure ValidateSelf; virtual;
  end;

  ESmarterNodeException = class(Exception)
  private
    FN1, FN2: TSmarterCommonNode;
  public
    constructor CreateCtxt(const S:string; N1, N2: TSmarterCommonNode);
    property N1: TSmarterCommonNode read FN1;
    property N2: TSmarterCommonNode read FN2;
  end;

  ETreeIntegrityException = class(ESmarterNodeException);

  TSQL92Node = class(TSmarterCommonNode)
  end;

  TSQLCompilerNode = class(TSmarterCommonNode)
  end;

implementation

uses
  DLList;

const
  S_INSERTING_NIL_CHILD = 'Trying to insert a NIL node into tree structure.';
  S_INTERNAL_NODE = 'AST contains compiler internal nodes. Should have been removed by now.';

{ ESmarterNodeException }


constructor ESmarterNodeException.CreateCtxt(const S:string; N1, N2: TSmarterCommonNode);
begin
  inherited Create(S);
  FN1 := N1;
  FN2 := N2;
end;

{ TSmarterCommonNode }

destructor TSmarterCommonNode.Destroy;
begin
  if not DLItemIsEmpty(@FSiblingListEntry) then
    DLListRemoveObj(@FSiblingListEntry);
  inherited;
end;

function TSmarterCommonNode.FirstChild: TSmarterCommonNode;
begin
  result := FContainedListHead.Flink.Owner as TSmarterCommonNode;
end;

function TSmarterCommonNode.LastChild: TSmarterCommonNode;
begin
  result := FContainedListHead.Blink.Owner as TSmarterCommonNode;
end;

function TSmarterCommonNode.NextSibling: TSmarterCommonNode;
begin
  result := FSiblingListEntry.Flink.Owner as TSmarterCommonNode;
end;

function TSmarterCommonNode.PrevSibling: TSmarterCommonNode;
begin
  result := FSiblingListEntry.Blink.Owner as TSmarterCommonNode;
end;

procedure TSmarterCommonNode.InsertHeadChild(Dest: TSmarterCommonNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ETreeIntegrityException.Create(S_INSERTING_NIL_CHILD);
end;

procedure TSmarterCommonNode.InsertTailChild(Dest: TSmarterCommonNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ETreeIntegrityException.Create(S_INSERTING_NIL_CHILD);
end;

procedure TSmarterCommonNode.InsertAfterSibling(Dest: TSmarterCommonNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ETreeIntegrityException.Create(S_INSERTING_NIL_CHILD);
end;

procedure TSmarterCommonNode.InsertBeforeSibling(Dest: TSmarterCommonNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ETreeIntegrityException.Create(S_INSERTING_NIL_CHILD);
end;

procedure TSmarterCommonNode.ValidateAST;
var
  N: TSmarterCommonNode;
begin
  ValidateSelf;
  N := FirstChild;
  while Assigned(N) do
  begin
    N.ValidateAST;
    N := N.NextSibling;
  end;
end;

procedure TSmarterCommonNode.ValidateSelf;
begin
  if not (Self is TSQL92Node) then
    raise ESmarterNodeException.CreateCtxt(S_INTERNAL_NODE, self, nil);
end;

{ TSQL92Node }

{ TSQLCOmpilerNode }

end.
