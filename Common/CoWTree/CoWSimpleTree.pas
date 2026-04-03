unit CoWSimpleTree;

{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the �Software�), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED �AS IS�, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

interface

uses
  CoWTree;

type
  TCowSimpleTree = class(TCowTree)
  protected
    Root: TCowTreeItem;
  public
    destructor Destroy; override;
    //Add will add an item to thre tree, and report whether the addition worked.
    //If the item is a duplicate (by key), then no addition will be performed.
    function Add(item: TCoWTreeItem): boolean;
    //Searches for an item which matches the one provided by key, and
    //removes it from the tree and also deletes it - indicates whether
    //deletion worked.
    function Remove(item: TCoWTreeItem): boolean;
    //Searches for items with key matching the one specified. Returns item if
    //search OK, nil otherwise.
    function Search(item: TCoWTreeItem): TCoWTreeItem;
    //Searches for a close (approximate) match
    function SearchNear(item: TCoWTreeItem): TCoWTreeItem;
    function First: TCoWTreeItem;
    function Last: TCoWTreeItem;
    function NeighbourNode(Node: TCoWTreeItem; Lower: boolean): TCoWTreeItem;
    function HasItems: boolean;
    function RootItem: TCowTreeItem;
    procedure Clear;
  end;

implementation

uses
  Reffed;

procedure TCowSimpleTree.Clear;
begin
  Root.Release;
  Root := nil;
end;

destructor TCowSimpleTree.Destroy;
begin
  Clear;
  inherited;
end;

function TCowSimpleTree.Add(item: TCoWTreeItem): boolean;
var
  OutP: TCowTreeItem;
  h, found: TChkBool;
begin
{$IFOPT C+}
  found := TChkBool.Create;
  h := TChkBool.Create;
  try
{$ENDIF}
    if not Assigned(item) then
    begin
      result := false;
      exit;
    end;
    if not Item.Unitary then //Expect newly created refcount of 1.
    begin
      Assert(false);
      result := false;
      exit;
    end;
    OutP := SearchAndInsert(item, Root, h, found);
    result := not RdCk(found);
    Assert(result = Assigned(Outp));
    if Assigned(OutP) then
    begin
      Root.Release;
      Root := OutP;
    end
{$IFOPT C+}
  finally
    found.Free;
    h.Free;
  end;
{$ENDIF}
end;

function TCowSimpleTree.Remove(item: TCowTreeItem): Boolean;
var
  OutP: TCowTreeItem;
  h, found: TChkBool;
begin
{$IFOPT C+}
  found := TChkBool.Create;
  h := TChkBool.Create;
  try
{$ENDIF}
    if not Assigned(item) then
    begin
      result := false;
      exit;
    end;
    OutP := Delete(item, Root, h, found);
    result := RdCk(found);
    Assert(result = (Root <> OutP));
    if (Root <> OutP) then
    begin
      Root.Release;
      Root := OutP;
    end
{$IFOPT C+}
  finally
    found.Free;
    h.Free;
  end;
{$ENDIF}
end;

function TCowSimpleTree.Search(item: TCoWTreeItem): TCoWTreeItem;
var
  Inp, Outp: TCowTreeItem;
begin
  if not Assigned(item) then
  begin
    result := nil;
    exit;
  end;
  InP := Root;
  Outp := SearchItem(item, Inp);
  result := OutP;
end;

function TCowSimpleTree.SearchNear(item: TCowTreeItem): TCowTreeItem;
var
  Inp, Outp: TCowTreeItem;
begin
  if not Assigned(item) then
  begin
    result := nil;
    exit;
  end;
  InP := Root;
  Outp := SearchNearItem(item, Inp);
  result := OutP;
end;

function TCowSimpleTree.First: TCowTreeItem;
begin
  result := LeftMost(Root);
end;

function TCowSimpleTree.Last: TCowTreeItem;
begin
  result := RightMost(Root);
end;

function TCowSimpleTree.NeighbourNode(Node: TCoWTreeItem; Lower: boolean): TCoWTreeItem;
var
  FoundOrigin: TChkBool;
begin
{$IFOPT C+}
  FoundOrigin := TChkBool.Create;
  try
{$ENDIF}
    WrCk(FoundOrigin, false);
    result := FindNeighbour(Root, Node, FoundOrigin, Lower);
{$IFOPT C+}
  finally
    FoundOrigin.Free;
  end;
{$ENDIF}
end;

function TCowSimpleTree.HasItems: boolean;
begin
  result := Assigned(Root);
end;

function TCowSimpleTree.RootItem: TCowTreeItem;
begin
  result := Root;
end;

end.

