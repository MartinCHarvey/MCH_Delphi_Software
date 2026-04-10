unit CoWABTree;

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
  //Can build A-B indexes to check current, next.
  TCowSel = (abMain, abNext);

  TCowABTree = class(TCowTree)
  protected
    Roots: array [TCowSel] of TCoWTreeItem;
  public
    destructor Destroy; override;
    //Add will add an item to thre tree, and report whether the addition worked.
    //If the item is a duplicate (by key), then no addition will be performed.
    function Add(item: TCoWTreeItem; Src, Dest: TCowSel): boolean;
    //Searches for an item which matches the one provided by key, and
    //removes it from the tree and also deletes it - indicates whether
    //deletion worked.
    function Remove(item: TCoWTreeItem; Src,Dest: TCowSel): boolean;
    //Searches for items with key matching the one specified. Returns item if
    //search OK, nil otherwise.
    function Search(item: TCoWTreeItem; Sel: TCowSel): TCoWTreeItem;
    //Searches for a close (approximate) match
    function SearchNear(item: TCoWTreeItem; Sel: TCowSel): TCoWTreeItem;
    function First(Sel: TCowSel): TCoWTreeItem;
    function Last(Sel: TCowSel): TCoWTreeItem;
    function NeighbourNode(Node: TCoWTreeItem; Sel: TCowSel; Lower: boolean): TCoWTreeItem;
    function HasItems(Sel: TCowSel): boolean;
    function RootItem(Sel: TCowSel): TCowTreeItem;
    procedure Clear(Sel: TCowSel);
  end;

implementation

uses
  Reffed;

procedure TCoWABTree.Clear(Sel: TCowSel);
var
  Tmp: TReffed;
begin
  Tmp := Roots[Sel];
  Roots[Sel] := nil;
  Tmp.Release;
end;

destructor TCoWABTree.Destroy;
var
  Sel: TCowSel;
begin
  for Sel := Low(Sel) to High(Sel) do
    Clear(Sel);
  inherited destroy;
end;


function TCoWABTree.Add(item: TCoWTreeItem; Src,Dest: TCowSel): boolean;
var
  InP, OutP: TCowTreeItem;
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
    InP := Roots[Src];
    OutP := SearchAndInsert(item, InP, h, found);
    if Assigned(OutP) then
    begin
      Roots[Dest].Release;
      Roots[Dest] := OutP;
      Assert(not RdCk(found));
    end
    else
      Assert(RdCk(Found));
    result := not RdCk(found);
{$IFOPT C+}
  finally
    found.Free;
    h.Free;
  end;
{$ENDIF}
end;

function TCoWABTree.Remove(item: TCowTreeItem; Src, Dest: TCowSel): Boolean;
var
  InP, OutP: TCowTreeItem;
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
    InP := Roots[Src];
    OutP := Delete(item, InP, h, found);
    if (InP <> OutP) then
    begin
      Roots[Dest].Release;
      Roots[Dest] := OutP;
      Assert(RdCk(found));
    end
    else
      Assert(not RdCk(Found));
    result := RdCk(found);
{$IFOPT C+}
  finally
    found.Free;
    h.Free;
  end;
{$ENDIF}
end;

function TCoWABTree.Search(item: TCoWTreeItem; Sel: TCowSel): TCoWTreeItem;
var
  Inp, Outp: TCowTreeItem;
begin
  if not Assigned(item) then
  begin
    result := nil;
    exit;
  end;
  InP := Roots[Sel];
  Outp := SearchItem(item, Inp);
  result := OutP;
end;

function TCoWABTree.SearchNear(item: TCowTreeItem; Sel: TCowSel): TCowTreeItem;
var
  Inp, Outp: TCowTreeItem;
begin
  if not Assigned(item) then
  begin
    result := nil;
    exit;
  end;
  InP := Roots[Sel];
  Outp := SearchNearItem(item, Inp);
  result := OutP;
end;

function TCoWABTree.First(Sel: TCowSel): TCowTreeItem;
begin
  result := LeftMost(Roots[Sel]);
end;

function TCoWABTree.Last(Sel: TCowSel): TCowTreeItem;
begin
  result := RightMost(Roots[Sel]);
end;

function TCoWABTree.NeighbourNode(Node: TCoWTreeItem; Sel: TCowSel; Lower: boolean): TCoWTreeItem;
var
  FoundOrigin: TChkBool;
begin
{$IFOPT C+}
  FoundOrigin := TChkBool.Create;
  try
{$ENDIF}
    WrCk(FoundOrigin, false);
    result := FindNeighbour(Roots[sel], Node, FoundOrigin, Lower);
{$IFOPT C+}
  finally
    FoundOrigin.Free;
  end;
{$ENDIF}
end;

function TCoWABTree.HasItems(Sel: TCowSel): boolean;
begin
  result := Assigned(Roots[Sel]);
end;

function TCoWABTree.RootItem(Sel: TCowSel): TCowTreeItem;
begin
  result := Roots[Sel];
end;

end.
