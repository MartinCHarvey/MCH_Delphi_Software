//
//  Taken from Nicklaus Wirth :
//    Algorithmen und Datenstrukturen ( in Pascal )
//    Balanced Binary Trees p 250 ++

unit BinaryTree;
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

{
  Coding in this unit makes use of the fact that classes declared in the
  same unit can access each others internal datastructures. This simplifies
  things quite a lot, and also means (paradoxically) that we can have *more*
  protection against outside interference by maing the balance and children of
  TBinTreeItem private.
}

interface

uses Classes;

type
  TBinTreeItem = class
  private
    //TBinTree delcared in same unit, so can change these fields, but make
    //private so no other code can frobnicate them.
    left, right: TBinTreeItem;
    bal: - 1..1;
    procedure FreeSubTree;
  protected
    function Compare(Other: TBinTreeItem;
                     AllowKeyDedupe: boolean): integer; virtual; abstract; // data
   // a < self :-1  a=self :0  a > self :+1
    procedure CopyFrom(Source: TBinTreeItem); virtual; abstract; // data
  end;

  TBinTraversalOrder = (btoPreOrderLoHi, btoInOrderLoHi, btoPostOrderLoHi,
    btoPreOrderHiLo, btoInOrderHiLo, btoPostOrderHiLo);

  TBinTree = class;

  TTraversalEvent = procedure(Tree: TBinTree;
    Item: TBinTreeItem;
    Level: integer) of object;

  TBinTree = class
  private
    root: TBinTreeItem;
    h: boolean; //Wish I could give this a more useful name!
    found: boolean;
    procedure DelHelper(var r: TBinTreeItem; var q: TBinTreeItem);
    procedure Delete(item: TBinTreeItem; var p: TBinTreeItem);
    procedure SearchAndInsert(item: TBinTreeItem; var p: TBinTreeItem);
    procedure SearchNearItem(item: TBinTreeItem; var p: TBinTreeItem);
    procedure SearchItem(item: TBinTreeItem; var p: TBinTreeItem);
    procedure BalanceLeft(var p: TBinTreeItem; dl: boolean);
    procedure BalanceRight(var p: TBinTreeItem; dl: boolean);
    procedure TraverseItem(Item: TBinTreeItem; TravEvt: TTraversalEvent; Order:
      TBinTraversalOrder; Level: integer);
    function GetHasItems: boolean;
    function FindNeighbour(Current, Origin: TBinTreeItem; var FoundOrigin: boolean; FindLowerNeighbour: boolean): TBinTreeItem;
    function LeftMost(Item: TBinTreeItem): TBinTreeItem;
    function RightMost(Item: TBinTreeItem): TBinTreeItem;
  public
    destructor Destroy; override;
    //Add will add an item to thre tree, and report whether the addition worked.
    //If the item is a duplicate (by key), then no addition will be performed.
    function Add(item: TBinTreeItem): boolean;
    //Searches for an item which matches the one provided by key, and
    //removes it from the tree and also deletes it - indicates whether
    //deletion worked.
    function Remove(item: TBinTreeItem): boolean;
    //Searches for items with key matching the one specified. Returns item if
    //search OK, nil otherwise.
    function Search(item: TBinTreeItem): TBinTreeItem;
    //Searches for a close (approximate) match
    function SearchNear(item: TBinTreeItem): TBinTreeItem;
    function Traverse(TravEvt: TTraversalEvent; Order: TBinTraversalOrder):
      boolean;
    function First: TBinTreeItem;
    function Last: TBinTreeItem;
    function NeighbourNode(Node: TBinTreeItem; Lower: boolean): TBinTreeItem;
    property HasItems: boolean read GetHasItems;
    property RootItem: TBinTreeItem read root;
  end;

implementation

const
  S_UNKNOWN_TREE_TRAVERSAL_ORDER =
    'Tried to traverse a binary tree, specifying an unknown order.';

(************************************
 * TBinTreeItem                     *
 ************************************)

procedure TBinTreeItem.FreeSubTree;
begin
  if Assigned(left) then left.FreeSubTree;
  if Assigned(right) then right.FreeSubTree;
  left := nil;
  right := nil;
  Free;
end;

(************************************
 * TBinTree                         *
 ************************************)

destructor TBinTree.destroy;
begin
  if Assigned(root) then root.FreeSubTree;
  inherited destroy;
end;

procedure TBinTree.SearchAndInsert(item: TBinTreeItem; var p: TBinTreeItem);
var
  comparison: integer;
begin
  if p = nil then
  begin // word not in tree, insert it
    p := item;
    h := true;
    found := false;
    with p do
    begin //these lines not strictly necessary, except vars are public.
      left := nil;
      right := nil;
      bal := 0;
    end;
  end
  else
  begin
    comparison := item.compare(p, true);
    if (comparison > 0) then // new < current
    begin
      searchAndInsert(item, p.left);
      if h and not found then BalanceLeft(p, false);
    end
    else if (comparison < 0) then // new > current
    begin
      searchAndInsert(item, p.right);
      if h and not found then balanceRight(p, false);
    end
    else
    begin
      h := false;
      found := true;
      //Don't insert the new item.
    end;
  end;
end; //searchAndInsert

// returns a pointer to the equal item if found, nil otherwise

function TBinTree.FindNeighbour(Current, Origin: TBinTreeItem; var FoundOrigin: boolean;
                                FindLowerNeighbour: boolean): TBinTreeItem;
var
  comparison: integer;
begin
  result := nil;
  if Assigned(Current) then
  begin
    comparison := Origin.Compare(current, true);
    if comparison > 0 then
    begin
      result := FindNeighbour(Current.left, Origin, FoundOrigin, FindLowerNeighbour);
      if FoundOrigin and not Assigned(result) then
      begin
        //All child nodes we have looked at are less than this, we are the next highest.
        if not FindLowerNeighbour then
          result := Current;
      end;
    end
    else if comparison < 0 then
    begin
      result := FindNeighbour(Current.right, Origin, FoundOrigin, FindLowerNeighbour);
      if FoundOrigin and not Assigned(result) then
      begin
        //All child nodes we have looked at are more than this, we are the next lowest
        if FindLowerNeighbour then
          result := Current;
      end;
    end
    else
    begin
      //Found a match.
      FoundOrigin := true;
      if FindLowerNeighbour then
        result := RightMost(Current.left)
      else
        result := LeftMost(Current.right);
    end;
  end;
end;

procedure TBinTree.SearchNearItem(item: TBinTreeItem; var p: TBinTreeItem);
var
  comparison: integer;
  lastp: TBinTreeItem;
begin
  lastp := p;
  while true do
  begin
    if p = nil then
    begin
      p := lastp; //close match.
      exit;
    end;
    comparison := item.compare(p, false);
    lastp := p;
    if comparison > 0 then
      p := p.left
    else if comparison < 0 then
      p := p.right
    else
      exit; //exact match.
  end;
end;

procedure TBinTree.SearchItem(item: TBinTreeItem; var p: TBinTreeItem);
var
  comparison: integer;
begin
  while true do
  begin
    if p = nil then exit;
    comparison := item.compare(p, false);
    if comparison > 0 then
      p := p.left
    else if comparison < 0 then
      p := p.right
    else //found a match
      exit;
  end;
end;

procedure TBinTree.balanceRight(var p: TBinTreeItem; Dl: boolean);
var p1, p2: TBinTreeItem;
begin
  case p.bal of
    - 1: begin
        p.bal := 0;
        if not dl then h := false;
      end;
    0: begin
        p.bal := + 1;
        if dl then h := false;
      end;
    + 1: begin // new balancing
        p1 := p.right;
        if (p1.bal = + 1) or ((p1.bal = 0) and dl) then
        begin // single rr rotation
          p.right := p1.left; p1.left := p;
          if not dl then p.bal := 0
          else begin
            if p1.bal = 0 then begin
              p.bal := + 1; p1.bal := -1; h := false;
            end
            else begin
              p.bal := 0; p1.bal := 0;
                              (* h:=false; *)
            end;
          end;
          p := p1;
        end
        else begin // double rl rotation
          p2 := p1.left;
          p1.left := p2.right;
          p2.right := p1;
          p.right := p2.left;
          p2.left := p;
          if p2.bal = + 1 then p.bal := -1 else p.bal := 0;
          if p2.bal = -1 then p1.bal := + 1 else p1.bal := 0;
          p := p2;
          if dl then p2.bal := 0;
        end;
        if not dl then begin
          p.bal := 0;
          h := false;
        end;
      end;
  end; // case
end;

procedure TBinTree.balanceLeft(var p: TBinTreeItem; dl:
  boolean);
var p1, p2: TBinTreeItem;
begin
  case p.bal of
    1: begin
        p.bal := 0;
        if not dl then h := false;
      end;
    0: begin
        p.bal := -1;
        if dl then h := false;
      end;
    - 1: (* if (p.Left<>nil) or not dl then *)
      begin // new balancing
        p1 := p.left;
        if (p1.bal = -1) or ((p1.bal = 0) and dl) then
        begin // single ll rotation
          p.left := p1.right; p1.right := p;
          if not dl then p.bal := 0
          else begin
            if p1.bal = 0 then begin
              p.bal := -1;
              p1.bal := + 1;
              h := false;
            end
            else begin
              p.bal := 0;
              p1.bal := 0;
                             (* h:=false; *)
            end;
          end;
          p := p1;
        end
        else
        begin //double lr rotation
          p2 := p1.right;
          P1.Right := p2.left;
          p2.left := p1;
          p.left := p2.right;
          p2.right := p;
          if p2.bal = -1 then p.bal := + 1 else p.bal := 0;
          if p2.bal = + 1 then p1.bal := -1 else p1.bal := 0;
          p := p2; if dl then p2.bal := 0;
        end;
        if not dl then begin
          p.bal := 0;
          h := false;
        end;
      end; { -1 }
  end; { case }
end;

procedure TBinTree.DelHelper(var r: TBinTreeItem; var q: TBinTreeItem);
begin //h=false
  if r.right <> nil then
  begin
    DelHelper(r.right, q);
    if h then balanceLeft(r, True);
  end
  else
  begin
    q.CopyFrom(r);
    q := r;
    r := r.left; h := true;
  end;
end;

procedure TBinTree.Delete(item: TBinTreeItem; var p: TBinTreeItem);
var
  q: TBinTreeItem; //h=false;
  comparison: integer;

begin { main of delete }
  if (p = nil) then
  begin
    found := false;
    h := false;
  end
  else
  begin
    comparison := item.compare(p, true);
    if (comparison > 0) {(x < p^.key)} then
    begin
      delete(item, p.left);
      if h then balanceRight(p, True);
    end
    else if (comparison < 0) {(x > p^.key)} then
    begin
      delete(item, p.right);
      if h then balanceLeft(p, True);
    end
    else
    begin // remove q
      found := true;
      q := p;
      if q.right = nil then
      begin
        p := q.left; h := true;
      end
      else if (q.left = nil) then
      begin
        p := q.right; h := true;
      end
      else
      begin
        DelHelper(q.left, q);
        if h then balanceRight(p, True);
      end;
      q.free; {dispose(q)};
    end;
  end;
end; { delete }

function TBinTree.Add(item: TBinTreeItem): boolean;
begin
  if not Assigned(item) then
  begin
    result := false;
    exit;
  end;
  SearchAndInsert(item, root);
  result := not found;
end;

function TBinTree.Remove(item: TBinTreeItem): Boolean;
begin
  if not Assigned(item) then
  begin
    result := false;
    exit;
  end;
  Delete(item, root);
  result := found;
end;

function TBinTree.Search(item: TBinTreeItem): TBinTreeItem;
begin
  if not Assigned(item) then
  begin
    result := nil;
    exit;
  end;
  result := root;
  SearchItem(item, result);
end;

function TBinTree.SearchNear(item: TBinTreeItem): TBinTreeItem;
begin
  if not Assigned(item) then
  begin
    result := nil;
    exit;
  end;
  result := root;
  SearchNearItem(item, result);
end;

function TBinTree.First: TBinTreeItem;
begin
  result := LeftMost(root);
end;

function TBinTree.Last: TBinTreeItem;
begin
  result := RightMost(root);
end;

function TBinTree.LeftMost(Item: TBinTreeItem): TBinTreeItem;
begin
  result := item;
  if Assigned(result) then
    while Assigned(result.left) do
      result := result.left;
end;

function TBinTree.RightMost(Item: TBinTreeItem): TBinTreeItem;
begin
  result := item;
  if Assigned(result) then
    while Assigned(result.right) do
      result := result.right;
end;

function TBinTree.NeighbourNode(Node: TBinTreeItem; Lower: boolean): TBinTreeItem;
var
  FoundOrigin: boolean;
begin
  FoundOrigin := false;
  result := FindNeighbour(root, Node, FoundOrigin, Lower);
end;

function TBinTree.Traverse(TravEvt: TTraversalEvent; Order: TBinTraversalOrder):
  boolean;
begin
  result := true;
  if not Assigned(TravEvt) then
    result := false
  else
    TraverseItem(root, TravEvt, Order, 0);
end;

procedure TBinTree.TraverseItem(Item: TBinTreeItem; TravEvt: TTraversalEvent;
  Order: TBinTraversalOrder; Level: integer);
begin
  if not Assigned(Item) then exit;
  case Order of
    btoPreOrderLoHi:
      begin
        TravEvt(self, item, level);
        TraverseItem(item.left, TravEvt, Order, Succ(Level));
        TraverseItem(item.right, TravEvt, Order, Succ(Level));
      end;
    btoInOrderLoHi:
      begin
        TraverseItem(item.left, TravEvt, Order, Succ(Level));
        TravEvt(self, item, level);
        TraverseItem(item.right, TravEvt, Order, Succ(Level));
      end;
    btoPostOrderLoHi:
      begin
        TraverseItem(item.left, TravEvt, Order, Succ(Level));
        TraverseItem(item.right, TravEvt, Order, Succ(Level));
        TravEvt(self, item, level);
      end;
    btoPreOrderHiLo:
      begin
        TravEvt(self, item, level);
        TraverseItem(item.right, TravEvt, Order, Succ(Level));
        TraverseItem(item.left, TravEvt, Order, Succ(Level));
      end;
    btoInOrderHiLo:
      begin
        TraverseItem(item.right, TravEvt, Order, Succ(Level));
        TravEvt(self, item, level);
        TraverseItem(item.left, TravEvt, Order, Succ(Level));
      end;
    btoPostOrderHiLo:
      begin
        TraverseItem(item.right, TravEvt, Order, Succ(Level));
        TraverseItem(item.left, TravEvt, Order, Succ(Level));
        TravEvt(self, item, level);
      end;
  else
    Assert(false, S_UNKNOWN_TREE_TRAVERSAL_ORDER);
  end;
end;

function TBinTree.GetHasItems: boolean;
begin
  result := Assigned(root);
end;

end.
