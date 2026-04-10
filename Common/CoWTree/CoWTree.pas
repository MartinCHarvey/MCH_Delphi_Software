unit CoWTree;

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

{
  This unit provides for a very generic copy-on-write tree.
  Handling of roots, required atomicity etc is likely to be
  very specific to the end use, so only a generic class is provided here;

  Example uses are then provided in the other files.
}

//TODO - Optional "NoCow" mode when it's allowed.

// N.B. TCowTreeItem is a child of TReffed, so using trackables
// will slow this down quite a bit (there are a lot of alloc/deallocs).

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  Reffed;

type
  TBal = -1..1;
  TCowTree = class;

  //Tree items are immutable. Once set up, should not be changed
  TCoWTreeItem = class(TReffed)
  private
    //Do not write to these directly.
    __left, __right: TCoWTreeItem;
    __bal: TBal;
{$IFOPT C+}
    _setLeft, _setRight, _setBal: boolean;
    procedure SetLeft(NewVal: TCowTreeItem);
    procedure SetRight(NewVal: TCowTreeItem);
    procedure SetBal(NewVal: TBal);
{$ENDIF}
  protected
    constructor Create; virtual;
    function DupNoInit(SrcTree: TCowTree): TCowTreeItem; virtual;
    function DupInit(SrcTree: TCowTree): TCowTreeItem;
    function Compare(Other: TCoWTreeItem;
                     AllowKeyDedupe: boolean): integer; virtual; abstract;
    //CopyFrom copies reffed children (providing the keys).
    procedure CopyFrom(Source: TCoWTreeItem); virtual; abstract;
{$IFOPT C+}
    property left: TCowTreeItem read __left write SetLeft;
    property right: TCowTreeItem read __right write SetRight;
    property bal: TBal read __bal write SetBal;
{$ELSE}
    property left: TCowTreeItem read __left write __left;
    property right: TCowTreeItem read __right write __right;
    property bal: TBal read __bal write __bal;
{$ENDIF}
  public
    destructor Destroy; override;
  end;

  //Little bit of silly debug checking for boolean flags indicating
  //node found, height rebalance needed.
  //resolved to a boolean in release (Opt C-), separate class in (Opt C+)
{$IFOPT C+}
{$IFDEF USE_TRACKABLES}
  TChkBool = class(TTrackable)
{$ELSE}
  TChkBool = class
{$ENDIF}
  private
    FVal: boolean;
    FSet: boolean;
  protected
    function Read: boolean;
    procedure Write(NewVal: boolean);
  end;
{$ELSE}
  TChkBool = boolean;
{$ENDIF}

  //All three search, two balance functions, and Delete.
  TDoubleItemVars = record
    q, r: TCowTreeItem;
  end;

  TCowTree = class(TReffed)
  private
    function DelHelper(InQR: TDoubleItemVars; var h: TChkBool; var found:TChkBool): TDoubleItemVars;
    function BalanceLeft(Inp: TCowTreeItem; dl: boolean; var h:TChkBool): TCowTreeItem;
    function BalanceRight(Inp: TCowTreeItem; dl: boolean; var h:TChkBool): TCowTreeItem;
  protected
    function Delete(item: TCoWTreeItem; Inp: TCowTreeItem;
                             var h: TChkBool; var found:TChkBool): TCowTreeItem;
    function SearchAndInsert(item: TCoWTreeItem; Inp: TCowTreeItem;
                             var h: TChkBool; var found:TChkBool): TCowTreeItem;
    function SearchNearItem(item: TCoWTreeItem; Inp: TCowTreeItem): TCowTreeItem;
    function SearchItem(item: TCoWTreeItem; Inp: TCowTreeItem): TCowTreeItem;
    function FindNeighbour(Current, Origin: TCoWTreeItem; var FoundOrigin: TChkBool; FindLowerNeighbour: boolean): TCoWTreeItem;
    function LeftMost(Item: TCoWTreeItem): TCoWTreeItem;
    function RightMost(Item: TCoWTreeItem): TCoWTreeItem;
  public

    //Oh? You were looking for some public usable methods here?
    //You'll find those in derived classes dealing with what to do with roots
    //and multiple tree copies.
  end;

  TCowTreeItemClass = class of TCowTreeItem;

  function RdCk(B: TChkBool): boolean;
{$IFOPT C-}
    inline;
{$ENDIF}
  procedure WrCk(var B: TChkBool; NewVal:Boolean);
{$IFOPT C-}
    inline;
{$ENDIF}

implementation

uses LockAbstractions;

{ Misc functions }

function RdCk(B: TChkBool): boolean;
begin
{$IFOPT C+}
  result := B.Read;
{$ELSE}
  result := B;
{$ENDIF}
end;

procedure WrCk(var B: TChkBool; NewVal:Boolean);
begin
{$IFOPT C+}
  B.Write(NewVal);
{$ELSE}
  B := NewVal;
{$ENDIF}
end;

{$IFOPT C+}
{ TChkBool }

function TChkBool.Read: boolean;
begin
  Assert(FSet);
  result := FVal;
end;

procedure TChkBool.Write(NewVal: Boolean);
begin
  FSet := true;
  FVal := NewVal;
end;
{$ENDIF}

{ TCowTreeItem }

{$IFOPT C+}
procedure TCowTreeItem.SetLeft(NewVal: TCowTreeItem);
begin
  Assert(not _setLeft);
  _setLeft := true;
  __left := NewVal;
end;

procedure TCowTreeItem.SetRight(NewVal: TCowTreeItem);
begin
  Assert(not _setRight);
  _setRight := true;
  __right := NewVal;
end;

procedure TCowTreeItem.SetBal(NewVal: TBal);
begin
  Assert(not _setBal);
  _setBal := true;
  __bal := NewVal;
end;
{$ENDIF}

destructor TCowTreeItem.Destroy;
begin
  left.Release;
  right.Release;
  inherited;
end;

constructor TCowTreeItem.Create;
begin
  inherited;
end;

function TCowTreeItem.DupNoInit(SrcTree: TCowTree): TCowTreeItem;
begin
  result := TCowTreeItemClass(self.ClassType).Create;
end;

function TCowTreeItem.DupInit(SrcTree: TCowTree): TCowTreeItem;
begin
  result := DupNoInit(SrcTree);
  result.CopyFrom(self);
end;

{ TCowTree }

// returns a pointer to the equal item if found, nil otherwise
//No tree mods.
function TCowTree.FindNeighbour(Current, Origin: TCowTreeItem; var FoundOrigin: TChkBool;
                                FindLowerNeighbour: boolean): TCowTreeItem;
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
      if RdCk(FoundOrigin) and not Assigned(result) then
      begin
        //All child nodes we have looked at are less than this, we are the next highest.
        if not FindLowerNeighbour then
          result := Current;
      end;
    end
    else if comparison < 0 then
    begin
      result := FindNeighbour(Current.right, Origin, FoundOrigin, FindLowerNeighbour);
      if RdCk(FoundOrigin) and not Assigned(result) then
      begin
        //All child nodes we have looked at are more than this, we are the next lowest
        if FindLowerNeighbour then
          result := Current;
      end;
    end
    else
    begin
      //Found a match.
      WrCk(FoundOrigin, true);
      if FindLowerNeighbour then
        result := RightMost(Current.left)
      else
        result := LeftMost(Current.right);
    end;
  end;
end;

//Item is searching from (initially root), p is result.
//No tree mods.
function TCowTree.SearchNearItem(item: TCoWTreeItem; Inp: TCowTreeItem): TCowTreeItem;
//procedure TCowTree.SearchNearItem(item: TCoWTreeItem; var p: TCowTreeItem);
var
  comparison: integer;
  lastp: TCowTreeItem;
begin
  lastp := InP;
  while true do
  begin
    if InP = nil then
    begin
      result := lastp; //close match.
      exit;
    end;
    comparison := item.compare(InP, false);
    lastp := InP;
    if comparison > 0 then
      InP := InP.left
    else if comparison < 0 then
      InP := InP.right
    else
    begin
      result := InP;
      exit; //exact match.
    end;
  end;
end;

//Item is searching from (initially root), p is result.
//No tree mods.
function TCowTree.SearchItem(item: TCoWTreeItem; Inp: TCowTreeItem): TCowTreeItem;
//procedure TCowTree.SearchItem(item: TCowTreeItem; var p: TCowTreeItem);
var
  comparison: integer;
begin
  while true do
  begin
    if not Assigned(InP) then
    begin
      result := nil;
      exit;
    end;
    comparison := item.compare(InP, false);
    if comparison > 0 then
      InP := InP.left
    else if comparison < 0 then
      InP := InP.right
    else //found a match
    begin
      result := InP;
      exit;
    end;
  end;
end;

//Slight change here. On failure, return NIL, rather than
//unchanged root. It makes exception handling easier.
function TCowTree.SearchAndInsert(item: TCoWTreeItem; Inp: TCowTreeItem;
                         var h: TChkBool; var found:TChkBool): TCowTreeItem;
var
  comparison: integer;
  Retp, CallP: TCowTreeItem;
  NewNode, Tmp: TCowTreeItem;
begin
  NewNode := nil;
  Tmp := nil;
  RetP := nil;

  try
    if not Assigned(InP) then
    begin
      // insert new node (ownership of "item" transferred here)
      NewNode := item;
      NewNode.left := nil;
      NewNode.right := nil;
      NewNode.bal := 0;

      WrCk(h, true);
      WrCk(found, false);

      result := NewNode;
      NewNode := nil; // transfer ownership
      Exit;
    end;

    comparison := item.compare(InP, true);

    if comparison > 0 then
    begin
      // go left
      CallP := InP.left;
      Retp := SearchAndInsert(item, CallP, h, found);
      if Assigned(RetP) then //subtree ptr changed, but we know what it was before.
      begin
        Assert(RetP <> CallP);
        Tmp := InP.DupInit(self);
        // LEFT comes from recursion → TRANSFER (NO AddRef)
        Tmp.left := RetP;
        RetP := nil;
        // RIGHT comes from existing tree → SHARE (AddRef)
{$IFOPT C+}
        Tmp.right := InP.right.AddRef as TCowTreeItem;
{$ELSE}
        Tmp.right := TCowTreeItem(InP.right.AddRef);
{$ENDIF}
        Tmp.bal := InP.bal;
      end;

      if RdCk(h) and not RdCk(found) then
      begin
        Assert(Assigned(Tmp));
        CallP := Tmp;
        Retp := BalanceLeft(CallP, false, h);
        if RetP <> CallP then
        begin
          Tmp.Release;  //Release newly created temp.
          Tmp := RetP;
          RetP := nil;
        end;
      end;
    end
    else if comparison < 0 then
    begin
      // go right
      CallP := InP.right;
      Retp := SearchAndInsert(item, CallP, h, found);
      if Assigned(RetP) then  //subtree ptr changed, but we know what it was before.
      begin
        Assert(RetP <> CallP);
        Tmp := InP.DupInit(self);
        // LEFT comes from existing tree → SHARE (AddRef)
{$IFOPT C+}
        Tmp.left := InP.left.AddRef as TCowTreeItem;
{$ELSE}
        Tmp.left := TCowTreeItem(InP.left.AddRef);
{$ENDIF}
        // RIGHT comes from recursion → TRANSFER (NO AddRef)
        Tmp.right := RetP;
        RetP := nil;
        Tmp.bal := InP.bal;
      end;

      if RdCk(h) and not RdCk(found) then
      begin
        Assert(Assigned(Tmp));
        CallP := Tmp;
        Retp := BalanceRight(CallP, false, h);
        if RetP <> CallP then
        begin
          Tmp.Release;  //Release newly created temp.
          Tmp := RetP;
          RetP := nil;
        end;
      end;
    end
    else
    begin
      // duplicate
      WrCk(h, false);
      WrCk(found, true);
      //Tmp still nil, RetP nil.
    end;

    Result := Tmp;
    Tmp := nil;

  finally
    //These releases will normally do nothing
    NewNode.Release;
    Tmp.Release;
    //And now can disambiguate between exception and insert fail.
    RetP.Release;
  end;
end;


function TCowTree.BalanceRight(InP: TCowTreeItem;
                              dl: boolean; var h:TChkBool): TCowTreeItem;
var
  p, p1, p2: TCowTreeItem;
  NewRoot, NewP, NewP1, NewP2: TCowTreeItem;
begin
  NewRoot := nil;
  NewP := nil;
  NewP1 := nil;
  NewP2 := nil;

  p := InP;
  try
    case p.bal of
      -1:
        begin
          NewRoot := p.DupInit(self);
{$IFOPT C+}
          NewRoot.left := p.left.AddRef as TCowTreeItem;
          NewRoot.right := p.right.AddRef as TCowTreeItem;
{$ELSE}
          NewRoot.left := TCowTreeItem(p.left.AddRef);
          NewRoot.right := TCowTreeItem(p.right.AddRef);
{$ENDIF}
          NewRoot.bal := 0;
          if not dl then WrCk(h, false);
        end;

      0:
        begin
          NewRoot := p.DupInit(self);
{$IFOPT C+}
          NewRoot.left := p.left.AddRef as TCowTreeItem;
          NewRoot.right := p.right.AddRef as TCowTreeItem;
{$ELSE}
          NewRoot.left := TCowTreeItem(p.left.AddRef);
          NewRoot.right := TCowTreeItem(p.right.AddRef);
{$ENDIF}
          NewRoot.bal := +1;
          if dl then WrCk(h, false);
        end;

      +1:
        begin
          p1 := p.right;
          if (p1.bal = +1) or ((p1.bal = 0) and dl) then
          begin
            // ===== SINGLE RR ROTATION =====
            // NewP (old root becomes left child)
            NewP := p.DupInit(self);
{$IFOPT C+}
            NewP.left := p.left.AddRef as TCowTreeItem;
            NewP.right := p1.left.AddRef as TCowTreeItem;
{$ELSE}
            NewP.left := TCowTreeItem(p.left.AddRef);
            NewP.right := TCowTreeItem(p1.left.AddRef);
{$ENDIF}
            // NewRoot (p1 becomes new root)
            NewRoot := p1.DupInit(self);
            NewRoot.left := NewP;   // transfer ownership
            NewP := nil;
{$IFOPT C+}
            NewRoot.right := p1.right.AddRef as TCowTreeItem;
{$ELSE}
            NewRoot.right := TCowTreeItem(p1.right.AddRef);
{$ENDIF}
            if not dl then
            begin
              NewRoot.left.bal := 0;
              NewRoot.bal := 0;
            end
            else
            begin
              if p1.bal = 0 then
              begin
                NewRoot.left.bal := +1;
                NewRoot.bal := -1;
                WrCk(h, false);
              end
              else
              begin
                NewRoot.left.bal := 0;
                NewRoot.bal := 0;
              end;
            end;
          end
          else
          begin
            // ===== DOUBLE RL ROTATION =====
            p2 := p1.left;
            // NewP (left subtree)
            NewP := p.DupInit(self);
{$IFOPT C+}
            NewP.left := p.left.AddRef as TCowTreeItem;
            NewP.right := p2.left.AddRef as TCowTreeItem;
{$ELSE}
            NewP.left := TCowTreeItem(p.left.AddRef);
            NewP.right := TCowTreeItem(p2.left.AddRef);
{$ENDIF}
            // NewP1 (right subtree)
            NewP1 := p1.DupInit(self);
{$IFOPT C+}
            NewP1.left := p2.right.AddRef as TCowTreeItem;
            NewP1.right := p1.right.AddRef as TCowTreeItem;
{$ELSE}
            NewP1.left := TCowTreeItem(p2.right.AddRef);
            NewP1.right := TCowTreeItem(p1.right.AddRef);
{$ENDIF}
            // NewRoot (p2 becomes root)
            NewRoot := p2.DupInit(self);
            NewRoot.left := NewP;   // transfer
            NewRoot.right := NewP1; // transfer
            NewP := nil;
            NewP1 := nil;
            if p2.bal = +1 then
              NewRoot.left.bal := -1
            else
              NewRoot.left.bal := 0;
            if p2.bal = -1 then
              NewRoot.right.bal := +1
            else
              NewRoot.right.bal := 0;
            if dl then
              NewRoot.bal := 0
            else
              NewRoot.bal := p2.bal;
          end;
          if not dl then
          begin
            // final rebalance overwrite
            NewP2 := NewRoot.DupInit(self);
{$IFOPT C+}
            NewP2.left := NewRoot.left.AddRef as TCowTreeItem;
            NewP2.right := NewRoot.right.AddRef as TCowTreeItem;
{$ELSE}
            NewP2.left := TCowTreeItem(NewRoot.left.AddRef);
            NewP2.right := TCowTreeItem(NewRoot.right.AddRef);
{$ENDIF}
            NewP2.bal := 0;
            NewRoot.Release; // discard old root
            NewRoot := NewP2;
            NewP2 := nil;
            WrCk(h, false);
          end;
        end;
    end;

    Result := NewRoot;
    NewRoot := nil; // transfer ownership
  finally
    //These releases will normally do nothing
    NewRoot.Release;
    NewP.Release;
    NewP1.Release;
    NewP2.Release;
  end;
end;

function TCowTree.BalanceLeft(InP: TCowTreeItem;
                             dl: boolean; var h:TChkBool): TCowTreeItem;
var
  p, p1, p2: TCowTreeItem;
  NewRoot, NewP, NewP1, NewP2: TCowTreeItem;
begin
  NewRoot := nil;
  NewP := nil;
  NewP1 := nil;
  NewP2 := nil;

  p := InP;
  try
    case p.bal of
      +1:
        begin
          NewRoot := p.DupInit(self);
{$IFOPT C+}
          NewRoot.left := p.left.AddRef as TCowTreeItem;
          NewRoot.right := p.right.AddRef as TCowTreeItem;
{$ELSE}
          NewRoot.left := TCowTreeItem(p.left.AddRef);
          NewRoot.right := TCowTreeItem(p.right.AddRef);
{$ENDIF}
          NewRoot.bal := 0;
          if not dl then WrCk(h, false);
        end;

      0:
        begin
          NewRoot := p.DupInit(self);
{$IFOPT C+}
          NewRoot.left := p.left.AddRef as TCowTreeItem;
          NewRoot.right := p.right.AddRef as TCowTreeItem;
{$ELSE}
          NewRoot.left := TCowTreeItem(p.left.AddRef);
          NewRoot.right := TCowTreeItem(p.right.AddRef);
{$ENDIF}
          NewRoot.bal := -1;
          if dl then WrCk(h, false);
        end;

      -1:
        begin
          p1 := p.left;
          if (p1.bal = -1) or ((p1.bal = 0) and dl) then
          begin
            // ===== SINGLE LL ROTATION =====
            // NewP (old root becomes right child)
            NewP := p.DupInit(self);
{$IFOPT C+}
            NewP.left := p1.right.AddRef as TCowTreeItem;
            NewP.right := p.right.AddRef as TCowTreeItem;
{$ELSE}
            NewP.left := TCowTreeItem(p1.right.AddRef);
            NewP.right := TCowTreeItem(p.right.AddRef);
{$ENDIF}
            // NewRoot (p1 becomes new root)
            NewRoot := p1.DupInit(self);
{$IFOPT C+}
            NewRoot.left := p1.left.AddRef as TCowTreeItem;
{$ELSE}
            NewRoot.left := TCowTreeItem(p1.left.AddRef);
{$ENDIF}
            NewRoot.right := NewP;
            NewP := nil;

            if not dl then
            begin
              NewRoot.right.bal := 0;
              NewRoot.bal := 0;
            end
            else
            begin
              if p1.bal = 0 then
              begin
                NewRoot.right.bal := -1;
                NewRoot.bal := +1;
                WrCk(h, false);
              end
              else
              begin
                NewRoot.right.bal := 0;
                NewRoot.bal := 0;
              end;
            end;
          end
          else
          begin
            // ===== DOUBLE LR ROTATION =====

            p2 := p1.right;
            // NewP (right subtree)
            NewP := p.DupInit(self);
{$IFOPT C+}
            NewP.left := p2.right.AddRef as TCowTreeItem;
            NewP.right := p.right.AddRef as TCowTreeItem;
{$ELSE}
            NewP.left := TCowTreeItem(p2.right.AddRef);
            NewP.right := TCowTreeItem(p.right.AddRef);
{$ENDIF}
            // NewP1 (left subtree)
            NewP1 := p1.DupInit(self);
{$IFOPT C+}
            NewP1.left := p1.left.AddRef as TCowTreeItem;
            NewP1.right := p2.left.AddRef as TCowTreeItem;
{$ELSE}
            NewP1.left := TCowTreeItem(p1.left.AddRef);
            NewP1.right := TCowTreeItem(p2.left.AddRef);
{$ENDIF}
            // NewRoot (p2 becomes root)
            NewRoot := p2.DupInit(self);
            NewRoot.left := NewP1;
            NewRoot.right := NewP;
            NewP := nil;
            NewP1 := nil;

            if p2.bal = -1 then
              NewRoot.right.bal := +1
            else
              NewRoot.right.bal := 0;

            if p2.bal = +1 then
              NewRoot.left.bal := -1
            else
              NewRoot.left.bal := 0;

            if dl then
              NewRoot.bal := 0
            else
              NewRoot.bal := p2.bal;
          end;

          if not dl then
          begin
            NewP2 := NewRoot.DupInit(self);
{$IFOPT C+}
            NewP2.left := NewRoot.left.AddRef as TCowTreeItem;
            NewP2.right := NewRoot.right.AddRef as TCowTreeItem;
{$ELSE}
            NewP2.left := TCowTreeItem(NewRoot.left.AddRef);
            NewP2.right := TCowTreeItem(NewRoot.right.AddRef);
{$ENDIF}
            NewP2.bal := 0;

            NewRoot.Release;
            NewRoot := NewP2;
            NewP2 := nil;

            WrCk(h, false);
          end;
        end;
    end;

    Result := NewRoot;
    NewRoot := nil;
  finally
    //These releases will normally do nothing
    NewRoot.Release;
    NewP.Release;
    NewP1.Release;
    NewP2.Release;
  end;
end;

//When inserting, we always had initial ref of 1, and transferred it up.
//However, when deleting, if we don't create a temp, but use an existing node
//in the tree, then we need to AddRef.
function TCowTree.DelHelper(InQR: TDoubleItemVars;
                           var h: TChkBool;
                           var found: TChkBool): TDoubleItemVars;
var
  CallQR, RetQR: TDoubleItemVars;
  CallP, RetP: TCowTreeItem;
  Tmp: TCowTreeItem;
begin
  Tmp := nil;
  try
    if Assigned(InQR.r.right) then
    begin
      {
        DelHelper(r.right, q);
        if h then balanceLeft(r, True);
      }
      CallQR.r := InQR.r.right;
      CallQR.q := InQR.q;

      RetQR := DelHelper(CallQR, h, found);
      //TODO Always modifies return? could optimise this out.
      if RetQR.r <> CallQR.r then
      begin
        Tmp := InQR.r.DupInit(self);
{$IFOPT C+}
        Tmp.left := InQR.r.left.AddRef as TCoWTreeItem;
{$ELSE}
        Tmp.left := TCowTreeItem(InQR.r.left.AddRef);
{$ENDIF}
        Tmp.bal := InQR.r.bal;
        Tmp.right := RetQR.r; //Transfer back up from recursion.
        RetQR.r := Tmp;
        Tmp := nil;
      end;

      if RdCk(h) then
      begin
        CallP := RetQR.r;
        RetP := balanceLeft(CallP, True, h);
        if RetP <> CallP then
        begin
          //InQR.r changed to new value, was tmp.
          RetQR.r.Release; //Release old tmp.
          RetQR.r := RetP;
        end;
      end;
      Result := RetQR;
      RetQR.q := nil;
      RetQR.r := nil;
    end
    else
    begin
      {
        q.CopyFrom(r);
        q := r;
        r := r.left; h := true;
      }
      //Original code passed q back as the node to delete. (q = r).
      //We pass back a modified q with new key (obtained from rightmost(q.left))
      //r is still the primary "modified subtree" returned back up.

      Tmp := InQR.q.DupNoInit(self);
      Tmp.CopyFrom(InQR.r); //Copy the key across from other node.
      Tmp.bal := InQR.q.bal; //Copy the balance across from original node, but not the children.
      RetQR.q := Tmp; //Return this modified Q to caller.
      Tmp := nil;

      RetQR.r := InQR.r.left; //Return modified R to caller.
      RetQR.r.AddRef; //And ref it, as it's part of an existing tree.
      WrCk(h, true);
      result := RetQR;
      RetQR.q := nil;
      RetQR.r := nil;
    end;
  finally
    //These releases will normally do nothing
    Tmp.Release;
    RetQR.q.Release; //Release newly created node on exception.
    RetQR.r.Release; //Release newly modified subtree on exception.
  end;
end;

//When inserting, we always had initial ref of 1, and transferred it up.
//However, when deleting, if we don't create a temp, but use an existing node
//in the tree, then we need to AddRef.

function TCowTree.Delete(item: TCoWTreeItem;
                        Inp: TCowTreeItem;
                        var h: TChkBool;
                        var found: TChkBool): TCowTreeItem;
var
  comparison: integer;
  q: TCowTreeItem;
  CallQR, RetQR: TDoubleItemVars;
  CallP, RetP: TCowTreeItem;
  Tmp: TCowTreeItem;
begin
  Tmp := nil;
  RetQR.q := nil;
  RetQR.r := nil;

  try
    if InP = nil then
    begin
      WrCk(found, false);
      WrCk(h, false);
      Result := Inp;
      Exit;
    end;
    comparison := item.compare(InP, true);
    if comparison > 0 then
    begin
      {
        delete(item, p.left);
        if h then balanceRight(p, True);
      }
      CallP := InP.left;
      RetP := Delete(item, CallP, h, found);
      if RetP <> CallP then
      begin
        Tmp := InP.DupInit(self);
        Tmp.left := RetP; //Transfer from called.
{$IFOPT C+}
        Tmp.right := InP.right.AddRef as TCowTreeItem;
{$ELSE}
        Tmp.right := TCowTreeItem(InP.right.AddRef);
{$ENDIF}
        Tmp.bal := InP.bal;
      end
      else
      begin
        Assert(not RdCk(h));
        Assert(not RdCk(found));
        Tmp := Inp;
      end;

      if RdCk(h) then
      begin
        CallP := Tmp;
        RetP := balanceRight(CallP, True, h);
        if RetP <> CallP then
        begin
          Tmp := RetP; //Rebalance has done all the copying for us.
          CallP.Release; //Release the tmp generated above.
        end;
      end;
    end
    else if comparison < 0 then
    begin
      {
        delete(item, p.right);
        if h then balanceLeft(p, True);
      }
      CallP := InP.right;
      RetP := Delete(item, CallP, h, found);
      if RetP <> CallP then
      begin
        Tmp := InP.DupInit(self);
{$IFOPT C+}
        Tmp.left := InP.left.AddRef as TCowTreeItem;
{$ELSE}
        Tmp.left := TCowTreeItem(InP.left.AddRef);
{$ENDIF}
        Tmp.right := RetP; //Transfer from called.
        Tmp.bal := InP.bal;
      end
      else
      begin
        Assert(not RdCk(h));
        Assert(not RdCk(found));
        Tmp := Inp;
      end;

      if RdCk(h) then
      begin
        CallP := Tmp;
        RetP := balanceLeft(CallP, True, h);
        if RetP <> CallP then
        begin
          Tmp := RetP; //Rebalance has done all the copying for us.
          CallP.Release;
        end;
      end;
    end
    else
    begin
      //Found node.
      WrCk(found, true);
      q := InP;
      if q.right = nil then
      begin
        { p := q.left; h := true; }
        //Skip over this node, return child to caller.
        Tmp := q.left;
        Tmp.AddRef;
        WrCk(h, true);
      end
      else if q.left = nil then
      begin
        { p := q.right; h := true; }
        //Skip over this node, return child to caller.
        Tmp := q.right;
        Tmp.AddRef;
        WrCk(h, true);
      end
      else
      begin
      {
          //previously q = p ...
          DelHelper(q.left, q);
          if h then balanceRight(p, True);
      } //Nothing here changes return value.

        CallQR.r := q.left;
        CallQR.q := q;
        RetQR := DelHelper(CallQR, h, found);

        //OK. RetQR now has modified both Q and R values.
        //The q is a node with the modified key,
        //and the r is the modified left subchild.
        Assert(RetQR.q <> CallQr.q);
        Assert(RetQR.r <> CallQr.r);
        //OK, modified Q we can now re-use as Tmp.
        //Subject to same rules as other temps w.r.t exceptions.
        Tmp := RetQR.q;
        RetQR.q := nil;
        //Key and balance initialised, subtrees not yet set.
        Tmp.left := RetQR.r; //Transfer ownership up.
        RetQR.r := nil;
        //Get the right pointer from the original Q.
{$IFOPT C+}
        Tmp.right := CallQR.q.right.AddRef as TCowTreeItem;
{$ELSE}
        Tmp.right := TCowTreeItem(CallQR.q.right.AddRef);
{$ENDIF}
        if RdCk(h) then
        begin
          CallP := Tmp;
          //Del helper modified q.left (also p.left)
          //InP set to its replacement, which is guaranteed a new node.
          RetP := balanceRight(CallP, True, h);
          if RetP <> CallP then
          begin
            //Rebalance has modified it again, and its guaranteed a temp creation.
            Tmp.Release;
            Tmp := RetP;
          end;
        end;
      end;
    end;
    Result := Tmp;
    Tmp := nil;
  finally
    Tmp.Release;
    RetQR.q.Release;
    RetQR.r.Release;
  end;
end;

function TCowTree.LeftMost(Item: TCowTreeItem): TCowTreeItem;
begin
  result := item;
  if Assigned(result) then
    while Assigned(result.left) do
      result := result.left;
end;

function TCowTree.RightMost(Item: TCowTreeItem): TCowTreeItem;
begin
  result := item;
  if Assigned(result) then
    while Assigned(result.right) do
      result := result.right;
end;

end.
