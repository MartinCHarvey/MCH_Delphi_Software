unit CommonNodes;
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

uses Trackables, DLList, SysUtils, Classes, CoCoBase;

const
  NodeTypeInvalid = 0;

  // TODO - In addition to common or garden "IterateNodes"
  // we need a more specific "find helper" class to maintain a stack
  // and keep track of search subsets.
  // Need a generic one for common nodes,
  // and a specific one for HTML, JS etc nodes.
  //
  // Start out by doing tokenization of HTML value pairs.
  //

type
  TCommonNode = class;
  TCNodeDecisionFunc = function(Node: TCommonNode;
    Ref1, Ref2, Ref3: TObject): boolean;
  TCNodeExecFunc = procedure(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
  TCNodeRecurseFunc = function(Node: TCommonNode;
    Ref1, Ref2, Ref3: TObject): boolean;

{$IFDEF USE_TRACKABLES}

  TNodeSearchOpts = class(TTrackable)
{$ELSE}
  TNodeSearchOpts = class
{$ENDIF}
  end;

  TCommonNode = class(TTrackable)
  protected
    FContainerNode: TCommonNode;
    // Roughly where in source code this node is.
    FContainedListHead: TDLEntry;
    FSiblingListEntry: TDLEntry;

    FCol, FLine: int64;

    FObj1, FObj2: TObject;
    FNodeType: byte;
    FStringsFixed: boolean;
  protected
    // A little hacky, but almost all parser code is AnsiStr and AnsiChar,
    // so we need to un-ansify unicode strings.
    // Additionally, later need to apply escape/unescape rules for langs
    // allowing unicode or control character escapes.
    procedure FixStrings; virtual;
  public
    // These functions to only consider parsed information, not temporary
    // linking or fixup information (Obj1, Obj2, StringsFixed) which
    // are mostly for GUI linking.

    // Same as other node with respect to local data.
    // Call interhited first to eliminnate gross node class or type
    // mismatches.
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; virtual;
    // I am recursively a subset of other. Not necessary to override
    // unless have children not accounted for by list entries.
    function RecSubset(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; virtual;

    // Trees are recursively the same - TODO. This does not consider
    // ordering, need a better recursion algorithm for true equality.
    function RecEqual(Other: TCommonNode; Opts: TNodeSearchOpts): boolean;

    // String representation of node only.
    function AsText: string; virtual;

    constructor Create;
    constructor CreateWithTracker(Tracker: TTracker);
    destructor Destroy; override;
    procedure FixupChildContainedPtrs;
    procedure FixupChildContainedPtrsRec;

    procedure IterateNodes(Decision: TCNodeDecisionFunc; Exec: TCNodeExecFunc;
      Recurse: TCNodeRecurseFunc; Ref1, Ref2, Ref3: TObject;
      ExecBeforeRecurse: boolean = true; OmitSiblings: boolean = false);

    property Col: int64 read FCol write FCol;
    property Line: int64 read FLine write FLine;

    property ContainedListHead: TDLEntry read FContainedListHead;
    property SiblingListEntry: TDLEntry read FSiblingListEntry;
    property ContainerNode: TCommonNode read FContainerNode
      write FContainerNode;

    // Arbitrary TObject properties for linking.
    property Obj1: TObject read FObj1 write FObj1;
    property Obj2: TObject read FObj2 write FObj2;

    // Node type used for gross disambiguation during iteration.
    property NodeType: byte read FNodeType;
  end;

  EParseAbort = class(Exception)
  private
    FLine, FColumn: integer;
  public
    property Line: integer read FLine write FLine;
    property Column: integer read FColumn write FColumn;
    constructor Create(Msg: string; Ln, Col: integer);
  end;

{$IFDEF USE_TRACKABLES}

  TNodeSearchBailout = class(TTrackable)
{$ELSE}
  TNodeSearchBailout = class
{$ENDIF}
  private
    FFoundNode: TCommonNode;
    function GetFound: boolean;
  public
    procedure Reset;
    property AlreadyFound: boolean read GetFound;
    property FoundNode: TCommonNode read FFoundNode write FFoundNode;
  end;

  TNavRecurse = (nvhAllChildren, nvhImmedChildren, nvhLaterSiblings);

  TNodeNavHelperBailout = class(TNodeSearchBailout)
  private
    FRootNode: TCommonNode;
    FRecurse: TNavRecurse;
  public
    property RootNode: TCommonNode read FRootNode write FRootNode;
    property Recurse: TNavRecurse read FRecurse write FRecurse;
  end;

{$IFDEF USE_TRACKABLES}

  TNodeNavHelper = class(TTrackable)
{$ELSE}
  TNodeNavHelper = class
{$ENDIF}
  private
    FStack: TList;
  protected
    // Recursive = recurse beyond immediate children.
    // Children = find children not siblings.
    function FindCommon(var Node: TCommonNode; Subset: TCommonNode;
      Opts: TNodeSearchOpts; Recurse: TNavRecurse): boolean;
    function GetStackTop: TCommonNode;

    function FindChildN(var Node: TCommonNode; Subset: TCommonNode;
      Opts: TNodeSearchOpts): boolean;
    function FindNextN(var Node: TCommonNode; Subset: TCommonNode;
      Opts: TNodeSearchOpts): boolean;
    function FindChildRecN(var Node: TCommonNode; Subset: TCommonNode;
      Opts: TNodeSearchOpts): boolean;
    function PushChildN(var Node: TCommonNode; Subset: TCommonNode;
      Opts: TNodeSearchOpts): boolean;
    function PushChildRecN(var Node: TCommonNode; Subset: TCommonNode;
      Opts: TNodeSearchOpts): boolean;
  public
    procedure PopN(var Node: TCommonNode);
    procedure StackEmpty(ExpectEmpty: boolean);
    procedure Push(Node: TCommonNode);
    constructor Create;
    destructor Destroy; override;
  end;

  TStringNavHelper = class(TNodeNavHelper)
  private
    FSearchDict: TStringList;
    FOpts: TNodeSearchOpts;
    FLastSearch: string;
  protected
    function GetTreeFragmentForString(Fragment: string): TCommonNode; virtual;
    function CreateParser: TCoCoRGrammar; virtual; abstract;
    function CreateOpts: TNodeSearchOpts; virtual; abstract;
    function MakeSearchStringAnsi(Search: string): AnsiString; virtual;
    procedure PostParse(var ParseResult: TCommonNode); virtual;
  public
    constructor Create;
    destructor Destroy; override;
    function FindChildS(var Node: TCommonNode; Fragment: string): boolean;
    function FindNextS(var Node: TCommonNode; Fragment: string): boolean;
    function FindChildRecS(var Node: TCommonNode; Fragment: string): boolean;
    function PushChildS(var Node: TCommonNode; Fragment: string): boolean;
    function PushChildRecS(var Node: TCommonNode; Fragment: string): boolean;
    property Opts: TNodeSearchOpts read FOpts;
    property LastSearch: string read FLastSearch;
  end;

  // Node recursion functions for string fixing.
function ExecuteAlways(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
function ExecuteNotSelf(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
procedure FixNodeStrings(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
function RecurseAlways(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;

// Node recursion functions for partial tree matching and searching.
// Bailout is ref1, fragment is ref2, opts is ref 3.
function TreeMatchDecision(Node: TCommonNode;
  Ref1, Ref2, Ref3: TObject): boolean;
procedure TreeMatchExecute(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
function TreeMatchRecurse(Node: TCommonNode; Ref1, Ref2, Ref3: TObject)
  : boolean;

// Node recursion functions for navigation helper.
function NavHelperDecision(Node: TCommonNode;
  Ref1, Ref2, Ref3: TObject): boolean;
procedure NavHelperExecute(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
function NavHelperRecurse(Node: TCommonNode; Ref1, Ref2, Ref3: TObject)
  : boolean;

implementation

{ EParseAbort }

uses
  HTMLEscapeHelper
{$IFDEF DEBUG_SEARCH}
    , GlobalLog
{$ENDIF}
    ;

constructor EParseAbort.Create(Msg: string; Ln, Col: integer);
begin
  inherited Create(Msg);
  Line := Ln;
  Column := Col;
end;

{ TCommonNode }

function TCommonNode.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := (Self.NodeType = Other.NodeType) and
    (Self.ClassType = Other.ClassType);
end;

function TCommonNode.RecSubset(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
var
  AccountedFor: TList;
  OwnChild, OtherChild: TCommonNode;
  OtherChildAccountedFor: boolean;
  FoundOwnChildSubset: boolean;
  i: integer;
begin
{$IFDEF DEBUG_SEARCH}
  GLogLog(SV_TRACE, 'Rec subset: ' + Self.AsText + ' ' + Other.AsText);
{$ENDIF}
  result := LclEqual(Other, Opts);
  if result then
  begin
    AccountedFor := TList.Create;
    try
      OwnChild := Self.ContainedListHead.FLink.Owner as TCommonNode;
      while Assigned(OwnChild) do
      begin
        FoundOwnChildSubset := false;

        OtherChild := Other.ContainedListHead.FLink.Owner as TCommonNode;
        while Assigned(OtherChild) and not FoundOwnChildSubset do
        begin
          OtherChildAccountedFor := false;
          for i := 0 to Pred(AccountedFor.Count) do
          begin
            if AccountedFor.Items[i] = OtherChild then
            begin
              OtherChildAccountedFor := true;
              break;
            end;
          end;
          if not OtherChildAccountedFor then
          begin
            if OwnChild.RecSubset(OtherChild, Opts) then
            begin
              AccountedFor.Add(OtherChild);
              FoundOwnChildSubset := true;
            end;
          end;
          OtherChild := OtherChild.SiblingListEntry.FLink.Owner as TCommonNode;
        end;
        if not FoundOwnChildSubset then
        begin
{$IFDEF DEBUG_SEARCH}
          GLogLog(SV_TRACE, 'Rec subset - FAIL, child not subset ' + Self.AsText
            + ' ' + Other.AsText);
{$ENDIF}
          result := false;
          break;
        end;
        OwnChild := OwnChild.SiblingListEntry.FLink.Owner as TCommonNode;
      end;
    finally
      AccountedFor.Free;
    end;
  end
  else
  begin
{$IFDEF DEBUG_SEARCH}
    GLogLog(SV_TRACE, 'Rec subset - FAIL, not local equal ' + Self.AsText + ' '
      + Other.AsText);
{$ENDIF}
  end;
{$IFDEF DEBUG_SEARCH}
  if result then
  begin
    GLogLog(SV_TRACE, 'Rec subset - PASS ' + Self.AsText + ' ' + Other.AsText);
  end;
{$ENDIF}
end;

// Trees are recursively the same.
function TCommonNode.RecEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := Self.RecSubset(Other, Opts) and Other.RecSubset(Self, Opts);
end;

constructor TCommonNode.Create;
begin
  inherited;
  DLItemInitList(@FContainedListHead);
  DLItemInitObj(Self, @FSiblingListEntry);
end;

constructor TCommonNode.CreateWithTracker(Tracker: TTracker);
begin
  inherited;
  DLItemInitList(@FContainedListHead);
  DLItemInitObj(Self, @FSiblingListEntry);
end;

destructor TCommonNode.Destroy;
var
  C, N: TCommonNode;
begin
  C := ContainedListHead.FLink.Owner as TCommonNode;
  while Assigned(C) do
  begin
    N := C.SiblingListEntry.FLink.Owner as TCommonNode;
    DLListRemoveObj(@C.SiblingListEntry);
    C.Free;
    C := N;
  end;
  Assert(DlItemIsEmpty(@ContainedListHead));
  inherited;
end;

procedure TCommonNode.FixupChildContainedPtrs;
var
  N: TCommonNode;
{$IFOPT C+}
  OwnClassName: string;
  ChildClassName: string;
{$ENDIF}
begin
{$IFOPT C+}
  OwnClassName := Self.ClassName;
{$ENDIF}
  N := ContainedListHead.FLink.Owner as TCommonNode;
  while Assigned(N) do
  begin
{$IFOPT C+}
    ChildClassName := N.ClassName;
{$ENDIF}
    N.FContainerNode := Self;
    N := N.SiblingListEntry.FLink.Owner as TCommonNode;
  end;
end;

procedure TCommonNode.FixupChildContainedPtrsRec;
var
  N: TCommonNode;
{$IFOPT C+}
  OwnClassName: string;
  ChildClassName: string;
{$ENDIF}
begin
  FixupChildContainedPtrs;
{$IFOPT C+}
  OwnClassName := Self.ClassName;
{$ENDIF}
  N := ContainedListHead.FLink.Owner as TCommonNode;
  while Assigned(N) do
  begin
{$IFOPT C+}
    ChildClassName := N.ClassName;
{$ENDIF}
    N.FixupChildContainedPtrsRec;
    N := N.SiblingListEntry.FLink.Owner as TCommonNode;
  end;
end;

procedure TCommonNode.IterateNodes(Decision: TCNodeDecisionFunc;
  Exec: TCNodeExecFunc; Recurse: TCNodeRecurseFunc; Ref1, Ref2, Ref3: TObject;
  ExecBeforeRecurse: boolean; OmitSiblings: boolean);
var
  N, C: TCommonNode;
{$IFOPT C+}
  OwnClassName: string;
  ChildClassName: string;
{$ENDIF}
begin
  // Not starting fron a list head, so need to check whether we are
  // not connected to a list.
  N := Self;
  while Assigned(N) do
  begin
{$IFOPT C+}
    OwnClassName := N.ClassName;
{$ENDIF}
    if ExecBeforeRecurse then
    begin
      if Decision(N, Ref1, Ref2, Ref3) then
        Exec(N, Ref1, Ref2, Ref3);
    end;

    if Recurse(N, Ref1, Ref2, Ref3) then
    begin
      C := N.ContainedListHead.FLink.Owner as TCommonNode;
      if Assigned(C) then
      begin
{$IFOPT C+}
        ChildClassName := C.ClassName;
{$ENDIF}
        C.IterateNodes(Decision, Exec, Recurse, Ref1, Ref2, Ref3,
          ExecBeforeRecurse, false);
      end;
    end;

    if not ExecBeforeRecurse then
    begin
      if Decision(N, Ref1, Ref2, Ref3) then
        Exec(N, Ref1, Ref2, Ref3);
    end;

    if DlItemIsEmpty(@N.SiblingListEntry) or OmitSiblings then
      N := nil
    else
      N := N.SiblingListEntry.FLink.Owner as TCommonNode;
  end;
end;

function TCommonNode.AsText: string;
begin
  result := '';
end;

procedure TCommonNode.FixStrings;
begin
  // Really should not run this twice, as it'll just munge stuff.
  Assert(not FStringsFixed);
  FStringsFixed := true;
end;

// Node recursion functions for string fixing.
function ExecuteAlways(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
begin
  result := true;
end;

function ExecuteNotSelf(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
begin
  Assert(Assigned(Ref1));
  result := Node <> Ref1;
end;

procedure FixNodeStrings(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
begin
  Node.FixStrings;
end;

function RecurseAlways(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
begin
  result := true;
end;

// Node recursion functions for partial tree matching and searching.
function TreeMatchDecision(Node: TCommonNode;
  Ref1, Ref2, Ref3: TObject): boolean;
var
  Bail: TNodeSearchBailout;
begin
  Bail := Ref1 as TNodeSearchBailout;
  result := not Bail.AlreadyFound;
end;

procedure TreeMatchExecute(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
var
  Bail: TNodeSearchBailout;
  Other: TCommonNode;
  Opts: TNodeSearchOpts;
begin
  Bail := Ref1 as TNodeSearchBailout;
  Other := Ref2 as TCommonNode;
  Opts := Ref3 as TNodeSearchOpts;
  Assert(not Bail.AlreadyFound);
  if Other.RecSubset(Node, Opts) then
    Bail.FoundNode := Node;
end;

function TreeMatchRecurse(Node: TCommonNode; Ref1, Ref2, Ref3: TObject)
  : boolean;
var
  Bail: TNodeSearchBailout;
begin
  Bail := Ref1 as TNodeSearchBailout;
  result := not Bail.AlreadyFound;
end;

{
  TNavRecurse = (nvhAllChildren,
  nvhImmedChildren,
  nvhLaterSiblings);
}

function NavHelperDecision(Node: TCommonNode;
  Ref1, Ref2, Ref3: TObject): boolean;
var
  Bail: TNodeNavHelperBailout;
begin
  result := false;
  Bail := Ref1 as TNodeNavHelperBailout;
  if (Node = Bail.RootNode) or (Bail.AlreadyFound) then
    exit;
  case Bail.Recurse of
    nvhAllChildren, nvhLaterSiblings:
      result := true;
    // nvhImmedChildren: result := Node.ContainerNode = Bail.RootNode;
    nvhImmedChildren:
      begin
        Assert(Node.ContainerNode = Bail.RootNode);
        result := true;
      end;
  else
    Assert(false);
  end;
end;

procedure NavHelperExecute(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
var
  Bail: TNodeNavHelperBailout;
  Other: TCommonNode;
  Opts: TNodeSearchOpts;
begin
  Bail := Ref1 as TNodeNavHelperBailout;
  Other := Ref2 as TCommonNode;
  Opts := Ref3 as TNodeSearchOpts;
  Assert(not Bail.AlreadyFound);
  if Other.RecSubset(Node, Opts) then
    Bail.FoundNode := Node;
end;

function NavHelperRecurse(Node: TCommonNode; Ref1, Ref2, Ref3: TObject)
  : boolean;
var
  Bail: TNodeNavHelperBailout;
begin
  result := false;
  Bail := Ref1 as TNodeNavHelperBailout;
  if Bail.AlreadyFound then
    exit;
  case Bail.Recurse of
    nvhAllChildren:
      result := true;
    nvhImmedChildren:
      result := Node = Bail.RootNode;
    nvhLaterSiblings:
      result := false;
  else
    Assert(false);
  end;
end;

{ TNodeSearchBailout }

procedure TNodeSearchBailout.Reset;
begin
  FFoundNode := nil;
end;

function TNodeSearchBailout.GetFound: boolean;
begin
  result := Assigned(FFoundNode);
end;

{ TNodeNavHelper }

function TNodeNavHelper.FindCommon(var Node: TCommonNode; Subset: TCommonNode;
  Opts: TNodeSearchOpts; Recurse: TNavRecurse): boolean;
var
  Bail: TNodeNavHelperBailout;
begin
{$IFDEF DEBUG_SEARCH}
  GLogLog(SV_TRACE, 'Common search algorithm start.');
{$ENDIF}
  result := false;
  Bail := TNodeNavHelperBailout.Create;
  try
    // 3 Cases:
    // 1. Find all children.
    // 2. Find immediate children.
    // 3. Find siblings, but not children.
    Bail.RootNode := Node;
    Bail.Recurse := Recurse;
    Node.IterateNodes(NavHelperDecision, NavHelperExecute, NavHelperRecurse,
      Bail, Subset, Opts, true, Recurse <> nvhLaterSiblings);
    if Bail.AlreadyFound then
    begin
      Node := Bail.FoundNode;
      result := true;
    end;
  finally
    Bail.Free;
  end;
{$IFDEF DEBUG_SEARCH}
  GLogLog(SV_TRACE, 'Common search algorithm end.');
{$ENDIF}
end;

function TNodeNavHelper.GetStackTop: TCommonNode;
begin
  Assert(FStack.Count > 0);
  result := TCommonNode(FStack[Pred(FStack.Count)]);
end;

constructor TNodeNavHelper.Create;
begin
  inherited;
  FStack := TList.Create;
end;

destructor TNodeNavHelper.Destroy;
begin
  FStack.Free;
  inherited;
end;

function TNodeNavHelper.FindChildN(var Node: TCommonNode; Subset: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := FindCommon(Node, Subset, Opts, nvhImmedChildren);
end;

function TNodeNavHelper.FindNextN(var Node: TCommonNode; Subset: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := FindCommon(Node, Subset, Opts, nvhLaterSiblings);
end;

function TNodeNavHelper.FindChildRecN(var Node: TCommonNode;
  Subset: TCommonNode; Opts: TNodeSearchOpts): boolean;
begin
  result := FindCommon(Node, Subset, Opts, nvhAllChildren);
end;

function TNodeNavHelper.PushChildN(var Node: TCommonNode; Subset: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
var
  OldNode: TCommonNode;
begin
  OldNode := Node;
  result := FindChildN(Node, Subset, Opts);
  if result then
    Push(OldNode);
end;

function TNodeNavHelper.PushChildRecN(var Node: TCommonNode;
  Subset: TCommonNode; Opts: TNodeSearchOpts): boolean;
var
  OldNode: TCommonNode;
begin
  OldNode := Node;
  result := FindChildRecN(Node, Subset, Opts);
  if result then
    Push(OldNode);
end;

procedure TNodeNavHelper.PopN(var Node: TCommonNode);
begin
  Assert(FStack.Count > 0);
  Node := TCommonNode(FStack.Items[Pred(FStack.Count)]);
  FStack.Delete(Pred(FStack.Count));
end;

procedure TNodeNavHelper.Push(Node: TCommonNode);
begin
  FStack.Add(Node);
end;

procedure TNodeNavHelper.StackEmpty(ExpectEmpty: boolean);
begin
  Assert((FStack.Count = 0) or (not ExpectEmpty));
  FStack.Clear;
end;

{ TStringNavHelper }

function TStringNavHelper.GetTreeFragmentForString(Fragment: string)
  : TCommonNode;
var
  idx: integer;
  FragmentAnsi: AnsiString;
  FragmentStream: TMemoryStream;
  Parser: TCoCoRGrammar;
begin
{$IFDEF DEBUG_SEARCH}
  GLogLog(SV_TRACE, 'Get tree fragment for string: ' + Fragment);
{$ENDIF}
  FLastSearch := Fragment;
  if Length(Fragment) = 0 then
  begin
    result := nil;
    exit;
  end;
  if FSearchDict.Find(Fragment, idx) then
  begin
    result := FSearchDict.Objects[idx] as TCommonNode;
{$IFDEF DEBUG_SEARCH}
    GLogLog(SV_TRACE, 'Tree fragment cached: ' + Fragment);
{$ENDIF}
    exit;
  end;
  // Need to add a new entry to dictionary.
  FragmentStream := TMemoryStream.Create;
  Parser := CreateParser;
  result := nil;
  try
    FragmentAnsi := MakeSearchStringAnsi(Fragment);
    FragmentStream.Write(FragmentAnsi[1], Length(FragmentAnsi));
    Parser.SourceStream := FragmentStream;
    FragmentStream.Seek(0, soFromBeginning);
    Parser.Execute;
    if Assigned(Parser.ParseResult) then
    begin
      result := Parser.ParseResult as TCommonNode;
      PostParse(result);
      if Assigned(result) then
      begin
{$IFDEF DEBUG_SEARCH}
        GLogLog(SV_TRACE, 'Tree fragment added: ' + Fragment);
{$ENDIF}
        FSearchDict.AddObject(Fragment, result);
      end
      else
      begin
{$IFDEF DEBUG_SEARCH}
        GLogLog(SV_WARN, 'No tree fragment!: ' + Fragment);
{$ENDIF}
      end;
    end;
  finally
    (Parser.ParseTracker as TTracker).ResetWithoutFree;
    Parser.ParseResult := nil;
    Parser.SourceStream := nil;
    FragmentStream.Free;
    Parser.Free;
  end;
end;

constructor TStringNavHelper.Create;
begin
  inherited;
  FSearchDict := TStringList.Create;
  FSearchDict.Sorted := true;
  FOpts := CreateOpts;
end;

destructor TStringNavHelper.Destroy;
var
  idx: integer;
begin
  for idx := 0 to Pred(FSearchDict.Count) do
    FSearchDict.Objects[idx].Free;
  FSearchDict.Free;
  FOpts.Free;
  inherited;
end;

function TStringNavHelper.FindChildS(var Node: TCommonNode;
  Fragment: string): boolean;
var
  Template: TCommonNode;
begin
  result := false;
  Template := GetTreeFragmentForString(Fragment);
  if not Assigned(Template) then
    exit;
  result := FindChildN(Node, Template, FOpts);
end;

function TStringNavHelper.FindNextS(var Node: TCommonNode;
  Fragment: string): boolean;
var
  Template: TCommonNode;
begin
  result := false;
  Template := GetTreeFragmentForString(Fragment);
  if not Assigned(Template) then
    exit;
  result := FindNextN(Node, Template, FOpts);
end;

function TStringNavHelper.FindChildRecS(var Node: TCommonNode;
  Fragment: string): boolean;
var
  Template: TCommonNode;
begin
  result := false;
  Template := GetTreeFragmentForString(Fragment);
  if not Assigned(Template) then
    exit;
  result := FindChildRecN(Node, Template, FOpts);
end;

function TStringNavHelper.PushChildS(var Node: TCommonNode;
  Fragment: string): boolean;
var
  Template: TCommonNode;
begin
  result := false;
  Template := GetTreeFragmentForString(Fragment);
  if not Assigned(Template) then
    exit;
  result := PushChildN(Node, Template, FOpts);
end;

function TStringNavHelper.PushChildRecS(var Node: TCommonNode;
  Fragment: string): boolean;
var
  Template: TCommonNode;
begin
  result := false;
  Template := GetTreeFragmentForString(Fragment);
  if not Assigned(Template) then
    exit;
  result := PushChildRecN(Node, Template, FOpts);
end;

procedure TStringNavHelper.PostParse(var ParseResult: TCommonNode);
begin
end;

function TStringNavHelper.MakeSearchStringAnsi(Search: string): AnsiString;
begin
  result := CheckedUnicodeToUTF8(Search);
end;

end.
