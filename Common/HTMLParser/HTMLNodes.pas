unit HTMLNodes;
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

uses
  Trackables, Classes, DLList, CommonNodes, HTMLEscapeHelper, CoCoBase;

const
  NodeTypeHTML = 2;

type
  THTMLBlock = class;

  TReconstructHint = (trhNone, trhOpenForParentBlock, trhCloseForParentBlock,
                      trhOmitTopTag);

  THTMLNode = class(TCommonNode)
  protected
  public
    constructor Create;
    constructor CreateWithTracker(Tracker: TTracker);
    function ReconstructedHTML(Hint: TReconstructHint): string; virtual;
  end;

  //N.B ValPairs are normally represented as strings whether
  //tvtString or tvtTokenSequence at the HTML level.
  //This handling allows for further tokenization
  //whether originally presented as strings or not.
  TValPairHandling = (vphStringMatch, vphIgnoreValues, vphTokenize);

  THTMLNodeSearchOpts = class(TNodeSearchOpts)
  private
    FValPairHandling: TValPairHandling;
  public
    property ValPairHandling:TValPairHandling read FValPairHandling write FValPairHandling;
  end;

  TPostParseStats = record
    BlocksCoalesced: integer;
    TagsMatched: integer;
    OpenTagsUnmatched: integer;
    CloseTagsUnmatched: integer;
  end;

  THTMLDocument = class(THTMLNode)
  private
    FCanonicalURL: string;
    FUnparsedScriptData: boolean;
  protected
    procedure HTMLBlockCoalesce(var Stats: TPostParseStats);
    procedure HTMLTagMatch(var Stats: TPostParseStats; EventList: TList);
  public
    function AsText: string; override;
    procedure HTMLPostParse(var Stats: TPostParseStats; EventList: TList);
    procedure HTMLGenRefList(RefList: TList; EventList: TList);
    property CanonicalURL: string read FCanonicalURL write FCanonicalURL;
    property UnparsedScriptData: boolean read FUnparsedScriptData write FUnparsedScriptData;
  end;

  THTMLTag = class;

  THTMLBlock = class(THTMLNode)
  private
    FTag: THTMLTag;
    FText: string;
  protected
    procedure FixStrings; override;
  public
    function ReconstructedHTML(Hint: TReconstructHint): string; override;

    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean; override;
    function RecSubset(Other: TCommonNode; Opts: TNodeSearchOpts): boolean; override;
    function AsText: string; override;

    destructor Destroy; override;
    { HTML Post parse step matches up tags and blocks, to unflatten and create
      a tree structure, and identifies which parts of of the page contain
      scripts that will need future parsing or interpretation }

    property Tag: THTMLTag read FTag write FTag;
    property Text: string read FText write FText;
  end;

  THTMLValueType = (tvtNone,
                    tvtString,
                    tvtTokenSequence);

  THTMLValuePair = class(THTMLNode)
  private
    FValName: string;
    FValData: string;
    FValType: THTMLValueType;
  protected
    procedure FixStrings; override;
    procedure Tokenize;
    procedure UnTokenize;
  public
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean; override;
    function RecSubset(Other: TCommonNode; Opts: TNodeSearchOpts): boolean; override;
    function AsText: string; override;

    property ValName: string read FValName write FValName;
    property ValData: string read FValData write FValData;
    property ValType: THTMLValueType read FValType write FValType;
  end;

  THTMLValueToken = class(THTMLNode)
  private
    FToken: string;
  protected
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;override;
    property Token: string read FToken write FToken;
  end;

  THTMLTagType = (ttOpen,
                  ttClose,
                  ttComplete,
                  ttPling,
                  ttQuery);

  THTMLTag = class(THTMLNode)
  private
    FTagType: THTMLTagType;
    FName: string;
  protected
    procedure FixStrings; override;
    function AsTextHelper(PrintTagType: THTMLTagType): string;
  public
    function ReconstructedHTML(Hint: TReconstructHint): string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean; override;
    function AsText: string; override;

    function FindValuePairByName(Name: string): THTMLValuePair;
    procedure AddValuePair(Pair: THTMLValuePair; out OK: boolean; out Merged: boolean);

    property TagType: THTMLTagType read FTagType write FTagType;
    property Name: string read FName write FName;
  end;

  THTMLScriptTag = class(THTMLTag)
  private
    FScriptData: TStream;
    FDocType: THTMLDocType;
    FFailedParse: boolean;
    //Parsed script nodes
  public
    function ReconstructedHTML(Hint: TReconstructHint): string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean; override;
    function AsText: string; override;

    destructor Destroy;override;
    destructor DestroySelfOnly(Sender: TTracker); override;
    procedure GenUnresolvedRef(RefList: TList; Doc: THTMLDocument; EventList: TList);
    procedure ReferencedDataFetched(Doc: THTMLDocument; NewData: TStream);
    property ScriptData: TStream read FScriptData write FScriptData;
    property DocType: THTMLDocType read FDocType write FDocType;
    property FailedParse: boolean read FFailedParse write FFailedParse;
  end;

  TUnresolvedReference = class(TTrackable)
  private
    FURL: string;
    FCanonicalURL:string;
    FParseItem: TObject;
    FDocument: THTMLDocument;
    FScriptTag: THTMLScriptTag;
  public
    property URL:string read FURL write FURL;
    property CanonicalURL: string read FCanonicalURL write FCanonicalURL;
    property Document: THTMLDocument read FDocument write FDocument;
    property ParseItem: TObject read FParseItem write FParseItem;
    property ScriptTag: THTMLScriptTag read FScriptTag write FScriptTag;
  end;

  THTMLNavHelper = class(TStringNavHelper)
  private
  protected
    function GetValPairHandling: TValPairHandling;
    procedure SetValPairHandling(NH: TValPairHandling);
    function CreateParser: TCoCoRGrammar; override;
    function CreateOpts: TNodeSearchOpts; override;
    procedure PostParse(var ParseResult: TCommonNode); override;
  public
    property ValPairHandling:TValPairHandling read GetValPairHandling write SetValPairHandling;
    function FindChild(var Node: THTMLNode; Fragment: string): boolean;
    function FindNext(var Node: THTMLNode; Fragment: string): boolean;
    function FindChildRec(var Node: THTMLNode; Fragment: string): boolean;
    function PushChild(var Node: THTMLNode; Fragment: string): boolean;
    function PushChildRec(var Node: THTMLNode; Fragment: string): boolean;
    procedure Pop(var Node: THTMLNode);
  end;

//Node recusion functions for script node checking and parsing.
function ScriptNodeCheck(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
procedure ScriptNodeParse(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
function ScriptNodeRecurse(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;

//Trim down to the HTML block that contains the tag or tag group specified,
//when passing a string to the parser that is later used as part of a tree
//match algorithm.
function HTMLBlockStubTrim(Tree: TCommonNode): TCommonNode;

const
  HTMLValueTypeStrs: array[THTMLValueType] of string =
                   ('tvtNone',
                    'tvtString',
                    'tvtTokenSequence');

  HTMLTagTypeStrs: array[THTMLTagType] of string =
                   ('ttOpen',
                    'ttClose',
                    'ttComplete',
                    'ttPling',
                    'ttQuery');

implementation

uses
  SysUtils, HTMLParseEvents, HTMLParser, HTMLGrammar
{$IFDEF DEBUG_SEARCH}
  , GlobalLog
{$ENDIF}
  ;

type
  TOpenTagItem = class(TTrackable)
  public
    StackLink: TDLEntry;
    ContainerBlock: THTMLBlock;
    function MatchesClosingTag(Block: THTMLBlock): boolean;
    constructor Create;
  end;

  TOpenTagStack = class(TTrackable)
  public
    StackHead: TDLEntry;
    procedure OpeningTag(Block: THTMLBlock);
    //Returns matching block containing opening tag if necessary.
    function UnwindForClosingTag(ClosingTag: THTMLBlock;
                                out OpeningTag: THTMLBlock;
                                out TagsSkipped:TOpenTagStack): boolean;
    function Pop: THTMLBlock;
    constructor Create;
    destructor Destroy;override;
  end;

function ScriptNodeCheck(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
begin
  result := (Node is THTMLBlock)
    and Assigned((Node as THTMLBlock).Tag)
    and ((Node as THTMLBlock).Tag is THTMLScriptTag);
end;

function ScriptNodeRecurse(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
begin
  result := (Node is THTMLNode); //Don't recurse through scripts.
end;

procedure ScriptNodeGenRef(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
var
  RefList: TList;
  EventList: TList;
  Doc: THTMLDocument;
  T: THTMLScriptTag;
begin
  RefList := Ref1 as TList;
  EventList := Ref3 as TList;
  Doc := Ref2 as THTMLDocument;
  T := ((Node as THTMLBlock).Tag as THTMLScriptTag);
  if not T.FailedParse and DlItemIsEmpty(@(Node as THTMLBlock).ContainedListHead) then
    T.GenUnresolvedRef(RefList, Doc, EventList);
end;

procedure ScriptNodeParse(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
var
  ParseItem: THTMLParseItem;
begin
  ParseItem := Ref1 as THTMLParseItem;
  Assert(Node is THTMLNode);
  ParseItem.ParseScriptForBlock(Node as THTMLNode);
end;

function HTMLBlockStubTrim(Tree: TCommonNode): TCommonNode;
var
  Node: THTMLNode;
begin
  Assert(Assigned(Tree));
  Assert(Tree is THTMLDocument);
  Node := DLListRemoveHead(@Tree.ContainedListHead).Owner as THTMLBlock;
  Node.ContainerNode := nil;
  Tree.Free;
  result := Node;
end;

{ THTMLNode }

constructor THTMLNode.Create;
begin
  inherited;
  FNodeType := NodeTypeHTML;
end;

constructor THTMLNode.CreateWithTracker(Tracker: TTracker);
begin
  inherited;
  FNodeType := NodeTypeHTML;
end;

function THTMLNode.ReconstructedHTML(Hint: TReconstructHint): string;
var
  ChNode: THTMLNode;
begin
  result := '';
  ChNode := ContainedListHead.FLink.Owner as THTMLNode;
  while Assigned(ChNode) do
  begin
    result := result + ChNode.ReconstructedHTML(trhNone);
    ChNode := ChNode.SiblingListEntry.FLink.Owner as THTMLNode;
  end;
end;

{ TOpenTagItem }

function TOpenTagItem.MatchesClosingTag(Block: THTMLBlock):boolean;
begin
  Assert(Assigned(ContainerBlock) and Assigned(ContainerBlock.Tag)
    and (ContainerBlock.Tag.TagType = ttOpen));
  result := Assigned(Block.Tag) and (Block.Tag.TagType = ttClose)
    and (CompareText(Block.Tag.Name, ContainerBlock.Tag.Name) = 0);
end;

constructor TOpenTagItem.Create;
begin
  inherited;
  DLItemInitObj(self, @StackLink);
end;

{ TOpenTagStack }

procedure TOpenTagStack.OpeningTag(Block: THTMLBlock);
var
  NewItem: TOpenTagItem;
begin
  Assert(Assigned(Block) and Assigned(Block.Tag) and
    (Block.Tag.TagType = ttOpen) and (Length(Block.Tag.Name) > 0));
  NewItem := TOpenTagItem.Create;
  NewItem.ContainerBlock := Block;
  DLListInsertHead(@StackHead, @NewItem.StackLink);
end;

//Returns matching block containing opening tag if necessary.
function TOpenTagStack.UnwindForClosingTag(ClosingTag: THTMLBlock;
                                        out OpeningTag: THTMLBlock;
                                        out TagsSkipped:TOpenTagStack): boolean;
var
  StackItem: TOpenTagItem;
  TopItem: TOpenTagItem;
begin
  result := false;
  StackItem := StackHead.FLink.Owner as TOpenTagItem;
  while Assigned(StackItem) do
  begin
    if StackItem.MatchesClosingTag(ClosingTag) then
    begin
      result := true;
      OpeningTag := StackItem.ContainerBlock;
      TagsSkipped := TOpenTagStack.Create;
      TopItem := StackHead.FLink.Owner as TOpenTagItem;
      while TopItem <> StackItem do
      begin
        DLListRemoveObj(@TopItem.StackLink);
        //Push in reverse order.
        DLListInsertTail(@TagsSkipped.StackHead, @TopItem.StackLink);
        TopItem := StackHead.FLink.Owner as TOpenTagItem;
      end;
      DLListRemoveObj(@StackItem.StackLink);
      StackItem.Free;
      exit;
    end;
    StackItem := StackItem.StackLink.FLink.Owner as TOpenTagItem;
  end;
end;

function TOpenTagStack.Pop: THTMLBlock;
var
  TopItem: TOpenTagItem;
  PEntry: PDLEntry;
begin
  PEntry := DLListRemoveHead(@StackHead);
  if Assigned(PEntry) then
    TopItem := PEntry.Owner as TOpenTagItem
  else
    TopItem := nil;
  if Assigned(TopItem) then
  begin
    result := TopItem.ContainerBlock;
    TopItem.Free;
  end
  else
    result := nil;
end;


constructor TOpenTagStack.Create;
begin
  inherited;
  DLItemInitList(@StackHead);
end;

destructor TOpenTagStack.Destroy;
var
  StackTop: TOpenTagItem;
begin
  StackTop := StackHead.FLink.Owner as TOpenTagItem;
  while Assigned(StackTop) do
  begin
    DLListRemoveObj(@StackTop.StackLink);
    StackTop.Free;
    StackTop := StackHead.FLink.Owner as TOpenTagItem;
  end;
  inherited;
end;


{ THTMLDocument }

function THTMLDocument.AsText:string;
begin
  result := 'Document:';
end;

{
  Ideal Algorithm:

  Way to do HTML tag matching is stack wise, except that:
  1. Some tags can't be nested (<p> <li>, so encountering a new one, implicitly
     closes the old one.
  2. If we encounter a closing tag which matches things further up the stack,
     then it's OK to unwind over several items.
  3. Unwinding over non-nestable tags is to be expected, and should implcitly
     close them.
  4. Unwinding over nestable tags indicates closing tag missing (- mark as warning).
  5. A closing tag that we can't find a matching opener to unwind over is
     likewise an error.
  6. Keep count of tags matched, not matched, etc, and use it to update
     the list of non-nestable tags

  Current Algorithm:

  1. Match stack wise, and tree-ify matching tags.
  2. Keep count of tags matched and unmatched.
  3. No special handling of non-nestable tags yet.
  4. Keep unmatched tags in the block chain as-is.
}


procedure THTMLDocument.HTMLTagMatch(var Stats: TPostParseStats; EventList: TList);
var
  OpenTagStack, SkipTagStack: TOpenTagStack;
  Cur, Next, MatchedOpen: THTMLBlock;
  ContainedCur, ContainedNext: THTMLBlock;
begin
  Stats.TagsMatched := 0;
  Stats.OpenTagsUnmatched := 0;
  Stats.CloseTagsUnmatched := 0;

  OpenTagStack := TOpenTagStack.Create;
  try
    Cur := ContainedListHead.FLink.Owner as THTMLBlock;
    while Assigned(Cur) do
    begin
      Next := Cur.SiblingListEntry.FLink.Owner as THTMLBlock;
      if Assigned(Cur.Tag) then
      begin
        if Cur.Tag.TagType = ttOpen then
        begin
          OpenTagStack.OpeningTag(Cur);
        end
        else if Cur.Tag.TagType = ttClose then
        begin
          if OpenTagStack.UnwindForClosingTag(Cur, MatchedOpen, SkipTagStack) then
          begin
            while SkipTagStack.Pop <> nil do
              Inc(Stats.OpenTagsUnmatched);
            SkipTagStack.Free; //TODO - more fancy handling of non-nestable tags.
            //OK, can turn matched open tag into ttComplete with contained block list.
            MatchedOpen.Tag.TagType := ttComplete;
            ContainedCur := MatchedOpen.SiblingListEntry.FLink.Owner as THTMLBlock;
            //Want to add parsed script data to children of MatchedOpen, not HTML.
            if (ContainedCur <> Cur) and (MatchedOpen.Tag is THTMLScriptTag) then
            begin
              //Oh dear, bad things are likely to happen, both HTML nodes and
              //a parse tree under the block containing a script tag is not on,
              //The script parser will think results already done.
              EventList.Add(TParseEvent.CreateFromHtmlBlockError(
                Cur.Tag.Col, Cur.Tag.Line,
                'Script block already has HTML nodes as children, script cannot be parsed'));
            end;
            while ContainedCur <> Cur do
            begin
              ContainedNext := ContainedCur.SiblingListEntry.FLink.Owner as THTMLBlock;
              DLListRemoveObj(@ContainedCur.SiblingListEntry);
              DLListInsertTail(@MatchedOpen.ContainedListHead, @ContainedCur.SiblingListEntry);
              ContainedCur := ContainedNext;
            end;
            //Not any more - since we do a full recursive parse,
            //we fix up all pointers recursively at the end.
            //MatchedOpen.FixupChildContainedPtrs;
            DLListRemoveObj(@Cur.SiblingListEntry);
            Cur.Free;
            Inc(Stats.TagsMatched, 2);
          end
          else
            Inc(Stats.CloseTagsUnmatched);
        end;
        //Not to wory about ttComplete.
      end;
      Cur := Next;
    end;
  finally
    OpenTagStack.Free;
  end;
end;

procedure THTMLDocument.HTMLBlockCoalesce(var Stats: TPostParseStats);
var
  Cur, Prev: THTMLBlock;
begin
  Stats.BlocksCoalesced := 0;
  Cur := ContainedListHead.FLink.Owner as THTMLBlock;
  Prev := nil;
  while Assigned(Cur) do
  begin
    if Assigned(Prev) and
      (not Assigned(Prev.Tag) and not Assigned(Cur.Tag)) then
    begin
      //Both blocks are textual.
      Cur.Text := Prev.Text + Cur.Text;
      DLListRemoveObj(@Prev.SiblingListEntry);
      Prev.Free;
      Inc(Stats.BlocksCoalesced);
    end;
    Prev := Cur;
    Cur := Cur.SiblingListEntry.FLink.Owner as THTMLBlock;
  end;
end;

procedure THTMLDocument.HTMLPostParse(var Stats: TPostParseStats; EventList: TList);
begin
  HTMLBlockCoalesce(Stats);
  HTMLTagMatch(Stats, EventList);
  FixupChildContainedPtrsRec;
  IterateNodes(ExecuteAlways, FixNodeStrings, RecurseAlways, self, nil, nil);
end;

procedure THTMLDocument.HTMLGenRefList(RefList: TList; EventList: TList);
begin
  IterateNodes(ScriptNodeCheck, ScriptNodeGenRef, ScriptNodeRecurse, RefList, self, EventList);
end;

{ THTMLBlock }

function THTMLBlock.ReconstructedHTML(Hint: TReconstructHint): string;
var
  HasChildren: boolean;
begin
  HasChildren := not DlItemIsEmpty(@ContainedListHead);
  Assert((Length(FText) = 0) = Assigned(Tag)); //One or the other, not both.

  if not Assigned(Tag) then
    result := EscapeToHTML(FText)
  else
  begin
    //No text in children, or script tag will give us contained stream
    if (not HasChildren )or (Tag is THTMLScriptTag) then
    begin
      if Hint <> trhOmitTopTag then
        result := Tag.ReconstructedHTML(trhNone)
      else
        result := '';
    end
    else
    begin
      result := '';
      if Hint <> trhOmitTopTag then
        result := result + Tag.ReconstructedHTML(trhOpenForParentBlock);
      result := result + inherited ReconstructedHTML(trhNone);
      if Hint <> trhOmitTopTag then
        result := result + Tag.ReconstructedHTML(trhCloseForParentBlock);
    end;
  end;
end;

function THTMLBlock.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
begin
  result := inherited
    and ((Other as THTMLBlock).FText = self.FText);
end;

function THTMLBlock.RecSubset(Other: TCommonNode; Opts: TNodeSearchOpts): boolean;
begin
{$IFDEF DEBUG_SEARCH}
  GLogLog(SV_TRACE, 'Block Rec subset: ' + Self.AsText + ' ' + Other.AsText);
{$ENDIF}
  result := inherited;
  if result then
  begin
{$IFDEF DEBUG_SEARCH}
  GLogLog(SV_TRACE, 'Block Rec subset:, check tags:' + Self.AsText + ' ' + Other.AsText);
{$ENDIF}
    if Assigned(Tag) then
    begin
      if not Assigned((Other as THTMLBlock).Tag) then
        result := false
      else
        result := Tag.RecSubset((Other as THTMLBlock).Tag, Opts);
    end;
  end
  else
  begin
{$IFDEF DEBUG_SEARCH}
  GLogLog(SV_TRACE, 'Block Rec subset, FAIL not local equal: ' + Self.AsText + ' ' + Other.AsText);
{$ENDIF}
  end;
{$IFDEF DEBUG_SEARCH}
  if result then
    GLogLog(SV_TRACE, 'Block Rec subset PASS: ' + Self.AsText + ' ' + Other.AsText);
{$ENDIF}
end;


procedure THTMLBlock.FixStrings;
begin
  if Assigned(Tag) then
    Tag.FixStrings;
  FixAnsifiedBackToUnicode(FText);
  inherited;
end;

function THTMLBlock.AsText: string;
begin
  if Assigned(Tag) then
    result := Tag.AsText
  else
    result := FText;
end;

destructor THTMLBlock.Destroy;
begin
  FTag.Free;
  inherited;
end;

{ THTMLValuePair }

function THTMLValuePair.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
var
  VPHandling: TValPairHandling;
begin
{$IFDEF DEBUG_SEARCH}
  GLogLog(SV_TRACE, 'Value pair local equal: ' + Self.AsText + ' ' + Other.AsText);
{$ENDIF}
  result := inherited
    and ((Other as THTMLValuePair).FValType = self.FValType)
    and (CompareText((Other as THTMLValuePair).FValName, self.FValName) = 0);
{$IFDEF DEBUG_SEARCH}
  if result then
    GLogLog(SV_TRACE, 'Value pair local equal, PASS: ' + Self.AsText + ' ' + Other.AsText)
  else
    GLogLog(SV_TRACE, 'Value pair local equal, FAIL: ' + Self.AsText + ' ' + Other.AsText);
{$ENDIF}
  VPHandling := vphStringMatch;
  if Assigned(Opts) and (Opts is THTMLNodeSearchOpts) then
    VPHandling := (Opts as THTMLNodeSearchOpts).ValPairHandling;
  case VPHandling of
    vphStringMatch: result := result and ((Other as THTMLValuePair).FValData = self.FValData);
    vphIgnoreValues: ;//Ignore values.;
    vphTokenize: ;//Deal with values in Rec subset;
  else
    Assert(false);
  end;
{$IFDEF DEBUG_SEARCH}
  if result then
    GLogLog(SV_TRACE, 'Value pair local equal after stringmatch, PASS: ' + Self.AsText + ' ' + Other.AsText)
  else
    GLogLog(SV_TRACE, 'Value pair local equal after stringmatch, FAIL: ' + Self.AsText + ' ' + Other.AsText);
{$ENDIF}
end;

function THTMLValuePair.RecSubset(Other: TCommonNode; Opts: TNodeSearchOpts): boolean;
var
  VPHandling: TValPairHandling;
begin
{$IFDEF DEBUG_SEARCH}
  GLogLog(SV_TRACE, 'Value pair rec subset: ' + Self.AsText + ' ' + Other.AsText);
{$ENDIF}
  if Other is THTMLValuePair then
  begin
    VPHandling := vphStringMatch;
    if Assigned(Opts) and (Opts is THTMLNodeSearchOpts) then
      VPHandling := (Opts as THTMLNodeSearchOpts).ValPairHandling;
{$IFDEF DEBUG_SEARCH}
    case VPHandling of
      vphStringMatch: GLogLog(SV_TRACE, 'VP rec subset match algo: vphStringMatch');
      vphIgnoreValues: GLogLog(SV_TRACE, 'VP rec subset match algo: vphIgnoreValues');
      vphTokenize: GLogLog(SV_TRACE, 'VP rec subset match algo: vphTokenize');
    else
      Assert(false);
    end;
{$ENDIF}
    case VPHandling of
      vphStringMatch,
      vphIgnoreValues:
      begin
        UnTokenize;
        (Other as THTMLValuePair).UnTokenize;
      end;
      vphTokenize:
      begin
        Tokenize;
        (Other as THTMLValuePair).Tokenize;
      end
    else
      Assert(false);
    end;
  end;
  result := inherited;
{$IFDEF DEBUG_SEARCH}
  if not result then
    GLogLog(SV_TRACE, 'Value pair rec subset FAIL ' + Self.AsText + ' ' + Other.AsText)
  else
    GLogLog(SV_TRACE, 'Value pair rec subset PASS ' + Self.AsText + ' ' + Other.AsText)
{$ENDIF}
end;

procedure THTMLValuePair.Tokenize;
var
  Token: string;
  idx: integer;

  procedure MakeToken(var T: string);
  var
    CToken: THTMLValueToken;
  begin
    CToken := THTMLValueToken.Create;
    CToken.Token := T;
    DLListInsertTail(@ContainedListHead, @CToken.SiblingListEntry);
    T := '';
  end;

begin
  if DlItemIsEmpty(@ContainedListHead) then
  begin
{$IFDEF DEBUG_SEARCH}
    GLogLog(SV_TRACE, 'Tokenizing: ' + Self.AsText);
{$ENDIF}

    //Assume tokens are a-z A-Z 0-9 #128 - #255 (Same as JS identifiers).
    for idx := 1 to Length(FValData) do
    begin
      if CharInSet(FValData[idx], ['0'..'9',
                           'a'..'z',
                           'A'..'Z',
                           '-', '_',
                           #128..#255]) then
      begin
        Token := Token + FValData[idx];
      end
      else
      begin
        if Length(Token) > 0 then
          MakeToken(Token);
      end;
    end;
    if Length(Token) > 0 then
      MakeToken(Token);
    FixupChildContainedPtrs;
  end;
end;

procedure THTMLValuePair.UnTokenize;
var
  C,N: TCommonNode;
begin
  C := ContainedListHead.FLink.Owner as TCommonNode;
  if Assigned(C) then
  begin
{$IFDEF DEBUG_SEARCH}
    GLogLog(SV_TRACE, 'Untokenizing: ' + Self.AsText);
{$ENDIF}
  end;
  while Assigned(C) do
  begin
    N := C.SiblingListEntry.FLink.Owner as TCommonNode;
    DLListRemoveObj(@C.SiblingListEntry);
    C.Free;
    C := N;
  end;
end;

function THTMLValuePair.AsText: string;
begin
  if ValType = tvtString then
    result := ValName + '="' + ValData + '"'
  else if ValType = tvtTokenSequence then
    result := ValName + '=' + ValData
  else
  begin
    Assert(ValType = tvtNone);
    result := ValName;
  end;
end;

procedure THTMLValuePair.FixStrings;
begin
  FixAnsifiedBackToUnicode(FValName);
  FixAnsifiedBackToUnicode(FValData);
  inherited;
end;

{ THTMLValueToken }

function THTMLValueToken.AsText: string;
begin
  result := Token;
end;

function THTMLValueToken.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
begin
{$IFDEF DEBUG_SEARCH}
    GLogLog(SV_TRACE, 'Value token local equal: ' + Self.AsText + ' ' + Other.AsText);
{$ENDIF}
  result := inherited;
  result := result and (CompareText(Self.Token, (Other as THTMLValueToken).Token) = 0);
{$IFDEF DEBUG_SEARCH}
  if result then
    GLogLog(SV_TRACE, 'Value token local equal, PASS: ' + Self.AsText + ' ' + Other.AsText)
  else
    GLogLog(SV_TRACE, 'Value token local equal, FAIL: ' + Self.AsText + ' ' + Other.AsText);
{$ENDIF}
end;

{ THTMLTag }

function THTMLTag.AsTextHelper(PrintTagType: THTMLTagType): string;
var
  VP: THTMLValuePair;
begin
  if PrintTagType = ttClose then
    result := '</'
  else if PrintTagType = ttPling then
    result := '<!'
  else if PrintTagType =ttQuery then
    result := '<?'
  else
    result := '<';

  result := result + Name;

  if PrintTagType <> ttClose then
  begin
    VP := ContainedListHead.FLink.Owner as THTMLValuePair;
    while Assigned(VP) do
    begin
      if not ((PrintTagType = ttPling) or (PrintTagType = ttQuery)) then
        result := result + ' ' + VP.AsText
      else
        result := result + VP.FValData;

      VP := VP.SiblingListEntry.FLink.Owner as THTMLValuePair;
    end;
  end;

  if PrintTagType <> ttComplete then
    result := result + '>'
  else
    result := result + '/>';
end;


function THTMLTag.ReconstructedHTML(Hint: TReconstructHint): string;
var
  PrintTagType: THTMLTagType;
begin
  PrintTagType := TagType;
  if PrintTagType = ttComplete then
  begin
    if Hint = trhOpenForParentBlock then
      PrintTagType := ttOpen
    else if Hint = trhCloseForParentBlock then
      PrintTagType := ttClose;
  end;
  result := AsTextHelper(PrintTagType);
end;

function THTMLTag.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
begin
  result := inherited
    and ((Other as THTMLTag).FTagType = self.FTagType)
    and (CompareText((Other as THTMLTag).FName, self.FName) = 0);
end;

procedure THTMLTag.FixStrings;
begin
  FixAnsifiedBackToUnicode(FName);
  IterateNodes(ExecuteNotSelf, FixNodeStrings, RecurseAlways, self, nil, nil);
  inherited;
end;

function THTMLTag.AsText;
begin
 result := AsTextHelper(TagType);
end;

function THTMLTag.FindValuePairByName(Name: string): THTMLValuePair;
var
  N: THTMLNode;
begin
  N := ContainedListHead.FLink.Owner as THTMLValuePair;
  while Assigned(N) do
  begin
    result := N as THTMLValuePair;
    if AnsiCompareText(result.ValName, Name) = 0 then
      exit;
    N := N.SiblingListEntry.FLink.Owner as THTMLValuePair;
  end;
  result := nil;
end;

//TODO - amalgamate with previous tag if necessary
//previous has no value, current has no name - whitespace error.
procedure THTMLTag.AddValuePair(Pair: THTMLValuePair; out OK: boolean; out Merged: boolean);
var
  Previous: THTMLValuePair;
begin
  OK := false;
  if not Assigned(Pair) then
    exit;
  Merged := false;
  if Length(Pair.ValName) = 0 then
  begin
    //In some cases, may be able to amalgamate with previous VP.
    if not DlItemIsEmpty(@ContainedListHead) then
    begin
      Previous := ContainedListHead.BLink.Owner as THTMLValuePair;
      if Length(Previous.ValData) = 0 then
      begin
        //Whitespace error with value pairs, amalgamate with previous.
        Previous.ValData := Pair.ValData;
        OK := true;
        Merged := true;
      end;
    end;
  end
  else
  begin
    if not Assigned(FindValuePairByName(Pair.ValName)) then
    begin
      DLListInsertTail(@ContainedListHead, @Pair.SiblingListEntry);
      Pair.ContainerNode := self;
      OK := true;
    end;
  end;
end;

{ THTMLScriptTag }

function THTMLScriptTag.ReconstructedHTML(Hint: TReconstructHint): string;
var
  ScriptAnsi: AnsiString;
  OldPos: int64;
begin
  if Assigned(FScriptData) and (FScriptData.Size > 0) then
  begin
    result := inherited ReconstructedHTML(trhOpenForParentBlock);
    SetLength(ScriptAnsi, FScriptData.Size);
    OldPos := FScriptData.Position;
    FScriptData.Seek(0, soFromBeginning);
    FScriptData.Read(ScriptAnsi[1], Length(ScriptAnsi));
    FScriptData.Seek(OldPos, soFromBeginning);
    result := result + ScriptAnsi + inherited ReconstructedHTML(trhCloseForParentBlock);
  end
  else
    result := inherited ReconstructedHTML(trhNone);
end;

function THTMLScriptTag.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
begin
  result := inherited
    and ((Other as THTMLScriptTag).FDocType = self.FDocType)
    and ((Other as THTMLScriptTag).FFailedParse = self.FFailedParse)
    and not self.FFailedParse;
    //TODO - if failed parse, check streams equal?
end;

function THTMLScriptTag.AsText;
begin
  result := inherited + ' ' + DocTypeStrs[DocType];
end;

procedure THTMLScriptTag.GenUnresolvedRef(RefList: TList; Doc: THTMLDocument; EventList:TList);
var
  VP: THTMLValuePair;
  URes: TUnresolvedReference;
begin
  if Assigned(FScriptData) and (FScriptData.Size = 0) then //Empty data between tags.
  begin
    FScriptData.Free;
    FScriptData := nil;
  end;
  if not Assigned(FScriptData) then
  begin
    //Script data not inline, potentially unresolved reference.
    //Need to find out source for unresolved ref.
    VP := FindValuePairByName('src');
    if Assigned(VP) then
    begin
      URes := TUnresolvedReference.Create;
      URes.URL := VP.ValData;
      URes.Document := Doc;
      URes.ScriptTag := self;
      RefList.Add(URes);
    end
    else
      EventList.Add(TParseEvent.CreateFromScriptReferencingErr
      (FCol, FLine, 'Could not find src link for script tag'));
  end
  else
  begin
    //Prefer inline data to src links, but inline data shouldn't be NULL.
    //See also THTMLGrammar.ParseScript
    VP := FindValuePairByName('src');
    if Assigned(VP) then
      EventList.Add(TParseEvent.CreateFromScriptReferencingErr
      (FCol, FLine, 'Script tag has both inline data, and src link, using former.'));
    Doc.UnparsedScriptData := true;
  end;
end;

procedure THTMLScriptTag.ReferencedDataFetched(Doc: THTMLDocument; NewData: TStream);
begin
  if Assigned(NewData) then
  begin
    Assert(not Assigned(FScriptData));
    FScriptData := NewData;
  end;
  if Assigned(FScriptData) and (FScriptData.Size = 0) then //Empty data as fetched
  begin
    FScriptData.Free;
    FScriptData := nil;
  end;
  if Assigned(FScriptData) then
    Doc.UnparsedScriptData := true;
end;

destructor THTMLScriptTag.Destroy;
begin
  FScriptData.Free;
  inherited;
end;

destructor THTMLScriptTag.DestroySelfOnly(Sender: TTracker);
begin
  FScriptData.Free;
  inherited;
end;

{ THTMLNavHelper }

function THTMLNavHelper.GetValPairHandling: TValPairHandling;
begin
  result := (Opts as THTMLNodeSearchOpts).ValPairHandling;
end;

procedure THTMLNavHelper.SetValPairHandling(NH: TValPairHandling);
begin
  (Opts as THTMLNodeSearchOpts).ValPairHandling := NH;
end;

function THTMLNavHelper.CreateParser: TCoCoRGrammar;
begin
  result := THTMLGrammar.Create(nil);
  with result as THTMLGrammar do
  begin
    ClearSourceStream := false;
    GenListWhen := glNever;
    ParentObject := self;
  end;
end;

function THTMLNavHelper.CreateOpts: TNodeSearchOpts;
begin
  result := THTMLNodeSearchOpts.Create;
end;

procedure THTMLNavHelper.PostParse(var ParseResult: TCommonNode);
var
  EventList: TList;
  Stats: TPostParseStats;
  idx: integer;
begin
  EventList := TList.Create;
  try
    (ParseResult as THTMLDocument).HTMLPostParse(Stats, EventList);
    ParseResult := HTMLBlockStubTrim(ParseResult) as TCommonNode;
  finally
    for idx := 0 to Pred(EventList.Count) do
      TObject(EventList.Items[idx]).Free;
    EventList.Free;
  end;
end;

function THTMLNavHelper.FindChild(var Node: THTMLNode; Fragment: string): boolean;
begin
  result := FindChildS(TCommonNode(Node), Fragment);
end;

function THTMLNavHelper.FindNext(var Node: THTMLNode; Fragment: string): boolean;
begin
  result := FindNextS(TCommonNode(Node), Fragment);
end;

function THTMLNavHelper.FindChildRec(var Node: THTMLNode; Fragment: string): boolean;
begin
  result := FindChildRecS(TCommonNode(Node), Fragment);
end;

function THTMLNavHelper.PushChild(var Node: THTMLNode; Fragment: string): boolean;
begin
  result := PushChildS(TCommonNode(Node), Fragment);
end;

function THTMLNavHelper.PushChildRec(var Node: THTMLNode; Fragment: string): boolean;
begin
  result := PushChildRecS(TCommonNode(Node), Fragment);
end;

procedure THTMLNavHelper.Pop(var Node: THTMLNode);
begin
  PopN(TCommonNode(Node));
end;

end.
