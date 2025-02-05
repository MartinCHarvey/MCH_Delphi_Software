unit JSONNodes;
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

uses CommonNodes, Trackables, CocoBase;

const
  NodeTypeJSON = 4;

type
  TJSONNode = class(TCommonNode)
  public
    constructor Create;
    constructor CreateWithTracker(Tracker: TTracker);
  end;

  TJSONDocument = class(TJSONNode)
  public
    function AsText: string; override;
  end;

  // Value contained in common node list system
  TJSONMember = class(TJSONNode)
  private
    FName: string;
  protected
    procedure FixStrings; override;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts): boolean; override;
    function RecSubset(Other: TCommonNode; Opts: TNodeSearchOpts): boolean; override;
    property Name: string read FName write FName;
  end;

  TJSONSimpleValueType = (svtFalse, svtTrue, svtNull, svtNumber, svtString);

  TJSONValue = class(TJSONNode);

  TJSONSimpleValue = class(TJSONValue)
  private
    FValType: TJSONSimpleValueType;
    FStrData: string;
  protected
    procedure FixStrings; override;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts): boolean; override;
    property StrData: string read FStrData write FStrData;
    property ValType: TJSONSimpleValueType read FValType write FValType;
  end;

  TJSONCOntainerType = (jctObject, jctArray);

  // Things in containers use the common node list system for containement.
  TJSONContainer = class(TJSONValue)
  private
    FContainerType: TJSONCOntainerType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts): boolean; override;
    property ContainerType: TJSONCOntainerType read FContainerType write FContainerType;
  end;

  // Don't care, or any of the primitive or container types.
  TJSONSearchFieldType = (sftConventional, sftAnyField, sftBoolean, sftNull, sftNumber, sftString,
    sftObject, sftArray);

  TJSONSearchOpts = class(TNodeSearchOpts)
  private
    FFieldType: TJSONSearchFieldType;
  public
    property FieldType: TJSONSearchFieldType read FFieldType write FFieldType;
  end;

  TJSONNavHelper = class(TStringNavHelper)
  private
  protected
    function CreateParser: TCoCoRGrammar; override;
    function CreateOpts: TNodeSearchOpts; override;
    procedure PostParse(var ParseResult: TCommonNode); override;
    function GetTreeFragmentForString(Fragment: string): TCommonNode; override;
  public
    function FindChild(var Node: TJSONNode; Fragment: string;
      FieldType: TJSONSearchFieldType = sftConventional): boolean;
    function FindNext(var Node: TJSONNode; Fragment: string;
      FieldType: TJSONSearchFieldType = sftConventional): boolean;
    function FindChildRec(var Node: TJSONNode; Fragment: string;
      FieldType: TJSONSearchFieldType = sftConventional): boolean;
    function PushChild(var Node: TJSONNode; Fragment: string;
      FieldType: TJSONSearchFieldType = sftConventional): boolean;
    function PushChildRec(var Node: TJSONNode; Fragment: string;
      FieldType: TJSONSearchFieldType = sftConventional): boolean;
    procedure Pop(var Node: TJSONNode);

    function NavToMember(var Node: TJSONNode; Fragment: string;
      FieldType: TJSONSearchFieldType = sftAnyField; DoPush: boolean = true): boolean;
    function NavToMemberValue(var Node: TJSONNode; Fragment: string;
      FieldType: TJSONSearchFieldType = sftAnyField; DoPush: boolean = true): boolean;
    function NavMemberToValue(var Node: TJSONNode; FieldType: TJSONSearchFieldType = sftAnyField;
      DoPush: boolean = true): boolean;
  end;

implementation

uses
  HTMLEscapeHelper, JSONGrammar, DLList;

const
  S_JSON_DOCUMENT: string = '<document>';
  S_SEP: string = ':';
  S_CONTAINER_BRA: string = '<container>';

  TJSONSimpleValueStrings: array [TJSONSimpleValueType] of string = ('false', 'true', 'null',
    'svtNumber', 'svtString');

  TJSONContainerStrings: array [TJSONCOntainerType] of string = ('{}', '[]');

function JSONMatchFieldTypes(Node: TJSONNode; FieldType: TJSONSearchFieldType): boolean;
begin
  result := false;
  if Assigned(Node) then
  begin
    case FieldType of
      sftConventional, sftAnyField:
        result := true;
      sftBoolean, sftNull, sftNumber, sftString:
        begin
          if Node is TJSONSimpleValue then
          begin
            with Node as TJSONSimpleValue do
            begin
              case FieldType of
                sftBoolean:
                  result := (ValType = svtFalse) or (ValType = svtTrue);
                sftNull:
                  result := ValType = svtNull;
                sftNumber:
                  result := ValType = svtNumber;
                sftString:
                  result := ValType = svtString;
              end;
            end;
          end;
        end;
      sftObject, sftArray:
        begin
          if Node is TJSONContainer then
          begin
            with Node as TJSONContainer do
            begin
              case FieldType of
                sftObject:
                  result := ContainerType = jctObject;
                sftArray:
                  result := ContainerType = jctArray;
              end;
            end;
          end;
        end;
    end;
  end;
end;

{ TJSONDocument }

function TJSONDocument.AsText: string;
begin
  result := S_JSON_DOCUMENT;
end;

{ TJSONNode }

constructor TJSONNode.Create;
begin
  inherited;
  FNodeType := NodeTypeJSON;
end;

constructor TJSONNode.CreateWithTracker(Tracker: TTracker);
begin
  inherited;
  FNodeType := NodeTypeJSON;
end;

{ TJSONMember }

function TJSONMember.AsText: string;
begin
  result := FName + S_SEP;
end;

function TJSONMember.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and ((Other as TJSONMember).FName = self.FName);
end;

function TJSONMember.RecSubset(Other: TCommonNode; Opts: TNodeSearchOpts): boolean;
var
  Val, OtherVal: TJSONValue;
  SVal: TJSONSimpleValue;
  Cont, OtherCont: TJSONContainer;
begin
  if Assigned(Opts) and (Opts is TJSONSearchOpts) and
    ((Opts as TJSONSearchOpts).FFieldType <> sftConventional) then
  begin
    result := false;
    if LclEqual(Other, Opts) then
    begin
      if DlItemIsEmpty(@ContainedListHead) = DlItemIsEmpty(@Other.ContainedListHead) then
      begin
        if not DlItemIsEmpty(@ContainedListHead) then
        begin
          Val := ContainedListHead.FLink.Owner as TJSONValue;
          OtherVal := Other.ContainedListHead.FLink.Owner as TJSONValue;
          if (Val is TJSONSimpleValue) and (OtherVal is TJSONSimpleValue) then
          begin
            SVal := Val as TJSONSimpleValue;
            // Don't need to check values, but do need to check they are of
            // the same general type.
            case (Opts as TJSONSearchOpts).FieldType of
              sftAnyField:
                result := true;
              sftNull:
                result := SVal.ValType = svtNull;
              sftBoolean:
                result := (SVal.ValType = svtFalse) or (SVal.ValType = svtTrue);
              sftNumber:
                result := SVal.ValType = svtNumber;
              sftString:
                result := SVal.ValType = svtString;
            else
              Assert(false);
            end;
          end
          else if (Val is TJSONContainer) and (OtherVal is TJSONContainer) then
          begin
            Cont := Val as TJSONContainer;
            OtherCont := OtherVal as TJSONContainer;
            if Cont.ContainerType = OtherCont.ContainerType then
            begin
              result := true;
              Assert((Cont.ContainerType = jctObject) = ((Opts as TJSONSearchOpts)
                .FieldType = sftObject));
              Assert((Cont.ContainerType = jctArray) = ((Opts as TJSONSearchOpts)
                .FieldType = sftArray))
            end;
          end;
        end
        else
          result := true;
      end;
    end;
  end
  else
    result := inherited RecSubset(Other, Opts);
end;

procedure TJSONMember.FixStrings;
begin
  UnescapeJSONString(FName);
  inherited;
end;

{ TJSONSimpleValue }

function TJSONSimpleValue.AsText: string;
begin
  if (FValType <> svtNumber) and (FValType <> svtString) then
    result := TJSONSimpleValueStrings[FValType]
  else
    result := FStrData;
end;

function TJSONSimpleValue.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and ((Other as TJSONSimpleValue).FValType = self.FValType) and
    ((Other as TJSONSimpleValue).FStrData = self.FStrData);
end;

procedure TJSONSimpleValue.FixStrings;
begin
  if FValType = svtString then
  begin
    UnescapeJSONString(FStrData);
    inherited;
  end;
end;

{ TJSONContainer }

function TJSONContainer.AsText: string;
begin
  result := TJSONContainerStrings[FContainerType] + S_CONTAINER_BRA;
end;

function TJSONContainer.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and ((Other as TJSONContainer).FContainerType = self.FContainerType);
end;

{ TJSONNavHelper }

function TJSONNavHelper.CreateParser: TCoCoRGrammar;
begin
  result := TJSONGrammar.Create(nil);
  with result as TJSONGrammar do
  begin
    ClearSourceStream := false;
    GenListWhen := glNever;
    ParentObject := self;
  end;
end;

function TJSONNavHelper.CreateOpts: TNodeSearchOpts;
begin
  result := TJSONSearchOpts.Create;
end;

function TrimSearchTree(Doc: TJSONDocument; FieldType: TJSONSearchFieldType): TJSONNode;
var
  N: TJSONNode;
  C: TJSONContainer;
begin
  result := Doc;
  N := nil;
  // Always remove the document.
  if not DlItemIsEmpty(@Doc.ContainedListHead) then
  begin
    N := Doc.ContainedListHead.FLink.Owner as TJSONNode;
    Assert(N is TJSONContainer);
    // If doing any field matching, remove the uppermost container.
    if FieldType <> sftConventional then
    begin
      C := N as TJSONContainer;
      if (C.ContainerType = jctObject) and not DlItemIsEmpty(@C.ContainedListHead) then
      begin
        N := C.ContainedListHead.FLink.Owner as TJSONNode;
        Assert(N is TJSONMember);
      end
      else
        N := nil;
    end;
  end;
  if Assigned(N) then
  begin
    result := N;
    DLListRemoveObj(@result.FSiblingListEntry);
    result.FContainerNode := nil;
    Doc.Free;
  end;
end;

procedure TJSONNavHelper.PostParse(var ParseResult: TCommonNode);
begin
  with ParseResult as TJSONDocument do
  begin
    FixupChildContainedPtrsRec;
    IterateNodes(ExecuteAlways, FixNodeStrings, RecurseAlways, self, nil, nil);
  end;
  ParseResult := TrimSearchTree(ParseResult as TJSONDocument, (Opts as TJSONSearchOpts).FieldType);
end;

function TJSONNavHelper.GetTreeFragmentForString(Fragment: string): TCommonNode;
var
  SubString: string;
begin
  if Assigned(Opts) and (Opts is TJSONSearchOpts) then
  begin
    if (Opts as TJSONSearchOpts).FFieldType <> sftConventional then
    begin
      SubString := '';
      case (Opts as TJSONSearchOpts).FieldType of
        sftConventional:
          ;
        sftAnyField, sftNull:
          SubString := 'null';
        sftBoolean:
          SubString := 'true';
        sftNumber:
          SubString := '0';
        sftString:
          SubString := '""';
        sftObject:
          SubString := '{}';
        sftArray:
          SubString := '[]';
      end;
      if Length(SubString) > 0 then
        Fragment := ' { "' + Fragment + '" : ' + SubString + '}';
    end;
  end;
  result := inherited GetTreeFragmentForString(Fragment);
end;

function TJSONNavHelper.FindChild(var Node: TJSONNode; Fragment: string;
  FieldType: TJSONSearchFieldType = sftConventional): boolean;
begin
  (Opts as TJSONSearchOpts).FieldType := FieldType;
  result := FindChildS(TCommonNode(Node), Fragment);
end;

function TJSONNavHelper.FindNext(var Node: TJSONNode; Fragment: string;
  FieldType: TJSONSearchFieldType = sftConventional): boolean;
begin
  (Opts as TJSONSearchOpts).FieldType := FieldType;
  result := FindNextS(TCommonNode(Node), Fragment);
end;

function TJSONNavHelper.FindChildRec(var Node: TJSONNode; Fragment: string;
  FieldType: TJSONSearchFieldType = sftConventional): boolean;
begin
  (Opts as TJSONSearchOpts).FieldType := FieldType;
  result := FindChildRecS(TCommonNode(Node), Fragment);
end;

function TJSONNavHelper.PushChild(var Node: TJSONNode; Fragment: string;
  FieldType: TJSONSearchFieldType = sftConventional): boolean;
begin
  (Opts as TJSONSearchOpts).FieldType := FieldType;
  result := PushChildS(TCommonNode(Node), Fragment);
end;

function TJSONNavHelper.PushChildRec(var Node: TJSONNode; Fragment: string;
  FieldType: TJSONSearchFieldType = sftConventional): boolean;
begin
  (Opts as TJSONSearchOpts).FieldType := FieldType;
  result := PushChildRecS(TCommonNode(Node), Fragment);
end;

procedure TJSONNavHelper.Pop(var Node: TJSONNode);
begin
  PopN(TCommonNode(Node));
end;

function TJSONNavHelper.NavToMember(var Node: TJSONNode; Fragment: string;
  FieldType: TJSONSearchFieldType = sftAnyField; DoPush: boolean = true): boolean;
var
  NNode: TJSONNode;
begin
  result := false;
  NNode := Node;
  if (NNode is TJSONContainer) and
    ((NNode as TJSONContainer).ContainerType = TJSONCOntainerType.jctObject) then
  begin
    if FindChild(NNode, Fragment, FieldType) then
    begin
      result := true;
      if DoPush then
        Push(Node);
      Node := NNode;
    end;
  end;
end;

function TJSONNavHelper.NavToMemberValue(var Node: TJSONNode; Fragment: string;
  FieldType: TJSONSearchFieldType = sftAnyField; DoPush: boolean = true): boolean;
var
  NNode: TJSONNode;
begin
  result := false;
  NNode := Node;
  if NavToMember(NNode, Fragment, FieldType, false) then
  begin
    if NavMemberToValue(NNode, FieldType, false) then
    begin
      result := true;
      if DoPush then
        Push(Node);
      Node := NNode;
    end;
  end;
end;

function TJSONNavHelper.NavMemberToValue(var Node: TJSONNode;
  FieldType: TJSONSearchFieldType = sftAnyField; DoPush: boolean = true): boolean;
var
  CH: TJSONNode;
begin
  result := false;
  if Node is TJSONMember then
  begin
    if not DlItemIsEmpty(@Node.ContainedListHead) then
    begin
      CH := Node.ContainedListHead.FLink.Owner as TJSONNode;
      if JSONMatchFieldTypes(CH, FieldType) then
      begin
        result := true;
        if DoPush then
          Push(Node);
        Node := CH;
      end;
    end;
  end;
end;

end.
