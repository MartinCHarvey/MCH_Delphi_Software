unit SSStreamables;
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
  This unit defines a base streamable class, which can be used
  as an ancestor to classes in streamable structures.

  If you want to stream classes from a different ancestor, then obviously,
  you need to duplicate the code here for that other classtype.
}
interface

uses SysUtils, Classes, StreamingSystem, Trackables;

type
{$M+}
{$IFDEF USE_TRACKABLES}
  TObjStreamable = class(TTrackable)
{$ELSE}
  TObjStreamable = class
{$ENDIF}
  private
    FCreatingInStreamer: boolean;
    procedure AssignError(Source: TObjStreamable);
  protected
    procedure AssignTo(Dest: TObjStreamable); virtual;
    procedure CustomMarshal(Sender: TDefaultSSController); virtual;
    procedure CustomUnmarshal(Sender: TDefaultSSController); virtual;
    constructor CreateViaStreamer; virtual;
    property CreatingInStreamer: boolean read FCreatingInStreamer;
  public
    constructor Create; virtual;
    procedure Assign(Source: TObjStreamable); virtual;
    procedure DeepAssign(Source: TObjStreamable); virtual;
    class function Clone(Source: TObjStreamable):TObjStreamable;
    class function DeepClone(Source: TObjStreamable):TObjStreamable;
  published
  end;

  TObjStreamableClass = class of TObjStreamable;

  TObjStreamableList = class(TObjStreamable)
  private
    FItems: TList;
    FItemType: TObjStreamableClass;
  protected
    procedure AssignTo(Dest: TObjStreamable); override;
    procedure SetStreamableClassType(CType: TObjStreamableClass);
    procedure CustomMarshal(Sender: TDefaultSSController); override;
    procedure CustomUnmarshal(Sender: TDefaultSSController); override;
    function GetCapacity: integer;
    procedure SetCapacity(NewCapacity: integer);
    function GetCount: integer;
    procedure SetCount(NewCount: integer);
    function Get(Index: integer): TObjStreamable;
    procedure Put(Index: integer; New: TObjStreamable);
  public
    constructor Create; override;
    destructor Destroy; override;
    procedure Assign(Source: TObjStreamable); override;
    procedure DeepAssign(Source:TObjStreamable); override;
    function Add(Item: TObjStreamable): Integer;
    procedure Clear; virtual;
    procedure Delete(Index: Integer);
    procedure Exchange(Index1, Index2: Integer);
    function Expand: TObjStreamableList;
    function Extract(Item: TObjStreamable): TObjStreamable;
    function First: TObjStreamable;
    function IndexOf(Item: TObjStreamable): Integer;
    procedure Insert(Index: Integer; Item: TObjStreamable);
    function Last: TObjStreamable;
    procedure Move(CurIndex, NewIndex: Integer);
    function Remove(Item: TObjStreamable): Integer;
    procedure Pack;
    procedure Sort(Compare: TListSortCompare);
    property Capacity: Integer read GetCapacity write SetCapacity;
    property Count: Integer read GetCount write SetCount;
    property Items[Index: Integer]: TObjStreamable read Get write Put;
  end;


function ObjStreamableConstructor(ClassType: TClass): TObject;
procedure ObjStreamableCustomMarshal(Obj: TObject; Sender:
  TDefaultSSController);
procedure ObjStreamableCustomUnMarshal(Obj: TObject; Sender:
  TDefaultSSController);

const
  TObjDefaultHeirarchy: THeirarchyInfo =
    (BaseClass: TObjStreamable;
    MemberClasses: nil;
    ConstructHelper: ObjStreamableConstructor;
    CustomMarshal: ObjStreamableCustomMarshal;
    CustomUnmarshal: ObjStreamableCustomUnmarshal);

implementation

uses
  System.Types, SSAbstracts;

resourcestring
  SAssignError = 'Cannot assign a %s to a %s';

{ Misc functions }

function ObjStreamableConstructor(ClassType: TClass): TObject;
begin
  result := TObjStreamableClass(ClassType).CreateViaStreamer;
end;

procedure ObjStreamableCustomMarshal(Obj: TObject; Sender:
  TDefaultSSController);
begin
  TObjStreamable(Obj).CustomMarshal(Sender);
end;

procedure ObjStreamableCustomUnMarshal(Obj: TObject; Sender:
  TDefaultSSController);
begin
  TObjStreamable(Obj).CustomUnmarshal(Sender);
end;

{ TObjStreamable }

procedure TObjStreamable.CustomMarshal(Sender: TDefaultSSController);
begin
end;

procedure TObjStreamable.CustomUnmarshal(Sender: TDefaultSSController);
begin
end;

constructor TObjStreamable.Create;
begin
  inherited;
end;

constructor TObjStreamable.CreateViaStreamer;
begin
  inherited;
  FCreatingInStreamer := true;
  try
    Self.Create;
  finally
    FCreatingInStreamer := false;
  end;
end;


procedure TObjStreamable.Assign(Source: TObjStreamable);
begin
  if Source <> nil then Source.AssignTo(Self) else AssignError(nil);
end;

procedure TObjStreamable.AssignError(Source: TObjStreamable);
var
  SourceName: string;
begin
  if Source <> nil then
    SourceName := Source.ClassName
  else
    SourceName := 'nil';
  raise EConvertError.CreateResFmt(@SAssignError, [SourceName, ClassName]);
end;

procedure TObjStreamable.AssignTo(Dest: TObjStreamable);
begin
  Dest.AssignError(Self);
end;

class function TObjStreamable.Clone(Source: TObjStreamable):TObjStreamable;
var
  SrcClass: TObjStreamableClass;
begin
  if not Assigned(Source) then
    result := nil
  else
  begin
    Assert(Source.ClassType.InheritsFrom(TObjStreamable));
    SrcClass := TObjStreamableClass(Source.ClassType);
    result := SrcClass.Create;
    result.Assign(Source);
  end;
end;

class function TObjStreamable.DeepClone(Source: TObjStreamable):TObjStreamable;
var
  SrcClass: TObjStreamableClass;
begin
  if not Assigned(Source) then
    result := nil
  else
  begin
    Assert(Source.ClassType.InheritsFrom(TObjStreamable));
    SrcClass := TObjStreamableClass(Source.ClassType);
    result := SrcClass.Create;
    result.DeepAssign(Source);
  end;
end;

procedure TObjStreamable.DeepAssign(Source: TObjStreamable);
begin
  Assign(Source);
  //Derived classes should call inherited first, and then clone.
end;

{ TObjStreamableList }

procedure TObjStreamableList.SetStreamableClassType(CType: TObjStreamableClass);
begin
  Assert(CType <> nil);
  FItemType := CType;
end;

procedure TObjStreamableList.CustomMarshal(Sender: TDefaultSSController);
var
  Idx: integer;
begin
  inherited;
  Sender.StreamArrayStart('ItemList');
  for Idx := 0 to Pred(FItems.Count) do
    Sender.StreamClass('', TObjStreamable(FItems[idx]));
  Sender.StreamArrayEnd('ItemList');
end;

procedure TObjStreamableList.CustomUnmarshal(Sender: TDefaultSSController);
var
  Idx, Count: integer;
  Obj: TObject;
begin
  inherited;
  Sender.UnstreamArrayStart('ItemList', Count);
  FItems.Clear;
  for Idx := 0 to Pred(Count) do
  begin
    Sender.UnstreamClass('', Obj);
    if Assigned (FItemType) and not (Obj is FItemType) then
      Sender.UserSignalError(sssError,  'Bad class type :' + Obj.ClassName +
        ' expected child of ' + FItemType.ClassName);
    FItems.Add(Obj);
  end;
  Sender.UnstreamArrayEnd('ItemList');
end;

procedure TObjStreamableList.DeepAssign(Source: TObjStreamable);
var
  Idx: integer;
  NewObj: TObjStreamable;
  S: TObjStreamableList;
begin
  if Assigned(Source) and (Source is TObjStreamableList) then
  begin
    S := TObjStreamableList(Source);
    for Idx := 0 to Pred(FItems.Count) do
      TObjStreamable(FItems[Idx]).Free;
    FItems.Clear;
    for Idx := 0 to Pred(S.FItems.Count) do
    begin
      NewObj := DeepClone(TObjStreamable(S.FItems[idx]));
      FItems.Add(NewObj);
    end;
  end;
  //Don't call inherited, as that calls Assign, which is fine
  //for binary fields, but not for objs by reference.
end;

procedure TObjStreamableList.Assign(Source: TObjStreamable);
var
  Idx: integer;
  S: TObjStreamableList;
begin
  if Assigned(Source) and (Source is TObjStreamableList) then
  begin
    S := TObjStreamableList(Source);
    for Idx := 0 to Pred(FItems.Count) do
      TObjStreamable(FItems[Idx]).Free;
    FItems.Clear;
    for Idx := 0 to Pred(S.FItems.Count) do
      FItems.Add(S.FItems[idx]);
  end;
  inherited; //OK, but override AssignTo.
end;

procedure TObjStreamableList.AssignTo(Dest: TObjStreamable);
begin
  //Override, no call inherited, no assign error.
end;

function TObjStreamableList.GetCapacity: integer;
begin
  result := FItems.Capacity;
end;

procedure TObjStreamableList.SetCapacity(NewCapacity: integer);
begin
  FItems.Capacity := NewCapacity;
end;

function TObjStreamableList.GetCount: integer;
begin
  result := FItems.Count;
end;

procedure TObjStreamableList.SetCount(NewCount: integer);
begin
  FItems.Count := NewCount;
end;

function TObjStreamableList.Get(Index: integer): TObjStreamable;
begin
  result := TObjStreamable(FItems[Index]);
end;

procedure TObjStreamableList.Put(Index: integer; New: TObjStreamable);
begin
  FItems[Index] := New;
end;

constructor TObjStreamableList.Create;
begin
  inherited;
  FItems := TList.Create;
  FItemType := nil;
end;

destructor TObjStreamableList.Destroy;
var
  idx: integer;
begin
  for idx  := 0 to Pred(FItems.Count) do
    TObjStreamable(FItems.Items[idx]).Free;
  FItems.Free;
  inherited;
end;

function TObjStreamableList.Add(Item: TObjStreamable): Integer;
begin
  result := FItems.Add(Item);
end;

procedure TObjStreamableList.Clear;
begin
  FItems.Clear;
end;

procedure TObjStreamableList.Delete(Index: Integer);
begin
  FItems.Delete(Index);
end;

procedure TObjStreamableList.Exchange(Index1, Index2: Integer);
begin
  FItems.Exchange(Index1, Index2);
end;

function TObjStreamableList.Expand: TObjStreamableList;
var
  NList: TList;
begin
  NList := FItems.Expand;
  if NList <> FItems then
  begin
    FItems.Free;
    FItems := NList;
  end;
  result := Self;
end;

function TObjStreamableList.Extract(Item: TObjStreamable): TObjStreamable;
begin
  result := FItems.Extract(Item);
end;

function TObjStreamableList.First: TObjStreamable;
begin
  result := FItems.First;
end;

function TObjStreamableList.IndexOf(Item: TObjStreamable): Integer;
begin
  result := FItems.IndexOf(Item);
end;

procedure TObjStreamableList.Insert(Index: Integer; Item: TObjStreamable);
begin
  FItems.Insert(Index, Item);
end;

function TObjStreamableList.Last: TObjStreamable;
begin
  result := FItems.Last;
end;

procedure TObjStreamableList.Move(CurIndex, NewIndex: Integer);
begin
  FItems.Move(CurIndex, NewIndex);
end;

function TObjStreamableList.Remove(Item: TObjStreamable): Integer;
begin
  result := FItems.Remove(Item);
end;

procedure TObjStreamableList.Pack;
begin
  FItems.Pack;
end;

procedure TObjStreamableList.Sort(Compare: TListSortCompare);
begin
  FItems.Sort(Compare);
end;



end.


