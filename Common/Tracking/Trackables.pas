unit Trackables;
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
  This file defines a very simple "garbage collector" (actually
  object tracker) that keeps track of allocated objects.

  This allows me to keep an eye on potential memory leaks and corrupt
  data structures.
}

interface

uses
  BinaryTree, Classes, SyncObjs;

const
  S_EMPTY = '';

type
  TTrackable = class;

  TTracker = class
  private
    FCrit: TCriticalSection;
    FTree: TBinTree;
    FTraversalRef: TObject;
    procedure TraversePrintItem(Tree: TBinTree; Item: TBinTreeItem; Level:
      integer);
    procedure TraverseDeregister(Tree: TBinTree; Item: TBinTreeItem; Level:
      integer);
    procedure TraverseSumUsage(Tree: TBinTree; Item: TBinTreeItem; Level:
      integer);
    function CreateTrackString(Inst: TTrackable): string;
    function GetHasItems: boolean;
  public
    procedure TrackInstance(Inst: TTrackable; StartTracking: boolean);
    procedure FreeTrackedClassesAndReset;
    procedure ResetWithoutFree;
    function GetTrackedDetails: TStrings;
    function GetTrackedMemUsage: TStrings;
    constructor Create;
    destructor Destroy; override;
    property HasItems: boolean read GetHasItems;
  end;

  TTrackable = class(TObject)
  private
{$IFOPT C+}
{$IFDEF MAGIC_CHECKS}
    Magic: Cardinal;
{$ENDIF}
{$ENDIF}
    FTracker: TTracker;
  protected
    function GetExtraInfoText: string; dynamic;
    procedure TrackerDeregister(Tracker: TTracker);
  public
{$IFOPT C+}
    class function NewInstance: TObject {$IFDEF AUTOREFCOUNT} unsafe {$ENDIF}; override;
    procedure FreeInstance; override;
{$ENDIF}

    constructor Create;
    constructor CreateWithTracker(Tracker: TTracker);
    destructor Destroy; override;
    destructor DestroySelfOnly(Sender: TTracker); virtual;
    procedure FreeSelfOnly(Sender: TTracker);
  end;

{$IFDEF USE_TRACKABLES}
  TMemStreamTrackProxy = class(TTrackable)
  end;
  TFileStreamTrackProxy = class(TTrackable)
  end;

  TTrackedMemoryStream = class(TMemoryStream)
  private
    FTrackProxy: TMemStreamTrackProxy;
  public
    constructor Create;
    destructor Destroy; override;
  end;

  TTrackedFileStream = class(TFileStream)
  private
    FTrackProxy: TFileStreamTrackProxy;
  public
    constructor Create(const AFileName: string; Mode: Word); overload;
    constructor Create(const AFileName: string; Mode: Word; Rights: Cardinal); overload;
    destructor Destroy; override;
  end;
{$ELSE}
  TTrackedMemoryStream = TMemoryStream;
  TTrackedFileStream = TFileStream;
{$ENDIF}

{$IFOPT C+}
var
  AppGlobalTracker: TTracker;
{$ENDIF}  

implementation

uses
  SysUtils, IOUtils;

const
  S_INST = 'Instance of ';
  S_INST2 = ' at location 0x';
  S_INST3 = ' with extra info: ';
  S_EXCEPTION_GETTING_DETAILS =
    '<Exception getting object details - deallocated memory still registered?> ';
  S_ASKED_TO_TRACK_NIL =
    'Memory tracker was asked to track a nil pointer.';
  S_TRACKER_TREE_ADD_FAILED =
    'Memory tracker couldn''t add a reference to a memory block. Duplicate?';
  S_TRACKER_TREE_REMOVE_FAILED =
    'Memory tracker couldn''t remove a reference to a memory block. Lost?';
  S_POINTERS_ODD_SIZE =
    'Can''t convert pointers to ordinal. What platform is this?';
  MAGIC1 = $CAFEBABE;
  MAGIC2 = $0BADF00D;

type
  TTrackItem = class(TBinTreeItem)
  private
    FItem: TTrackable;
  protected
    function Compare(Other: TBinTreeItem; AllowKeyDedupe: boolean): integer; override;
    procedure CopyFrom(Source: TBinTreeItem); override;
  end;

{$IFOPT C+}
var MemMgr: TMemoryManager;
{$ENDIF}

(************************************
 * TTracker                         *
 ************************************)

function TTracker.CreateTrackString(Inst: TTrackable): string;
var
  EInfo: string;
begin
  result := S_INST + Inst.ClassName + S_INST2 + IntToHex(UInt64(Inst), 16);
  EInfo := Inst.GetExtraInfoText;
  if Length(EInfo) > 0 then
    result := result + S_INST3 + EInfo;
end;

procedure TTracker.TrackInstance(Inst: TTrackable; StartTracking: boolean);
var
  TrackItem: TTrackItem;
begin
  FCrit.Acquire;
  try
    Assert(Assigned(inst), S_ASKED_TO_TRACK_NIL);
    TrackItem := TTrackItem.Create;
    TrackItem.FItem := inst;
    if StartTracking then
    begin
      if not FTree.Add(TrackItem) then
      begin
        Assert(false, S_TRACKER_TREE_ADD_FAILED);
        TrackItem.Free;
      end
    end
    else
    begin
      if not FTree.Remove(TrackItem) then
        Assert(false, S_TRACKER_TREE_REMOVE_FAILED);
    //Removal is by value equivalence, so free our temp item.
      TrackItem.Free;
    end;
  finally
    FCrit.Release;
  end;
end;

procedure TTracker.TraversePrintItem(Tree: TBinTree; Item: TBinTreeItem; Level:
  integer);
begin
  try
    (FTraversalRef as TStrings).Add(CreateTrackString(TTrackItem(Item).FItem));
  except
    on E: Exception do (FTraversalRef as TStrings).Add(S_EXCEPTION_GETTING_DETAILS + E.Message);
  end;
end;

procedure TTracker.TraverseSumUsage(Tree: TBinTree; Item: TBinTreeItem; Level:
  integer);
var
  Name: string;
  Size: UIntPtr;
  InfoList: TStringList;
  Index: integer;
begin
  Name := TTrackItem(Item).FItem.ClassName;
  Size := TTRackItem(Item).FItem.InstanceSize;
  InfoList := (FTraversalRef as TStringList);
  if not InfoList.Find(Name, Index) then
    Index := InfoList.Add(Name);
  InfoList.Objects[Index] := TObject(UintPtr(InfoList.Objects[Index]) + Size);
end;

function TTracker.GetTrackedDetails: TStrings;
begin
  FCrit.Acquire;
  try
    result := TStringList.Create;
    FTraversalRef := result;
    try
      FTree.Traverse(TraversePrintItem, btoInOrderLoHi);
    finally
      FTRaversalRef := nil;
    end;
  finally
    FCrit.Release;
  end;
end;

function TTracker.GetTrackedMemUsage: TStrings;
var
  ByTypeUsage: TStringList;
  idx: integer;
begin
  ByTypeUsage := TStringList.Create;
  FCrit.Acquire;
  try
    FTraversalRef := ByTypeUsage;
    ByTypeUsage.Sorted := true;
    FTree.Traverse(TraverseSumUsage, btoInOrderLoHi);
    result := TStringList.Create;
    result.Add('Mem usage snapshot: ');
    result.Add('');
    for idx := 0 to Pred(ByTypeUsage.Count) do
    begin
      result.Add(ByTypeUsage.Strings[idx] + ':  '
        + UIntToStr(UintPtr(ByTypeUsage.Objects[idx])));
    end;
  finally
    FTraversalRef := nil;
    ByTypeUsage.Free;
    FCrit.Release;
  end;
end;

procedure TTracker.TraverseDeregister(Tree: TBinTree; Item: TBinTreeItem;
  Level: integer);
begin
  TTrackItem(Item).FItem.TrackerDeregister(self);
end;

procedure TTracker.ResetWithoutFree;
begin
  FCrit.Acquire;
  try
    FTree.Traverse(TraverseDeregister, btoInOrderLoHi);
    FTree.Free;
    FTree := TBinTree.Create;
  finally
    FCrit.Release;
  end;
end;

procedure TTracker.FreeTrackedClassesAndReset;
begin
  FCrit.Acquire;
  try
    while HasItems do
      (FTree.RootItem as TTrackItem).FItem.FreeSelfOnly(self);
  finally
    FCrit.Release;
  end;
end;

function TTracker.GetHasItems: boolean;
begin
  FCrit.Acquire;
  try
    result := FTree.HasItems;
  finally
    FCrit.Release;
  end;
end;

constructor TTracker.Create;
begin
  inherited;
  FCrit := TCriticalSection.Create;
  FTree := TBinTree.Create;
end;

destructor TTracker.Destroy;
begin
  FTree.Free;
  FCrit.Free;
  inherited;
end;

(************************************
 * TTrackable                        *
 ************************************)

{$IFOPT C+}
function _LGetMem(Size: NativeInt): Pointer;
begin
  if Size <= 0 then
    Exit(nil);
  Result := MemMgr.GetMem(Size);
  if Result = nil then
    Error(reOutOfMemory);
end;

function _LFreeMem(P: Pointer): Integer;
begin
  if P = nil then
    Exit(0);
  Result := MemMgr.FreeMem(P);
  if Result <> 0 then
    Error(reInvalidPtr);
end;

class function TTrackable.NewInstance: TObject;
begin
  Result := InitInstance(_LGetMem(InstanceSize));
{$IFDEF AUTOREFCOUNT}
  Result.FRefCount := 1;
{$ENDIF}
end;

procedure TTrackable.FreeInstance;
begin
  CleanupInstance;
  FillChar(Pointer(self)^, InstanceSize, $CD);
  _LFreeMem(Pointer(Self));
end;
{$ENDIF}

constructor TTrackable.Create;
begin
  inherited;
{$IFOPT C+}
{$IFDEF MAGIC_CHECKS}
  Magic := MAGIC1;
{$ENDIF}
  AppGlobalTracker.TrackInstance(self, true);
{$ENDIF}
end;

constructor TTrackable.CreateWithTracker(Tracker: TTracker);
begin
  inherited Create();
  FTracker := Tracker;
  FTracker.TrackInstance(self, true);
{$IFOPT C+}
{$IFDEF MAGIC_CHECKS}
  Magic := MAGIC2;
{$ENDIF}
  AppGlobalTracker.TrackInstance(self, true);
{$ENDIF}
end;

procedure TTrackable.TrackerDeregister(Tracker: TTracker);
begin
  Assert(FTracker = Tracker);
  if FTracker = Tracker then
    FTracker := nil;
end;

destructor TTrackable.Destroy;
begin
  if Assigned(FTracker) then
    FTracker.TrackInstance(self, false);
{$IFOPT C+}
{$IFDEF MAGIC_CHECKS}
  Assert((Magic = MAGIC1) or (Magic = MAGIC2));
{$ENDIF}
  AppGlobalTracker.TrackInstance(self, false);
{$ENDIF}
  inherited;
end;

function TTrackable.GetExtraInfoText: string;
begin
  result := S_EMPTY;
end;

destructor TTrackable.DestroySelfOnly(Sender: TTracker);
begin
  if Assigned(FTracker) then
    FTracker.TrackInstance(self, false);
{$IFOPT C+}
  AppGlobalTracker.TrackInstance(self, false);
{$ENDIF}
end;

procedure TTrackable.FreeSelfOnly(Sender: TTracker);
begin
  if Assigned(Self) then
    DestroySelfOnly(Sender);
end;

(**********************************************
 * TTrackedMemoryStream, TTrackedFileStream   *
 *********************************************)

{$IFDEF USE_TRACKABLES}
constructor TTrackedMemoryStream.Create;
begin
  inherited;
  FTrackProxy := TMemStreamTrackProxy.Create;
end;

destructor TTrackedMemoryStream.Destroy;
begin
  FTrackProxy.Free;
  inherited;
end;

constructor TTrackedFileStream.Create(const AFileName: string; Mode: Word);
begin
  inherited Create(AFileName, Mode);
  FTrackProxy := TFileStreamTrackProxy.Create;
end;

constructor TTrackedFileStream.Create(const AFileName: string; Mode: Word; Rights: Cardinal);
begin
  inherited Create(AFileName, Mode, Rights);
  FTrackProxy := TFileStreamTrackProxy.Create;
end;

destructor TTrackedFileStream.Destroy;
begin
  FTrackProxy.Free;
  inherited;
end;

{$ENDIF}

(************************************
 * TTrackItem                       *
 ************************************)

//Turn off overflow checking for pointer compare stuff.
{$OVERFLOWCHECKS OFF}
{$HINTS OFF}

function TTrackItem.Compare(Other: TBinTreeItem; AllowKeyDedupe: boolean): integer;
var
  OwnInt, OtherInt: Cardinal;
  Own64, Other64: UInt64;
begin
  if sizeof(Pointer) = sizeof(integer) then
  begin
    OwnInt := Cardinal(FItem);
    OtherInt := Cardinal(TTrackItem(Other).FItem);
    if OtherInt > OwnInt then
      result := 1
    else if OtherInt < OwnInt then
      result := -1
    else
      result := 0;
  end
  else if sizeof(Pointer) = sizeof(int64) then
  begin
    Own64 := UInt64(FItem);
    Other64 := UInt64(TTRackItem(Other).FItem);
    if Other64 > Own64 then
      result := 1
    else if Other64 < Own64 then
      result := -1
    else
      result := 0;
  end
  else
    Assert(false, S_POINTERS_ODD_SIZE);
end;

{$HINTS ON}

procedure TTrackItem.CopyFrom(Source: TBinTreeItem);
begin
  FItem := TTrackItem(Source).FItem;
end;

{$IFOPT C+}
var
  TmpStrings: TStrings;
  TmpName: string;
{$ENDIF}

initialization
{$IFOPT C+}
  GetMemoryManager(MemMgr);
  AppGlobalTracker := TTracker.Create;
{$ENDIF}
finalization
{$IFOPT C+}
  if AppGlobalTracker.HasItems then
  begin
    try
      Assert(false);
    except on Exception do ; end;
    TmpName := TPath.GetTempFileName();
    TmpStrings := AppGlobalTracker.GetTrackedDetails;
    try
      TmpStrings.SaveToFile(TmpName + '_memleaks.txt');
    except on Exception do ; end; //Hides a multitude of file save probs.
    TmpStrings.Free;
    DeleteFile(TmpName);
  end;
  AppGlobalTracker.Free;
  AppGlobalTracker := nil;
{$ENDIF}
end.

