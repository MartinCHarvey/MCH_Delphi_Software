unit StreamingSystem;
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
  This unit defines a base default streaming system in which the controller,
  reader and writer interact in a standard manner to produce reading and writing
  of datastructures to and from a stream.
}

{
  Rules for streaming arrays (which are different from the rest).

  When streaming, only zero length names are accepted for fields in the array.
  Additionally, everything streamed in the array must have the same major type.
  The streaming system inserts the ordinals (0,1,2,3) for the items in the
  array.

  Likewise, at unstream time, the count of items in the array is given, and
  names passed to the unstream functions should be zero length. The streaming
  system will then insert appropriate ordinals, and look for them.
}

interface

uses SysUtils, TypInfo, Classes, SSAbstracts, SSIntermediates, IndexedStore,
  Trackables;

type
  TDefaultSSController = class;
  TClassArray = array of TClass;
  TConstructorFunc = function(ClassType: TClass): TObject;
  TCustomMarshalProc = procedure(Obj: TObject; Sender: TDefaultSSController);

  THeirarchyInfo = record
    BaseClass: TClass;
    MemberClasses: TClassArray;
    ConstructHelper: TConstructorFunc;
    CustomMarshal: TCustomMarshalProc;
    CustomUnmarshal: TCustomMarshalProc;
    //Insert more plug in functions here for custom marshalling if need be.
  end;

  TStreamSystem = class(TAbstractStreamSystem)
  private
  protected
    procedure CreateParts; override;
    function GetFailOnNoClass: boolean;
    function GetFailOnNoProperty: boolean;
    function GetFailOnClassUnused: boolean;
    function GetFailOnPropertyUnused: boolean;
    procedure SetFailOnNoClass(NewFail: boolean);
    procedure SetFailOnNoProperty(NewFail: boolean);
    procedure SetFailOnClassUnused(NewFail: boolean);
    procedure SetFailOnPropertyUnused(NewFail: boolean);
  public
    function RegisterHeirarchy(const Heirarchy: THeirarchyInfo): boolean;
    property FailOnNoClass: boolean read GetFailOnNoClass write
      SetFailOnNoClass;
    property FailOnNoProperty: boolean read GetFailOnNoProperty write
      SetFailOnNoProperty;
    property FailOnClassUnused: boolean read GetFailOnClassUnused write
      SetFailOnClassUnused;
    property FailOnPropertyUnused: boolean read GetFailOnPropertyUnused write
      SetFailOnPropertyUnused;
  end;

  THeirarchyLookupItem = record
    ClassType: TClass;
    ClassName: string;
  end;
  PHeirarchyLookupItem = ^THeirarchyLookupItem;

  TControllerState = (csIdle, csStreaming, csUnstreaming);

  TDefaultSSController = class(TAbstractSSController)
  private
    FHeirarchyInfo: THeirarchyInfo;
    FHeirarchyRegistered: boolean;
    FHeirarchyLookup: TList;
    FIrep: TSSITransaction;
    FObjectStore: TIndexedStore; //Indexes stay in sync with FIRep Instances and InstanceData's
    FFieldContext: TSSIList; //Indicates the context in which the current
                             //fields are being streamed / unstreamed
                             // e.g: List of properties in object,
                             //      List of members in a record
                             //      List of items in an array.
    //Converting To IRep
    //Building From IRep
    FFailOnNoClass: boolean;
    FFailOnNoProperty: boolean;
    FFailOnClassUnused: boolean;
    FFailOnPropertyUnused: boolean;
    FState: TControllerState;
  protected
    procedure SetObjectStoreStreamIndexes;
    procedure ClearObjectStoreIndexes;
    procedure SetObjectStoreFinalUnstreamIndexes;

    procedure GetOrdLims(MType: TSSIMinorType; var DefaultMin: Int64; var
      DefaultMax: Int64);
    procedure ClearHeirarchyLookup;
    procedure AddHeirarchyLookupItem(ClassType: TClass);
    function FindClassTypeByName(ClassName: string): TClass;
    //Converting To IRep
    procedure CheckPropUniqueName(Name: string);
    procedure StreamPropertyNaming(var Name: string);
    function AddObjectToBeStreamed(Obj: TObject): integer;
    procedure StreamObjectProperties(Obj: TObject);
    procedure TIStreamOrdinal(Obj: TObject; PropInfo: PPropInfo; PropTypeInfo:
      PTypeInfo; PropTypeData: PTypeData);
    procedure TIStreamInt64(Obj: TObject; PropInfo: PPropInfo; PropTypeInfo:
      PTypeInfo; PropTypeData: PTypeData);
    procedure TIStreamFloat(Obj: TObject; PropInfo: PPropInfo; PropTypeInfo:
      PTypeInfo; PropTypeData: PTypeData);
    procedure TIStreamString(Obj: TObject; PropInfo: PPropInfo; PropTypeInfo:
      PTypeInfo; PropTypeData: PTypeData);
    procedure TIStreamClass(Obj: TObject; PropInfo: PPropInfo; PropTypeInfo:
      PTypeInfo; PropTypeData: PTypeData);
    procedure StreamOrdinal(Name: string; Val: Int64; MType: TSSIMinorType;
      Max, Min: Int64);
  //Building From IRep
    function FindObjectById(ObjId: integer; var Val: TObject): boolean;
    function UnstreamQuickLookup(ArrayIdx: integer): TSSIProperty;
    function UnstreamFindByName(Name: string): TSSIProperty;
    function UnstreamPropertyName(Name: string): TSSIProperty;
    procedure UnstreamPropertyError(Msg: string);
    procedure UnStreamObjectProperties(Obj: TObject; Idx: integer);
    procedure TIUnstreamOrdinal(Obj: TObject; PropInfo: PPropInfo; PropTypeInfo:
      PTypeInfo; PropTypeData: PTypeData);
    procedure TIUnstreamInt64(Obj: TObject; PropInfo: PPropInfo; PropTypeInfo:
      PTypeInfo; PropTypeData: PTypeData);
    procedure TIUnstreamFloat(Obj: TObject; PropInfo: PPropInfo; PropTypeInfo:
      PTypeInfo; PropTypeData: PTypeData);
    procedure TIUnstreamString(Obj: TObject; PropInfo: PPropInfo; PropTypeInfo:
      PTypeInfo; PropTypeData: PTypeData);
    procedure TIUnstreamClass(Obj: TObject; PropInfo: PPropInfo; PropTypeInfo:
      PTypeInfo; PropTypeData: PTypeData);
    function UnStreamOrdinal(Name: string; var Val: Int64; ExMType:
      TSSIMinorType; ExMax, ExMin: Int64): boolean;
    procedure CreateObjectInstances;
    //Clean normal should also check for unused objects and unused
    //properties.
    procedure SignalUnusedFlag(IObj: TSSIntermediate);
    procedure CheckUnusedFlags(IObj: TSSIntermediate);
    procedure CleanObjectInstancesNormal;
    procedure CleanObjectInstancesError;
    procedure ClearObjectStore;
  public
    constructor Create;
    destructor Destroy; override;
    function RegisterHeirarchy(const Heirarchy: THeirarchyInfo): boolean;
    //Procedure to let "user" i.e. streamable classes indicate warnings or
    //errors. If the severity is "error" then streaming or unstreaming is
    //aborted.
    procedure UserSignalError(Severity: TSSSeverity; Msg: string);
    //Converting To IRep
    function ConvertStructureToIRep(Struct: TObject): TSSITransaction; override;
    //Helper functions to stream some data given the current context.
    procedure StreamUByte(Name: string; Val: byte);
    procedure StreamSByte(Name: string; Val: shortint);
    procedure StreamUWord(Name: string; Val: word);
    procedure StreamSWord(Name: string; Val: smallint);
    procedure StreamSLong(Name: string; Val: longint);
    procedure StreamULong(Name: string; Val: cardinal);
    procedure StreamEnum(Name: string; Val: longint);
    procedure StreamBool(Name: string; Val: boolean);
    procedure StreamInt64(Name: string; Val: Int64);
    procedure StreamUInt64(Name: string; Val: UInt64);
    procedure StreamDouble(Name: string; Val: double);
    procedure StreamFloat(Name: string; MType: TSSIMinorType; Val: extended);
    procedure StreamString(Name: string; MType: TSSIMinorType; MaxLen: longint;
      Val: string);
    procedure StreamClass(Name: string; Val: TObject);
    procedure StreamRecordStart(Name: string);
    procedure StreamRecordEnd(Name: string);
    procedure StreamArrayStart(Name: string);
    procedure StreamArrayEnd(Name: string);
    procedure StreamBlob(Name: string; Len: UInt64; Val: Pointer);
    //Converting From IRep
    function UnstreamUByte(Name: string; var Val: byte): boolean;
    function UnstreamSByte(Name: string; var Val: shortint): boolean;
    function UnstreamUWord(Name: string; var Val: word): boolean;
    function UnstreamSWord(Name: string; var Val: smallint): boolean;
    function UnstreamSLong(Name: string; var Val: longint): boolean;
    function UnstreamULong(Name: string; var Val: cardinal): boolean;
    function UnstreamEnum(Name: string; var Val: longint): boolean;
    function UnstreamBool(Name: string; var Val: boolean): boolean;
    function UnstreamInt64(Name: string; var Val: Int64): boolean;
    function UnstreamUInt64(Name: string; var Val: UInt64): boolean;
    function UnstreamDouble(Name: string; var Val: double): boolean;
    function UnstreamFloat(Name: string; MType: TSSIMinorType; var Val:
      extended): boolean;
    function UnstreamString(Name: string; MType: TSSIMinorType; MaxLen: longint;
      var Val: string): boolean;
    function UnstreamClass(Name: string; var Val: TObject): boolean;
    function UnstreamRecordStart(Name: string): boolean;
    function UnstreamRecordEnd(Name: string): boolean;
    function UnstreamArrayStart(Name: string; var Count: integer): boolean;
    function UnstreamArrayEnd(Name: string): boolean;
    function UnstreamBlob(Name: string; ExpectedLen: UInt64; var Val:TStream): boolean;
    function BuildStructureFromIRep(IRep: TSSITransaction): TObject; override;
    //Helper functions to unstream some data given the current context.
    //And some properties for unstream behaviour.
    property FailOnNoClass: boolean read FFailOnNoClass write FFailOnNoClass;
    property FailOnNoProperty: boolean read FFailOnNoProperty write
      FFailOnNoProperty;
    property FailOnClassUnused: boolean read FFailOnClassUnused write
      FFailOnClassUnused;
    property FailOnPropertyUnused: boolean read FFailOnPropertyUnused write
      FFailOnPropertyUnused;
  end;

  //Classes for speed optimization of streaming / unstreaming process.
{$IFDEF USE_TRACKABLES}
  TObjectCrossRef = class(TTrackable)
{$ELSE}
  TObjectCrossRef = class
{$ENDIF}
  public
    //Used both for streaming and unstreaming.
    FInstance: TObject;
    FIRepIndex: integer;
    FUsed: boolean;
    FUnstreamed: boolean;
    function UnstreamReady: boolean;
  end;

  //DupVal because in the unstream case, can have multiple classes unstream to NULL.
  TInstanceINode = class(TDuplicateValIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  end;

  TIRepIndexINode = class(TIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  end;

  TUnstreamNextINode = class(TDuplicateValIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  end;

  TInstanceSearchVal = class(TInstanceINode)
  protected
    FSearchVal: TObject;
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  public
    property SearchVal: TObject read FSearchVal write FSearchVal;
  end;

  TIRepIndexSearchVal = class(TIRepIndexINode)
  protected
    FSearchVal: integer;
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  public
    property SearchVal: integer read FSearchVal write FSearchVal;
  end;

  //TUnstreamNextSearchVal probably not required, but debug first/last functions carefully.

implementation


const
  IndexInstancePtr = 0;
  IndexInstanceId = 1;
  IndexUnstreamNext = 2;

  OBJ_ID_UNASSIGNED = -1;

  S_INVALID_OBJECT = 'Cannot stream this object (not in heirarchy)';
  S_NO_HEIRARCHY = 'Cannot perform action (no heirarchy registered)';
  S_HEIRARCHY_REQUIRED_FIELD_EMPTY =
    'The heirarchy to be registered is missing one or more required fields.';
  S_CLASS_TO_BE_REGISTERED_NIL =
    'The heirarchy contains a reference to a NIL class. Can''t register.';
  S_DUPLICATE_CLASSTYPE_IN_HEIRARCHY =
    'Duplicate class type in heirarchy (ignored).';
  S_STREAMING_OBJ_NOT_REGISTERED =
    'Streaming an object not registered for construction. May have problems ' +
    'reading the created stream.';
  S_UNTERM_REC_OR_ARRAY = 'Streaming: unterminated record or array.';
  S_UNSUPP_PROP_NAME = 'Streaming: unsupported property, name: ';
  S_UNSUPP_PROP_TYPE = ', type: ';
  S_UNSUPP_PROP_SUBTYPE = ', subtype: ';
  S_STRING_INTERNAL = 'Internal error: string: unsupported minor type';
  S_FLOAT_INTERNAL = 'Internal error: float: unsupported minor type';
  S_SHORT_STRING_TOO_LONG = 'Streaming short string, but may be too long';
  S_CLASS_NOT_STREAMED_BAD_TYPE =
    'Skipped an object because it''s the wrong type.';
  S_END_RECORD_IMPOSSIBLE = 'Can''t end the record, because we''re not in one!';
  S_END_RECORD_WRONG_NAME =
    'Can''t end the record, because the current record has a different name.';
  S_END_RECORD_CORRUPTED =
    'Can''t end the record because it''s parent is not a list. Corruption?';
  S_ORDINAL_UNKNOWN_TYPE =
    'Can''t stream the ordinal because it''s an unknown type.';
  S_STREAM_PROP_DUPLICATE_NAME = 'Cannot stream property (duplicate name).';
  S_ARRAY_ITEM_HAS_NAME = 'Cannot stream array item (property name supplied)';
  S_END_ARRAY_IMPOSSIBLE = 'Can''t end the array, because we''re not in one!';
  S_END_ARRAY_WRONG_NAME =
    'Can''t end the array, because the current array has a different name.';
  S_END_ARRAY_CORRUPTED =
    'Can''t end the array because it''s parent is not a list. Corruption?';
  S_INVALID_IREP =
    'Internal error - invalid intermediate representation';
  S_CREATE_HELPER_FAILED =
    'Class creation helper failed for class type: ';
  S_CLASS_TYPE_NOT_REGISTERED =
    'Class type not registered for creation: ';
  S_MISSING_INSTANCE_DATA =
    'Couldn''t find instance data for class. Invalid intermediate representation?';
  S_UNSTREAM_ORDINAL_TYPE_MISMATCH =
    'Cannot unstream ordinal, type mismatch';
  S_ORDINAL_LIMITS_DIFFER =
    'Limits on ordinal type are not as expected, ordinal may not unstream.';
  S_UNSTREAM_ORDINAL_OUT_OF_RANGE =
    'Cannot unstream ordinal type: value is out of range.';
  S_NO_PROPERTY_NAMED =
    'Couldn''t find a property named: ';
  S_UNSTREAM_ARRAY_ITEM_HAS_NAME =
    'Cannot unstream array item (property name supplied)';
  S_UNSTREAM_ARRAY_OUT_OF_BOUNDS =
    'Tried to unstream more items in an array than it contains.';
  S_UNSTREAM_INT64_TYPE_MISMATCH =
    'Cannot unstream Int64, type mismatch.';
  S_UNSTREAM_UINT64_TYPE_MISMATCH =
    'Cannot unstream UInt64, type mismatch.';
  S_UNSTREAM_FLOAT_TYPE_MISMATCH =
    'Cannot unstream float, type mismatch.';
  S_UNSTREAM_STRING_TYPE_MISMATCH =
    'Cannot unstream string, type mismatch.';
  S_STRING_LENGTHS_DIFFER =
    'Allowable string lengths are not as expected, string may not unstream';
  S_UNSTREAM_STRING_TOO_LONG =
    'Cannot unstream string: string data too long for recipient.';
  S_COULDNT_UNSTREAM_CLASS_NO_ID =
    'Cannot unstream class reference: ID not found.';
  S_UNSTREAM_CLASS_TYPE_MISMATCH =
    'Cannot unstream class, type mismatch';
  S_UNSTREAM_BLOB_TYPE_MISMATCH =
    'Cannnot unstream blob, type mismatch';
  S_UNSTREAM_RECORD_TYPE_MISMATCH =
    'Cannot unstream record, type mismatch';
  S_UNSTREAM_RECORD_NO_ITEMS =
    'Cannot unstream record, no items found.';
  S_UNSTREAM_END_RECORD_IMPOSSIBLE =
    'Cannot unstream end record, because we''re not in one.';
  S_UNSTREAM_END_RECORD_WRONG_NAME =
    'Cannot unstream end record, because the record doesn''t have this name';
  S_UNSTREAM_END_RECORD_CORRUPTED =
    'Cannot unstream end record, parent not list. Corruption?';
  S_UNSTREAM_ARRAY_TYPE_MISMATCH =
    'Cannot unstream array, type mismatch';
  S_UNSTREAM_ARRAY_NO_ITEMS =
    'Cannot unstream array, no items found.';
  S_UNSTREAM_END_ARRAY_IMPOSSIBLE =
    'Cannot unstream end array, because we''re not in one.';
  S_UNSTREAM_END_ARRAY_WRONG_NAME =
    'Cannot unstream end array, because the record doesn''t have this name';
  S_UNSTREAM_END_ARRAY_CORRUPTED =
    'Cannot unstream end array, parent not list. Corruption?';
  S_UNKNOWN_IREP_CHECKING_USED_FLAGS =
    'Discovered unknown classtype in IRep when checking used flags.';
  S_CLASS_UNUSED = 'Unused class, type: ';
  S_PROPERTY_UNUSED = 'Unused property, name: ';
  S_LIMIT_INTERNAL = 'Internal error determining ordinal limits';
  S_NOT_STREAMING = 'Cannot invoke a stream function when not streaming';
  S_NOT_UNSTREAMING = 'Cannot invoke an unstream function when not unstreaming';
  S_USER_ERROR = 'Error signalled by streamable classes: ';
  S_STRING_UNICODE_CONVERTED = 'Implicit conversion between long and unicode strings.';
  S_CANNOT_STREAM_NIL_BLOBS = 'Cannot stream nil blob objects';
  S_BLOB_TOO_LARGE = 'Cannot stream blob larger than 2GB.';
  S_UNSTREAM_BLOB_TOO_SMALL = 'Blob data less than expected, not unstreaming.';
  S_BLOB_MORE_DATA = 'Blob data more than expected! Still unstreaming.';

  KindNames: array[TTypeKind] of string =
    ('tkUnknown', 'tkInteger', 'tkChar', 'tkEnumeration', 'tkFloat',
    'tkString', 'tkSet', 'tkClass', 'tkMethod', 'tkWChar', 'tkLString',
    'tkWString', 'tkVariant', 'tkArray', 'tkRecord', 'tkInterface', 'tkInt64',
    'tkDynArray' , 'tkUString', 'tkClassRef', 'tkPointer', 'tkProcedure');

  S_UINT64_TYPINFO_NAME = 'UInt64';
  S_INT64_TYPINFO_NAME = 'Int64';

(************************************
 * Misc Functions                   *
 ************************************)

function SymbolNameToString(const SymName: TSymbolName): string;
begin
  result := string(SymName);
end;

(************************************
 * TStreamSystem                    *
 ************************************)

procedure TStreamSystem.CreateParts;
begin
  Controller := TDefaultSSController.Create;
  //No inherited call (base is abstract).
end;

function TStreamSystem.RegisterHeirarchy
  (const Heirarchy: THeirarchyInfo): boolean;
begin
  result := TDefaultSSController(Controller).RegisterHeirarchy(Heirarchy);
end;

function TStreamSystem.GetFailOnNoClass: boolean;
begin
  result := TDefaultSSController(Controller).FailOnNoClass;
end;

function TStreamSystem.GetfailOnNoProperty: boolean;
begin
  result := TDefaultSSController(Controller).FailOnNoProperty;
end;

function TStreamSystem.GetFailOnClassUnused: boolean;
begin
  result := TDefaultSSController(Controller).FailOnClassUnused;
end;

function TStreamSystem.GetFailOnPropertyUnused: boolean;
begin
  result := TDefaultSSController(Controller).FailOnPropertyUnused;
end;

procedure TStreamSystem.SetFailOnNoClass(NewFail: boolean);
begin
  TDefaultSSController(Controller).FailOnNoClass := NewFail;
end;

procedure TStreamSystem.SetFailOnNoProperty(NewFail: boolean);
begin
  TDefaultSSController(Controller).FailOnNoProperty := NewFail;
end;

procedure TStreamSystem.SetFailOnClassUnused(NewFail: boolean);
begin
  TDefaultSSController(Controller).FailOnClassUnused := NewFail;
end;

procedure TStreamSystem.SetFailOnPropertyUnused(NewFail: boolean);
begin
  TDefaultSSController(Controller).FailOnPropertyUnused := NewFail;
end;

(************************************
 * TDefaultSSController             *
 ************************************)

constructor TDefaultSSController.Create;
begin
  inherited;
  FObjectStore := TIndexedStore.Create;
  FObjectStore.AddIndex(TInstanceINode, IndexInstancePtr);
  FObjectStore.AddIndex(TIRepIndexINode, IndexInstanceId);
  FObjectStore.AddIndex(TUnstreamNextINode, IndexUnstreamNext);
  FHeirarchyLookup := TList.Create;
end;

procedure TDefaultSSController.ClearObjectStore;
var
  IRec, NextIRec: TItemRec;
  Item: TObject;
begin
  //Remove all indexes before removing items.
  ClearObjectStoreIndexes;
  IRec := FObjectStore.GetAnItem;
  while Assigned(IRec) do
  begin
    Item := IRec.Item;
    NextIRec := IRec;
    FObjectStore.GetAnotherItem(NextIRec);
    FObjectStore.RemoveItem(IRec);
    Item.Free;
    IRec := NextIRec;
  end;
end;

destructor TDefaultSSController.Destroy;
begin
  Assert(FObjectStore.Count = 0);
  FObjectStore.Free;
  ClearHeirarchyLookup;
  FHeirarchyLookup.Free;
  inherited;
end;

procedure TDefaultSSController.GetOrdLims(MType: TSSIMinorType; var DefaultMin:
  Int64; var DefaultMax: Int64);
begin
  case MType of
    mitOSByte:
      begin
        DefaultMin := Low(shortint);
        DefaultMax := High(shortint);
      end;
    mitOUByte:
      begin
        DefaultMin := Low(byte);
        DefaultMax := High(byte);
      end;
    mitOSWord:
      begin
        DefaultMin := Low(smallint);
        DefaultMax := High(smallint);
      end;
    mitOUWord:
      begin
        DefaultMin := Low(word);
        DefaultMax := High(word);
      end;
    mitOSLong:
      begin
        DefaultMin := Low(longint);
        DefaultMax := High(Longint);
      end;
    mitOULong:
      begin
        DefaultMin := Low(cardinal);
        DefaultMax := High(cardinal);
      end;
    mitOS64:
      begin
        DefaultMin := Low(Int64);
        DefaultMax := High(Int64);
      end;
    mitOU64:
      begin
        DefaultMin := Int64(Low(UInt64));
        DefaultMax := Int64(High(UInt64));
      end;
    mitSet:
      begin
        DefaultMin := Low(longint);
        DefaultMax := High(Longint);
      end;
  else
    raise EStreamSystemError.Create(S_LIMIT_INTERNAL);
  end;
end;

function TDefaultSSController.RegisterHeirarchy(const Heirarchy:
  THeirarchyInfo): boolean;
var
  Idx: integer;
begin
  result := Assigned(Heirarchy.BaseClass) and
    (Length(Heirarchy.MemberClasses) > 0) and
    Assigned(Heirarchy.ConstructHelper) and
    Assigned(Heirarchy.CustomMarshal) and
    Assigned(Heirarchy.CustomUnmarshal);
  if not Result then
    LogEvent(sssError, S_HEIRARCHY_REQUIRED_FIELD_EMPTY)
  else
  begin
    try
      ClearHeirarchyLookup;
      for Idx := 0 to Pred(Length(Heirarchy.MemberClasses)) do
        AddHeirarchyLookupItem(Heirarchy.MemberClasses[Idx]);
    except
      on E: EStreamSystemError do
      begin
        ClearHeirarchyLookup;
        result := false;
        LogEvent(sssError, E.Message);
      end;
    end;
  end;
  if result then
    FHeirarchyInfo := Heirarchy;
  FHeirarchyRegistered := result;
end;

procedure TDefaultSSController.UserSignalError(Severity: TSSSeverity; Msg:
  string);
begin
  if Severity = sssError then
    raise EStreamSystemError.Create(S_USER_ERROR + Msg)
  else
    LogEvent(Severity, S_USER_ERROR + Msg);
end;

procedure TDefaultSSController.ClearHeirarchyLookup;
var
  Idx: integer;
  Item: PHeirarchyLookupItem;
begin
  for Idx := 0 to Pred(FHeirarchyLookup.Count) do
  begin
    Item := PHeirarchyLookupItem(FHeirarchyLookup.Items[Idx]);
    Dispose(Item);
  end;
  FHeirarchyLookup.Clear;
end;

procedure TDefaultSSController.AddHeirarchyLookupItem(ClassType: TClass);
var
  Idx: integer;
  Item: PHeirarchyLookupItem;
begin
  //Check valid.
  if not Assigned(ClassType) then
    raise EStreamSystemError.Create(S_CLASS_TO_BE_REGISTERED_NIL);
  //Check not already registered in heirarchy.
  for Idx := 0 to Pred(FHeirarchyLookup.Count) do
  begin
    Item := PHeirarchyLookupItem(FHeirarchyLookup.Items[Idx]);
    if Item.ClassType = ClassType then
    begin
      LogEvent(sssWarning, S_DUPLICATE_CLASSTYPE_IN_HEIRARCHY);
      exit;
    end;
  end;
  //OK, not already registered.
  New(Item);
  Item.ClassType := ClassType;
  Item.ClassName := ClassType.ClassName;
  FHeirarchyLookup.Add(Item);
end;

function TDefaultSSController.FindClassTypeByName(ClassName: string): TClass;
var
  Idx: integer;
  Item: PHeirarchyLookupItem;
begin
  result := nil;
  for Idx := 0 to Pred(FheirarchyLookup.Count) do
  begin
    Item := PHeirarchyLookupItem(FHeirarchyLookup.Items[Idx]);
    if CompareText(Item.ClassName, ClassName) = 0 then
    begin
      result := Item.ClassType;
      exit;
    end;
  end;
end;

procedure TDefaultSSController.StreamOrdinal(Name: string; Val: Int64; MType:
  TSSIMinorType; Max, Min: Int64);
var
  OrdProp: TSSIProperty;
  DefaultMin, DefaultMax: Int64;
begin
  StreamPropertyNaming(Name);
  OrdProp := TSSIProperty.Create;
  FFieldContext.Add(OrdProp);
  GetOrdLims(MType, DefaultMin, DefaultMax);
  with OrdProp.PropData do
  begin
    PropName := Name;
    PropType := sMajOrd;
    PropSubType := MType;
    OrdData := Val;
    LimsApply := (Min <> DefaultMin) or (Max <> DefaultMax);
    if LimsApply then
    begin
      MinVal := Min;
      MaxVal := Max;
    end;
  end;
end;

procedure TDefaultSSController.StreamUByte(Name: string; Val: byte);
begin
  StreamOrdinal(Name, Val, mitOUByte, High(byte), Low(byte));
end;

procedure TDefaultSSController.StreamSByte(Name: string; Val: shortint);
begin
  StreamOrdinal(Name, Val, mitOSByte, High(Shortint), Low(Shortint));
end;

procedure TDefaultSSController.StreamUWord(Name: string; Val: word);
begin
  StreamOrdinal(Name, Val, mitOUWord, High(word), Low(Word));
end;

procedure TDefaultSSController.StreamSWord(Name: string; Val: smallint);
begin
  StreamOrdinal(Name, Val, mitOSword, High(smallint), Low(smallint));
end;

procedure TDefaultSSController.StreamSLong(Name: string; Val: longint);
begin
  StreamOrdinal(Name, Val, mitOSLong, High(longint), Low(longint));
end;

procedure TDefaultSSController.StreamULong(Name: string; Val: cardinal);
begin
  StreamOrdinal(Name, Val, mitOULong, High(cardinal), Low(cardinal));
end;

procedure TDefaultSSController.StreamEnum(Name: string; Val: longint);
begin
  StreamOrdinal(Name, Val, mitOSLong, High(longint), Low(longint));
end;

procedure TDefaultSSController.StreamBool(Name: string; Val: boolean);
begin
  StreamOrdinal(Name, Ord(Val), mitOUbyte, Ord(High(boolean)),
    Ord(Low(boolean)));
end;

procedure TDefaultSSController.StreamInt64(Name: string; Val: Int64);
begin
  StreamOrdinal(Name, Val, mitOS64, High(Int64), Low(Int64));
end;

procedure TDefaultSSController.StreamUInt64(Name: string; Val: UInt64);
begin
  StreamOrdinal(Name, Int64(Val), mitoU64, Int64(High(Uint64)), Int64(Low(Uint64)));
end;

procedure TDefaultSSController.StreamDouble(Name: string; Val: double);
begin
  StreamFloat(Name, mitFDouble, Val);
end;

procedure TDefaultSSController.StreamFloat(Name: string; MType: TSSIMinorType;
  Val: extended);
var
  FloatProp: TSSIProperty;
begin
  StreamPropertyNaming(Name);
  FloatProp := TSSIProperty.Create;
  FFieldContext.Add(FloatProp);
  with FloatProp.PropData do
  begin
    PropName := Name;
    PropType := sMajFloat;
    PropSubType := MType;
    FloatData := Val;
  end;
end;

procedure TDefaultSSController.StreamString(Name: string; MType: TSSIMinorType;
  MaxLen: longint; Val: string);
var
  StringProp: TSSIProperty;
begin
  StreamPropertyNaming(Name);
  StringProp := TSSIProperty.Create;
  FFieldContext.Add(StringProp);
  with StringProp.PropData do
  begin
    if (MType = mitShortString) and (Length(Val) > MaxLen) then
      LogEvent(sssWarning, S_SHORT_STRING_TOO_LONG);
    PropName := Name;
    PropType := sMajStr;
    PropSubType := MType;
    MaxStrLen := MaxLen;
    StrData := Val;
  end;
end;

procedure TDefaultSSController.StreamClass(Name: string; Val: TObject);
var
  ClassProp: TSSIProperty;
  ObjRefID: integer;
begin
  StreamPropertyNaming(Name);
  try
    ObjRefID := AddObjectToBeStreamed(Val);
  except
    on EStreamSystemError do
    begin
      LogEvent(sssWarning, S_CLASS_NOT_STREAMED_BAD_TYPE);
      ObjRefID := OBJ_ID_UNASSIGNED;
    end;
  end;
  ClassProp := TSSIProperty.Create;
  FFieldContext.Add(ClassProp);
  with ClassProp.PropData do
  begin
    PropName := Name;
    PropType := sMajClass;
    PropSubType := mitNone;
    ObjId := ObjRefId;
  end;
end;

procedure TDefaultSSController.StreamBlob(Name: string; Len: UInt64; Val: Pointer);
var
  BlobProp: TSSIProperty;
begin
  StreamPropertyNaming(Name);
  if not Assigned(Val) then
    raise EStreamSystemError.Create(S_CANNOT_STREAM_NIL_BLOBS);
  if Len > High(Longint) then
    raise EStreamSystemError.Create(S_BLOB_TOO_LARGE);
  BlobProp := TSSIProperty.Create;
  FFieldContext.Add(BlobProp);
  with BlobProp.PropData do
  begin
    PropName := Name;
    PropType := sMajBlob;
    PropSubType := mitNone;
    BlobData := TMemoryStream.Create;
    BlobData.WriteBuffer(Val^, Len);
  end;
end;

procedure TDefaultSSController.StreamRecordStart(Name: string);
var
  RecordProp: TSSIProperty;
begin
  StreamPropertyNaming(Name);
  RecordProp := TSSIProperty.Create;
  FFieldContext.Add(RecordProp);
  with RecordProp.PropData do
  begin
    PropName := Name;
    PropType := sMajRec;
    PropSubType := mitNone;
    Items := TSSIList.Create;
    Items.Parent := RecordProp;
    FFieldContext := Items;
  end;
end;

procedure TDefaultSSController.StreamRecordEnd(Name: string);
var
  RecordProp: TSSIProperty;
begin
  if not (Assigned(FFieldContext.Parent)
    and (FFieldContext.Parent is TSSIProperty))
    and (TSSIProperty(FFieldContext.Parent).PropData.PropType = sMajRec) then
    raise EStreamSystemError.Create(S_END_RECORD_IMPOSSIBLE);
  RecordProp := TSSIProperty(FFieldContext.Parent);
  if CompareText(RecordProp.PropData.PropName, Name) <> 0 then
    raise EStreamSystemError.Create(S_END_RECORD_WRONG_NAME);
  if not (Assigned(RecordProp.Parent)
    and (RecordProp.Parent is TSSIList)) then
    raise EStreamSystemError.Create(S_END_RECORD_CORRUPTED);
  FFieldContext := TSSIList(RecordProp.Parent);
end;

procedure TDefaultSSController.StreamArrayStart(Name: string);
var
  ArrayProp: TSSIProperty;
begin
  StreamPropertyNaming(Name);
  ArrayProp := TSSIProperty.Create;
  FFieldContext.Add(ArrayProp);
  with ArrayProp.PropData do
  begin
    PropName := Name;
    PropType := sMajArray;
    PropSubType := mitNone;
    Items := TSSIList.Create;
    Items.Parent := ArrayProp;
    FFieldContext := Items;
  end;
end;

procedure TDefaultSSController.StreamArrayEnd(Name: string);
var
  ArrayProp: TSSIProperty;
begin
  if not (Assigned(FFieldContext.Parent)
    and (FFieldContext.Parent is TSSIProperty))
    and (TSSIProperty(FFieldContext.Parent).PropData.PropType = sMajArray) then
    raise EStreamSystemError.Create(S_END_ARRAY_IMPOSSIBLE);
  ArrayProp := TSSIProperty(FFieldContext.Parent);
  if CompareText(ArrayProp.PropData.PropName, Name) <> 0 then
    raise EStreamSystemError.Create(S_END_ARRAY_WRONG_NAME);
  if not (Assigned(ArrayProp.Parent)
    and (ArrayProp.Parent is TSSIList)) then
    raise EStreamSystemError.Create(S_END_ARRAY_CORRUPTED);
  FFieldContext := TSSIList(ArrayProp.Parent);
end;

procedure TDefaultSSController.CheckPropUniqueName(Name: string);
var
  Idx: integer;
  ExistName: string;
begin
  for Idx := 0 to Pred(FFieldContext.Count) do
  begin
    ExistName := TSSIProperty(FFieldContext.Items[Idx]).PropData.PropName;
    if CompareText(Name, ExistName) = 0 then
      raise EStreamSystemError.Create(S_STREAM_PROP_DUPLICATE_NAME);
  end;
end;

procedure TDefaultSSController.StreamPropertyNaming(var Name: string);
var
  StreamingArray: boolean;
begin
  if FState <> csStreaming then
    raise EStreamSystemError.Create(S_NOT_STREAMING);
  StreamingArray := false;
  if Assigned(FFieldContext.Parent)
    and (FFieldContext.Parent is TSSIProperty) then
    StreamingArray := (TSSIProperty(FFieldContext.Parent).PropData.PropType =
      sMajArray);
  if not StreamingArray then
    CheckPropUniqueName(Name)
  else
  begin
    //TODO - check all items in an array are of the same major
    //and minor types.
    //Need to check given name is null, and invent a name.
    if Length(Name) > 0 then
      raise EStreamSystemError.Create(S_ARRAY_ITEM_HAS_NAME);
    //OK, now set the name to the appropriate index.
    Name := IntToStr(FFieldContext.Count);
  end;
end;

procedure TDefaultSSController.TIStreamOrdinal(Obj: TObject; PropInfo:
  PPropInfo; PropTypeInfo: PTypeInfo; PropTypeData: PTypeData);
var
  Data: longint;
  MinorType: TSSIMinorType;
  MinVal, MaxVal: Int64;
begin
  case PropTypeData.OrdType of
    otSByte: MinorType := mitOSByte;
    otUByte: MinorType := mitOUByte;
    otSWord: MinorType := mitOSWord;
    otUWord: MinorType := mitOUWord;
    otSLong: MinorType := mitOSLong;
    otULong: Minortype := mitOULong;
  else
    LogEvent(sssWarning, S_ORDINAL_UNKNOWN_TYPE);
    exit;
  end;
  if PropTypeInfo.Kind in [tkInteger, tkChar, tkEnumeration, tkWChar] then
  begin
    if PropTypeData.OrdType <> otULong then
    begin
      MinVal := PropTypeData.MinValue;
      MaxVal := PropTypeData.MaxValue;
    end
    else
    begin
      MinVal := Cardinal(PropTypeData.MinValue);
      MaxVal := Cardinal(PropTypeData.MaxValue);
    end;
  end
  else
  begin
    //TODO - Concerned about size checking for sets - need to check
    //ordinality of base type?
    MinorType := mitSet;
    MinVal := Low(longint);
    MaxVal := High(Longint);
  end;
  Data := GetOrdProp(Obj, PropInfo);
  StreamOrdinal(SymbolNameToString(PropInfo.Name), Data, MinorType, MaxVal, MinVal);
end;

procedure TDefaultSSController.TIStreamInt64(Obj: TObject; PropInfo: PPropInfo;
  PropTypeInfo: PTypeInfo; PropTypeData: PTypeData);
var
  Data: Int64;
begin
  Data := GetInt64Prop(Obj, PropInfo);
  if PropTypeInfo.Name = S_UINT64_TYPINFO_NAME then
    StreamUInt64(SymbolNameToString(PropInfo.Name), Uint64(Data))
  else
  begin
    Assert(PropTypeInfo.Name = S_INT64_TYPINFO_NAME);
    StreamInt64(SymbolNameToString(PropInfo.Name), Data);
  end;
end;

procedure TDefaultSSController.TIStreamFloat(Obj: TObject; PropInfo: PPropInfo;
  PropTypeInfo: PTypeInfo; PropTypeData: PTypeData);
var
  Data: extended;
  MinorType: TSSIMinorType;
begin
  Data := GetFloatProp(Obj, PropInfo);
  Assert(ProptypeInfo.Kind = tkFloat);
  case PropTypeData.FloatType of
    ftSingle: MinorType := mitFSingle;
    ftDouble: Minortype := mitFDouble;
    ftExtended: Minortype := mitFExtended;
  else
    raise EStreamSystemError.Create(S_FLOAT_INTERNAL);
  end;
  StreamFloat(SymbolNameToString(PropInfo.Name), MinorType, Data);
end;

procedure TDefaultSSController.TIStreamString(Obj: TObject; PropInfo: PPropInfo;
  PropTypeInfo: PTypeInfo; PropTypeData: PTypeData);
var
  Data: string;
begin
  Data := GetStrProp(Obj, PropInfo);
  case PropTypeInfo.Kind of
    tkLString: StreamString(SymbolNameToString(PropInfo.Name), mitAnsiString, 0, Data);
    tkString: StreamString(SymbolNameToString(PropInfo.Name), mitShortString,
        PropTypeData.MaxLength, Data);
    tkUString: StreamString(SymbolNameToString(PropInfo.Name), mitUnicodeString, 0, Data);
  else
    raise EStreamSystemError.Create(S_STRING_INTERNAL);
  end;
end;


procedure TDefaultSSController.TIStreamClass(Obj: TObject; PropInfo: PPropInfo;
  PropTypeInfo: PTypeInfo; PropTypeData: PTypeData);
var
  Data: TObject;
begin
  Data := GetObjectProp(Obj, PropInfo, FHeirarchyInfo.BaseClass);
  StreamClass(SymbolNameToString(PropInfo.Name), Data);
end;

procedure TDefaultSSController.StreamObjectProperties(Obj: TObject);
var
  Inst: TSSIInstanceData;
  ObjTypeInfo: PTypeInfo;
  PropCount, PropIdx: integer;
  PropList: PPropList;
  PropInfo: PPropInfo;
  PropTypeInfo: PTypeInfo;
  PropTypeData: PTypeData;
begin
  Inst := TSSIInstanceData.Create;
  FIRep.InstancesData.Add(Inst);
  FFieldContext := Inst.Properties;
  //Get RTTI for object.
  ObjTypeInfo := PTypeInfo(Obj.ClassInfo);
  Assert(ObjTypeInfo.Kind = tkClass);
  Assert(CompareText(SymbolNameToString(ObjTypeInfo.Name), Obj.ClassName) = 0);
  //Get property list for object.
  PropCount := GetPropList(ObjTypeInfo, tkAny, nil);
  if PropCount > 0 then
  begin
    GetMem(PropList, PropCount * sizeof(pointer));
    try
      GetPropList(ObjTypeInfo, tkAny, PropList);
      //Property list now filled in with PPropInfo
      for PropIdx := 0 to Pred(PropCount) do
      begin
        //Deal with each property in turn.
        PropInfo := GetPropInfo(ObjTypeInfo,
          SymbolNameToString(PropList[PropIdx].Name));
        PropTypeInfo := PropInfo.PropType^;
        PropTypeData := GetTypeData(PropTypeInfo);
        //OK, now have just about everything we need to actually
        //stream the data.
        case PropTypeInfo.Kind of
          tkInteger, tkChar, tkEnumeration, tkSet, tkWChar:
            TIStreamOrdinal(Obj, PropInfo, PropTypeInfo, PropTypeData);
          tkInt64: TIStreamInt64(Obj, PropInfo, PropTypeInfo, PropTypeData);
          tkFloat: TIStreamFloat(Obj, PropInfo, PropTypeInfo, PropTypeData);
          tkString, tkLString, tkUString:
            TIStreamString(Obj, PropInfo, PropTypeInfo, PropTypeData);
          tkClass: TIStreamClass(Obj, PropInfo, PropTypeInfo, PropTypeData);
        else
          LogEvent(sssWarning, S_UNSUPP_PROP_NAME
            + SymbolNameToString(PropInfo.Name)
            + S_UNSUPP_PROP_TYPE
            + KindNames[PropTypeInfo.Kind]);
        end;
      end;
    finally
      FreeMem(PropList);
    end;
  end;
  //Do custom streaming stuff as required.
  FHeirarchyInfo.CustomMarshal(Obj, self);
  if FFieldContext <> Inst.Properties then
    raise EStreamSystemError.Create(S_UNTERM_REC_OR_ARRAY);
end;

function TDefaultSSController.AddObjectToBeStreamed(Obj: TObject): integer;
var
  NewInst: TSSIInstance;
  ObjectSearchVal: TInstanceSearchVal;
  RV: TIsRetVal;
  IRec: TItemRec;
  XRef: TObjectCrossRef;
begin
  if Assigned(Obj) and (not (Obj is FHeirarchyInfo.BaseClass)) then
    raise EStreamSystemError.Create(S_INVALID_OBJECT);
  if not Assigned(Obj) then
    result := OBJ_ID_UNASSIGNED
  else
  begin
    //Already in list?
    Assert(FIRep.Instances.Count = FObjectStore.Count);
    ObjectSearchVal := TInstanceSearchVal.Create;
    try
      ObjectSearchVal.SearchVal := Obj;
      RV := FObjectStore.FindByIndex(IndexInstancePtr, ObjectSearchVal, IRec);
      Assert(RV in [rvOK, rvNotFound]);
      if RV = rvOK then
      begin
        XRef := Irec.Item as TObjectCrossRef;
        result := XRef.FIRepIndex;
        exit;
      end;
    finally
      ObjectSearchVal.Free;
    end;
    //Not in list.
    //Check whether we're likely to be able to unstream this.
    if not Assigned(FindClassTypeByName(Obj.ClassName)) then
      LogEvent(sssWarning, S_STREAMING_OBJ_NOT_REGISTERED);
    //Okay, and now just add it.
    result := FObjectStore.Count;
    XRef := TObjectCrossRef.Create;
    XRef.FInstance := Obj;
    XRef.FIRepIndex := result;
    RV := FObjectStore.AddItem(XRef, IRec);
    Assert(RV = RVOK);
    NewInst := TSSIInstance.Create;
    NewInst.ClassTypeString := Obj.ClassName;
    FIRep.Instances.Add(NewInst);
  end;
end;

//Optimise number of indexes we have to speed things up.
procedure TDefaultSSController.SetObjectStoreStreamIndexes;
var
  RV: TIsRetVal;
begin
  if not FObjectStore.HasIndex(IndexInstancePtr) then
  begin
    RV := FObjectStore.AddIndex(TInstanceINode, IndexInstancePtr);
    Assert(RV = rvOK);
  end;
  if FObjectStore.HasIndex(IndexInstanceId) then
  begin
    RV := FObjectStore.DeleteIndex(IndexInstanceId);
    Assert(RV = rvOK);
  end;
  if FObjectStore.HasIndex(IndexUnstreamNext) then
  begin
    RV := FObjectStore.DeleteIndex(IndexUnstreamNext);
    Assert(RV = rvOK);
  end;
end;

function TDefaultSSController.ConvertStructureToIRep(Struct: TObject):
  TSSITransaction;
var
  NextObject: TObject;
  IRec: TItemRec;
begin
  FIrep := TSSITransaction.Create;
  Assert(FObjectStore.Count = 0);
  SetObjectStoreStreamIndexes;
  try
    FState := csStreaming;
    try
      if not FHeirarchyRegistered then
        raise EStreamSystemError.Create(S_NO_HEIRARCHY);
      AddObjectToBeStreamed(Struct);
      IRec := FObjectStore.GetAnItem;
      while Assigned(IRec) do
      begin
        NextObject := (IRec.Item as TObjectCrossRef).FInstance;
        StreamObjectProperties(NextObject);
        FObjectStore.GetAnotherItem(IRec);
      end;
    finally
      FState := csIdle;
    end;
  except
    on E: Exception do
    begin
      FIrep.Free;
      FIrep := nil;
      ClearObjectStore;
      if (E is EStreamSystemError) then
        LogEvent(sssError, E.Message)
      else
        raise;
    end;
  end;
  result := FIrep;
  FIrep := nil;
  ClearObjectStore;
end;

function TDefaultSSController.FindObjectById(ObjId: integer; var Val: TObject):
  boolean;
var
  IDSearchVal: TIRepIndexSearchVal;
  RV: TISRetVal;
  IRec: TItemRec;
  XLink: TObjectCrossRef;
begin
  result := true;
  if ObjId = OBJ_ID_UNASSIGNED then
  begin
    Val := nil;
    exit;
  end;
  Assert(FObjectStore.Count = FIrep.Instances.Count);
  IDSearchVal := TIRepIndexSearchVal.Create;
  try
    IDSearchVal.FSearchVal := ObjId;
    RV := FObjectStore.FindByIndex(IndexInstanceId, IDSearchVal, IRec);
    Assert(RV in [rvOK, rvNotFound]);
    if RV = rvOK then
    begin
      XLink := IRec.Item as TObjectCrossRef;
      if not XLink.FUsed then
      begin
        RV := FObjectStore.RemoveItem(IRec);
        Assert(RV = rvOK);
        XLink.FUsed := true;
        RV := FObjectStore.AddItem(XLink, IRec);
        Assert(RV = rvOK);
      end;
      Val := XLink.FInstance;
      exit;
    end;
  finally
    IDSearchVal.Free;
  end;
  result := false;
  UnstreamPropertyError(S_COULDNT_UNSTREAM_CLASS_NO_ID);
end;

function TDefaultSSController.UnstreamQuickLookup(ArrayIdx: Integer): TSSIProperty;
var
  IP: TSSIProperty;
begin
  result := nil;
  if (ArrayIdx >= 0) and (ArrayIdx < FFieldContext.Count) then
  begin
    IP := TSSIProperty(FFieldContext.Items[ArrayIdx]);
    if CompareText(IntToStr(ArrayIdx), IP.PropData.PropName) = 0 then
    begin
      IP.SSUsed := true;
      result := IP;
      exit;
    end;
  end;
end;

function TDefaultSSController.UnstreamFindByName(Name: string): TSSIProperty;
var
  Idx: integer;
  IP: TSSIProperty;
begin
  //N.B This is slow for things with lots of properties.
  //Not used for arrays for this reason.
  for Idx := 0 to Pred(FFieldContext.Count) do
  begin
    IP := TSSIProperty(FFieldContext.Items[Idx]);
    if CompareText(Name, IP.PropData.PropName) = 0 then
    begin
      IP.SSUsed := true;
      result := IP;
      exit;
    end;
  end;
  result := nil;
  UnstreamPropertyError(S_NO_PROPERTY_NAMED + Name);
end;

function TDefaultSSController.UnstreamPropertyName(Name: string): TSSIProperty;
var
  StreamingArray: boolean;
  ArrayIdx: integer;
  FCP: TSSIProperty;
begin
  if FState <> csUnstreaming then
    raise EStreamSystemError.Create(S_NOT_UNSTREAMING);
  StreamingArray := false;
  FCP := nil;
  if Assigned(FFieldContext.Parent)
    and (FFieldContext.Parent is TSSIProperty) then
  begin
    FCP := TSSIProperty(FFieldContext.Parent);
    StreamingArray := FCP.PropData.PropType = sMajArray;
  end;
  if not StreamingArray then
    result := UnstreamFindByName(Name)
  else
  begin
    //TODO - Check all items in an array are of the same major type and subtype.
    if Length(Name) > 0 then
    begin
      UnstreamPropertyError(S_UNSTREAM_ARRAY_ITEM_HAS_NAME);
      result := nil;
      exit;
    end;
    //OK, now need to work out suitable integer for name, and increment
    //appropriately.
    ArrayIdx := FCP.SSIdx;
    if (ArrayIdx >= FFieldContext.Count) then
    begin
      UnstreamPropertyError(S_UNSTREAM_ARRAY_OUT_OF_BOUNDS);
      result := nil;
      exit;
    end;
    FCP.SSIdx := ArrayIdx + 1;
    result := UnstreamQuickLookup(ArrayIdx);
    if not Assigned(result) then
      result := UnstreamFindByName(IntToStr(ArrayIdx));
  end;
end;

procedure TDefaultSSController.UnstreamPropertyError(Msg: string);
begin
  if FFailOnNoProperty then
    raise EStreamSystemError.Create(Msg)
  else
    LogEvent(sssWarning, Msg);
end;

function TDefaultSSController.UnStreamOrdinal(Name: string; var Val: Int64;
  ExMType: TSSIMinorType; ExMax, ExMin: Int64): boolean;
var
  OrdProp: TSSIProperty;
  PropMin, PropMax: Int64;
  OutOfRange: boolean;
begin
  result := false;
  OrdProp := UnStreamPropertyName(Name);
  if not Assigned(OrdProp) then
    exit;
  if (OrdProp.PropData.PropType <> sMajOrd)
    or (OrdProp.PropData.PropSubType <> ExMType) then
  begin
    UnstreamPropertyError(S_UNSTREAM_ORDINAL_TYPE_MISMATCH);
    exit;
  end;
  //For both expected and actual limits, if both zero, then change them to
  //defaults for property subtype.
  GetOrdLims(OrdProp.PropData.PropSubType, PropMin, PropMax);
  if OrdProp.PropData.LimsApply then
  begin
    PropMin := OrdProp.PropData.MinVal;
    PropMax := OrdProp.PropData.MaxVal;
  end;
  if (PropMin <> ExMin) or (PropMax <> ExMax) then
    LogEvent(sssWarning, S_ORDINAL_LIMITS_DIFFER);
  //Most range checking can be done with int64, except uint64!
  if OrdProp.PropData.PropSubType <> mitOU64 then
    OutOfRange :=
      (OrdProp.PropData.OrdData < ExMin) or (OrdProp.PropData.OrdData > ExMax)
  else
    OutOfRange :=
      (Uint64(OrdProp.PropData.OrdData) < Uint64(ExMin))
        or (Uint64(OrdProp.PropData.OrdData) > Uint64(ExMax));
  if OutOfRange then
  begin
    UnstreamPropertyError(S_UNSTREAM_ORDINAL_OUT_OF_RANGE);
    exit;
  end;
  result := true;
  Val := OrdProp.PropData.OrdData;
end;

function TDefaultSSController.UnstreamUByte(Name: string; var Val: byte):
  boolean;
var
  IntVal: Int64;
begin
  result := UnstreamOrdinal(Name, IntVal, mitOUByte, High(byte), Low(Byte));
  if result then Val := IntVal;
end;

function TDefaultSSController.UnstreamSByte(Name: string; var Val: shortint):
  boolean;
var
  IntVal: Int64;
begin
  result := UnstreamOrdinal(Name, IntVal, mitOSByte, High(shortint),
    Low(shortint));
  if result then Val := IntVal;
end;

function TDefaultSSController.UnstreamUWord(Name: string; var Val: word):
  boolean;
var
  IntVal: Int64;
begin
  result := UnstreamOrdinal(Name, IntVal, mitOUWord, High(word), Low(Word));
  if result then Val := IntVal;
end;

function TDefaultSSController.UnstreamSWord(Name: string; var Val: smallint):
  boolean;
var
  IntVal: Int64;
begin
  result := UnstreamOrdinal(Name, IntVal, mitOSword, High(smallint),
    Low(smallint));
  if result then Val := IntVal;
end;

function TDefaultSSController.UnstreamSLong(Name: string; var Val: longint):
  boolean;
var
  IntVal: Int64;
begin
  result := UnstreamOrdinal(Name, IntVal, mitOSLong, High(longint),
    Low(longint));
  if Result then Val := IntVal;
end;

function TDefaultSSController.UnstreamULong(Name: string; var Val: cardinal):
  boolean;
var
  IntVal: Int64;
begin
  result := UnstreamOrdinal(Name, IntVal, mitOULong, High(cardinal),
    Low(cardinal));
  if result then Val := IntVal;
end;

function TDefaultSSController.UnstreamEnum(Name: string; var Val: longint):
  boolean;
var
  IntVal: Int64;
begin
  result := UnstreamOrdinal(Name, IntVal, mitOSLong, High(longint),
    Low(longint));
  if result then Val := IntVal;
end;

function TDefaultSSController.UnstreamBool(Name: string; var Val: boolean):
  boolean;
var
  IntVal: Int64;
begin
  result := UnstreamOrdinal(Name, IntVal, mitOUByte, Ord(High(boolean)),
    Ord(Low(boolean)));
  if result then
    Val := IntVal <> 0;
end;

function TDefaultSSController.UnstreamInt64(Name: string; var Val: Int64):
  boolean;
var
  Val64: int64;
begin
  result := UnstreamOrdinal(Name, Val64, mitOS64, High(Int64), Low(Int64));
  if result then
    Val := Val64;
end;

function TDefaultSSController.UnstreamUInt64(Name: string; var Val: UInt64):
  boolean;
var
  Val64: int64;
begin
  result := UnstreamOrdinal(Name, Val64, mitOU64, Int64(High(UInt64)), Int64(Low(UInt64)));
  if result then
    Val := Uint64(Val64);
end;

function TDefaultSSController.UnstreamDouble(Name: string; var Val: double):
  boolean;
var
  ExtVal: Extended;
begin
  result := UnstreamFloat(Name, mitFDouble, ExtVal);
  if result then
    Val := ExtVal;
end;

function TDefaultSSController.UnstreamFloat(Name: string; MType: TSSIMinorType;
  var Val: extended): boolean;
var
  FloatProp: TSSIProperty;
begin
  result := false;
  FloatProp := UnstreamPropertyName(Name);
  if not Assigned(FloatProp) then
    exit;
  if (FloatProp.PropData.Proptype <> sMajFloat) or
    not (FloatProp.PropData.PropSubType in [mitFSingle, mitFDouble,
    mitFExtended]) then
  begin
    UnstreamPropertyError(S_UNSTREAM_FLOAT_TYPE_MISMATCH);
    exit;
  end;
  result := true;
  Val := FloatProp.PropData.FloatData;
end;

function TDefaultSSController.UnstreamString(Name: string; MType: TSSIMinorType;
  MaxLen: longint; var Val: string): boolean;
var
  StrProp: TSSIProperty;
begin
  result := false;
  StrProp := UnstreamPropertyName(Name);
  if not Assigned(StrProp) then
    exit;
  if (StrProp.PropData.PropType <> sMajStr) or
    not (StrProp.PropData.PropSubType in [mitANSIString,
                                          mitShortString,
                                          mitUnicodeString]) then
  begin
    UnstreamPropertyError(S_UNSTREAM_STRING_TYPE_MISMATCH);
    exit;
  end;
  if (StrProp.PropData.PropSubType = mitShortString) then
  begin
    if (StrProp.PropData.MaxStrLen <> MaxLen) then
      LogEvent(sssWarning, S_STRING_LENGTHS_DIFFER);
    if Length(StrProp.PropData.StrData) > MaxLen then
    begin
      UnstreamPropertyError(S_UNSTREAM_STRING_TOO_LONG);
      exit;
    end;
  end
  else
  begin
    if StrProp.PropData.PropSubType <> MType then
    begin
      LogEvent(sssWarning, S_STRING_UNICODE_CONVERTED +
        '(' + TSSIMinorTypeNames[StrProp.PropData.PropSubType] +
        ' to ' + TSSIMinorTypeNames[MType] + ')');
    end;
  end;
  result := true;
  Val := StrProp.PropData.StrData;
end;

function TDefaultSSController.UnstreamClass(Name: string; var Val: TObject):
  boolean;
var
  ClassProp: TSSIProperty;
begin
  result := false;
  ClassProp := UnstreamPropertyName(Name);
  if not Assigned(ClassProp) then
    exit;
  if (ClassProp.PropData.PropType <> sMajClass) or
    (ClassProp.PropData.PropSubType <> mitNone) then
  begin
    UnstreamPropertyError(S_UNSTREAM_CLASS_TYPE_MISMATCH);
    exit;
  end;
  result := FindObjectById(ClassProp.PropData.ObjId, Val);
end;

function TDefaultSSController.UnstreamBlob(Name: string; ExpectedLen: UInt64; var Val:TStream):boolean;
var
  BlobProp: TSSIProperty;
begin
  result := false;
  BlobProp := UnstreamPropertyName(Name);
  if not Assigned(BlobProp) then
    exit;
  if (BlobProp.PropData.PropType <> sMajBlob) or
    (BlobProp.PropData.PropSubType <> mitNone) then
  begin
    UnstreamPropertyError(S_UNSTREAM_BLOB_TYPE_MISMATCH);
    exit;
  end;
  if not Assigned(BlobProp.PropData.BlobData)
    or (BlobProp.PropData.BlobData.Size < ExpectedLen) then
  begin
    UnstreamPropertyError(S_UNSTREAM_BLOB_TOO_SMALL);
    exit;
  end;
  if (BlobProp.PropData.BlobData.Size > ExpectedLen) then
      LogEvent(sssWarning, S_BLOB_MORE_DATA);
  //I think it's OK to transfer ownership of the stream at this point.
  Val := BlobProp.PropData.BlobData;
  BlobProp.PropData.BlobData := nil;
  result := true;
end;

function TDefaultSSController.UnstreamRecordStart(Name: string): boolean;
var
  RecProp: TSSIProperty;
begin
  result := false;
  RecProp := UnstreamPropertyName(Name);
  if not Assigned(RecProp) then
    exit;
  if (RecProp.PropData.PropType <> sMajRec) or
    (RecProp.PropData.PropSubType <> mitNone) then
  begin
    UnstreamPropertyError(S_UNSTREAM_RECORD_TYPE_MISMATCH);
    exit;
  end;
  if not Assigned(RecProp.PropData.Items) then
  begin
    UnstreamPropertyError(S_UNSTREAM_RECORD_NO_ITEMS);
    exit;
  end;
  FFieldContext := RecProp.PropData.Items;
  result := true;
end;

//When unstreaming for record ends, errors here are always fatal, because
//we've likely shafted the context.

function TDefaultSSController.UnstreamRecordEnd(Name: string): boolean;
var
  RecordProp: TSSIProperty;
begin
  if not (Assigned(FFieldContext.Parent)
    and (FFieldContext.Parent is TSSIProperty)
    and (TSSIProperty(FFieldContext.Parent).PropData.PropType = sMajRec)) then
    raise EStreamSystemError.Create(S_UNSTREAM_END_RECORD_IMPOSSIBLE);
  RecordProp := TSSIProperty(FFieldContext.Parent);
  if CompareText(RecordProp.PropData.PropName, Name) <> 0 then
    raise EStreamSystemError.Create(S_UNSTREAM_END_RECORD_WRONG_NAME);
  if not (Assigned(RecordProp.Parent)
    and (RecordProp.Parent is TSSIList)) then
    raise EStreamSystemError.Create(S_UNSTREAM_END_RECORD_CORRUPTED);
  FFieldContext := TSSIList(RecordProp.Parent);
  result := true;
end;

function TDefaultSSController.UnstreamArrayStart(Name: string; var Count:
  integer): boolean;
var
  ArrayProp: TSSIProperty;
begin
  result := false;
  ArrayProp := UnstreamPropertyName(Name);
  if not Assigned(ArrayProp) then
    exit;
  if (ArrayProp.PropData.PropType <> sMajArray) or
    (ArrayProp.PropData.PropSubType <> mitNone) then
  begin
    UnstreamPropertyError(S_UNSTREAM_ARRAY_TYPE_MISMATCH);
    exit;
  end;
  if not Assigned(ArrayProp.PropData.Items) then
  begin
    UnstreamPropertyError(S_UNSTREAM_ARRAY_NO_ITEMS);
    exit;
  end;
  FFieldContext := ArrayProp.PropData.Items;
  Count := FFieldContext.Count;
  ArrayProp.SSIdx := 0;
  result := true;
end;

function TDefaultSSController.UnstreamArrayEnd(Name: string): boolean;
var
  ArrayProp: TSSIProperty;
begin
  if not (Assigned(FFieldContext.Parent)
    and (FFieldContext.Parent is TSSIProperty)
    and (TSSIProperty(FFieldContext.Parent).PropData.PropType = sMajArray)) then
    raise EStreamSystemError.Create(S_UNSTREAM_END_ARRAY_IMPOSSIBLE);
  ArrayProp := TSSIProperty(FFieldContext.Parent);
  if CompareText(ArrayProp.PropData.PropName, Name) <> 0 then
    raise EStreamSystemError.Create(S_UNSTREAM_END_ARRAY_WRONG_NAME);
  if not (Assigned(ArrayProp.Parent)
    and (ArrayProp.Parent is TSSIList)) then
    raise EStreamSystemError.Create(S_UNSTREAM_END_ARRAY_CORRUPTED);
  FFieldContext := TSSIList(ArrayProp.Parent);
  result := true;
end;

procedure TDefaultSSController.TIUnstreamOrdinal(Obj: TObject; PropInfo:
  PPropInfo; PropTypeInfo: PTypeInfo; PropTypeData: PTypeData);
var
  Data: Int64;
  ExpectedMinorType: TSSIMinorType;
  ExpectedMinVal, ExpectedMaxVal: Int64;
begin
  case PropTypeData.OrdType of
    otSByte: ExpectedMinorType := mitOSByte;
    otUByte: ExpectedMinorType := mitOUByte;
    otSWord: ExpectedMinorType := mitOSWord;
    otUWord: ExpectedMinorType := mitOUWord;
    otSLong: ExpectedMinorType := mitOSLong;
    otULong: ExpectedMinorType := mitOULong;
  else
    UnstreamPropertyError(S_ORDINAL_UNKNOWN_TYPE);
    exit;
  end;
  if PropTypeInfo.Kind in [tkInteger, tkChar, tkEnumeration, tkWChar] then
  begin
    if PropTypeData.OrdType <> otULong then
    begin
      ExpectedMinVal := PropTypeData.MinValue;
      ExpectedMaxVal := PropTypeData.MaxValue;
    end
    else
    begin
      ExpectedMinVal := Cardinal(PropTypeData.MinValue);
      ExpectedMaxVal := Cardinal(PropTypeData.MaxValue);
    end;
  end
  else
  begin
    //TODO - Conerned about size checking for sets - need to check
    //ordinality of base type?
    ExpectedMinorType := mitSet;
    ExpectedMinVal := Low(longint);
    ExpectedMaxVal := High(longint);
  end;
  if UnstreamOrdinal(SymbolNameToString(PropInfo.Name), Data, ExpectedMinorType, ExpectedMaxVal,
    ExpectedMinVal) then
    SetOrdProp(Obj, PropInfo, Data);
end;

procedure TDefaultSSController.TIUnstreamInt64(Obj: TObject; PropInfo:
  PPropInfo; PropTypeInfo: PTypeInfo; PropTypeData: PTypeData);
var
  IData: Int64;
  UData: UInt64;
begin
  if PropTypeInfo.Name = S_UINT64_TYPINFO_NAME then
  begin
    if UnstreamUInt64(SymbolNameToString(PropInfo.Name), UData) then
      SetInt64Prop(Obj, PropInfo, Int64(UData))
  end
  else
  begin
    Assert(PropTypeInfo.Name = S_INT64_TYPINFO_NAME);
    if UnStreamInt64(SymbolNameToString(PropInfo.Name), IData) then
      SetInt64Prop(Obj, PropInfo, IData);
  end;
end;

procedure TDefaultSSController.TIUnstreamFloat(Obj: TObject; PropInfo:
  PPropInfo; PropTypeInfo: PTypeInfo; PropTypeData: PTypeData);
var
  Data: extended;
  MinorType: TSSIMinorType;
begin
  case PropTypeData.FloatType of
    ftSingle: MinorType := mitFSingle;
    ftDouble: Minortype := mitFDouble;
    ftExtended: MinorType := mitFExtended;
  else
    raise EStreamSystemError.Create(S_FLOAT_INTERNAL);
  end;
  if UnstreamFloat(SymbolNameToString(PropInfo.Name), MinorType, Data) then
    SetFloatProp(Obj, PropInfo, Data);
end;

procedure TDefaultSSController.TIUnstreamString(Obj: TObject; PropInfo:
  PPropInfo; PropTypeInfo: PTypeInfo; PropTypeData: PTypeData);
var
  Data: string;
  MinType: TSSIMinortype;
  MaxLen: integer;
begin
  MaxLen := 0;
  case PropTypeInfo.Kind of
    tkString:
      begin
        MinType := mitShortString;
        MaxLen := PropTypeData.MaxLength;
      end;
    tkLString: MinType := mitAnsiString;
    tkUString: MinType := mitUnicodeString;
  else
    UnstreamPropertyError(S_STRING_INTERNAL);
    exit;
  end;
  if UnstreamString(SymbolNameToString(PropInfo.Name), MinType, MaxLen, Data) then
    SetStrProp(Obj, PropInfo, Data);
end;

procedure TDefaultSSController.TIUnstreamClass(Obj: TObject; PropInfo:
  PPropInfo; PropTypeInfo: PTypeInfo; PropTypeData: PTypeData);
var
  Data: TObject;
begin
  if UnstreamClass(SymbolNameToString(PropInfo.Name), Data) then
    SetObjectProp(Obj, PropInfo, Data);
end;

procedure TDefaultSSController.UnStreamObjectProperties(Obj: TObject; Idx:
  integer);
var
  InstData: TSSIInstanceData;
  ObjTypeInfo: PTypeInfo;
  PropCount, PropIdx: integer;
  PropList: PPropList;
  PropInfo: PPropInfo;
  PropTypeInfo: PTypeInfo;
  PropTypeData: PTypeData;
begin
  if (Idx >= 0) and (Idx < FIrep.InstancesData.Count) then
    InstData := FIRep.InstancesData.Items[Idx] as TSSIInstanceData
  else
    raise EStreamSystemError.Create(S_MISSING_INSTANCE_DATA);

  FFieldContext := InstData.Properties;
  //Get RTTI for object.
  ObjTypeInfo := PTypeInfo(Obj.ClassInfo);
  Assert(ObjTypeInfo.Kind = tkClass);
  Assert(CompareText(SymbolNameToString(ObjTypeInfo.Name), Obj.ClassName) = 0);
  //Get property list for object.
  PropCount := GetPropList(ObjTypeInfo, tkAny, nil);
  if PropCount > 0 then
  begin
    GetMem(PropList, PropCount * sizeof(pointer));
    try
      GetPropList(ObjTypeInfo, tkAny, PropList);
      //Property list now filled in with PPropInfo
      for PropIdx := 0 to Pred(PropCount) do
      begin
        //Deal with each property in turn.
        PropInfo := GetPropInfo(ObjTypeInfo,
          SymbolNameToString(PropList[PropIdx].Name));
        PropTypeInfo := PropInfo.PropType^;
        PropTypeData := GetTypeData(PropTypeInfo);
        //OK, now have just about everything we need to actually
        //stream the data.
        case PropTypeInfo.Kind of
          tkInteger, tkChar, tkEnumeration, tkSet, tkWChar:
            TIUnstreamOrdinal(Obj, PropInfo, PropTypeInfo, PropTypeData);
          tkInt64: TIUnstreamInt64(Obj, PropInfo, PropTypeInfo, PropTypeData);
          tkFloat: TIUnstreamFloat(Obj, PropInfo, PropTypeInfo, PropTypeData);
          tkString, tkLString, tkUString: TIUnstreamString(Obj, PropInfo, PropTypeInfo,
              PropTypeData);
          tkClass: TIUnstreamClass(Obj, PropInfo, PropTypeInfo, PropTypeData);
        else
          LogEvent(sssWarning, S_UNSUPP_PROP_NAME
            + SymbolNameToString(PropInfo.Name)
            + S_UNSUPP_PROP_TYPE
            + KindNames[PropTypeInfo.Kind]);
        end;
      end;
    finally
      FreeMem(PropList);
    end;
  end;
  //Do custom streaming stuff as required.
  FHeirarchyInfo.CustomUnmarshal(Obj, self);
  if FFieldContext <> InstData.Properties then
    raise EStreamSystemError.Create(S_UNTERM_REC_OR_ARRAY);
end;

//Initially do not need any indexes at all.
procedure TDefaultSSController.ClearObjectStoreIndexes;
var
  RV: TISRetVal;
begin
  if FObjectStore.HasIndex(IndexInstancePtr) then
  begin
    RV := FObjectStore.DeleteIndex(IndexInstancePtr);
    Assert(RV = rvOK);
  end;
  if FObjectStore.HasIndex(IndexInstanceId) then
  begin
    RV := FObjectStore.DeleteIndex(IndexInstanceId);
    Assert(RV = rvOK);
  end;
  if FObjectStore.HasIndex(IndexUnstreamNext) then
  begin
    RV := FObjectStore.DeleteIndex(IndexUnstreamNext);
    Assert(RV = rvOK);
  end;
end;

//Then need by ID and unstream next, but not Ptr.
procedure TDefaultSSController.SetObjectStoreFinalUnstreamIndexes;
var
  RV: TIsRetVal;
begin
  if FObjectStore.HasIndex(IndexInstancePtr) then
  begin
    RV := FObjectStore.DeleteIndex(IndexInstancePtr);
    Assert(RV = rvOK);
  end;
  if not FObjectStore.HasIndex(IndexInstanceId) then
  begin
    RV := FObjectStore.AddIndex(TIRepIndexINode, IndexInstanceId);
    Assert(RV = rvOK);
  end;
  if not FObjectStore.HasIndex(IndexUnstreamNext) then
  begin
    RV := FObjectStore.AddIndex(TUnstreamNextINode, IndexUnstreamNext);
    Assert(RV = rvOK);
  end;
end;

function TDefaultSSController.BuildStructureFromIRep(IRep: TSSITransaction):
  TObject;
var
{$IFOPT C+}
  RootFound: boolean;
{$ENDIF}
  UnstreamedAClass: boolean;
  IRec: TItemRec;
  RV: TIsRetVal;
  XRef: TObjectCrossRef;
begin
  result := nil;
  Assert(FObjectStore.Count = 0);
  ClearObjectStoreIndexes;
  try
    FState := csUnstreaming;
    try
      if not FHeirarchyRegistered then
        raise EStreamSystemError.Create(S_NO_HEIRARCHY);
      if not (Assigned(IRep) and
        Assigned(IRep.Instances) and
        Assigned(IRep.InstancesData) and
        (IRep.Instances.Count > 0) and
        (IRep.Instances.Count = IRep.InstancesData.Count)) then
        raise EStreamSystemError.Create(S_INVALID_IREP);
      FIrep := IRep;
      CreateObjectInstances;
      SetObjectStoreFinalUnstreamIndexes;
{$IFOPT C+}
      RootFound := FindObjectById(0, result);
      Assert(RootFound);
{$ELSE}
      FindObjectById(0, result);
{$ENDIF}
      //Only unstream object properties for objects that are actually used.
      //This then means that we can clean up unused objects without worrying about
      //them having references to used objects.
      UnstreamedAClass := true;
      while UnstreamedAClass do
      begin
        UnstreamedAClass := false;
        RV := FObjectStore.FirstByIndex(IndexUnstreamNext, IRec);
        Assert(RV = rvOK); //if root exists, will always have an object.
        XRef := IRec.Item as TObjectCrossRef;
        if Xref.UnstreamReady then
        begin
          UnstreamObjectProperties(XRef.FInstance, XRef.FIRepIndex);
          RV := FObjectStore.RemoveItem(IRec);
          Assert(RV = rvOK);
          XRef.FUnstreamed := true;
          RV := FObjectStore.AddItem(XRef, IRec);
          Assert(RV = rvOK);
          UnstreamedAClass := true;
        end;
      end;
      CheckUnusedFlags(FIRep);
      CleanObjectInstancesNormal;
    finally
      FState := csIdle;
    end;
  except
    on E: Exception do
    begin
      CleanObjectInstancesError;
      result := nil;
      FIrep := nil;
      if (E is EStreamSystemError) then
        LogEvent(sssError, E.Message)
      else
        raise;
    end;
  end;
  FIrep := nil;
end;

procedure TDefaultSSController.CreateObjectInstances;
var
  Idx: integer;
  NewObj: TObject;
  NewClass: TClass;
  Inst: TSSIInstance;
  FailMsg: string;
  XRef: TObjectCrossRef;
  RV: TISRetVal;
  IRec: TItemRec;
begin
  for Idx := 0 to Pred(FIRep.Instances.Count) do
  begin
    Inst := TSSIInstance(FIRep.Instances.Items[Idx]);
    //First of all, need to convert class name to class type.
    NewClass := FindClassTypeByName(Inst.ClassTypeString);
    if Assigned(NewClass) then
    begin
      NewObj := FHeirarchyInfo.ConstructHelper(NewClass);
      FailMsg := S_CREATE_HELPER_FAILED;
    end
    else
    begin
      NewObj := nil;
      FailMsg := S_CLASS_TYPE_NOT_REGISTERED;
    end;
    if not Assigned(NewObj) then
    begin
      if FFailOnNoClass or (Idx = 0) then
        raise EStreamSystemError.Create(FailMsg +
          Inst.ClassTypeString)
      else
        LogEvent(sssWarning, FailMsg + Inst.ClassTypeString);
    end;
    XRef := TObjectCrossRef.Create;
    XRef.FInstance := NewObj;
    XRef.FIRepIndex := Idx;
    RV := FObjectStore.AddItem(XRef, IRec);
    Assert(RV = rvOK);
  end;
end;

procedure TDefaultSSController.SignalUnusedFlag(IObj: TSSIntermediate);
var
  II: TSSIInstance;
  IP: TSSIProperty;
begin
  if not Assigned(IObj) then exit;
  if IObj.ClassType = TSSIInstance then
  begin
    II := TSSIInstance(IObj);
    if FFailOnClassUnused then
      raise EStreamSystemError.Create(S_CLASS_UNUSED + II.ClassTypeString)
    else
      LogEvent(sssWarning, S_CLASS_UNUSED + II.ClassTypeString);
  end
  else if IObj.ClassType = TSSIProperty then
  begin
    IP := TSSIProperty(IObj);
    if FFailOnPropertyUnused then
      raise EStreamSystemError.Create(S_PROPERTY_UNUSED + IP.PropData.PropName)
    else
      LogEvent(sssWarning, S_PROPERTY_UNUSED + IP.PropData.PropName);
  end
  else
    raise EStreamSystemError.Create(S_UNKNOWN_IREP_CHECKING_USED_FLAGS);
end;

procedure TDefaultSSController.CheckUnusedFlags(IObj: TSSIntermediate);
var
  Idx: integer;
  IL: TSSIList;
  IP: TSSIProperty;
  IRec: TItemRec;
  XRef: TObjectCrossRef;
begin
  if not Assigned(IObj) then exit;
  if IObj.ClassType = TSSITransaction then
  begin
    IRec := FObjectStore.GetAnItem;
    while Assigned(IRec) do
    begin
      XRef := IRec.Item as TObjectCrossRef;
      if not XRef.FUsed then
        SignalUnusedFlag(TSSITransaction(IObj).Instances.Items[XRef.FIRepIndex]);
      FObjectStore.GetAnotherItem(IRec);
    end;
    CheckUnusedFlags(TSSITransaction(IObj).InstancesData)
  end
  else if IObj.ClassType = TSSIList then
  begin
    IL := TSSIList(IObj);
    for Idx := 0 to Pred(IL.Count) do
      CheckUnusedFlags(IL.Items[Idx]);
  end
  else if IObj.ClassType = TSSIInstanceData then
  begin
    CheckUnusedFlags(TSSIInstanceData(IObj).Properties)
  end
  else if IObj.ClassType = TSSIProperty then
  begin
    IP := TSSIProperty(IObj);
    if not IP.SSUsed then SignalUnusedFlag(IP);
    if IP.PropData.PropType in [sMajRec, sMajArray] then
    begin
      CheckUnusedFlags(IP.PropData.Items);
    end;
  end
  else
    raise EStreamSystemError.Create(S_UNKNOWN_IREP_CHECKING_USED_FLAGS);
end;

procedure TDefaultSSController.CleanObjectInstancesNormal;
var
  XRef: TObjectCrossRef;
  IRec: TItemRec;
begin
  Assert(FIrep.Instances.Count = FObjectStore.Count);
  IRec := FObjectStore.GetAnItem;
  while Assigned(IRec) do
  begin
    XRef := IRec.Item as TObjectCrossRef;
    if not XRef.FUsed then
    begin
      XRef.FInstance.Free;
      //Don't NIL it out, we need to remove Xref from tree.
    end;
    FObjectStore.GetAnotherItem(IRec);
  end;
  //Sneaky here in that we delete indexes without adding
  //or removing any items...
  ClearObjectStore;
end;

procedure TDefaultSSController.CleanObjectInstancesError;
var
  IRec: TItemRec;
begin
  IRec := FObjectStore.GetAnItem;
  while Assigned(IRec) do
  begin
    (IRec.Item as TObjectCrossRef).FInstance.Free;
    FObjectStore.GetAnotherItem(IRec);
  end;
  //Sneaky here in that we delete indexes without adding
  //or removing any items...
  ClearObjectStore;
end;

{ TObjectCrossRef }

function TObjectCrossRef.UnstreamReady: boolean;
begin
  result := Assigned(FInstance)
            and FUsed
            and (not FUnstreamed);
end;

{ TInstanceINode }

function TInstanceINode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
var
  Own, Other: TObjectCrossRef;
begin
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OwnItem is TObjectCrossRef);
  Assert(OtherItem is TObjectCrossRef);
  Own := OwnItem as TObjectCrossRef;
  Other := OtherItem as TObjectCrossRef;
  result := ComparePointers(Own.FInstance, Other.FInstance);
end;

{ TIRepIndexINode }

function TIRepIndexINode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
var
  Own, Other: TObjectCrossRef;
begin
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OwnItem is TObjectCrossRef);
  Assert(OtherItem is TObjectCrossRef);
  Own := OwnItem as TObjectCrossRef;
  Other := OtherItem as TObjectCrossRef;
  if Other.FIRepIndex > Own.FIRepIndex then
    result := 1
  else if Other.FIRepIndex < Own.FIRepIndex then
    result := -1
  else
    result := 0;
end;

{ TUnstreamNextINode }

function TUnstreamNextINode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
var
  OwnUnstreamReady, OtherUnstreamReady: integer;
  Own, Other: TObjectCrossRef;
begin
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OwnItem is TObjectCrossRef);
  Assert(OtherItem is TObjectCrossRef);
  Own := OwnItem as TObjectCrossRef;
  Other := OtherItem as TObjectCrossRef;
  OwnUnstreamReady := Ord(Own.UnstreamReady);
  OtherUnstreamReady := Ord(Other.UnstreamReady);
  //Do this the other way round, so UnstreamReady items come first ("lowest").
  if OtherUnstreamReady > OwnUnstreamReady then
    result := -1
  else if OtherUnstreamReady < OwnUnstreamReady then
    result := 1
  else
    result := 0;
end;

{ TInstanceSearchVal }

function TInstanceSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
var
  Other: TObjectCrossRef;
begin
  Assert(not Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OtherItem is TObjectCrossRef);
  Other := OtherItem as TObjectCrossRef;
  result := ComparePointers(FSearchVal, Other.FInstance);
end;

{ TIRepIndexSearchVal }

function TIRepIndexSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
var
  Other: TObjectCrossRef;
begin
  Assert(not Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OtherItem is TObjectCrossRef);
  Other := OtherItem as TObjectCrossRef;
  if Other.FIRepIndex > FSearchVal then
    result := 1
  else if Other.FIRepIndex < FSearchVal then
    result := -1
  else
    result := 0;
end;

end.
