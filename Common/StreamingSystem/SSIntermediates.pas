unit SSIntermediates;
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
  Copyright (c) Martin Harvey 2005.

  This unit defines the intermediate representation of data in the
  streaming system.
}
interface

uses Trackables, Classes;

type
  TSSIntermediate = class(TTrackable);
  TSSIChild = class(TSSIntermediate)
  private
    FParent: TSSIntermediate;
  public
    property Parent: TSSIntermediate read FParent write FParent;
  end;

  TSSIList = class(TSSIChild)
  private
    FList: TList;
  protected
    function GetCount: integer;
    function GetItem(Idx: integer): TSSIntermediate;
  public
    constructor Create;
    constructor CreateWithTracker(Tracker: TTracker);
    destructor Destroy; override;
    destructor DestroySelfOnly(Sender: TTracker); override;
    procedure Add(Item: TSSIntermediate);
    procedure Delete(Idx: integer);
    property Count: integer read GetCount;
    property Items[Idx: integer]: TSSIntermediate read GetItem;
    procedure Clear;
  end;

  TSSITransaction = class(TSSIntermediate)
  private
    FInstances: TSSIList; //List of TSSIInstance
    FInstancesData: TSSIList; //List of TSSIInstanceData
  protected
  public
    constructor Create;
    constructor CreateWithTracker(Tracker: TTracker);
    destructor Destroy; override;
    property Instances: TSSIList read FInstances write FInstances;
    property InstancesData: TSSIList read FInstancesData write FInstancesData;
  end;

  TSSIInstance = class(TSSIntermediate)
  private
    FClassTypeString: string;
  protected
  public
    property ClassTypeString: string
      read FClassTypeString write FClassTypeString;
  end;

  TSSIInstanceData = class(TSSIntermediate)
  private
    FProperties: TSSIList; //List of TSSIProperty
  protected
  public
    constructor Create;
    constructor CreateWithTracker(Tracker: TTracker);
    destructor Destroy; override;
    property Properties: TSSIList read FProperties write FProperties;
  end;

{

  Quick reminder from TypInfo:
  TTypeKind = (tkUnknown, tkInteger, tkChar, tkEnumeration, tkFloat,
    tkString, tkSet, tkClass, tkMethod, tkWChar, tkLString, tkWString,
    tkVariant, tkArray, tkRecord, tkInterface, tkInt64, tkDynArray, tkUString,
    tkClassRef, tkPointer, tkProcedure);

  TTypeKinds = set of TTypeKind;

  TOrdType = (otSByte, otUByte, otSWord, otUWord, otSLong, otULong);

  TFloatType = (ftSingle, ftDouble, ftExtended, ftComp, ftCurr);

  TMemberVisibility = (mvPrivate, mvProtected, mvPublic, mvPublished);

  TMethodKind = (mkProcedure, mkFunction, mkConstructor, mkDestructor,
    mkClassProcedure, mkClassFunction, mkClassConstructor, mkClassDestructor,
    mkOperatorOverload,
    mkSafeProcedure, mkSafeFunction);

  TOrdType = (otSByte, otUByte, otSWord, otUWord, otSLong, otULong);

  TFloatType = (ftSingle, ftDouble, ftExtended, ftComp, ftCurr);

  Properties that can be got and set via TypInfo that concern us  boil down to:
  Ord, str, Float, Int64 (no need to worry about variants, methods &c)
  We also need to add support for records, arrays and class references
}

  TSSIMajorType = (sMajOrd, sMajStr, sMajFloat, sMajRec,
    sMajArray, sMajClass, sMajBlob);
  TSSIMinorType = (mitNone, //Major types without a subtype
    //Ordinals
    mitOSByte, mitOUByte, mitOSWord, mitOUWord, mitOSLong, mitOULong, mitSet,
    //Special subtype for unsigned 64 bit ints.
    mitOS64, mitOU64,
    //Floats
    mitFSingle, mitFDouble, mitFExtended,
    //Strings
    mitShortString, mitAnsiString, mitUnicodeString);

  TSSIStringEncode = (scAscii, scHashEnc);

const
  TSSIMajorTypeNames: array[TSSIMajorType] of string =
    ('sMajOrd', 'sMajStr', 'sMajFloat', 'sMajRec', 'sMajArray',
    'sMajClass', 'sMajBlob');
  TSSIMinorTypeNames: array[TSSIMinorType] of string =
    ('mitNone', 'mitOSByte', 'mitOUByte', 'mitOSWord', 'mitOUWord', 'mitOSLong',
    'mitOULong', 'mitSet', 'mitOS64','mitOU64', 'mitFSingle', 'mitFDouble', 'mitFExtended',
    'mitShortString', 'mitAnsiString', 'mitUnicodeString');

  TSSIStringEncodeNames: array [TSSIStringEncode] of string =
    ('scAscii', 'scHashEnc');

type
  TSSIPropertyData = record
    PropName: string; //Just the name of this property, not considering
                      //enclosing properties.
    PropType: TSSIMajorType;
    PropSubType: TSSIMinorType;
    //String types declared here instead of variant part
    //since long strings not allowed in variant parts.
    //The string here should handle all needed bits of string
    //data.
    StrData: string;
    BlobData: TStream; //Or dynamic array of byte.

    case TSSIMajorType of
      sMajOrd: (OrdData: Int64; MinVal, MaxVal: Int64; LimsApply: boolean);
      sMajStr: (MaxStrLen: Longint; //MaxLen only the case for ShortStr.
                StrRequiredEncode: TSSIStringEncode);
      sMajFloat: (FloatData: extended);
      sMajRec, sMajArray: (Items: TSSIList; );
      //Records contain a list of TSSIProperty where the prop name corresponds
      //to the field in the data.
      //Arrays contain a list of TSSIProperty where the prop name is the integer
      //index in the array (e.g. "1", "2" etc).
      sMajClass: (ObjId: integer);
  end;


  TSSIProperty = class(TSSIChild)
  private
    //These two reserved for controller.
    //They do not need to be marshalled or unmarshalled.
    //During unmarshalling, they should be set to zero (as is Delphi default).
    FSSIdx: integer;
    FSSUsed: boolean;
  protected
  public
    PropData: TSSIPropertyData;
    destructor Destroy; override;
    property SSIdx: integer read FSSIdx write FSSIdx;
    property SSUsed: boolean read FSSUsed write FSSUsed;
  end;

implementation

(************************************
 * TSSIList                         *
 ************************************)

function TSSIList.GetCount: integer;
begin
  result := FList.Count;
end;

function TSSIList.GetItem(Idx: integer): TSSIntermediate;
begin
{$IFOPT C+}
  result := TObject(FList.Items[Idx]) as TSSIntermediate;
{$ELSE}
  result := TSSIntermediate(FList.Items[Idx]);
{$ENDIF}
end;

constructor TSSIList.Create;
begin
  inherited;
  FList := TList.Create;
end;

constructor TSSIList.CreateWithTracker(Tracker: TTracker);
begin
  inherited;
  FList := TList.Create;
end;

destructor TSSIList.Destroy;
var
  Idx: integer;
begin
  for Idx := 0 to Pred(FList.Count) do
    TObject(FList.Items[Idx]).Free;
  FList.Free;
  inherited;
end;

destructor TSSIList.DestroySelfOnly(Sender: TTracker);
begin
  FList.Free;
  inherited;
end;

procedure TSSIList.Add(Item: TSSIntermediate);
begin
  FList.Add(Item);
  if (Item is TSSIChild) then
    TSSIChild(Item).Parent := self;
end;

procedure TSSIList.Delete(Idx: integer);
begin
  FList.Delete(Idx);
end;

procedure TSSIList.Clear;
begin
  FList.Clear;
end;

(************************************
 * TSSITransaction                  *
 ************************************)

constructor TSSITransaction.Create;
begin
  inherited;
  FInstances := TSSIList.Create;
  FInstancesData := TSSIList.Create;
end;

constructor TSSITransaction.CreateWithTracker(Tracker: TTracker);
begin
  inherited;
  FInstances := TSSIList.CreateWithTracker(Tracker);
  FInstancesData := TSSIList.CreateWithTracker(Tracker);
end;

destructor TSSITransaction.Destroy;
begin
  FInstances.Free;
  FInstancesData.Free;
  inherited;
end;

(************************************
 * TSSIInstanceData                 *
 ************************************)

constructor TSSIInstanceData.Create;
begin
  inherited;
  FProperties := TSSIList.Create;
end;

constructor TSSIInstanceData.CreateWithTracker(Tracker: TTracker);
begin
  inherited;
  FProperties := TSSIList.CreateWithTracker(Tracker);
end;

destructor TSSIInstanceData.Destroy;
begin
  FProperties.Free;
  inherited;
end;

(************************************
 * TSSIProperty                     *
 ************************************)

destructor TSSIProperty.Destroy;
begin
  if PropData.PropType in [sMajrec, sMajArray] then
    PropData.Items.Free;
  inherited;
  //Expect unused unless PropData.PropType = sMajBlob
  PropData.BlobData.Free;
end;

end.

