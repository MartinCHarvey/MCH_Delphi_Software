unit StreamSysBinary;
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

uses StreamingSystem, SSAbstracts, SSIntermediates, Classes;

type
  TStreamTag = type byte;

  TStreamSysBinary = class(TStreamSystem)
  protected
    procedure CreateParts; override;
  public
  end;

  TSSBinaryReader = class(TAbstractSSReader)
  private
    FStream: TStream;
  protected
    function ReadBlob: TStream;
    function ReadString: string;
    procedure ReadList(ListTag: TStreamTag; List: TSSIList);
    function ReadInstance: TSSIInstance;
    function ReadInstanceData: TSSIInstanceData;
    function ReadProperty: TSSIProperty;
  public
    function ReadIRepFromStream(Stream: TStream): TSSITransaction; override;
  end;

  TSSBinaryWriter = class(TAbstractSSWriter)
  private
    FStream: TStream;
  protected
    procedure WriteBlob(Blob: TStream);
    procedure WriteString(const S:string);
    procedure WriteList(ListTag: TStreamTag; List: TSSIList);
    procedure WriteInstance(Inst: TSSIInstance);
    procedure WriteInstanceData(Data: TSSIInstanceData);
    procedure WriteProperty(Prop: TSSIProperty);
  public
    function WriteIRepToStream(Stream: TStream; IRep: TSSITransaction): boolean; override;
  end;

  { Stream / Prococol design.
    Go for speed - I do not want to make complicated parsing decisions in
    the reader
      - Use length fields as much as possible rather than tags.
      - If at all possible keep things fixed format.

    Less is definitely more.
  }

const
  //This might double as a version flag too if we need to make the counts
  //64 bit, or cardinal ...
  StreamSysBinaryMagic: Int32 =  $384FDED6;

implementation

uses
  SysUtils;

const
  TagInstancesList = 0;
  TagInstancesDataList = 1;
  TagPropertiesList = 2;
  TagContainedPropertiesList = 3; //TODO - diff between array and record props?

  S_INTERNAL_BAD_TAG = 'Internal error: bad tag';
  S_UNKNOWN_STREAM_FMT_VERSION = 'Unknown stream format or version.';
  S_BAD_LIST_TAG = 'Read bad list tag from stream.';
  S_READ_BAD_PROPERTY_RECORD = 'Read bad property record from stream';

{ TStreamSysBinary }

procedure TStreamSysBinary.CreateParts;
begin
  inherited;
  Reader := TSSBinaryReader.Create;
  Writer := TSSBinaryWriter.Create;
end;

{ TSSBinaryReader }

function TSSBinaryReader.ReadBlob: TStream;
var
  BlobLen: Int64;
begin
  FStream.Read(BlobLen, sizeof(BlobLen));
  if BlobLen > 0 then
  begin
    result := TMemoryStream.Create;
    result.CopyFrom(FStream, BlobLen);
  end
  else
    result := nil;
end;

function TSSBinaryReader.ReadString: string;
var
  StrLen: integer;
begin
  FStream.Read(StrLen, sizeof(StrLen));
  SetLength(result, StrLen);
  if StrLen > 0 then
    FStream.Read(result[1], StrLen * sizeof(result[1]));
end;

function TSSBinaryReader.ReadInstance: TSSIInstance;
var
  S: string;
begin
  S := ReadString;
  result := TSSIInstance.Create;
  result.ClassTypeString := S;
end;

//TODO - Not sure for speed whether to have an exception
//handler, or a temporary list.
function TSSBinaryReader.ReadInstanceData: TSSIInstanceData;
begin
  result := TSSIInstanceData.Create;
  try
    ReadList(TagPropertiesList, result.Properties);
  except
    on E: Exception do
    begin
      result.Free;
      raise;
    end;
  end;
end;


function TSSBinaryReader.ReadProperty: TSSIProperty;
var
  PropDataCopy: TSSIPropertyData;
begin
  //TODO - Could remove this small copy for speed...
  FStream.Read(PropDataCopy, sizeof(PropDataCopy));
  //Just check for the errors which will kill the program.
  //TODO - Check sizes make sense here.
  if Assigned(Pointer(PropDataCopy.PropName))
    or Assigned(Pointer(PropDataCopy.StrData))
    or Assigned(Pointer(PropDataCopy.BlobData))
    or ((PropDataCopy.PropType in [sMajRec, sMajArray])
      and (Assigned(PropDataCopy.Items))) then
    raise EStreamSystemError.Create(S_READ_BAD_PROPERTY_RECORD);
  result := TSSIProperty.Create;
  result.PropData := PropDataCopy;
  result.PropData.PropName := ReadString;
  result.PropData.StrData := ReadString;
  case result.PropData.PropType of
    sMajRec, sMajArray:
    begin
      result.PropData.Items := TSSIList.Create;
      result.PropData.Items.Parent := result;
      ReadList(TagContainedPropertiesList, result.PropData.Items);
    end;
    sMajBlob: result.PropData.BlobData := ReadBlob;
  end;
end;

procedure TSSBinaryReader.ReadList(ListTag: TStreamTag; List: TSSIList);
var
  Idx, LCount: Int32;
  RTag: TStreamTag;
begin
  FStream.Read(RTag, sizeof(RTag));
  if RTag <> ListTag then
    raise EStreamSystemError.Create(S_BAD_LIST_TAG);
  FStream.Read(LCount, sizeof(LCount));
  for idx := 0 to Pred(LCount) do
  begin
    case ListTag of
      TagInstancesList:  List.Add(ReadInstance);
      TagInstancesDataList: List.Add(ReadInstanceData);
      TagPropertiesList,
      TagContainedPropertiesList: List.Add(ReadProperty);
    else
      raise EStreamSystemError.Create(S_INTERNAL_BAD_TAG);
    end;
  end;
  FStream.Read(RTag, sizeof(RTag));
  if RTag <> ListTag then
    raise EStreamSystemError.Create(S_BAD_LIST_TAG);
end;

function TSSBinaryReader.ReadIRepFromStream(Stream: TStream): TSSITransaction;
var
  MagicInt: Int32;
begin
  if not Assigned(Stream) then
  begin
    result := nil;
    exit;
  end;
  FStream := Stream;
  result := nil;
  try
    FStream.Read(MagicInt, sizeof(MagicInt));
    if MagicInt <> StreamSysBinaryMagic + sizeof(Pointer) then
      raise EStreamSystemError.Create(S_UNKNOWN_STREAM_FMT_VERSION);
    result := TSSITransaction.Create;
    ReadList(TagInstancesList, result.Instances);
    ReadList(TagInstancesDataList, result.InstancesData);
  except
    on E: Exception do
    begin
      result.Free;
      result := nil;
    end;
  end;
end;

{ TSSBinaryWriter }

procedure TSSBinaryWriter.WriteBlob(Blob: TStream);
var
  BlobLen: Int64;
begin
  if not Assigned(Blob) then
    BlobLen := 0
  else
    BlobLen := Blob.Size;
  FStream.Write(BlobLen, sizeof(BlobLen));
  if BlobLen > 0 then
  begin
    Blob.Seek(0, soFromBeginning);
    FStream.CopyFrom(Blob, BlobLen);
  end;
end;

procedure TSSBinaryWriter.WriteString(const S:string);
//Don't worry about any fancy encoding, string raw data.
var
  StrLen: integer;
begin
  StrLen := Length(S); //Length in characters, not bytes.
  FStream.Write(StrLen, sizeof(StrLen));
  if StrLen > 0 then
    FStream.Write(S[1], StrLen * sizeof(S[1]));
end;

procedure TSSBinaryWriter.WriteInstance(Inst: TSSIInstance);
begin
  WriteString(Inst.ClassTypeString);
end;

procedure TSSBinaryWriter.WriteInstanceData(Data: TSSIInstanceData);
begin
  WriteList(TagPropertiesList, Data.Properties);
end;

procedure TSSBinaryWriter.WriteProperty(Prop: TSSIProperty);
var
  PropDataCopy: TSSIPropertyData;
begin
  //Binary copy, but then null out all non fixed fields.
  PropDataCopy := Prop.PropData;
  SetLength(PropDataCopy.PropName ,0); //TODO - Check this NIL's.
  SetLength(PropDataCopy.StrData, 0);  //TODO - Check this NIL's.
  if Assigned(PropDataCopy.BlobData) then
    PropDataCopy.BlobData := nil; //Don't free...
  if PropDataCopy.PropType in [sMajRec, sMajArray] then
    PropDataCopy.Items := nil;
  //And just blat to stream.
  FStream.Write(PropDataCopy, sizeof(PropDataCopy));

  WriteString(Prop.PropData.PropName);
  WriteString(Prop.PropData.StrData);
  case Prop.PropData.PropType of
    sMajRec, sMajArray: WriteList(TagContainedPropertiesList, Prop.PropData.Items);
    sMajBlob: WriteBlob(Prop.PropData.BlobData);
  end;
end;

procedure TSSBinaryWriter.WriteList(ListTag: TStreamTag; List: TSSIList);
var
  Idx, LCount: Int32;
begin
  FStream.Write(ListTag, sizeof(ListTag));
  LCount := List.Count;
  FStream.Write(LCount, sizeof(LCount));
  for idx := 0 to Pred(LCount) do
  begin
    case ListTag of
      TagInstancesList: WriteInstance(TSSIInstance(List.Items[Idx]));
      TagInstancesDataList: WriteInstanceData(TSSIInstanceData(List.Items[Idx]));
      TagPropertiesList,
      TagContainedPropertiesList: WriteProperty(TSSIProperty(List.Items[Idx]));
    else
      raise EStreamSystemError.Create(S_INTERNAL_BAD_TAG);
    end;
  end;
  FStream.Write(ListTag, sizeof(ListTag));
end;

function TSSBinaryWriter.WriteIRepToStream(Stream: TStream; IRep: TSSITransaction): boolean;
var
  MagicInt: Int32;
begin
  if not Assigned(Stream) and Assigned(IRep) then
  begin
    result := false;
    exit;
  end;
  FStream := Stream;
  MagicInt := StreamSysBinaryMagic + sizeof(Pointer);
  FStream.Write(MagicInt, sizeof(MagicInt));
  WriteList(TagInstancesList, IRep.Instances);
  WriteList(TagInstancesDataList, IRep.InstancesData);
  result := true;
end;

end.
