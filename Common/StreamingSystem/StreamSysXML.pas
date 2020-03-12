unit StreamSysXML;
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
  This unit defines a textual streaming system which reads and write to
  an "XML style" text document. This unit will probably be the default used
  by client applications that want to textually stream classes.
}

interface

uses StreamingSystem, TrivXML, SSAbstracts, Classes, SSIntermediates;

type
  TStreamSysXML = class(TStreamSystem)
  protected
    procedure CreateParts; override;
    function GetWsp: boolean;
    procedure SetWsp(NewWsp: boolean);
  public
    property WhitespaceToFile: boolean read GetWsp write SetWsp;
  end;

  TSSXMLReader = class(TAbstractSSReader)
  private
  protected
  public
    function ReadIRepFromStream(Stream: TStream): TSSITransaction; override;
  end;

  TSSXMLWriter = class(TAbstractSSWriter)
  private
    FStream: TStream;
    FIndent: integer;
    FWhitespaceToFile: boolean;
  protected
    //General writer functions
    procedure WriteIndent;
    procedure WriteStringToStream(Str: string);
    procedure WriteStartTag(STag: string);
    procedure WriteItem(Item: string);
    procedure WriteEndTag(ETag: string);
    function HashEncString(InString: string): string;
    function EscapeString(InString: string): string;
    function MimeEncBlob(Blob: TStream): string;
    //Writer functions for specific bits of the IRep.
    procedure WriteStrPropData(Prop: TSSIProperty);
    procedure WriteBlobPropData(Prop: TSSIProperty);
    procedure CalcStrPropEncoding(Prop: TSSIProperty);
    procedure WritePropData(Prop: TSSIProperty);
    //Gets basic name, type, subtype attributes.
    function GetPropAttrStr(Prop: TSSIProperty): string;
    //Gets extended addtributes, such as min / max val and rnages.
    function PropHasData(Prop: TSSIProperty): boolean;
    procedure WriteProperties(Properties: TSSIList);
    procedure WriteInstances(Instances: TSSIList);
    procedure WriteInstancesData(InstancesData: TSSIList);
  public
    function WriteIRepToStream(Stream: TStream; IRep: TSSITransaction): boolean;
      override;
    property WhitespaceToFile: boolean read FWhitespaceToFile write FWhitespaceToFile;
  end;

implementation

uses CoCoBase, SysUtils, Trackables, IdCoderMIME;

const
  S_PARSE_LINE = 'Parse error, line: ';
  S_PARSE_COL = ', col: ';
  S_PARSE_DESC = ' : ';

(************************************
 * TStreamSysXML                    *
 ************************************)

procedure TStreamSysXML.CreateParts;
begin
  inherited;
  Reader := TSSXMLReader.Create;
  Writer := TSSXMLWriter.Create;
  WhitespaceToFile := true;
end;

function TStreamSysXML.GetWsp: boolean;
begin
  result := (Writer as TSSXMLWriter).WhitespaceToFile;
end;

procedure TStreamSysXML.SetWsp(NewWsp: boolean);
begin
  (Writer as TSSXMLWriter).WhitespaceToFile := NewWsp;
end;

(************************************
 * TSSXMLReader                     *
 ************************************)

function TSSXMLReader.ReadIRepFromStream(Stream: TStream): TSSITransaction;
var
  TrivXML: TTrivXML;
  ErrIdx: integer;
  CurErr: TCoCoError;
  OwnStream: TMemoryStream;
begin
  result := nil;
  if not Assigned(Stream) then exit;
  TrivXML := TTrivXML.Create(nil);
  try
    if Stream is TMemoryStream then
    begin
      TrivXML.SourceStream := TMemoryStream(Stream);
      OwnStream := nil;
    end
    else
    begin
      OwnStream := TMemoryStream.Create;
      OwnStream.CopyFrom(Stream, 0);
      TrivXML.SourceStream := OwnStream;
    end;
    try
      try
        TrivXML.Execute;
      except
        on Exception do
        begin
          (TrivXML.ParseTracker as TTracker).FreeTrackedClassesAndReset;
          raise;
        end;
      end;
      if TrivXML.Successful then
      begin
        result := TrivXML.ParseResult as TSSITransaction;
        (TrivXML.ParseTracker as TTracker).ResetWithoutFree;
      end
      else
      begin
        TrivXML.ParseResult.Free;
        (TrivXML.ParseTracker as TTracker).FreeTrackedClassesAndReset;
        for ErrIdx := 0 to Pred(TrivXML.ErrorList.Count) do
        begin
          CurErr := TCoCoError(TrivXML.ErrorList.Items[ErrIdx]);
          LogEvent(sssError, S_PARSE_LINE + IntToStr(CurErr.Line) +
            S_PARSE_COL + IntToStr(CurErr.Col) +
            S_PARSE_DESC +
            String(TrivXML.ErrorStr(CurErr.ErrorCode, CurErr.Data)));
        end;
      end;
      //No need to free own stream, TrivXML owns it.
    finally
      TrivXML.SourceStream := nil;
      OwnStream.Free;
    end;
  finally
    TrivXML.Free;
  end;
end;

(************************************
 * TSSXMLWriter                     *
 ************************************)

//String constants for writing XML. These should match those in the ATG
//file for the TrivXML reader. Unfortunately, because many of the constants
//are not defined as constants itself, but as the structure of the lexer
//state machine, it is hard to keep the definitions shared. Pity.
const
  //String constants same as .atg file constants
  S_CTYPE_STR = 'ClassType';
  S_OBJ_ID = 'ObjId';
  S_PROP_NAME = 'Name';
  S_PROP_TYPE = 'Type';
  S_PROP_SUBTYPE = 'Subtype';
  S_MIN_VAL = 'MinVal';
  S_MAX_VAL = 'MaxVal';
  S_MAX_STRLEN = 'MaxStrLen';
  S_STR_ENCODE = 'Encoding';
  //String constants encoded into lexer state machine.
  S_TXmlStart = '<TrivXML>';
  S_TXmlEnd = '</TrivXML>';
  S_TransStart = '<Transaction>';
  S_TransEnd = '</Transaction>';
  S_InstancesStart = '<Instances>';
  S_InstancesEnd = '</Instances>';
  S_InstsDataStart = '<InstancesData>';
  S_InstsDataEnd = '</InstancesData>';
  S_InstTagStart = '<Instance';
  S_TagClose = '>';
  S_TagShortClose = '/>';
  S_IDatTagStart = '<InstData';
  S_IDatTagEnd = '</InstData>';
  S_PropTagStart = '<Property';
  S_PropTagEnd = '</Property>';
  S_eq = '=';
  //Other string constants.
  S_CR_LF = #13#10;
  S_SPACE = ' ';
  S_ERR_WRITING_STREAM = 'Error writing to stream: ';
  S_INVALID_INSTANCES = 'Invalid instances writing to stream';
  S_INVALID_INSTANCES_DATA = 'Invalid instances data writing to stream';
  S_INVALID_PROPS_DATA = 'Invalid properties data writing to stream';
  S_INVALID_PROP_CONTENTS = 'Invalid property contents';
  S_UNKNOWN_PROP_TYPE = 'Unknown property type';
  S_BAD_PROP_FOR_ENCODING = 'Bad property for string encoding.';
  S_HASH = '#';

function TSSXMLWriter.HashEncString(InString: string): string;
var
  idx: integer;
begin
  //Might be quicker ways to do this...
  result := '''';
  for idx := 1 to Length(InString) do
  begin
    result := result + S_HASH;
    result := result + IntToStr(Ord(InString[idx]));
  end;
  result := result + '''';
end;

function TSSXMLWriter.EscapeString(InString: string): string;
var
  TempStr: string;
begin
(* &quot; <= '
   &cr; <= CHR(13)
   &lf; <= CHR(10)
   &amp; <= &
   Remember to do escapes for ampersands first!
 *)
  TempStr := InString;
  TempStr := StringReplace(TempStr, '&', '&amp;', [rfReplaceAll]);
  TempStr := StringReplace(TempStr, #10, '&lf;', [rfReplaceAll]);
  TempStr := StringReplace(TempStr, #13, '&cr;', [rfReplaceAll]);
  TempStr := StringReplace(TempStr, '''', '&apos;', [rfReplaceAll]);
  TempStr := StringReplace(TempStr, '"', '&quot;', [rfReplaceAll]);
  TempStr := StringReplace(TempStr, '<', '&lt;', [rfReplaceAll]);
  TempStr := StringReplace(TempStr, '>', '&gt;', [rfReplaceAll]);
  result := '''' + TempStr + '''';
end;

function TSSXMLWriter.MimeEncBlob(Blob: TStream): string;
var
  Coder: TIdEncoderMIME;
begin
  Coder := TIdEncoderMIME.Create(nil);
  try
    Blob.Seek(0, soFromBeginning);
    result := '''' + Coder.EncodeStream(Blob) + '''';
  finally
    Coder.Free;
  end;
end;

procedure TSSXMLWriter.WriteIndent;
var
  Str: string;
  idx: integer;
begin
  if (FIndent > 0) and FWhitespaceToFile then
  begin
    SetLength(Str, FIndent);
    for idx := 1 to FIndent do
      Str[idx] := S_SPACE;
    WriteStringToStream(Str);
  end;
end;

procedure TSSXMLWriter.WriteStartTag(STag: string);
begin
  WriteIndent;
  WriteStringToStream(STag);
  WriteStringToStream(S_CR_LF);
  Inc(FIndent);
end;

procedure TSSXMLWriter.WriteItem(Item: string);
begin
  WriteIndent;
  WriteStringToStream(Item);
  WriteStringToStream(S_CR_LF);
end;

procedure TSSXMLWriter.WriteEndTag(ETag: string);
begin
  if FIndent > 0 then Dec(FIndent);
  WriteIndent;
  WriteStringToStream(ETag);
  WriteStringToStream(S_CR_LF);
end;

procedure TSSXMLWriter.WriteStringToStream(Str: string);
var
  STLen: integer;
  PDat: pointer;
  AnsiRep: AnsiString;
begin
  AnsiRep := AnsiString(Str);
  STLen := Length(AnsiRep);
  PDat := @AnsiRep[1];
  FStream.WriteBuffer(PDat^, STLen);
end;

procedure TSSXMLWriter.WriteInstances(Instances: TSSIList);
var
  Idx: integer;
  Inst: TSSIInstance;
  TagStr: string;
begin
  if not Assigned(Instances) then
    raise EStreamSystemError.Create(S_INVALID_INSTANCES);
  WriteStartTag(S_InstancesStart);
  for Idx := 0 to Pred(Instances.Count) do
  begin
    Inst := TSSIInstance(Instances.Items[Idx]);
    if not Assigned(Inst) then
      raise EStreamSystemError.Create(S_INVALID_INSTANCES);
    TagStr := S_InstTagStart +
      S_SPACE +
      S_CTYPE_STR + S_eq + EscapeString(Inst.ClassTypeString) +
      S_TagShortClose;
    WriteItem(TagStr);
  end;
  WriteEndTag(S_InstancesEnd);
end;

procedure TSSXMLWriter.WriteStrPropData(Prop: TSSIProperty);
begin
  if Prop.PropData.PropType <> sMajStr then
    raise EStreamSystemError.Create(S_BAD_PROP_FOR_ENCODING);
  if Prop.PropData.StrRequiredEncode = scHashEnc then
    WriteItem(HashEncString(Prop.PropData.StrData))
  else //scAscii
    WriteItem(EscapeString(Prop.PropData.StrData));
end;

procedure TSSXMLWriter.WriteBlobPropData(Prop: TSSIProperty);
begin
  if (Prop.PropData.PropType <> sMajBlob)
    or (not Assigned(Prop.PropData.BlobData)) then
    raise EStreamSystemError.Create(S_BAD_PROP_FOR_ENCODING);
  WriteItem(MimeEncBlob(Prop.PropData.BlobData));
end;

procedure TSSXMLWriter.CalcStrPropEncoding(Prop: TSSIProperty);
var
  idx: integer;
begin
  if Prop.PropData.PropType <> sMajStr then
    raise EStreamSystemError.Create(S_BAD_PROP_FOR_ENCODING);
  Prop.PropData.StrRequiredEncode := scAscii;
  if Prop.PropData.PropSubType = mitUnicodeString then
  begin
    for idx := 1 to Length(Prop.PropData.StrData) do
    begin
      if Ord(Prop.PropData.StrData[idx]) > High(byte) then
      begin
        Prop.PropData.StrRequiredEncode := scHashEnc;
        exit;
      end;
    end;
  end;
end;

procedure TSSXMLWriter.WritePropData(Prop: TSSIProperty);
begin
  case Prop.PropData.PropType of
    sMajOrd: WriteItem(IntToStr(Prop.PropData.OrdData));
    sMajStr: WriteStrPropData(Prop);
    sMajBlob: WriteBlobPropData(Prop);
    sMajFloat: WriteItem(FloatToStr(Prop.PropData.FloatData));
    sMajRec,
      sMajArray: WriteProperties(Prop.PropData.Items);
    sMajClass: WriteItem(IntToStr(Prop.PropData.ObjId));
  else
    raise EStreamSystemError.Create(S_UNKNOWN_PROP_TYPE);
  end;
end;

function TSSXMLWriter.GetPropAttrStr(Prop: TSSIProperty): string;
begin
  result := S_SPACE + S_PROP_NAME + S_EQ + EscapeString(Prop.PropData.PropName)
    +
    S_SPACE + S_PROP_TYPE + S_EQ +
    EscapeString(TSSIMajorTypeNames[Prop.PropData.PropType]);
  if Prop.PropData.PropSubType <> mitNone then
  begin
    result := result + S_SPACE + S_PROP_SUBTYPE + S_EQ +
      EscapeString(TSSIMinorTypeNames[Prop.PropData.PropSubType]);
  end;
  if (Prop.PropData.PropType = sMajOrd) and Prop.PropData.LimsApply then
  begin
    result := result + S_SPACE + S_MIN_VAL + S_EQ +
      EscapeString(IntToStr(Prop.PropData.MinVal)) +
      S_SPACE + S_MAX_VAL + S_EQ + EscapeString(IntToStr(Prop.PropData.MaxVal));
  end
  else if (Prop.PropData.PropType = sMajStr) then
  begin
    CalcStrPropEncoding(Prop);
    if (Prop.PropData.PropSubType = mitShortString) then
    begin
      result := result + S_SPACE + S_MAX_STRLEN + S_EQ +
        EscapeString(IntToStr(Prop.PropData.MaxStrLen));
    end;
    if Prop.PropData.StrRequiredEncode <> scAscii then
    begin
      result := result + S_SPACE + S_STR_ENCODE + S_EQ +
        EscapeString(TSSIStringEncodeNames[Prop.PropData.StrRequiredEncode]);
    end;
  end;
end;

function TSSXMLWriter.PropHasData(Prop: TSSIProperty): boolean;
begin
  result := true;
  if (Prop.PropData.PropType in [sMajRec, sMajArray]) then
  begin
    if not Assigned(Prop.PropData.Items) then
      raise EStreamSystemError.Create(S_INVALID_PROP_CONTENTS);
    if (Prop.PropData.Items.Count = 0) then
      result := false;
  end;
end;

procedure TSSXMLWriter.WriteProperties(Properties: TSSIList);
var
  Idx: integer;
  Prop: TSSIProperty;
  TagStr: string;
begin
  if not Assigned(Properties) then
    raise EStreamSystemError.Create(S_INVALID_PROPS_DATA);
  for Idx := 0 to Pred(Properties.Count) do
  begin
    Prop := TSSIProperty(Properties.Items[Idx]);
    if not Assigned(Prop) then
      raise EStreamSystemError.Create(S_INVALID_PROPS_DATA);
    TagStr := S_PropTagStart + GetPropAttrStr(Prop);
    if PropHasData(Prop) then
    begin
      TagStr := TagStr + S_TagClose;
      WriteStartTag(TagStr);
      WritePropData(Prop);
      WriteEndTag(S_PropTagEnd);
    end
    else
    begin
      TagStr := Tagstr + S_TagShortClose;
      WriteItem(TagStr);
    end;
  end;
end;


procedure TSSXMLWriter.WriteInstancesData(InstancesData: TSSIList);
var
  Idx: integer;
  InstData: TSSIInstanceData;
  TagStr: string;
begin
  if not Assigned(InstancesData) then
    raise EStreamSystemError.Create(S_INVALID_INSTANCES_DATA);
  WriteStartTag(S_InstsDataStart);
  for Idx := 0 to Pred(InstancesData.Count) do
  begin
    InstData := TSSIInstanceData(InstancesData.Items[Idx]);
    if not Assigned(InstData) then
      raise EStreamSystemError.Create(S_INVALID_INSTANCES_DATA);
    TagStr := S_IDatTagStart;
    if InstData.Properties.Count > 0 then
    begin
      WriteStartTag(TagStr + S_TagClose);
      WriteProperties(InstData.Properties);
      WriteEndTag(S_IDatTagEnd);
    end
    else
      WriteItem(TagStr + S_TagShortClose);
  end;
  WriteEndTag(S_InstsDataEnd);
end;

function TSSXMLWriter.WriteIRepToStream(Stream: TStream; IRep: TSSITransaction):
  boolean;
begin
  result := false;
  if not (Assigned(Stream) and Assigned(IRep)) then exit;
  FIndent := 0;
  FStream := TMemoryStream.Create;
  try
    //Write stuff into memory copy.
    try
      WriteStartTag(S_TXmlStart);
      WriteStartTag(S_TransStart);
      WriteInstances(IRep.Instances);
      WriteInstancesData(IRep.InstancesData);
      WriteEndTag(S_TransEnd);
      WriteEndTag(S_TXmlEnd);
      //Now copy into final stream.
      Stream.CopyFrom(FStream, 0);
      result := true;
    except
      on E: EStreamError do
        LogEvent(sssError, S_ERR_WRITING_STREAM + E.Message);
      on E: EStreamSystemError do LogEvent(sssError, E.Message);
    end;
  finally
    FStream.Free;
  end;
end;


end.
