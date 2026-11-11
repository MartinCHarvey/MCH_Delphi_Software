unit FractStreaming;
{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

interface

uses
  StreamSysXML;

function GetStreamSystemForTransaction: TStreamSysXML;
function GetTransactionErr: string;

implementation

uses
  FractSettings, StreamingSystem, SSStreamables, Trackables,
  SSAbstracts, FractLegacy;

type
{$IFDEF USE_TRACKABLES}
  TErrCatcher = class(TTrackable)
{$ELSE}
  TErrCatcher = class
{$ENDIF}
  private
    FErrStr: string;
  public
    procedure HandleLogEvent(Sender: TAbstractSSComponent;
      Severity: TSSSeverity;
      Msg: string);
    property ErrStr: string read FErrStr write FErrStr;
  end;

var
  SS: TStreamSysXML;
  Catcher: TErrCatcher;
  Heirarchy: THeirarchyInfo;

function GetStreamSystemForTransaction: TStreamSysXML;
begin
  Catcher.ErrStr := '';
  result := SS;
end;

function GetTransactionErr: string;
begin
  result := Catcher.ErrStr;
end;

procedure SetupHeirarchy(var H: THeirarchyInfo);
begin
  H := TObjDefaultHeirarchy;
  with H do
  begin
    SetLength(MemberClasses, 13);
    MemberClasses[0] := TFractPalette;
    MemberClasses[1] := TColorMark;
    MemberClasses[2] := TFractItemList;
    MemberClasses[3] := TFractV2HistoryList;
    MemberClasses[4] := TFractV2WaypointList;
    MemberClasses[5] := TFractV2Comparable;
    MemberClasses[6] := TFractV2LocationSettings;
    MemberClasses[7] := TFractV2FormulaSettings;
    MemberClasses[8] := TFractV2SettingsBundle;
    MemberClasses[9] := TFractSettings;
    MemberClasses[10] := TFractVersionTag;
    MemberClasses[11] := TFractV2Environment;
    MemberClasses[12] := TFractV2VideoEnv;
  end;
end;

procedure TErrCatcher.HandleLogEvent(Sender: TAbstractSSComponent;
  Severity: TSSSeverity; Msg: string);
begin
  if (Length(FErrStr) = 0) and (Severity = sssError) then
    FErrStr := Msg;
end;

initialization
  SS := TStreamSysXML.Create;
  Catcher := TErrCatcher.Create;
  SetupHeirarchy(Heirarchy);
  SS.RegisterHeirarchy(Heirarchy);
  SS.FailOnNoClass := true;
  SS.FailOnNoProperty := false;
  SS.FailOnClassUnused := false;
  SS.FailOnPropertyUnused := false;
  SS.OnLogEvent := Catcher.HandleLogEvent;
finalization
  SS.Free;
  Catcher.Free;
end.
