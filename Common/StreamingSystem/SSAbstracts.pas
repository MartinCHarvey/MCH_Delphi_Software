unit SSAbstracts;
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

  The unit defines the abstract representations of the various parts in the
  streaming system. The implementation is that minimal one which makes use
  of the basic interfaces defined here: further specialisation may be needed
  in derived classes.
}
interface

uses SysUtils, Trackables, SSIntermediates, Classes;

type
  EStreamSystemError = class(Exception);

  TAbstractSSComponent = class;

  TSSSeverity = (sssInfo, sssWarning, sssError);

  TSSLogEvent = procedure(Sender: TAbstractSSComponent;
    Severity: TSSSeverity;
    Msg: string) of object;

  TAbstractStreamSystem = class;

{$IFDEF USE_TRACKABLES}
  TAbstractSSComponent = class(TTrackable)
{$ELSE}
  TAbstractSSComponent = class
{$ENDIF}
  private
    FStreamSystem: TAbstractStreamSystem;
    FLogEvent: TSSLogEvent;
  protected
    property StreamSystem: TAbstractStreamSystem
      read FStreamSystem write FStreamSystem;
    property OnLogEvent: TSSLogEvent read FLogEvent write FLogEvent;
    procedure LogEvent(Severity: TSSSeverity; Msg: string);
  public
  end;

  TAbstractSSController = class(TAbstractSSComponent)
  private
  protected
  public
    function ConvertStructureToIRep(Struct: TObject): TSSITransaction; virtual;
      abstract;
    function BuildStructureFromIRep(IRep: TSSITransaction): TObject; virtual;
      abstract;
  end;

  TAbstractSSReader = class(TAbstractSSComponent)
  private
  protected
  public
    function ReadIRepFromStream(Stream: TStream): TSSITransaction; virtual;
      abstract;
  end;

  TAbstractSSWriter = class(TAbstractSSComponent)
  private
  protected
  public
    function WriteIRepToStream(Stream: TStream; IRep: TSSITransaction): boolean;
      virtual; abstract;
  end;

  TAbstractStreamSystem = class(TAbstractSSComponent)
  private
    FController: TAbstractSSController;
    FReader: TAbstractSSReader;
    FWriter: TAbstractSSWriter;
  protected
    property Controller: TAbstractSSController read FController write
      FController;
    property Reader: TAbstractSSReader read FReader write FReader;
    property Writer: TAbstractSSWriter read FWriter write FWriter;
    procedure CreateParts; virtual; abstract;
    procedure HandleComponentLogEvent(Sender: TAbstractSSComponent;
      Severity: TSSSeverity; Msg: string);
  public
    constructor Create;
    destructor Destroy; override;
    function WriteStructureToStream(RootInstance: TObject; Stream: TStream):
      boolean;
    function ReadStructureFromStream(Stream: TStream): TObject;
    property OnLogEvent;
  end;

implementation

{$IFDEF DEBUG_DATABASE}
  uses GlobalLog;
{$ENDIF}

const
  S_WRITE_STRUCTURE_INVALID_PARAMS =
    'Invalid parameters to write structure call';
  S_READ_STRUCTURE_INVALID_PARAMS =
    'Invalid parameters to read structure call';

(************************************
 * TAbstractSSComponent             *
 ************************************)

procedure TAbstractSSComponent.LogEvent(Severity: TSSSeverity; Msg: string);
begin
  if Assigned(FLogEvent) then
    FLogEvent(self, Severity, Msg);
end;

(************************************
 * TAbstractStreamSystem            *
 ************************************)

procedure TAbstractStreamSystem.HandleComponentLogEvent(
  Sender: TAbstractSSComponent; Severity: TSSSeverity; Msg: string);
begin
  LogEvent(Severity, Msg);
end;

constructor TAbstractStreamSystem.Create;
begin
  inherited;
  CreateParts;
  Assert(Assigned(FController));
  Assert(Assigned(FReader));
  Assert(Assigned(FWriter));
  FController.OnLogEvent := HandleComponentLogEvent;
  FReader.OnLogEvent := HandleComponentLogEvent;
  FWriter.OnLogEvent := HandleComponentLogEvent;
end;

destructor TAbstractStreamSystem.Destroy;
begin
  FController.Free;
  FReader.Free;
  FWriter.Free;
  inherited;
end;

function TAbstractStreamSystem.WriteStructureToStream(RootInstance: TObject;
  Stream: TStream): boolean;
var
  StructureIRep: TSSITransaction;
begin
  if not (Assigned(RootInstance) and Assigned(Stream)) then
  begin
    LogEvent(sssError, S_WRITE_STRUCTURE_INVALID_PARAMS);
    result := false;
    exit;
  end;
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, DateTimeToStr(Now) +  ' Convert to IRep');
{$ENDIF}
  StructureIRep := FController.ConvertStructureToIRep(RootInstance);
  try
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, DateTimeToStr(Now) +  ' Write IRep to stream');
{$ENDIF}
    if Assigned(StructureIRep) then
      result := FWriter.WriteIRepToStream(Stream, StructureIRep)
    else
      result := false;
  finally
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, DateTimeToStr(Now) +  ' Done');
{$ENDIF}
    StructureIRep.Free;
  end;
end;

function TAbstractStreamSystem.ReadStructureFromStream(Stream: TStream):
  TObject;
var
  StructureIRep: TSSITransaction;
begin
  if not Assigned(Stream) then
  begin
    LogEvent(sssError, S_READ_STRUCTURE_INVALID_PARAMS);
    result := nil;
    exit;
  end;
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, DateTimeToStr(Now) +  ' Read IRep from stream');
{$ENDIF}
  StructureIRep := FReader.ReadIRepFromStream(Stream);
  try
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, DateTimeToStr(Now) + ' Build from IRep');
{$ENDIF}
    if Assigned(StructureIRep) then
      result := FController.BuildStructureFromIRep(StructureIRep)
    else
      result := nil;
  finally
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, DateTimeToStr(Now) + ' Done');
{$ENDIF}
    StructureIRep.Free;
  end;
end;

end.
