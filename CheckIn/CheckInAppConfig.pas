unit CheckInAppConfig;

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
  SSStreamables, IdExplicitTLSClientServerBase;

type
  TCheckInAppConfig = class(TObjStreamable)
  private
    FDBRootDir: string;
    FEndpointName: string;
    FMailerServer:string;
    FMailerUName:string;
    FMailerPasswd:string;
    FMailerPort: integer;
    FMailerUseTLS: TIdUseTLS;
    FMailerSenderEmailName: string;
    FMailerSenderEmailAddress: string;
  public
    constructor Create; override;
  published
    property DBRootDir:string read FDBRootDir write FDBRootDir;
    property EndpointName: string read FEndpointName write FEndpointName;
    property MailerServer:string read FMailerServer write FMailerServer;
    property MailerUName:string read FMailerUName write FMailerUName;
    property MailerPasswd:string read FMailerPasswd write FMailerPasswd;
    property MailerPort:integer read FMailerPort write FMailerPort;
    property MailerUseTLS: TIdUseTLS read FMailerUseTLS write FMailerUseTLS;
    property MailerSenderEmailName: string read FMailerSenderEmailName write FMailerSenderEmailName;
    property MailerSenderEmailAddress: string read FMailerSenderEmailAddress write FMailerSenderEmailAddress;
  end;

var
  GAppConfig: TCheckInAppConfig;

implementation

uses
  StreamingSystem, StreamSysXML, SysUtils, Classes, IoUtils;

const
  DEFAULT_DATA_ROOT_LOCATION = 'MemDB';
  DEFAULT_APP_DATA_DIR = 'CheckIn';
  DEFAULT_PREFS_FILENAME = 'CheckIn.prefs';

var
  RootDir, DBDir: string;
  ConfFilename: string;
  Heirarchy: THeirarchyInfo;

{$IFDEF MSWINDOWS}
const
  PathSep = '\';
{$ELSE}
const
  PathSep = '/';
{$ENDIF}

procedure AppendTrailingDirSlash(var Path: string);
begin
  if Length(Path) > 0 then
  begin
    if (Path[Length(Path)] <> PathSep) then
      Path := Path + PathSep;
  end;
end;

constructor TCheckInAppConfig.Create;
begin
  inherited;
  FDBRootDir:= DBDir;
  FMailerUseTLS := TIdUseTLS.utUseRequireTLS;
end;

procedure SetupHeirarchy(var H: THeirarchyInfo);
begin
  H := TObjDefaultHeirarchy;
  with H do
  begin
    SetLength(MemberClasses, 1);
    MemberClasses[0] :=  TCheckInAppConfig;
  end;
end;

procedure SetupPrefs;
var
  XMLStream: TStreamSysXML;
  FileStream: TFileStream;
begin
  RootDir := TPath.GetHomePath;
  AppendTrailingDirSlash(RootDir);
  if not DirectoryExists(RootDir) then
    TDirectory.CreateDirectory(RootDir);
  RootDir := RootDir + DEFAULT_APP_DATA_DIR;
  AppendTrailingDirSlash(RootDir);
  if not DirectoryExists(RootDir) then
    TDirectory.CreateDirectory(RootDir);
  DBDir := RootDir + DEFAULT_DATA_ROOT_LOCATION;
  AppendTrailingDirSlash(DBDir);
  if not DirectoryExists(DBDir) then
    TDirectory.CreateDirectory(DBDir);
  ConfFileName := RootDir + DEFAULT_PREFS_FILENAME;
  SetupHeirarchy(Heirarchy);

  FileStream := nil;
  XMLStream := nil;
  try
    try
      XMLStream := TStreamSysXML.Create;
      //Don't fail on no class / no property.
      XMLStream.RegisterHeirarchy(Heirarchy);
      FileStream := TFileStream.Create(ConfFileName, fmOpenRead);
      GAppConfig := XMLStream.ReadStructureFromStream(FileStream) as TCheckInAppConfig;
    except
      on Exception do ;
    end;
  finally
    FreeAndNil(FileStream);
    FreeAndNil(XMLStream);
  end;
  if not Assigned(GAppConfig) then
    GAppConfig := TCheckInAppConfig.Create;
end;

procedure FinishPrefs;
var
  XMLStream: TStreamSysXML;
  FileStream: TFileStream;
begin
  //TODO - Setup of machine name / address and keeping it up to date.
  FileStream := nil;
  XMLStream := nil;
  try
    try
      XMLStream := TStreamSysXML.Create;
      //Don't fail on no class / no property.
      XMLStream.RegisterHeirarchy(Heirarchy);
      FileStream := TFileStream.Create(ConfFileName, fmCreate);
      XMLStream.WriteStructureToStream(GAppConfig, FileStream);
    except
      on Exception do ;
    end;
  finally
    FreeAndNil(FileStream);
    FreeAndNil(XMLStream);
  end;
  GAppConfig.Free;
end;

initialization
  SetupPrefs;
finalization
  FinishPrefs;
end.
