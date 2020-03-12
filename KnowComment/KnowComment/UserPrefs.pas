unit UserPrefs;
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
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SysUtils, DataObjects, SSStreamables;

  //TODO - Read / write and streaming of preferences, with relative
  //times (x days old). currently just fixed TDateTimes.

type
  TUserPrefs = class(TObjStreamable)
  private
    FDataRootDir: string;
    FLoadBackDays: cardinal;
    FImageExpireDays: cardinal;
    FDataExpireDays: cardinal;
    FExpireMedia: boolean;
    FExpireComments: boolean;
    function GetLoadSince: TDateTime;
    function GetImageExpireBefore: TDateTime;
    function GetExpireBefore: TDateTime;
    function GetExpireLevels: TKListLevelSet;
  public
    constructor Create; override;
    property LoadSince: TDateTime read GetLoadSince;
    property ImageExpireBefore: TDateTime read GetImageExpireBefore;
    property ExpireBefore: TDateTime read GetExpireBefore;
    property ExpireLevels: TKListLevelSet read GetExpireLevels;
  published
    property LoadBackDays: cardinal read FLoadBackDays write FLoadBackDays;
    property ImageExpireDays: cardinal read FImageExpireDays write FImageExpireDays;
    property DataExpireDays: cardinal read FDataExpireDays write FDataExpireDays;
    property ExpireMedia: boolean read FExpireMedia write FExpireMedia;
    property ExpireComments: boolean read FExpireComments write FExpireComments;
    property DataRootDir: string read FDataRootDir write FDataRootDir;
  end;

var
  CurSessUserPrefs: TUserPrefs = nil;

implementation

uses
  IOUtils, StreamSysXML, StreamingSystem, Classes;

const
  DEFAULT_DATA_ROOT_LOCATION = 'MemDB';
  DEFAULT_APP_DATA_DIR = 'KnowComment';
  DEFAULT_PREFS_FILENAME = 'Knowcomment.prefs';

{$IFDEF MSWINDOWS}
const
  PathSep = '\';
{$ELSE}
const
  PathSep = '/';
{$ENDIF}

var
  ConfFilename: string;
  XMLStream: TStreamSysXML;
  Heirarchy: THeirarchyInfo;
  FileStream: TFileStream;
  RootDir, DBDir: string;

//TODO - Remove the need for this by properly combining and
//canonicalising paths with IoUtils.TPath
procedure AppendTrailingDirSlash(var Path: string);
begin
  if Length(Path) > 0 then
  begin
    if (Path[Length(Path)] <> PathSep) then
      Path := Path + PathSep;
  end;
end;

function TUserPrefs.GetLoadSince: TDateTime;
begin
  result := Now - FLoadBackDays;
end;

function TUserPrefs.GetImageExpireBefore: TDateTime;
begin
  result := Now - FImageExpireDays;
end;

function TUserPrefs.GetExpireBefore: TDateTime;
begin
  result := Now - FDataExpireDays;
end;

function TUserPrefs.GetExpireLevels: TKListLevelSet;
begin
  result := [];
  if FExpireMedia then
    result := result + [klMediaList];
  if FExpireComments then
    result := result + [klCommentList];
end;

constructor TUserPrefs.Create;
begin
  inherited;
  FDataRootDir:= DBDir;
  FLoadBackDays := 7;
  FImageExpireDays := 7;
  FDataExpireDays := 3650;
  FExpireMedia := true;
  FExpireComments := true;
end;

procedure SetupHeirarchy(var H: THeirarchyInfo);
begin
  H := TObjDefaultHeirarchy;
  with H do
  begin
    //Don't ever expect to create a TMemDBStreamable
    //or a TMemEntityMetadataItem, or a TMemEntityItem
    SetLength(MemberClasses, 1);
    MemberClasses[0] :=  TUserPrefs;
  end;
end;

initialization
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
  try
    try
      XMLStream := TStreamSysXML.Create;
      //Don't fail on no class / no property.
      XMLStream.RegisterHeirarchy(Heirarchy);
      FileStream := TFileStream.Create(ConfFileName, fmOpenRead);
      CurSessUSerPrefs := XMLStream.ReadStructureFromStream(FileStream) as TUserPrefs;
    except
      on Exception do ;
    end;
  finally
    FreeAndNil(FileStream);
    FreeAndNil(XMLStream);
  end;
  if not Assigned(CurSessUserPrefs) then
    CurSessUserPrefs := TUserPrefs.Create;
finalization
  try
    try
      XMLStream := TStreamSysXML.Create;
      //Don't fail on no class / no property.
      XMLStream.RegisterHeirarchy(Heirarchy);
      FileStream := TFileStream.Create(ConfFileName, fmCreate);
      XMLStream.WriteStructureToStream(CurSessUserPrefs, FileStream);
    except
      on Exception do ;
    end;
  finally
    FreeAndNil(FileStream);
    FreeAndNil(XMLStream);
  end;
  CurSessUserPrefs.Free;
end.
