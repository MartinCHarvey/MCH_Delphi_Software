program ProjectXPlant;
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

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils, System.IoUtils, Classes, Windows,
  PathFileUtils in 'PathFileUtils.pas',
  DelphiDpr in 'DelphiDpr.PAS',
  CocoBase in '..\CoCoDM\Distributable\Frames\CocoBase.pas';

//ParamStr and ParamCount.
//Get working directory?

type
  TFileExtSet = array of string;

var
  DprFileExts: TFileExtSet;
  PasFileExts: TFileExtSet;

function ExtensionMatches(Extension: string; ExtSet: TFileExtSet): boolean;
var
  Idx: integer;
begin
  for Idx := 0 to Pred(Length(ExtSet)) do
  begin
    if CompareText(Extension, ExtSet[idx]) = 0 then
    begin
      result := true;
      exit;
    end;
  end;
  result := false;
end;

function CreateDirectoryTreeToFile(F: TCanonicalPath): boolean;
var
  DirPath: TCanonicalPath;
  Idx: integer;
begin
  result := true;
  DirPath := TCanonicalPath.Create;
  try
    for Idx := 0 to Pred(F.ComponentCount) do
    begin
      DirPath.Assign(F);
      DirPath.ComponentCount := Idx;
      if not DirectoryExists(DirPath.Path) then
      begin
        if not CreateDirectory(PWChar(DirPath.Path), nil) then
        begin
          WriteLn('ERROR: Failed to Create Directory ' + DirPath.Path);
          WriteLn('LastError: ' + IntToHex(GetLastError(), 8));
          result := false;
          exit;
        end;
      end;
    end;
  finally
    DirPath.Free;
  end;
end;

procedure ProcessFileList(DprName: string; FileList: TStringList; OutDirectory: string);
var
  Idx, Idx2: integer;
  CanonSrcPaths, CanonDestPaths: TList;
  OutDir: TCanonicalPath;
  TopSrcDir: TCanonicalPath;
  CanonPath, CanonDestPath: TCanonicalPath;
  SrcPathStr, DestPathStr, Prompt: string;
  IsPasFile, IsDprFile: boolean;
  Exts: TFileExtSet;
begin
  CanonSrcPaths := TList.Create;
  CanonDestPaths := TList.Create;
  OutDir := TCanonicalPath.Create;
  TopSrcDir := TCanonicalPath.Create;
  try
    FileList.Insert(0, DprName); //Yes, we need to copy the dpr too.
    //Check output directory exists.
    if not DirectoryExists(OutDirectory) then
      raise Exception.Create('Output directory not found: ' + OutDirectory);

    OutDir.Path := OutDirectory;
    OutDir.MakeAbsolute;

    for Idx := 0 to Pred(FileList.Count) do
    begin
      CanonPath := TCanonicalPath.Create;
      CanonSrcPaths.Add(CanonPath);
      CanonPath.Path := FileList[Idx];
      if (Idx > 0) and (CanonPath.PathType = cptRelative) then
        CanonPath.MakeRelativeToFile(TCanonicalPath(CanonSrcPaths.Items[0]));
      CanonPath.MakeAbsolute;
    end;

    //Check files exist.
    Idx := 0;
    while Idx < CanonSrcPaths.Count do
    begin
      CanonPath := TCanonicalPath(CanonSrcPaths.Items[idx]);
      if not FileExists(CanonPath.Path) then
      begin
        WriteLn('WARNING: Skipping file: ' + CanonPath.Path);
        CanonPath.Free;
        CanonSrcPaths.Delete(idx);
      end
      else
        Inc(Idx);
    end;
    if FileList.Count <= 1 then
    begin
      WriteLn('Need at least two files to continue, aborting.');
      exit;
    end;

    for Idx := 0 to Pred(CanonSrcPaths.Count) do
    begin
      CanonPath := TCanonicalPath(CanonSrcPaths.Items[idx]);
      WriteLn('Canonical src path: ' + CanonPath.Path);
    end;

    //So far so good. Now find top directory of source.
    TopSrcDir.Assign(TCanonicalPath(CanonSrcPaths.Items[0]));
    for Idx := 0 to Pred(CanonSrcPaths.Count) do
    begin
      CanonPath := TCanonicalPath(CanonSrcPaths.Items[idx]);
      TopSrcDir.FindCommonRoot(CanonPath);
    end;

    WriteLn('Common root source path: ' + TopSrcDir.Path);

    WriteLn('');
    WriteLn('Canonical target directory: ' + OutDir.Path);

    //OK, now create the potential dest list of files.
    for Idx := 0 to Pred(CanonSrcPaths.Count) do
    begin
      CanonPath := TCanonicalPath(CanonSrcPaths.Items[idx]);
      CanonDestPath := TCanonicalPath.Create;
      CanonDestPath.Assign(CanonPath);
      CanonDestPath.ReRoot(TopSrcDir, OutDir);
      CanonDestPaths.Add(CanonDestPath);
      WriteLn('Canonical dest path: ' + CanonDestPath.Path);
    end;

    WriteLn('');
    WriteLn('Continue with the copy (y/n)?');
    ReadLn(Prompt);
    if not ((Prompt = 'y') or (Prompt = 'Y')) then
    begin
      WriteLn('OK, aborting.');
      exit;
    end;

    //Now do the copy.
    Assert(CanonSrcPaths.Count = CanonDestPaths.Count);
    for Idx := 0 to Pred(CanonSrcPaths.Count) do
    begin
      CanonPath := TCanonicalPath(CanonSrcPaths.Items[idx]);
      CanonDestPath := TCanonicalPath(CanonDestPaths.Items[Idx]);
      SrcPathStr := CanonPath.Path;
      DestPathStr := CanonDestPath.Path;
      IsPasFile := ExtensionMatches(TPath.GetExtension(SrcPathStr), PasFileExts);
      IsDprFile := ExtensionMatches(TPath.GetExtension(SrcPathStr), DprFileExts);
      if IsPasFile = IsDprFile then
      begin
        WriteLn('WARNING: Unknown file type, skipping: ' + SrcPathStr);
        continue;
      end;
      if IsPasFile then
        Exts := PasFileExts
      else
        Exts := DprFileExts;
      for Idx2 := 0 to Pred(Length(Exts)) do
      begin
        SrcPathStr := TPath.ChangeExtension(SrcPathStr, Exts[idx2]);
        DestPathStr := TPath.ChangeExtension(DestPathStr, Exts[idx2]);
        if FileExists(SrcPathStr) then
        begin
          if CreateDirectoryTreeToFile(CanonDestPath) then
          begin
            WriteLn('Copying: ' + SrcPathStr + ' to ' + DestPathStr);
            if not Windows.CopyFile(PwChar(SrcPathStr), PwChar(DestPathStr), false) then
            begin
              WriteLn('ERROR: Failed to copy ' + SrcPathStr + ' to ' + DestPathStr);
              WriteLn('LastError: ' + IntToHex(GetLastError(), 8));
            end;
          end;
        end;
      end;
    end;

  finally
    for Idx := 0 to Pred(CanonDestPaths.Count) do
      TObject(CanonDestPaths.Items[idx]).Free;
    CanonDestPaths.Free;
    for Idx := 0 to Pred(CanonSrcPaths.Count) do
      TObject(CanonSrcPaths.Items[idx]).Free;
    CanonSrcPaths.Free;
    OutDir.Free;
    TopSrcDir.Free;
  end;
end;

procedure Process(DprName: string; OutDirectory: string);
var
  FS: TFileStream;
  MS: TMemoryStream;
  DPRG: TDelphiDpr;
  FileList: TStringList;
  Idx: integer;
  CoCoError: TCoCoError;
begin
  FileList := TStringList.Create;
  try
    FS := TFileStream.Create(DprName, fmOpenRead);
    MS := nil;
    try
      MS := TMemoryStream.Create;
      MS.CopyFrom(FS, FS.Size);
      DPRG := TDelphiDpr.Create(nil);
      try
        DPRG.SourceStream := MS;
        DPRG.Execute;
        if DPRG.ErrorList.Count = 0 then
          FileList.Assign(DPRG.LocationList)
        else
        begin
          for Idx := 0 to Pred(DPRG.ErrorList.Count) do
          begin
            CocoError := TCocoError(DPRG.ErrorList.Items[Idx]);
            WriteLn('Error: ' + DPRG.ErrorStr(CocoError.ErrorCode, ''));
          end;
        end;
      finally
        DPRG.Free; //Frees MS.
      end;
    finally
      FS.Free;
    end;

    if FileList.Count > 0 then
      ProcessFileList(DprName, FileList, OutDirectory)
    else
      WriteLn('No files found in DPR, aborting.');
  finally
    FileList.Free;
  end;
end;

procedure PrintUsage;
begin
  WriteLn('Usage: ProjectXPlant <.dpr file> <output directory>');
end;

begin
  SetLength(DprFileExts, 3);
  DprFileExts[0] := '.dpr';
  DprFileExts[1] := '.dproj';
  DprFileExts[2] := '.res';

  SetLength(PasFileExts, 5);
  PasFileExts[0] := '.pas';
  PasFileExts[1] := '.dfm';
  PasFileExts[2] := '.agi';
  PasFileExts[3] := '.atg';
  PasFileExts[4] := '.fmx';

  try
    if ParamCount <>  2 then
    begin
      PrintUsage;
      Exit;
    end;

    Process(ParamStr(1), ParamStr(2));

  except
    on E: Exception do
      Writeln('Exception: ' + E.ClassName, ': ', E.Message);
  end;
end.
