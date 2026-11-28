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
  TCmdOpts = record
    Listing: boolean;
    Src, Dest: string;
    IgnorePaths: TStringList;
  end;

var
  DprFileExts: TFileExtSet;
  PasFileExts: TFileExtSet;
  SupplementaryExts: TFileExtSet;
  Opts: TCmdOpts;

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

procedure AddSupplementaryFiles(CanonicalList: TList);
var
  i, j: Integer;
  TmpDir, TmpSrch, TmpFile, NewFile: TCanonicalPath;
  TmpFiles: TStringList;
  SrcF: TCanonicalPath;
  SR: TSearchRec;
  SRet: integer;
begin
  TmpDir := TCanonicalPath.Create;
  TmpSrch := TCanonicalPath.Create;
  TmpFile := TCanonicalPath.Create;
  TmpFiles := TStringList.Create;
  try
    for i := 0 to Pred(CanonicalList.Count) do
    begin
      //Should be an absolute path.
      SrcF := TObject(CanonicalList.Items[i]) as TCanonicalPath;
      TmpDir.Assign(SrcF);
      TmpDir.ComponentCount := Pred(TmpDir.ComponentCount);
      if DirectoryExists(TmpDir.Path) then
      begin
        for j := 0 to Pred(Length(SupplementaryExts)) do
        begin
          TmpSrch.Assign(TmpDir);
          TmpSrch.AddComponent('*' +SupplementaryExts[j]);
          SRet := FindFirst(TmpSrch.Path, faNormal, SR);
          if SRet = 0 then
          begin
            try
              while SRet = 0 do
              begin
                TmpFile.Assign(TmpDir);
                TmpFile.AddComponent(SR.Name);
                TmpFiles.Add(TmpFile.Path);
                SRet := FindNext(SR);
              end;
            finally
              System.SysUtils.FindClose(SR);
            end;
          end;
        end;
      end
      else
        WriteLn('WARNING: Skipping dir (supplementary files): ' + TmpDir.Path);
    end;
    //Remove duplicates.
    TmpFiles.Sort;
    i := 0;
    while i < TmpFiles.Count do
    begin
      while (i < Pred(TmpFiles.Count)) and (TmpFiles[i] = TmpFiles[Succ(i)]) do
        TmpFiles.Delete(Succ(i));
      Inc(i);
    end;
    for i := 0 to Pred(TmpFiles.Count) do
    begin
      NewFile := TCanonicalPath.Create;
      NewFile.Path := TmpFiles[i];
      CanonicalList.Add(NewFile);
    end;
  finally
    TmpFiles.Free;
    TmpDir.Free;
    TmpSrch.Free;
    TmpFile.Free;
  end;
end;

procedure ProcessFileList(DprName: string; FileList: TStringList; OutDirectory: string);
var
  Idx, Idx2: integer;
  CanonSrcPaths, CanonDestPaths: TList;
  OutDir: TCanonicalPath;
  TopSrcDir: TCanonicalPath;
  CanonPath, CanonDestPath, Tmp1, Tmp2: TCanonicalPath;
  SrcPathStr, DestPathStr, Prompt: string;
  IsPasFile, IsDprFile: boolean;
  Exts: TFileExtSet;
  Del: boolean;
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

    //Check ignore paths exist, warn if they don't.
    CanonPath := TCanonicalPath.Create;
    try
      Idx := 0;
      while Idx < Opts.IgnorePaths.Count do
      begin
        CanonPath.Path := Opts.IgnorePaths[Idx];
        CanonPath.MakeAbsolute;
        if not DirectoryExists(CanonPath.Path) then
        begin
          WriteLn('WARNING: Skipping ignore directory: ' + CanonPath.Path);
          Opts.IgnorePaths.Delete(idx);
        end
        else
        begin
          Opts.IgnorePaths[idx] := CanonPath.Path;
          Inc(Idx);
        end;
      end;
    finally
      CanonPath.Free;
    end;

    //Now go on a quick trawl for other associated files which may differ by more
    //than just file extension, in all folders which have one of our
    //source files in.
    AddSupplementaryFiles(CanonSrcPaths);

    //And now remove all those files which are in some ignore directory path.
    Tmp1 := TCanonicalPath.Create;
    Tmp2 := TCanonicalPath.Create;
    try
      //OK, now remove those paths which are in the --ignore
      Idx := 0;
      while Idx < CanonSrcPaths.Count do
      begin
        Del := false;
        CanonPath := TCanonicalPath(CanonSrcPaths.Items[Idx]);
        for Idx2 := 0 to Pred(Opts.IgnorePaths.Count) do
        begin
          Tmp1.Path := CanonPath.Path;
          Tmp2.Path := Opts.IgnorePaths[Idx2];
          Tmp1.FindCommonRoot(Tmp2);
          if Tmp1.Path = Tmp2.Path then
          begin
            WriteLn('INFO: File in ignore path, skipping: ' + CanonPath.Path);
            CanonPath.Free;
            CanonSrcPaths.Delete(Idx);
            Del := true;
            break;
          end;
        end;
        if not Del then
          Inc(Idx);
      end;
    finally
      Tmp1.Free;
      Tmp2.Free;
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
      if IsPasFile then
        Exts := PasFileExts
      else if IsDprFile then
        Exts := DprFileExts
      else
      begin
        SetLength(Exts, 1);
        Exts[0] := TPath.GetExtension(SrcPathStr);
      end;
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

procedure Process;
var
  FS: TFileStream;
  MS: TMemoryStream;
  DPRG: TDelphiDpr;
  FileList: TStringList;
  Idx: integer;
  CoCoError: TCoCoError;
  ListName: string;
begin
  FileList := TStringList.Create;
  try
    FS := TFileStream.Create(Opts.Src, fmOpenRead);
    try
      MS := TMemoryStream.Create;
      MS.CopyFrom(FS, FS.Size);
      DPRG := TDelphiDpr.Create(nil);
      try
        if Opts.Listing then
        begin
          ListName := Opts.Src + '.lst';
          DPRG.GenListWhen := glAlways;
        end;
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
          if Opts.Listing then
            DPRG.ListStream.SaveToFile(ListName);
        end;
      finally
        DPRG.Free; //Frees MS.
      end;
    finally
      FS.Free;
    end;

    if FileList.Count > 0 then
      ProcessFileList(Opts.Src, FileList, Opts.Dest)
    else
      WriteLn('No files found in DPR, aborting.');
  finally
    FileList.Free;
  end;
end;

procedure PrintUsage;
begin
  WriteLn('Usage: ProjectXPlant <.dpr file> <output directory> [-l generate parser listing]');
  WriteLn('Opts: ''-l'', ''--list'': Generate parser listing');
  WriteLn('Opts: ''-i'', ''--ignore'': Ignore source paths.');
end;

function ParseOpts: boolean;
var
  i: integer;
begin
  result := false;
  if (ParamCount <  2) then
  begin
    PrintUsage;
    Exit;
  end;
  Opts.Src := ParamStr(1);
  Opts.Dest := ParamStr(2);
  i := 3;
  while i <= ParamCount do
  begin
    if (ParamStr(i) = '-l') or (ParamStr(i) = '--list') then
    begin
      Opts.Listing := true;
      Inc(i)
    end
    else if (ParamStr(i) = '-i') or (ParamStr(i) = '--ignore') then
    begin
      Inc(i);
      if i <= ParamCount then
      begin
        Opts.IgnorePaths.Add(ParamStr(i));
        Inc(i);
      end
      else
      begin
        WriteLn('Ignore parameter requires a directory');
        PrintUsage;
        Exit;
      end;
    end
    else
    begin
      WriteLn('Unrecognised parameter: ' + ParamStr(i));
      PrintUsage;
      exit;
    end;
  end;
  result := true;
end;

begin
  Opts.IgnorePaths := TStringList.Create;
  try
    SetLength(DprFileExts, 3);
    DprFileExts[0] := '.dpr';
    DprFileExts[1] := '.dproj';
    DprFileExts[2] := '.res';

    SetLength(PasFileExts, 3);
    PasFileExts[0] := '.pas';
    PasFileExts[1] := '.dfm';
    PasFileExts[4] := '.fmx';

    SetLength(SupplementaryExts, 6);
    SupplementaryExts[0] := '.agi';
    SupplementaryExts[1] := '.atg';
    SupplementaryExts[2] := '.l';
    SupplementaryExts[3] := '.y';
    SupplementaryExts[4] := '.cod';
    SupplementaryExts[5] := '.inc';

    try
      if not ParseOpts then
        exit;

      Process;
    except
      on E: Exception do
        Writeln('Exception: ' + E.ClassName, ': ', E.Message);

    end;
  finally
    Opts.IgnorePaths.Free;
  end;
end.
