unit PathFileUtils;
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
  SysUtils, IoUtils, Classes;


type
  TCanonicalPathType = (cptRelative,
                        cptRootRooted,
                        cptDriveRooted,
                        cptUNCRooted);

{$IFDEF USE_TRACKABLES}
  TCanonicalPath = class(TTrackable)
{$ELSE}
  TCanonicalPath = class
{$ENDIF}
  private
    FPathType: TCanonicalPathType;
    FPrefix: string; //UNC server or drive letter if applicable.
    FComponents: TStringList;
    //Directories / files, no separate ext, 0 is root folder, n is filename.
  protected
    procedure Canonicalise;
    function GetPath: string;
    procedure SetPath(NewPath: string);
    function SepIdx(Path: string): integer;
    function GetComponentCount: integer;
    procedure SetComponentCount(NewComponentCount: integer);
  public
    constructor Create;
    destructor Destroy; override;
    procedure MakeAbsolute; //Will be rooted somehow.
    procedure MakeRelativeToFile(FilePath: TCanonicalPath);
    procedure MakeRelativeToDir(DirPath: TCanonicalPath);
    procedure FindCommonRoot(OtherPath: TCanonicalPath);
    procedure Assign(Src: TCanonicalPath); virtual;
    procedure ReRoot(OldRoot, NewRoot: TCanonicalPath);
    property Path: string read GetPath write SetPath;
    property PathType: TCanonicalPathType read FPathType;
    property ComponentCount: integer read GetComponentCount write SetComponentCount;
    //TODO - More stuff here, to do with finding common sub-path,
    //and transplanting a path from one root to another.
  end;

procedure AppendTrailingDirSlash(var Path: string);

implementation

{ TCanonicalPath }

function TCanonicalPath.GetPath: string;
var
  Idx: integer;
begin
  //TODO - test this.
  case FPathType of
    cptRelative: result := '';
    cptRootRooted: result := TPath.DirectorySeparatorChar;
    cptDriveRooted:
      result := FPRefix + TPath.VolumeSeparatorChar + TPath.DirectorySeparatorChar;
    cptUNCRooted:
      result := TPath.DirectorySeparatorChar + TPath.DirectorySeparatorChar + FPRefix + TPath.DirectorySeparatorChar;
  end;
  for Idx := 0 to Pred(FComponents.Count) do
  begin
    result := result + FComponents.Strings[idx];
    if Idx <> Pred(FComponents.Count) then
      result := result + TPath.DirectorySeparatorChar;
  end;
end;

function TCanonicalPath.SepIdx(Path: string): integer;

  function Min(x, y: integer): integer;
  begin
    if x > y then
      result := y
    else
      result := x;
  end;

var
  SepPos, AltSepPos: integer;
begin
  SepPos := Pos(TPath.DirectorySeparatorChar, Path);
  AltSepPos := Pos(TPath.AltDirectorySeparatorChar, Path);
  if (SepPos > 0) and (AltSepPos > 0) then
    result := Min(SepPos, AltSepPos)
  else if (SepPos > 0) then
    result := SepPos
  else
    result := AltSepPos;
end;

procedure TCanonicalPath.SetPath(NewPath: string);

  procedure AddComponent(Component: string);
  begin
    if Length(Component) = 0 then
      raise Exception.Create('Empty element before end of path');
    FComponents.Add(Component);
  end;

var
  SepPos: integer;
  Component: string;
begin
  FComponents.Clear;
  if TPath.IsDriveRooted(NewPath) then
  begin
    FPathType := cptDriveRooted;
    FPrefix := NewPath[1];
    NewPath := NewPath.Substring(3);
  end
  else if TPath.IsUNCRooted(NewPath) then
  begin
    //TODO - check this.
    FPathType := cptUNCRooted;
    NewPath := NewPath.Substring(3);
    SepPos := SepIdx(NewPath);
    if SepPos > 0 then
    begin
      FPrefix := NewPath.Substring(0, Pred(SepPos));
      NewPath := NewPath.Substring(SepPos);
    end
    else
    begin
      FPrefix := NewPath;
      NewPath := '';
    end;
  end
  else if TPath.IsPathRooted(NewPath) then
  begin
    //TODO - Check this.
    FPathType := cptRootRooted;
    NewPath := NewPath.Substring(1); //Remove initial root /
  end
  else if TPath.IsRelativePath(NewPath) then
  begin
    FPathType := cptRelative;
    //No string manipulation needed.
  end
  else
    raise Exception.Create('Unknown path type');
  while Length(NewPath) > 0 do
  begin
    SepPos := SepIdx(NewPath);
    if SepPos = 0 then
      SepPos := Succ(Length(NewPath));
    Component := NewPath.Substring(0, Pred(SepPos));
    NewPath := NewPath.Substring(SepPos);
    if (Length(Component) > 0) or (Length(NewPath) > 0) then
      AddComponent(Component);
  end;
  Canonicalise;
end;

procedure TCanonicalPath.Canonicalise;
var
  Idx: integer;
begin
  if not (FPathType = cptRelative) then
  begin
    Idx := 0;
    while Idx < FComponents.Count do
    begin
      if FComponents.Strings[idx] = '.' then
        FComponents.Delete(idx)
      else if FComponents.Strings[idx] = '..' then
      begin
        if Idx > 0 then
        begin
          FComponents.Delete(Pred(Idx));
          FComponents.Delete(Pred(Idx));
          Dec(Idx);
        end
        else
          raise Exception.Create('No previous directory');
      end
      else
        Inc(Idx);
    end;
  end;
end;

constructor TCanonicalPath.Create;
begin
  inherited;
  FComponents := TStringList.Create;
end;

destructor TCanonicalPath.Destroy;
begin
  FComponents.Free;
  inherited;
end;

procedure TCanonicalPath.Assign(Src: TCanonicalPath);
begin
  FPathType := Src.FPathType;
  FPrefix := Src.FPrefix;
  FComponents.Assign(Src.FComponents);
end;

procedure TCanonicalPath.MakeAbsolute;
begin
  if FPathType = cptRelative then
    Path := (TPath.Combine(GetCurrentDir, Path));
end;

//TODO - Potentially better trailing slash handling to deal with
//files and directories...?
procedure TCanonicalPath.MakeRelativeToFile(FilePath: TCanonicalPath);
var
  SavedComp: string;
begin
  if FPathType = cptRelative then
  begin
    if FilePath.FComponents.Count > 0 then
    begin
      SavedComp := FilePath.FComponents.Strings[Pred(FilePath.FComponents.Count)];
      FilePath.FComponents.Delete(Pred(FilePath.FComponents.Count));
      MakeRelativeToDir(FilePath);
      FilePath.FComponents.Add(SavedComp);
    end
    else
      raise Exception.Create('Make relative: root is not file');
  end
  else
    raise Exception.Create('Can only make relative to file if path already relative.');
end;

procedure TCanonicalPath.MakeRelativeToDir(DirPath: TCanonicalPath);
begin
  if FPathType = cptRelative then
    Path := TPath.Combine(DirPath.Path, self.Path)
  else
    raise Exception.Create('Can only make relative to dir if path already relative.');
end;

procedure TCanonicalPath.FindCommonRoot(OtherPath: TCanonicalPath);
var
  Idx: integer;
begin
  if FPathType = cptRelative then
    raise Exception.Create('Cannot find comon root of relative paths');
  if (FPathType <> OtherPath.FPathType)
    or (FPrefix <> OtherPath.FPrefix) then
    raise Exception.Create('Paths have no common root');
  Idx := 0;
  while (Idx < FComponents.Count)
    and (Idx < OtherPath.FComponents.Count)
    and (FComponents[idx] = OtherPath.FComponents[idx]) do
    Inc(Idx);
  while FComponents.Count > Idx do
    FComponents.Delete(Pred(FComponents.Count));
end;

procedure TCanonicalPath.ReRoot(OldRoot, NewRoot: TCanonicalPath);
var
  Idx: integer;
begin
  if (OldRoot.FPathType = cptRelative)
    or (NewRoot.FPathType = cptRelative)
    or (FPathType = cptRelative) then
    raise Exception.Create('Cannot re-root relative paths');
  if (OldRoot.FPathType <> FPathType)
    or (OldRoot.FPrefix <> FPrefix) then
    raise Exception.Create('Path is not on old root');
  if FComponents.Count < OldRoot.FComponents.Count then
    raise Exception.Create('Path is not on old root');
  for Idx := 0 to Pred(OldRoot.FComponents.Count) do
  begin
    if FComponents[idx] <> OldRoot.FComponents[idx] then
      raise Exception.Create('Path is not on old root');
  end;
  for Idx := 0 to Pred(OldRoot.FComponents.Count) do
    FComponents.Delete(0);
  FPathType := NewRoot.FPathType;
  FPrefix := NewRoot.FPrefix;
  for Idx := Pred(NewRoot.FComponents.Count) downto 0 do
    FComponents.Insert(0, NewRoot.FComponents[Idx]);
end;

function TCanonicalPath.GetComponentCount: integer;
begin
  result := FComponents.Count;
end;

procedure TCanonicalPath.SetComponentCount(NewComponentCount: integer);
begin
  while FComponents.Count > NewComponentCount do
    FComponents.Delete(Pred(FComponents.Count));
end;

{ Misc functions }

procedure AppendTrailingDirSlash(var Path: string);
begin
  if Length(Path) > 0 then
  begin
    if ((Path[Length(Path)] <> TPath.DirectorySeparatorChar))
      and (Path[Length(Path)] <> TPath.AltDirectorySeparatorChar) then
      Path := Path + TPath.DirectorySeparatorChar;
  end;
end;

end.
