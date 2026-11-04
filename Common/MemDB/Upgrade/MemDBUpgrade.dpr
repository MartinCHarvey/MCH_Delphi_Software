program MemDBUpgrade;

{ Copies an old style MemDB database into a new MemDB2 database.

  Usage: MemDBUpgrade <source MemDB folder> <destination MemDB2 folder>

  The destination must be a different folder, and must be empty or not yet
  exist: CopyDatabase clears the destination before copying. }

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.IOUtils,
  MemDB in '..\MemDB.pas',
  MemDBApi in '..\MemDBApi.pas',
  MemDBBuffered in '..\MemDBBuffered.pas',
  MemDBIndexing in '..\MemDBIndexing.pas',
  MemDBJournal in '..\MemDBJournal.pas',
  MemDBMisc in '..\MemDBMisc.pas',
  MemDbStreamable in '..\MemDbStreamable.pas',
  MemDB2 in '..\..\MemDB2\MemDB2.pas',
  MemDB2Api in '..\..\MemDB2\MemDB2Api.pas',
  MemDB2BufBase in '..\..\MemDB2\MemDB2BufBase.pas',
  MemDB2Buffered in '..\..\MemDB2\MemDB2Buffered.pas',
  MemDB2Indexing in '..\..\MemDB2\MemDB2Indexing.pas',
  MemDB2Journal in '..\..\MemDB2\MemDB2Journal.pas',
  MemDB2Misc in '..\..\MemDB2\MemDB2Misc.pas',
  MemDB2Streamable in '..\..\MemDB2\MemDB2Streamable.pas',
  MemDB2Utils in '..\..\MemDB2\MemDB2Utils.pas',
  BinaryTree in '..\..\Balanced Tree\BinaryTree.pas',
  BufferedFileStream in '..\..\CachedStream\BufferedFileStream.pas',
  CoWTree in '..\..\CoWTree\CoWTree.pas',
  CRC32 in '..\..\CRC\CRC32.pas',
  DLList in '..\..\DLList\DLList.pas',
  DLThreadQueue in '..\..\WorkItems\DLThreadQueue.pas',
  GlobalLog in '..\..\Logging\GlobalLog.pas',
  IndexedStore in '..\..\Indexed Store\IndexedStore.pas',
  LockAbstractions in '..\..\LockAbstractions\LockAbstractions.pas',
  NullStream in '..\..\NullStream\NullStream.pas',
  Parallelizer in '..\..\Parallelizer\Parallelizer.pas',
  PatchFreeMem in '..\..\PatchFreeMem\PatchFreeMem.pas',
  PComp in '..\..\PComp\PComp.pas',
  Reffed in '..\..\Reffed\Reffed.pas',
  RWWLock in '..\..\LockAbstractions\RWWLock.pas',
  SRWLockWrapper in '..\..\SRWLockWrapper\SRWLockWrapper.pas',
  StripedLock in '..\..\LockAbstractions\StripedLock.pas',
  TinyLock in '..\..\LockAbstractions\TinyLock.pas',
  Trackables in '..\..\Tracking\Trackables.pas';

procedure Usage;
begin
  WriteLn('Usage: MemDBUpgrade <source MemDB folder> <destination MemDB2 folder>');
  WriteLn;
  WriteLn('Copies an old MemDB database into a new MemDB2 database.');
  WriteLn('The destination folder must be empty or not yet exist.');
end;

function DirHasEntries(const Dir: string): boolean;
begin
  result := TDirectory.Exists(Dir)
    and (Length(TDirectory.GetFileSystemEntries(Dir)) > 0);
end;

function CleanPath(const Path: string): string;
begin
  result := ExcludeTrailingPathDelimiter(ExpandFileName(Path));
end;

var
  SrcPath, DstPath: string;
  Src: MemDB.TMemDB;
  Dst: MemDB2.TMemDB;

begin
  ExitCode := 1;
  try
    if ParamCount <> 2 then
    begin
      Usage;
      Exit;
    end;
    SrcPath := CleanPath(ParamStr(1));
    DstPath := CleanPath(ParamStr(2));

    if SameText(SrcPath, DstPath) then
      raise Exception.Create('Source and destination must be different folders.');
    if not DirHasEntries(SrcPath) then
      raise Exception.Create('Source folder does not exist or is empty: ' + SrcPath);
    if DirHasEntries(DstPath) then
      raise Exception.Create('Destination folder is not empty: ' + DstPath
        + '. The copy replaces everything in the destination, so it must start empty.');
    if not ForceDirectories(DstPath) then
      raise Exception.Create('Could not create destination folder: ' + DstPath);

    Src := MemDB.TMemDB.Create;
    try
      Dst := MemDB2.TMemDB.Create;
      try
        WriteLn('Opening source (MemDB): ', SrcPath);
        if not Src.InitDB(SrcPath) then
          raise Exception.Create('Could not open source database.');

        WriteLn('Opening destination (MemDB2): ', DstPath);
        if not Dst.InitDB(DstPath) then
          raise Exception.Create('Could not open destination database.');

        WriteLn('Copying...');
        CopyDatabase(Src, Dst);

        WriteLn('Closing databases...');
        Dst.StopDB;
        Src.StopDB;
      finally
        Dst.Free;
      end;
    finally
      Src.Free;
    end;

    WriteLn('Done.');
    ExitCode := 0;
  except
    on E: Exception do
    begin
      WriteLn(ErrOutput, 'Error: ', E.ClassName, ': ', E.Message);
      ExitCode := 1;
    end;
  end;
end.
