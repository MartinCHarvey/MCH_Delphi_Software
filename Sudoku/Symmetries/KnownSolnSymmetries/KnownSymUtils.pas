unit KnownSymUtils;

interface

uses
  Classes;

type
  TSudokuSpecList = class
  private
    FStrings: TStringList;
    FIndex: integer;
  protected
  public
    constructor Create;
    destructor Destroy; override;
    procedure ReadFromFile(Filename: string); //Single threaded setup.
    function InterlockedGetString: string;
  end;

implementation

{ TSudokuSpecList }

uses BufferedFileStream, SyncObjs, IdGlobal, SysUtils;

constructor TSudokuSpecList.Create;
begin
  FStrings := TStringList.Create;
end;

destructor TSudokuSpecList.Destroy;
begin
  FStrings.Free;
end;

procedure TSudokuSpecList.ReadFromFile(Filename: string); //Single threaded setup.
var
  FS: TReadOnlyCachedFileStream;
  Line: string;
  PuzzleCount: integer;
  Idx: integer;

begin
  FS := TReadOnlyCachedFileStream.Create(Filename, 1024 * 1024);
  try
    Line := ReadLnFromStream(FS);
    Line := Line.Trim;
    PuzzleCount := StrToInt(Line); //Allow failures to fall out.
    WriteLn('Initial puzzle count: ' + IntToStr(PuzzleCount));
    for Idx := 0 to Pred(PuzzleCount) do
    begin
      Line := ReadLnFromStream(FS);
      Line := Line.Trim;
      //Check that we have 81 numbers 0..9
      if Line.Length <> 81 then
        raise Exception.Create('Line bad length');
      //Apart from that let MT solvers determine whether input good.
      FStrings.Add(Line);
    end;
  finally
    FS.Free;
  end;
end;

function TSudokuSpecList.InterlockedGetString: string;
var
  Idx: integer;
begin
  Idx := Pred(TInterlocked.Increment(FIndex));
  if Idx < FStrings.Count then
    result := FStrings[idx]
  else
    result := '';
end;

end.
