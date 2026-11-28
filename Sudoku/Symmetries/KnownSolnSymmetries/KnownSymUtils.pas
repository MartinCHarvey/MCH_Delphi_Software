unit KnownSymUtils;
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

uses BufferedFileStream, LockAbstractions, IdGlobal, SysUtils;

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
  Idx := Pred(LockAbstractions.InterlockedIncrement(FIndex));
  if Idx < FStrings.Count then
    result := FStrings[idx]
  else
    result := '';
end;

end.
