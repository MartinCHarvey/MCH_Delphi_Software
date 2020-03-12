unit Sets;
{$INCLUDE CocoCD.inc}
{ General set handling primitives }

interface

uses
  SysUtils,
  Classes;

const
  size = 16;
  Limit = 128;
type
  BITSET = set of 0..size - 1;
  BITARRAY = array[0..Limit] of BITSET;

procedure Clear(var s : BITARRAY); // s := {}
procedure Fill(var s : BITARRAY); { s := full set }
function IsIn(var s : BITARRAY; x : integer) : boolean; { x IN s ?      }
procedure Incl(var s : BITARRAY; x : integer); { INCL(s, x)    }
procedure Excl(var s : BITARRAY; x : integer); { EXCL(s, x)    }
function Includes(var s1, s2 : BITARRAY) : boolean; { s2 <= s1 ?    }
function Elements(var s : BITARRAY; { | s |         }
  var lastElem : integer) : integer; {               }
function Empty(var s : BITARRAY) : boolean; // s1 = {} ?
function Equal(var s1, s2 : BITARRAY) : boolean; { s1 = s2 ?     }
function Different(var s1, s2 : BITARRAY) : boolean; { s1 * s2 = 0 ? }
procedure Unite(var s1, s2 : BITARRAY); { s1 := s1 + s2 }
procedure Differ(var s1, s2 : BITARRAY); { s1 := s1 - s2 }
procedure Intersect(var s1, s2, s3 : BITARRAY); { s3 := s1 * s2 }

function SetToCode(s : BitArray; const Width : integer;
    const indent : integer) : AnsiString;

function PadL(S : AnsiString; Ch : AnsiChar; len : integer) : AnsiString;
function PadR(S : AnsiString; Ch : AnsiChar; len : integer) : AnsiString;

implementation

function PadL(S : AnsiString; Ch : AnsiChar; len : integer) : AnsiString;
var
  i : integer;
begin
  for i := 1 to len - (Length(s)) do
    s := Ch + s;
  Result := s;
end;

function PadR(S : AnsiString; Ch : AnsiChar; len : integer) : AnsiString;
var
  i : integer;
begin
  for i := 1 to len - (Length(s)) do
    s := s + Ch;
  Result := s;
end;

procedure Clear(var s : BITARRAY);
var
  i : integer;
begin
  i := 0;
  while i <= Limit do
  begin
    s[i] := [];
    inc(i)
  end
end;

{ Fill                 Set all elements in set s
---------------------------------------------------------------------------}

procedure Fill(var s : BITARRAY);
var
  i : integer;
begin
  i := 0;
  while i <= Limit do
  begin s[i] := [0..size - 1];
    inc(i)
  end
end;

{ Incl                 Include element x in set s
---------------------------------------------------------------------------}

procedure Incl(var s : BITARRAY; x : integer);
begin
  s[x div size] := s[x div size] + [x mod size]
end;

{ Excl
---------------------------------------------------------------------------}

procedure Excl(var s : BITARRAY; x : integer);
begin
  s[x div size] := s[x div size] - [x mod size]
end;

{ IsIn                 TRUE, if element x is contained in set s
---------------------------------------------------------------------------}

function IsIn(var s : BITARRAY; x : integer) : boolean;
begin
  Result := x mod size in s[x div size]
end;

{ Includes             TRUE, if s2 in s1
---------------------------------------------------------------------------}

function Includes(var s1, s2 : BITARRAY) : boolean;
var
  i : integer;
begin
  i := 0;
  while i <= Limit do
  begin
    if not (s2[i] <= s1[i]) then
    begin
      Result := false;
      EXIT;
    end;
    inc(i)
  end;
  Result := true;
end;

{ Elements             Return number of elements in set s
---------------------------------------------------------------------------}

function Elements(var s : BITARRAY; var lastElem : integer) : integer;
var
  i, n, max : integer;

begin
  i := 0;
  n := 0;
  max := (Limit + 1) * size;
  while i < max do
  begin
    if i mod size in s[i div size] then
    begin
      inc(n);
      lastElem := i
    end;
    inc(i)
  end;
  Result := n
end;

{ Empty                TRUE, if set s i sempty
---------------------------------------------------------------------------}

function Empty(var s : BITARRAY) : boolean;
var
  i : integer;
begin
  i := 0;
  while i <= Limit do
  begin
    if s[i] <> [] then
    begin
      Result := false;
      EXIT
    end;
    inc(i)
  end;
  Result := true
end;

{ Equal                TRUE, if set s1 and s2 are equal
---------------------------------------------------------------------------}

function Equal(var s1, s2 : BITARRAY) : boolean;
var
  i : integer;
begin
  i := 0;
  while i <= Limit do
  begin
    if s1[i] <> s2[i] then
    begin
      Result := false;
      EXIT
    end;
    inc(i)
  end;
  Result := true
end;

{ Different            TRUE, if set s1 and s2 are totally different
---------------------------------------------------------------------------}

function Different(var s1, s2 : BITARRAY) : boolean;
var
  i : integer;
begin
  i := 0;
  while i <= Limit do
  begin
    if s1[i] * s2[i] <> [] then
    begin
      Result := false;
      EXIT
    end;
    inc(i)
  end;
  Result := true
end;

{ Unite                s1 := s1 + s2
---------------------------------------------------------------------------}

procedure Unite(var s1, s2 : BITARRAY);
var
  i : integer;
begin
  i := 0;
  while i <= Limit do
  begin s1[i] := s1[i] + s2[i];
    inc(i)
  end
end;

{ Differ               s1 := s1 - s2
---------------------------------------------------------------------------}

procedure Differ(var s1, s2 : BITARRAY);
var
  i : integer;
begin
  i := 0;
  while i <= Limit do
  begin s1[i] := s1[i] - s2[i];
    inc(i)
  end
end;

{ Intersect            s3 := s1 * s2
---------------------------------------------------------------------------}

procedure Intersect(var s1, s2, s3 : BITARRAY);
var
  i : integer;
begin
  i := 0;
  while i <= Limit do
  begin s3[i] := s1[i] * s2[i];
    inc(i)
  end
end;

{ Print                Print set s
---------------------------------------------------------------------------}

function SetToCode(s : BitArray; const Width : integer;
    const indent : integer) : AnsiString;
const
  chCR = #13;
  chLF = #10;
  chEOL = chCR + chLF;  { End of line characters for Microsoft Windows }
var
  col : integer;
  i : integer;
  max : integer;
begin
  i := 0;
  col := indent;
  max := (Limit + 1) * size;
  Result := '{';
  while i < max do
  begin
    if IsIn(s, i) then
    begin
      if col + 4 > Width then
      begin
        Result := Result + chEOL;
        Result := Result + PadL('', ' ', indent);
        col := indent
      end;
      Result := Result + PadR(IntToStr(i), ' ', 3) + ',';
      inc(col, 4)
    end;
    inc(i)
  end;
  Result := Result + '}';
end; {SetToCode}

(******** Original Print procedure
procedure Print(Stream : TMemoryStream; const s : BITARRAY;
    const Width : integer; const indent : integer);
var
  col, i, max : integer;

  procedure StreamLn(s : AnsiString);
  begin
    if length(s) > 0 then
      Stream.WriteBuffer(s[1], length(s));
  end;

  procedure StreamLine(s : AnsiString);
  begin
    s := s + #13#10;
    StreamLn(s);
  end;

begin
  i := 0;
  col := indent;
  max := (Limit + 1) * size;
  StreamLn('{');
  while i < max do
  begin
    if IsIn(s, i) then
    begin
      if col + 4 > w then
      begin
        StreamLine('');
        StreamLn(PadL('', ' ', indent));
        col := indent
      end;
      StreamLn(PadR(IntToStr(i), ' ', 3) + ',');
      inc(col, 4)
    end;
    inc(i)
  end;
  StreamLn('}')
end; { Print }
*************)

end.

