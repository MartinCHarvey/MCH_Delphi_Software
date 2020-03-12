unit StreamTools;
{$INCLUDE CocoCD.inc}

interface

uses
  CRTypes,
  Classes,
  Sets,
  CRT;

type
  TStreamTools = class(TObject)
  private
    fStream: TStream;
    fTableHandler: TTableHandler;
  public
    procedure StreamLine(const S: AnsiString);
    procedure StreamLn(const s: AnsiString);
    procedure StreamI2(const i : integer; const n : integer);
    procedure StreamRange(s : CRTSet; const CompareTo : AnsiString);
    procedure StreamS1(s : AnsiString);
    procedure StreamSE(const i : integer);
    procedure StreamI(const i : integer);
    procedure StreamS(const s : AnsiString);
    procedure StreamSN(const i : integer);
    procedure StreamC(const ch : AnsiChar);
    procedure StreamChCond(const ch: AnsiChar);
    procedure StreamSet(s: CRTSet);
    procedure StreamBitSet(const s: Sets.BITSET; const offset: integer);
    procedure StreamB(const Width : integer);
    procedure ReplaceStrInStream(const FindText : array of AnsiString;
        const ReplaceText: array of AnsiString);

    property TableHandler : TTableHandler read fTableHandler write fTableHandler;
    property Stream : TStream read fStream write fStream;
  end; {TStreamTools}

function StringReplace(const S, OldPattern, NewPattern : AnsiString) : AnsiString;

implementation

uses
  SysUtils;

{ TStreamTools }

function StringReplace(const S, OldPattern, NewPattern : AnsiString) : AnsiString;
var
  SearchStr, Patt, NewStr : AnsiString;
  Offset : integer;
begin
  SearchStr := UpperCase(S);
  Patt := UpperCase(OldPattern);
  NewStr := S;
  Result := '';
  while SearchStr <> '' do
  begin
    Offset := AnsiPos(Patt, SearchStr);
    if Offset = 0 then
    begin
      Result := Result + NewStr;
      Break;
    end;
    Result := Result + Copy(NewStr, 1, Offset - 1) + NewPattern;
    NewStr := Copy(NewStr, Offset + Length(OldPattern), MaxInt);
    SearchStr := Copy(SearchStr, Offset + Length(Patt), MaxInt);
  end;
end; {StringReplace}

procedure TStreamTools.StreamLn(const s : AnsiString);
begin
  if Length(S) > 0 then
    Stream.WriteBuffer(S[1], length(S));
end; {StreamLn}

procedure TStreamTools.StreamLine(const S : AnsiString);
begin
  StreamLn(S + #13#10);
end; {StreamLine}

procedure TStreamTools.StreamS(const s: AnsiString);
var
  i : integer;
begin
  for i := 1 to Length(s) do
    if s[i] = '$' then
      StreamLine('')
    else
      StreamLn(s[i]);
end; {StreamS}

procedure TStreamTools.StreamS1(s: AnsiString);
var
  tmp: AnsiString;
  x: integer;
  bypass: boolean;
begin
  if s[1] = '"' then
  begin
    s[1] := '''';
    s[Length(s)] := ''''
  end;
  tmp := '';
  bypass := true;
  for x := 2 to Pred(Length(s)) do
  begin
    case s[x] of
      #0..#31, #34, #39, #127..#255:
      begin
        bypass := false;
        break;
      end;
    end;
  end;
  if not bypass then
  begin
    tmp := tmp + s[1];
    for x := 2 to Pred(Length(s)) do
    begin
      case s[x] of
        #0..#31, #34, #39, #127..#255:
          tmp := tmp + ''' +' + ('AnsiChar(' + IntToStr(Ord(s[x])) + ')' + '+ ''');
      else
        tmp := tmp + s[x];
      end;
    end;
    tmp := tmp + s[Length(s)]
  end
  else
    tmp := s;
  StreamS(tmp);
end; {StreamS1}

procedure TStreamTools.StreamSE(const i : integer);
begin
  StreamS('begin$');
  StreamS('sym := ');
  StreamSN(i);
  StreamS(';$');
end; {StreamSE}

procedure TStreamTools.StreamI(const i: integer);
begin
  StreamLn(IntToStr(i));
end; {StreamI}

procedure TStreamTools.StreamI2(const i, n: integer);
begin
  StreamS(PadL(IntToStr(i),' ',n));
end; {StreamI2}

procedure TStreamTools.StreamC(const ch: AnsiChar);
begin
  case ch of
    #0..#31, #127..#255, '''' :
      StreamLn('AnsiChar(' + IntToStr(Ord(ch)) + ')');
  else
    StreamLn(#39 + ch + #39);
  end;
end; {StreamC}

procedure TStreamTools.StreamSN(const i: integer);
var
  sn : CRTSymbolNode;
begin
  fTableHandler.GetSym(i, sn);
  if Length(sn.constant) > 0 then
    StreamS(sn.constant)
  else
    StreamI(i);
end; {StreamSN}

procedure TStreamTools.StreamRange(s: CRTSet; const CompareTo : AnsiString);
var
  lo, hi : array[0..31] of AnsiChar;
  top, i : integer;
  s1 : CRTSet;
begin
  {----- fill lo and hi }
  top := -1;
  i := 0;
  while i < 256 {PDT} do
  begin
    if Sets.IsIn(s, i) then
    begin
      inc(top);
      lo[top] := AnsiChar(i);
      inc(i);
      while (i < 256 {PDT}) and Sets.IsIn(s, i) do
        inc(i);
      hi[top] := AnsiChar(i - 1)
    end
    else
      inc(i)
  end;
    {----- print ranges }
  if (top = 1) and (lo[0] = #0) and (hi[1] = #255
    {PDT}) and (AnsiChar(ORD(hi[0]) + 2) = lo[1]) then
  begin
    Sets.Fill(s1);
    Sets.Differ(s1, s);
    { StreamS(Stream,'NOT ('); // this seems to work but needs more testing}
    StreamS('NOT ');
    StreamRange(s1, CompareTo);
  end
  else
  begin
    StreamS('(');
    i := 0;
    while i <= top do
    begin
      if hi[i] = lo[i] then
      begin
        StreamS('(CurrInputCh = ');
        StreamC(lo[i])
      end
      else if lo[i] = #0 then
      begin
        StreamS('(CurrInputCh <= ');
        StreamC(hi[i])
      end
      else if hi[i] = #255 {PDT} then
      begin
        StreamS('(CurrInputCh >= ');
        StreamC(lo[i])
      end
      else
      begin
        StreamS('(CurrInputCh >= ');
        StreamC(lo[i]);
        StreamS(') AND (CurrInputCh <= ');
        StreamC(hi[i])
      end;
      StreamS(')');
      if i < top then
      begin
        StreamS(' OR$');
      end;
      inc(i)
    end;
    StreamS(')');
  end;
end; {StreamRange}

procedure TStreamTools.StreamBitSet(const s : Sets.BITSET; const offset : integer);
const
  MaxLine = 76;
var
  first : boolean;
  i : integer;
  l, len : integer;
  sn : CRTSymbolNode;
begin
  i := 0;
  first := true;
  len := 20;
  while (i < Sets.size) and (offset + i <= ORD(fTableHandler.MaxT)) do
  begin
    if i in s then
    begin
      if first then
        first := false
      else
      begin StreamS(', ');
        inc(len, 2)
      end;
      fTableHandler.GetSym(offset + i, sn);
      l := Length(sn.constant);
      if l > 0 then
      begin
        if len + l > MaxLine then
        begin
          StreamS('$                    ');
          len := 20
        end;
        StreamS(sn.constant);
        inc(len, l);
        if offset > 0 then
        begin
          StreamS('-');
          StreamI(offset);
          inc(len, 3)
        end;
      end
      else
      begin
        if len + l > MaxLine then
        begin
          StreamS('$                    ');
          len := 20
        end;
        StreamI(i);
        inc(len, i div 10 + 1);
      end;
    end;
    inc(i)
  end
end; {StreamBitSet}

procedure TStreamTools.StreamChCond(const ch: AnsiChar);
begin
  StreamLn('(CurrInputCh = ');
  StreamC(ch);
  StreamLn(')')
end; {StreamChCond}

procedure TStreamTools.StreamSet(s : CRTSet);
var
  i : integer;
  first : boolean;
  Cnt : integer;
begin
  i := 0;
  Cnt := 0;
  first := true;
  while i <= fTableHandler.MaxT do
  begin
    if Sets.IsIn(s, i) then
    begin
      if first then
        first := false
      else
        StreamS(', ');

      inc(Cnt);
      if Cnt mod 8 = 0 then
        StreamS('$      ');

      StreamSN(i);
    end;
    inc(i)
  end
end; {StreamSet}

procedure TStreamTools.StreamB(const Width: integer);
var
  i : integer;
begin
  for i := 0 to Width do
    StreamS(' ');
end; {StreamB}

procedure TStreamTools.ReplaceStrInStream(const FindText : array of AnsiString;
    const ReplaceText: array of AnsiString);
var
  FileLines : TStrings;
  j : integer;
  i : integer;
  s : AnsiString;
begin
  FileLines := TStringList.Create;
  try
    Stream.Seek(0, soFromBeginning);
    FileLines.BeginUpdate;
    try
      FileLines.LoadFromStream(Stream);
    finally
      FileLines.EndUpdate;
    end;
    for i := 0 to FileLines.Count - 1 do
    begin
      s := FileLines[i];
      for j := Low(FindText) to High(FindText) do
        s := StringReplace(S, FindText[j], ReplaceText[j]);
      FileLines[i] := s;
    end;
    Stream.Size := 0;
    FileLines.SaveToStream(Stream);
  finally
    FileLines.Free;
  end;
end; {ReplaceStrInStream}

end.
