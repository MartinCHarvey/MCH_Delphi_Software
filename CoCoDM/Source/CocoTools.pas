unit CocoTools;

interface

function ASCIIName(const ASCII : AnsiChar) : AnsiString;

implementation

function ASCIIName(const ASCII : AnsiChar) : AnsiString;
var
  N : integer;
begin
  case ascii of
    #00 : Result := '_nul';
    #01 : Result := '_soh';
    #02 : Result := '_stx';
    #03 : Result := '_etx';
    #04 : Result := '_eot';
    #05 : Result := '_enq';
    #06 : Result := '_ack';
    #07 : Result := '_bel';
    #08 : Result := '_bs';
    #09 : Result := '_ht';
    #10 : Result := '_lf';
    #11 : Result := '_vt';
    #12 : Result := '_ff';
    #13 : Result := '_cr';
    #14 : Result := '_so';
    #15 : Result := '_si';
    #16 : Result := '_dle';
    #17 : Result := '_dc1';
    #18 : Result := '_dc2';
    #19 : Result := '_dc3';
    #20 : Result := '_dc4';
    #21 : Result := '_nak';
    #22 : Result := '_syn';
    #23 : Result := '_etb';
    #24 : Result := '_can';
    #25 : Result := '_em';
    #26 : Result := '_sub';
    #27 : Result := '_esc';
    #28 : Result := '_fs';
    #29 : Result := '_gs';
    #30 : Result := '_rs';
    #31 : Result := '_us';
    ' ' : Result := '_sp';
    '!' : Result := '_bang';
    '"' : Result := '_dquote';
    '#' : Result := '_hash';
    '$' : Result := '_dollar';
    '%' : Result := '_percent';
    '&' : Result := '_and';
    #39 : Result := '_squote';
    '(' : Result := '_lparen';
    ')' : Result := '_rparen';
    '*' : Result := '_star';
    '+' : Result := '_plus';
    ',' : Result := '_comma';
    '-' : Result := '_minus';
    '.' : Result := '_point';
    '/' : Result := '_slash';
    '0' : Result := '_zero';
    '1' : Result := '_one';
    '2' : Result := '_two';
    '3' : Result := '_three';
    '4' : Result := '_four';
    '5' : Result := '_five';
    '6' : Result := '_six';
    '7' : Result := '_seven';
    '8' : Result := '_eight';
    '9' : Result := '_nine';
    ':' : Result := '_colon';
    ';' : Result := '_semicolon';
    '<' : Result := '_less';
    '=' : Result := '_equal';
    '>' : Result := '_greater';
    '?' : Result := '_query';
    '@' : Result := '_at';
    'A'..'Z' : Result := '_' + ascii + '_';
    'a'..'z' : Result := '__' + ascii + '_';
    '[' : Result := '_lbrack';
    '\' : Result := '_backslash';
    ']' : Result := '_rbrack';
    '^' : Result := '_uparrow';
    '_' : Result := '_underscore';
    '`' : Result := '_accent';
    '{' : Result := '_lbrace';
    '|' : Result := '_bar';
    '}' : Result := '_rbrace';
    '~' : Result := '_tilde';
    #127 : Result := '_delete';
  else
    begin
      N := ORD(ascii);
      Result := 'ascii' + AnsiChar(N mod 10 + ORD('0'));
      N := N div 10;
      Result := Result + AnsiChar(N mod 10 + ORD('0')) + AnsiChar(N div 10 + ORD('0'));
    end
  end;
end;

end.

