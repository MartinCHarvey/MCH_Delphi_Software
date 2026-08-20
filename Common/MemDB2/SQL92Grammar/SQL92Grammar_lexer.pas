
(* lexical analyzer template (TP Lex V3.0), V1.0 3-2-91 AG  - MCH OO Mod 1*)

  (* global definitions: *)


unit SQL92Grammar_lexer;

        {

        Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

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

uses LexLib_oo;



type
  SQL92GrammarLexer = class(TPLYLexer)
    public
      function yylex: integer; override;
      function BestIdentifierOrLiteral(const TokenText: AnsiString): integer;
      function TokenName(Token: integer): string;
  end;

implementation


  (* local definitions: *)


  uses SysUtils, SQL92Grammar_parser;


function SQL92GrammarLexer.yylex : Integer;

procedure yyaction ( yyruleno : Integer );

begin
  (* actions: *)
  case yyruleno of
  1:
                        return (BestIdentifierOrLiteral(yytoken_text));
  2:
                        return(Ord(national_character_string_literal_start));
  3:
                        return(Ord(bit_string_literal_start));
  4:
                        return(Ord(string_literal_continuation));
  5:
                        return(Ord(hex_string_literal_start));
  6:
                        return(Ord(delimited_identifier));
  7:
                        return(Ord(digit));
  8:
                        return(Ord(not_equals_operator));
  9:
                        return(Ord(greater_than_or_equals_operator));
  10:
                        return(Ord(less_than_or_equals_operator));
  11:
                        return(Ord(concatenation_operator));
  12:
                        return(Ord(double_period));
  13:
                        return(Ord(space)); { TODO - Ignore by not returning anything? }
  14:
                        return(Ord(tab));
  15:
                        return(Ord(carriage_return));
  16:
                        return(Ord(line_feed));
  17:
                        return(Ord(double_quote));
  18:
                        return(Ord(percent));
  19:
                        return(Ord(ampersand));
  20:
                        return(Ord(quote));
  21:
                        return(Ord(left_paren));
  22:
                        return(Ord(right_paren));
  23:
                        return(Ord(left_bracket));
  24:
                        return(Ord(right_bracket));
  25:
                        return(Ord(asterisk));
  26:
                        return(Ord(plus_sign));
  27:
                        return(Ord(comma));
  28:
                        return(Ord(minus_sign));
  29:
                        return(Ord(period));
  30:
                        return(Ord(solidus));
  31:
                        return(Ord(colon));
  32:
                        return(Ord(semicolon));
  33:
                        return(Ord(less_than_operator));
  34:
                        return(Ord(equals_operator));
  35:
                        return(Ord(greater_than_operator));
  36:
                        return(Ord(question_mark));
  37:
                        return(Ord(underscore));
  38:
                        return(Ord(vertical_bar));
  39:
                        return(Ord(LEX_ERROR));

  end;
end(*yyaction*);

(* DFA table: *)

type YYTRec = record
                cc : set of AnsiChar;
                s  : Integer;
              end;

const

yynmarks   = 73;
yynmatches = 73;
yyntrans   = 94;
yynstates  = 50;

yyk : array [1..yynmarks] of Integer = (
  { 0: }
  { 1: }
  { 2: }
  1,
  39,
  { 3: }
  1,
  39,
  { 4: }
  1,
  39,
  { 5: }
  20,
  39,
  { 6: }
  1,
  39,
  { 7: }
  17,
  39,
  { 8: }
  7,
  39,
  { 9: }
  33,
  39,
  { 10: }
  35,
  39,
  { 11: }
  38,
  39,
  { 12: }
  29,
  39,
  { 13: }
  13,
  39,
  { 14: }
  14,
  39,
  { 15: }
  15,
  39,
  { 16: }
  16,
  { 17: }
  18,
  39,
  { 18: }
  19,
  39,
  { 19: }
  21,
  39,
  { 20: }
  22,
  39,
  { 21: }
  23,
  39,
  { 22: }
  24,
  39,
  { 23: }
  25,
  39,
  { 24: }
  26,
  39,
  { 25: }
  27,
  39,
  { 26: }
  28,
  39,
  { 27: }
  30,
  39,
  { 28: }
  31,
  39,
  { 29: }
  32,
  39,
  { 30: }
  34,
  39,
  { 31: }
  36,
  39,
  { 32: }
  37,
  39,
  { 33: }
  39,
  { 34: }
  1,
  { 35: }
  { 36: }
  { 37: }
  { 38: }
  4,
  { 39: }
  { 40: }
  { 41: }
  6,
  { 42: }
  8,
  { 43: }
  10,
  { 44: }
  9,
  { 45: }
  11,
  { 46: }
  12,
  { 47: }
  2,
  { 48: }
  3,
  { 49: }
  5
);

yym : array [1..yynmatches] of Integer = (
{ 0: }
{ 1: }
{ 2: }
  1,
  39,
{ 3: }
  1,
  39,
{ 4: }
  1,
  39,
{ 5: }
  20,
  39,
{ 6: }
  1,
  39,
{ 7: }
  17,
  39,
{ 8: }
  7,
  39,
{ 9: }
  33,
  39,
{ 10: }
  35,
  39,
{ 11: }
  38,
  39,
{ 12: }
  29,
  39,
{ 13: }
  13,
  39,
{ 14: }
  14,
  39,
{ 15: }
  15,
  39,
{ 16: }
  16,
{ 17: }
  18,
  39,
{ 18: }
  19,
  39,
{ 19: }
  21,
  39,
{ 20: }
  22,
  39,
{ 21: }
  23,
  39,
{ 22: }
  24,
  39,
{ 23: }
  25,
  39,
{ 24: }
  26,
  39,
{ 25: }
  27,
  39,
{ 26: }
  28,
  39,
{ 27: }
  30,
  39,
{ 28: }
  31,
  39,
{ 29: }
  32,
  39,
{ 30: }
  34,
  39,
{ 31: }
  36,
  39,
{ 32: }
  37,
  39,
{ 33: }
  39,
{ 34: }
  1,
{ 35: }
{ 36: }
{ 37: }
{ 38: }
  4,
{ 39: }
{ 40: }
{ 41: }
  6,
{ 42: }
  8,
{ 43: }
  10,
{ 44: }
  9,
{ 45: }
  11,
{ 46: }
  12,
{ 47: }
  2,
{ 48: }
  3,
{ 49: }
  5
);

yyt : array [1..yyntrans] of YYTrec = (
{ 0: }
  ( cc: [ #1..#8,#11,#12,#14..#31,'!','#','$','@','\',
            '^','`','{','}'..#255 ]; s: 33),
  ( cc: [ #9 ]; s: 14),
  ( cc: [ #10 ]; s: 16),
  ( cc: [ #13 ]; s: 15),
  ( cc: [ ' ' ]; s: 13),
  ( cc: [ '"' ]; s: 7),
  ( cc: [ '%' ]; s: 17),
  ( cc: [ '&' ]; s: 18),
  ( cc: [ '''' ]; s: 5),
  ( cc: [ '(' ]; s: 19),
  ( cc: [ ')' ]; s: 20),
  ( cc: [ '*' ]; s: 23),
  ( cc: [ '+' ]; s: 24),
  ( cc: [ ',' ]; s: 25),
  ( cc: [ '-' ]; s: 26),
  ( cc: [ '.' ]; s: 12),
  ( cc: [ '/' ]; s: 27),
  ( cc: [ '0'..'9' ]; s: 8),
  ( cc: [ ':' ]; s: 28),
  ( cc: [ ';' ]; s: 29),
  ( cc: [ '<' ]; s: 9),
  ( cc: [ '=' ]; s: 30),
  ( cc: [ '>' ]; s: 10),
  ( cc: [ '?' ]; s: 31),
  ( cc: [ 'A','C'..'M','O'..'W','Y','Z','a'..'z' ]; s: 2),
  ( cc: [ 'B' ]; s: 4),
  ( cc: [ 'N' ]; s: 3),
  ( cc: [ 'X' ]; s: 6),
  ( cc: [ '[' ]; s: 21),
  ( cc: [ ']' ]; s: 22),
  ( cc: [ '_' ]; s: 32),
  ( cc: [ '|' ]; s: 11),
{ 1: }
  ( cc: [ #1..#8,#11,#12,#14..#31,'!','#','$','@','\',
            '^','`','{','}'..#255 ]; s: 33),
  ( cc: [ #9 ]; s: 14),
  ( cc: [ #10 ]; s: 16),
  ( cc: [ #13 ]; s: 15),
  ( cc: [ ' ' ]; s: 13),
  ( cc: [ '"' ]; s: 7),
  ( cc: [ '%' ]; s: 17),
  ( cc: [ '&' ]; s: 18),
  ( cc: [ '''' ]; s: 5),
  ( cc: [ '(' ]; s: 19),
  ( cc: [ ')' ]; s: 20),
  ( cc: [ '*' ]; s: 23),
  ( cc: [ '+' ]; s: 24),
  ( cc: [ ',' ]; s: 25),
  ( cc: [ '-' ]; s: 26),
  ( cc: [ '.' ]; s: 12),
  ( cc: [ '/' ]; s: 27),
  ( cc: [ '0'..'9' ]; s: 8),
  ( cc: [ ':' ]; s: 28),
  ( cc: [ ';' ]; s: 29),
  ( cc: [ '<' ]; s: 9),
  ( cc: [ '=' ]; s: 30),
  ( cc: [ '>' ]; s: 10),
  ( cc: [ '?' ]; s: 31),
  ( cc: [ 'A','C'..'M','O'..'W','Y','Z','a'..'z' ]; s: 2),
  ( cc: [ 'B' ]; s: 4),
  ( cc: [ 'N' ]; s: 3),
  ( cc: [ 'X' ]; s: 6),
  ( cc: [ '[' ]; s: 21),
  ( cc: [ ']' ]; s: 22),
  ( cc: [ '_' ]; s: 32),
  ( cc: [ '|' ]; s: 11),
{ 2: }
  ( cc: [ '0'..'9','A'..'Z','_','a'..'z' ]; s: 34),
{ 3: }
  ( cc: [ '''' ]; s: 35),
  ( cc: [ '0'..'9','A'..'Z','_','a'..'z' ]; s: 34),
{ 4: }
  ( cc: [ '''' ]; s: 36),
  ( cc: [ '0'..'9','A'..'Z','_','a'..'z' ]; s: 34),
{ 5: }
  ( cc: [ #1..'&','('..#255 ]; s: 37),
  ( cc: [ '''' ]; s: 38),
{ 6: }
  ( cc: [ '''' ]; s: 39),
  ( cc: [ '0'..'9','A'..'Z','_','a'..'z' ]; s: 34),
{ 7: }
  ( cc: [ #1..'!','#'..#255 ]; s: 40),
  ( cc: [ '"' ]; s: 41),
{ 8: }
{ 9: }
  ( cc: [ '=' ]; s: 43),
  ( cc: [ '>' ]; s: 42),
{ 10: }
  ( cc: [ '=' ]; s: 44),
{ 11: }
  ( cc: [ '|' ]; s: 45),
{ 12: }
  ( cc: [ '.' ]; s: 46),
{ 13: }
{ 14: }
{ 15: }
{ 16: }
{ 17: }
{ 18: }
{ 19: }
{ 20: }
{ 21: }
{ 22: }
{ 23: }
{ 24: }
{ 25: }
{ 26: }
{ 27: }
{ 28: }
{ 29: }
{ 30: }
{ 31: }
{ 32: }
{ 33: }
{ 34: }
  ( cc: [ '0'..'9','A'..'Z','_','a'..'z' ]; s: 34),
{ 35: }
  ( cc: [ #1..'&','('..#255 ]; s: 35),
  ( cc: [ '''' ]; s: 47),
{ 36: }
  ( cc: [ '''' ]; s: 48),
  ( cc: [ '0','1' ]; s: 36),
{ 37: }
  ( cc: [ #1..'&','('..#255 ]; s: 37),
  ( cc: [ '''' ]; s: 38),
{ 38: }
  ( cc: [ '''' ]; s: 37),
{ 39: }
  ( cc: [ '''' ]; s: 49),
  ( cc: [ '0'..'9','A'..'F','a'..'f' ]; s: 39),
{ 40: }
  ( cc: [ #1..'!','#'..#255 ]; s: 40),
  ( cc: [ '"' ]; s: 41),
{ 41: }
  ( cc: [ '"' ]; s: 40),
{ 42: }
{ 43: }
{ 44: }
{ 45: }
{ 46: }
{ 47: }
  ( cc: [ '''' ]; s: 35)
{ 48: }
{ 49: }
);

yykl : array [0..yynstates-1] of Integer = (
{ 0: } 1,
{ 1: } 1,
{ 2: } 1,
{ 3: } 3,
{ 4: } 5,
{ 5: } 7,
{ 6: } 9,
{ 7: } 11,
{ 8: } 13,
{ 9: } 15,
{ 10: } 17,
{ 11: } 19,
{ 12: } 21,
{ 13: } 23,
{ 14: } 25,
{ 15: } 27,
{ 16: } 29,
{ 17: } 30,
{ 18: } 32,
{ 19: } 34,
{ 20: } 36,
{ 21: } 38,
{ 22: } 40,
{ 23: } 42,
{ 24: } 44,
{ 25: } 46,
{ 26: } 48,
{ 27: } 50,
{ 28: } 52,
{ 29: } 54,
{ 30: } 56,
{ 31: } 58,
{ 32: } 60,
{ 33: } 62,
{ 34: } 63,
{ 35: } 64,
{ 36: } 64,
{ 37: } 64,
{ 38: } 64,
{ 39: } 65,
{ 40: } 65,
{ 41: } 65,
{ 42: } 66,
{ 43: } 67,
{ 44: } 68,
{ 45: } 69,
{ 46: } 70,
{ 47: } 71,
{ 48: } 72,
{ 49: } 73
);

yykh : array [0..yynstates-1] of Integer = (
{ 0: } 0,
{ 1: } 0,
{ 2: } 2,
{ 3: } 4,
{ 4: } 6,
{ 5: } 8,
{ 6: } 10,
{ 7: } 12,
{ 8: } 14,
{ 9: } 16,
{ 10: } 18,
{ 11: } 20,
{ 12: } 22,
{ 13: } 24,
{ 14: } 26,
{ 15: } 28,
{ 16: } 29,
{ 17: } 31,
{ 18: } 33,
{ 19: } 35,
{ 20: } 37,
{ 21: } 39,
{ 22: } 41,
{ 23: } 43,
{ 24: } 45,
{ 25: } 47,
{ 26: } 49,
{ 27: } 51,
{ 28: } 53,
{ 29: } 55,
{ 30: } 57,
{ 31: } 59,
{ 32: } 61,
{ 33: } 62,
{ 34: } 63,
{ 35: } 63,
{ 36: } 63,
{ 37: } 63,
{ 38: } 64,
{ 39: } 64,
{ 40: } 64,
{ 41: } 65,
{ 42: } 66,
{ 43: } 67,
{ 44: } 68,
{ 45: } 69,
{ 46: } 70,
{ 47: } 71,
{ 48: } 72,
{ 49: } 73
);

yyml : array [0..yynstates-1] of Integer = (
{ 0: } 1,
{ 1: } 1,
{ 2: } 1,
{ 3: } 3,
{ 4: } 5,
{ 5: } 7,
{ 6: } 9,
{ 7: } 11,
{ 8: } 13,
{ 9: } 15,
{ 10: } 17,
{ 11: } 19,
{ 12: } 21,
{ 13: } 23,
{ 14: } 25,
{ 15: } 27,
{ 16: } 29,
{ 17: } 30,
{ 18: } 32,
{ 19: } 34,
{ 20: } 36,
{ 21: } 38,
{ 22: } 40,
{ 23: } 42,
{ 24: } 44,
{ 25: } 46,
{ 26: } 48,
{ 27: } 50,
{ 28: } 52,
{ 29: } 54,
{ 30: } 56,
{ 31: } 58,
{ 32: } 60,
{ 33: } 62,
{ 34: } 63,
{ 35: } 64,
{ 36: } 64,
{ 37: } 64,
{ 38: } 64,
{ 39: } 65,
{ 40: } 65,
{ 41: } 65,
{ 42: } 66,
{ 43: } 67,
{ 44: } 68,
{ 45: } 69,
{ 46: } 70,
{ 47: } 71,
{ 48: } 72,
{ 49: } 73
);

yymh : array [0..yynstates-1] of Integer = (
{ 0: } 0,
{ 1: } 0,
{ 2: } 2,
{ 3: } 4,
{ 4: } 6,
{ 5: } 8,
{ 6: } 10,
{ 7: } 12,
{ 8: } 14,
{ 9: } 16,
{ 10: } 18,
{ 11: } 20,
{ 12: } 22,
{ 13: } 24,
{ 14: } 26,
{ 15: } 28,
{ 16: } 29,
{ 17: } 31,
{ 18: } 33,
{ 19: } 35,
{ 20: } 37,
{ 21: } 39,
{ 22: } 41,
{ 23: } 43,
{ 24: } 45,
{ 25: } 47,
{ 26: } 49,
{ 27: } 51,
{ 28: } 53,
{ 29: } 55,
{ 30: } 57,
{ 31: } 59,
{ 32: } 61,
{ 33: } 62,
{ 34: } 63,
{ 35: } 63,
{ 36: } 63,
{ 37: } 63,
{ 38: } 64,
{ 39: } 64,
{ 40: } 64,
{ 41: } 65,
{ 42: } 66,
{ 43: } 67,
{ 44: } 68,
{ 45: } 69,
{ 46: } 70,
{ 47: } 71,
{ 48: } 72,
{ 49: } 73
);

yytl : array [0..yynstates-1] of Integer = (
{ 0: } 1,
{ 1: } 33,
{ 2: } 65,
{ 3: } 66,
{ 4: } 68,
{ 5: } 70,
{ 6: } 72,
{ 7: } 74,
{ 8: } 76,
{ 9: } 76,
{ 10: } 78,
{ 11: } 79,
{ 12: } 80,
{ 13: } 81,
{ 14: } 81,
{ 15: } 81,
{ 16: } 81,
{ 17: } 81,
{ 18: } 81,
{ 19: } 81,
{ 20: } 81,
{ 21: } 81,
{ 22: } 81,
{ 23: } 81,
{ 24: } 81,
{ 25: } 81,
{ 26: } 81,
{ 27: } 81,
{ 28: } 81,
{ 29: } 81,
{ 30: } 81,
{ 31: } 81,
{ 32: } 81,
{ 33: } 81,
{ 34: } 81,
{ 35: } 82,
{ 36: } 84,
{ 37: } 86,
{ 38: } 88,
{ 39: } 89,
{ 40: } 91,
{ 41: } 93,
{ 42: } 94,
{ 43: } 94,
{ 44: } 94,
{ 45: } 94,
{ 46: } 94,
{ 47: } 94,
{ 48: } 95,
{ 49: } 95
);

yyth : array [0..yynstates-1] of Integer = (
{ 0: } 32,
{ 1: } 64,
{ 2: } 65,
{ 3: } 67,
{ 4: } 69,
{ 5: } 71,
{ 6: } 73,
{ 7: } 75,
{ 8: } 75,
{ 9: } 77,
{ 10: } 78,
{ 11: } 79,
{ 12: } 80,
{ 13: } 80,
{ 14: } 80,
{ 15: } 80,
{ 16: } 80,
{ 17: } 80,
{ 18: } 80,
{ 19: } 80,
{ 20: } 80,
{ 21: } 80,
{ 22: } 80,
{ 23: } 80,
{ 24: } 80,
{ 25: } 80,
{ 26: } 80,
{ 27: } 80,
{ 28: } 80,
{ 29: } 80,
{ 30: } 80,
{ 31: } 80,
{ 32: } 80,
{ 33: } 80,
{ 34: } 81,
{ 35: } 83,
{ 36: } 85,
{ 37: } 87,
{ 38: } 88,
{ 39: } 90,
{ 40: } 92,
{ 41: } 93,
{ 42: } 93,
{ 43: } 93,
{ 44: } 93,
{ 45: } 93,
{ 46: } 93,
{ 47: } 94,
{ 48: } 94,
{ 49: } 94
);


var yyn : Integer;

label start, scan, action;

begin

start:

  (* initialize: *)

  yynew;

scan:

  (* mark positions and matches: *)

  for yyn := yykl[yystate] to     yykh[yystate] do yymark(yyk[yyn]);
  for yyn := yymh[yystate] downto yyml[yystate] do yymatch(yym[yyn]);

  if yytl[yystate]>yyth[yystate] then
  begin
    yytoken_overrun := false;
    goto action; (* dead state *)
  end;

  (* get next character: *)

  yyscan;

  (* determine action: *)

  yyn := yytl[yystate];
  while (yyn<=yyth[yystate]) and not (yyactchar in yyt[yyn].cc) do inc(yyn);
  if yyn>yyth[yystate] then
  begin
    yytoken_overrun := true;
    goto action;
  end;
    (* no transition on yyactchar in this state *)

  (* switch to new state: *)

  yystate := yyt[yyn].s;

  goto scan;

action:

  (* execute action: *)

  if yyfind(yyrule) then
    begin
      yyaction(yyrule);
      if yyreject then goto action;
    end
  else if not yydefault and yywrap then
    begin
      yyclear;
      return(0);
    end;

  if not yydone then goto start;

  update_token_text;

  yylex := yyretval;

end(*yylex*);



    function SQL92GrammarLexer.TokenName(Token: integer): string;
    begin
      result := '<invalid/unknown token>';
      case (Token) of
        identifier_body: result := 'identifier_body';
        national_character_string_literal_start: result := 'national_character_string_literal_start';
        bit_string_literal_start: result := 'bit_string_literal_start';
        string_literal_continuation: result := 'string_literal_continuation';
        hex_string_literal_start: result := 'hex_string_literal_start';
        delimited_identifier: result := 'delimited_identifier';
        digit: result := 'digit';
        not_equals_operator: result := 'not_equals_operator';
        greater_than_or_equals_operator: result := 'greater_than_or_equals_operator';
        less_than_or_equals_operator: result := 'less_than_or_equals_operator';
        concatenation_operator: result := 'concatenation_operator';
        double_period: result := 'double_period';
        space: result := 'space';
        tab: result := 'tab';
        carriage_return: result := 'carriage_return';
        line_feed: result := 'line_feed';
        double_quote: result := 'double_quote';
        percent: result := 'percent';
        ampersand: result := 'ampersand';
        quote: result := 'quote';
        left_paren: result := 'left_paren';
        right_paren: result := 'right_paren';
        left_bracket: result := 'left_bracket';
        right_bracket: result := 'right_bracket';
        asterisk: result := 'asterisk';
        plus_sign: result := 'plus_sign';
        comma: result := 'comma';
        minus_sign: result := 'minus_sign';
        period: result := 'period';
        solidus: result := 'solidus';
        colon: result := 'colon';
        semicolon: result := 'semicolon';
        less_than_operator: result := 'less_than_operator';
        equals_operator: result := 'equals_operator';
        greater_than_operator: result := 'greater_than_operator';
        question_mark: result := 'question_mark';
        underscore: result := 'underscore';
        vertical_bar: result := 'vertical_bar';

	_ABSOLUTE: result := '_ABSOLUTE';
	_ACTION: result := '_ACTION';
	_ADD: result := '_ADD';
	_ALL: result := '_ALL';
	_ALLOCATE: result := '_ALLOCATE';
	_ALTER: result := '_ALTER';
	_AND: result := '_AND';
	_ANY: result := '_ANY';
	_ARE: result := '_ARE';
	_AS: result := '_AS';
	_ASC: result := '_ASC';
	_ASSERTION: result := '_ASSERTION';
	_AT: result := '_AT';
	_AUTHORIZATION: result := '_AUTHORIZATION';
	_AVG: result := '_AVG';
	_BEGIN: result := '_BEGIN';
	_BETWEEN: result := '_BETWEEN';
	_BIT: result := '_BIT';
	_BIT_LENGTH: result := '_BIT_LENGTH';
	_BOTH: result := '_BOTH';
	_BY: result := '_BY';
	_CASCADE: result := '_CASCADE';
	_CASCADED: result := '_CASCADED';
	_CASE: result := '_CASE';
	_CAST: result := '_CAST';
	_CATALOG: result := '_CATALOG';
	_CHAR: result := '_CHAR';
	_CHARACTER: result := '_CHARACTER';
	_CHARACTER_LENGTH: result := '_CHARACTER_LENGTH';
	_CHAR_LENGTH: result := '_CHAR_LENGTH';
	_CHECK: result := '_CHECK';
	_CLOSE: result := '_CLOSE';
	_COALESCE: result := '_COALESCE';
	_COLLATE: result := '_COLLATE';
	_COLLATION: result := '_COLLATION';
	_COLUMN: result := '_COLUMN';
	_COMMIT: result := '_COMMIT';
	_CONNECT: result := '_CONNECT';
	_CONNECTION: result := '_CONNECTION';
	_CONSTRAINT: result := '_CONSTRAINT';
	_CONSTRAINTS: result := '_CONSTRAINTS';
	_CONTINUE: result := '_CONTINUE';
	_CONVERT: result := '_CONVERT';
	_CORRESPONDING: result := '_CORRESPONDING';
	_CREATE: result := '_CREATE';
	_CROSS: result := '_CROSS';
	_CURRENT: result := '_CURRENT';
	_CURRENT_DATE: result := '_CURRENT_DATE';
	_CURRENT_TIME: result := '_CURRENT_TIME';
	_CURRENT_TIMESTAMP: result := '_CURRENT_TIMESTAMP';
	_CURRENT_USER: result := '_CURRENT_USER';
	_CURSOR: result := '_CURSOR';
	_DATE: result := '_DATE';
	_DAY: result := '_DAY';
	_DEALLOCATE: result := '_DEALLOCATE';
	_DEC: result := '_DEC';
	_DECIMAL: result := '_DECIMAL';
	_DECLARE: result := '_DECLARE';
	_DEFAULT: result := '_DEFAULT';
	_DEFERRABLE: result := '_DEFERRABLE';
	_DEFERRED: result := '_DEFERRED';
	_DELETE: result := '_DELETE';
	_DESC: result := '_DESC';
	_DESCRIBE: result := '_DESCRIBE';
	_DESCRIPTOR: result := '_DESCRIPTOR';
	_DIAGNOSTICS: result := '_DIAGNOSTICS';
	_DISCONNECT: result := '_DISCONNECT';
	_DISTINCT: result := '_DISTINCT';
	_DOMAIN: result := '_DOMAIN';
	_DOUBLE: result := '_DOUBLE';
	_DROP: result := '_DROP';
	_ELSE: result := '_ELSE';
	_END: result := '_END';
	_END_EXEC: result := '_END-EXEC';
	_ESCAPE: result := '_ESCAPE';
	_EXCEPT: result := '_EXCEPT';
	_EXCEPTION: result := '_EXCEPTION';
	_EXEC: result := '_EXEC';
	_EXECUTE: result := '_EXECUTE';
	_EXISTS: result := '_EXISTS';
	_EXTERNAL: result := '_EXTERNAL';
	_EXTRACT: result := '_EXTRACT';
	_FALSE: result := '_FALSE';
	_FETCH: result := '_FETCH';
	_FIRST: result := '_FIRST';
	_FLOAT: result := '_FLOAT';
	_FOR: result := '_FOR';
	_FOREIGN: result := '_FOREIGN';
	_FOUND: result := '_FOUND';
	_FROM: result := '_FROM';
	_FULL: result := '_FULL';
	_GET: result := '_GET';
	_GLOBAL: result := '_GLOBAL';
	_GO: result := '_GO';
	_GOTO: result := '_GOTO';
	_GRANT: result := '_GRANT';
	_GROUP: result := '_GROUP';
	_HAVING: result := '_HAVING';
	_HOUR: result := '_HOUR';
	_IDENTITY: result := '_IDENTITY';
	_IMMEDIATE: result := '_IMMEDIATE';
	_IN: result := '_IN';
	_INDICATOR: result := '_INDICATOR';
	_INITIALLY: result := '_INITIALLY';
	_INNER: result := '_INNER';
	_INPUT: result := '_INPUT';
	_INSENSITIVE: result := '_INSENSITIVE';
	_INSERT: result := '_INSERT';
	_INT: result := '_INT';
	_INTEGER: result := '_INTEGER';
	_INTERSECT: result := '_INTERSECT';
	_INTERVAL: result := '_INTERVAL';
	_INTO: result := '_INTO';
	_IS: result := '_IS';
	_ISOLATION: result := '_ISOLATION';
	_JOIN: result := '_JOIN';
	_KEY: result := '_KEY';
	_LANGUAGE: result := '_LANGUAGE';
	_LAST: result := '_LAST';
	_LEADING: result := '_LEADING';
	_LEFT: result := '_LEFT';
	_LEVEL: result := '_LEVEL';
	_LIKE: result := '_LIKE';
	_LOCAL: result := '_LOCAL';
	_LOWER: result := '_LOWER';
	_MATCH: result := '_MATCH';
	_MAX: result := '_MAX';
	_MIN: result := '_MIN';
	_MINUTE: result := '_MINUTE';
	_MODULE: result := '_MODULE';
	_MONTH: result := '_MONTH';
	_NAMES: result := '_NAMES';
	_NATIONAL: result := '_NATIONAL';
	_NATURAL: result := '_NATURAL';
	_NCHAR: result := '_NCHAR';
	_NEXT: result := '_NEXT';
	_NO: result := '_NO';
	_NOT: result := '_NOT';
	_NULL: result := '_NULL';
	_NULLIF: result := '_NULLIF';
	_NUMERIC: result := '_NUMERIC';
	_OCTET_LENGTH: result := '_OCTET_LENGTH';
	_OF: result := '_OF';
	_ON: result := '_ON';
	_ONLY: result := '_ONLY';
	_OPEN: result := '_OPEN';
	_OPTION: result := '_OPTION';
	_OR: result := '_OR';
	_ORDER: result := '_ORDER';
	_OUTER: result := '_OUTER';
	_OUTPUT: result := '_OUTPUT';
	_OVERLAPS: result := '_OVERLAPS';
	_PAD: result := '_PAD';
	_PARTIAL: result := '_PARTIAL';
	_POSITION: result := '_POSITION';
	_PRECISION: result := '_PRECISION';
	_PREPARE: result := '_PREPARE';
	_PRESERVE: result := '_PRESERVE';
	_PRIMARY: result := '_PRIMARY';
	_PRIOR: result := '_PRIOR';
	_PRIVILEGES: result := '_PRIVILEGES';
	_PROCEDURE: result := '_PROCEDURE';
	_PUBLIC: result := '_PUBLIC';
	_READ: result := '_READ';
	_REAL: result := '_REAL';
	_REFERENCES: result := '_REFERENCES';
	_RELATIVE: result := '_RELATIVE';
	_RESTRICT: result := '_RESTRICT';
	_REVOKE: result := '_REVOKE';
	_RIGHT: result := '_RIGHT';
	_ROLLBACK: result := '_ROLLBACK';
	_ROWS: result := '_ROWS';
	_SCHEMA: result := '_SCHEMA';
	_SCROLL: result := '_SCROLL';
	_SECOND: result := '_SECOND';
	_SECTION: result := '_SECTION';
	_SELECT: result := '_SELECT';
	_SESSION: result := '_SESSION';
	_SESSION_USER: result := '_SESSION_USER';
	_SET: result := '_SET';
	_SIZE: result := '_SIZE';
	_SMALLINT: result := '_SMALLINT';
	_SOME: result := '_SOME';
	_SPACE: result := '_SPACE';
	_SQL: result := '_SQL';
	_SQLCODE: result := '_SQLCODE';
	_SQLERROR: result := '_SQLERROR';
	_SQLSTATE: result := '_SQLSTATE';
	_SUBSTRING: result := '_SUBSTRING';
	_SUM: result := '_SUM';
	_SYSTEM_USER: result := '_SYSTEM_USER';
	_TABLE: result := '_TABLE';
	_TEMPORARY: result := '_TEMPORARY';
	_THEN: result := '_THEN';
	_TIME: result := '_TIME';
	_TIMESTAMP: result := '_TIMESTAMP';
	_TIMEZONE_HOUR: result := '_TIMEZONE_HOUR';
	_TIMEZONE_MINUTE: result := '_TIMEZONE_MINUTE';
	_TO: result := '_TO';
	_TRAILING: result := '_TRAILING';
	_TRANSACTION: result := '_TRANSACTION';
	_TRANSLATE: result := '_TRANSLATE';
	_TRANSLATION: result := '_TRANSLATION';
	_TRIM: result := '_TRIM';
	_TRUE: result := '_TRUE';
	_UNION: result := '_UNION';
	_UNIQUE: result := '_UNIQUE';
	_UNKNOWN: result := '_UNKNOWN';
	_UPDATE: result := '_UPDATE';
	_UPPER: result := '_UPPER';
	_USAGE: result := '_USAGE';
	_USER: result := '_USER';
	_USING: result := '_USING';
	_VALUE: result := '_VALUE';
	_VALUES: result := '_VALUES';
	_VARCHAR: result := '_VARCHAR';
	_VARYING: result := '_VARYING';
	_VIEW: result := '_VIEW';
	_WHEN: result := '_WHEN';
	_WHENEVER: result := '_WHENEVER';
	_WHERE: result := '_WHERE';
	_WITH: result := '_WITH';
	_WORK: result := '_WORK';
	_WRITE: result := '_WRITE';
	_YEAR: result := '_YEAR';
	_ZONE: result := '_ZONE';

        _ADA: result := '_ADA';
        _C: result := '_C';
        _CATALOG_NAME: result := '_CATALOG_NAME';
        _CHARACTER_SET_CATALOG: result := '_CHARACTER_SET_CATALOG';
        _CHARACTER_SET_NAME: result := '_CHARACTER_SET_NAME';
        _CHARACTER_SET_SCHEMA: result := '_CHARACTER_SET_SCHEMA';
        _CLASS_ORIGIN: result := '_CLASS_ORIGIN';
        _COBOL: result := '_COBOL';
        _COLLATION_CATALOG: result := '_COLLATION_CATALOG';
        _COLLATION_NAME: result := '_COLLATION_NAME';
        _COLLATION_SCHEMA: result := '_COLLATION_SCHEMA';
        _COLUMN_NAME: result := '_COLUMN_NAME';
        _COMMAND_FUNCTION: result := '_COMMAND_FUNCTION';
        _COMMITTED: result := '_COMMITTED';
        _CONDITION_NUMBER: result := '_CONDITION_NUMBER';
        _CONNECTION_NAME: result := '_CONNECTION_NAME';
        _CONSTRAINT_CATALOG: result := '_CONSTRAINT_CATALOG';
        _CONSTRAINT_NAME: result := '_CONSTRAINT_NAME';
        _CONSTRAINT_SCHEMA: result := '_CONSTRAINT_SCHEMA';
        _COUNT: result := '_COUNT';
        _CURSOR_NAME: result := '_CURSOR_NAME';
        _DATA: result := '_DATA';
        _DATETIME_INTERVAL_CODE: result := '_DATETIME_INTERVAL_CODE';
        _DATETIME_INTERVAL_PRECISION: result := '_DATETIME_INTERVAL_PRECISION';
        _DYNAMIC_FUNCTION: result := '_DYNAMIC_FUNCTION';
        _E: result := '_E';
        _FORTRAN: result := '_FORTRAN';
        _LENGTH: result := '_LENGTH';
        _MESSAGE_LENGTH: result := '_MESSAGE_LENGTH';
        _MESSAGE_OCTET_LENGTH: result := '_MESSAGE_OCTET_LENGTH';
        _MESSAGE_TEXT: result := '_MESSAGE_TEXT';
        _MORE: result := '_MORE';
        _MUMPS: result := '_MUMPS';
        _NAME: result := '_NAME';
        _NULLABLE: result := '_NULLABLE';
        _NUMBER: result := '_NUMBER';
        _PASCAL: result := '_PASCAL';
        _PLI: result := '_PLI';
        _REPEATABLE: result := '_REPEATABLE';
        _RETURNED_LENGTH: result := '_RETURNED_LENGTH';
        _RETURNED_OCTET_LENGTH: result := '_RETURNED_OCTET_LENGTH';
        _RETURNED_SQLSTATE: result := '_RETURNED_SQLSTATE';
        _ROW_COUNT: result := '_ROW_COUNT';
        _SCALE: result := '_SCALE';
        _SCHEMA_NAME: result := '_SCHEMA_NAME';
        _SERIALIZABLE: result := '_SERIALIZABLE';
        _SERVER_NAME: result := '_SERVER_NAME';
        _SNAPSHOT: result := '_SNAPSHOT';
        _SUBCLASS_ORIGIN: result := '_SUBCLASS_ORIGIN';
        _TABLE_NAME: result := '_TABLE_NAME';
        _TYPE: result := '_TYPE';
        _UNCOMMITTED: result := '_UNCOMMITTED';
        _UNNAMED: result := '_UNNAMED';

      else
      end;
    end;

  function SQL92GrammarLexer.BestIdentifierOrLiteral(const TokenText: AnsiString): integer;
  var
    TokenUpper: AnsiString;
  begin
    TokenUpper := yytoken_text;
    Assert(Length(TokenUpper)>0);
    TokenUpper := UpperCase(TokenUpper);
    result := Ord(identifier_body);
    case TokenUpper[1] of
      'A' :
        if TokenUpper = ('ABSOLUTE') then result := Ord(_ABSOLUTE)
        else if TokenUpper = ('ACTION') then result := Ord(_ACTION)
        else if TokenUpper = ('ADA') then result := Ord(_ADA)
        else if TokenUpper = ('ADD') then result := Ord(_ADD)
        else if TokenUpper = ('ALL') then result := Ord(_ALL)
        else if TokenUpper = ('ALLOCATE') then result := Ord(_ALLOCATE)
        else if TokenUpper = ('ALTER') then result := Ord(_ALTER)
        else if TokenUpper = ('AND') then result := Ord(_AND)
        else if TokenUpper = ('ANY') then result := Ord(_ANY)
        else if TokenUpper = ('ARE') then result := Ord(_ARE)
        else if TokenUpper = ('AS') then result := Ord(_AS)
        else if TokenUpper = ('ASC') then result := Ord(_ASC)
        else if TokenUpper = ('ASSERTION') then result := Ord(_ASSERTION)
        else if TokenUpper = ('AT') then result := Ord(_AT)
        else if TokenUpper = ('AUTHORIZATION') then result := Ord(_AUTHORIZATION)
        else if TokenUpper = ('AVG') then result := Ord(_AVG);
      'B':
        if TokenUpper = ('BEGIN') then result := Ord(_BEGIN)
        else if TokenUpper = ('BETWEEN') then result := Ord(_BETWEEN)
        else if TokenUpper = ('BIT') then result := Ord(_BIT)
        else if TokenUpper = ('BIT_LENGTH') then result := Ord(_BIT_LENGTH)
        else if TokenUpper = ('BOTH') then result := Ord(_BOTH)
        else if TokenUpper = ('BY') then result := Ord(_BY);
      'C':
        if TokenUpper = ('C') then result := Ord(_C)
        else if TokenUpper = ('CASCADE') then result := Ord(_CASCADE)
        else if TokenUpper = ('CASCADED') then result := Ord(_CASCADED)
        else if TokenUpper = ('CASE') then result := Ord(_CASE)
        else if TokenUpper = ('CAST') then result := Ord(_CAST)
        else if TokenUpper = ('CATALOG') then result := Ord(_CATALOG)
        else if TokenUpper = ('CATALOG_NAME') then result := Ord(_CATALOG_NAME)
        else if TokenUpper = ('CHAR') then result := Ord(_CHAR)
        else if TokenUpper = ('CHARACTER') then result := Ord(_CHARACTER)
        else if TokenUpper = ('CHARACTER_LENGTH') then result := Ord(_CHARACTER_LENGTH)
        else if TokenUpper = ('CHARACTER_SET_CATALOG') then result := Ord(_CHARACTER_SET_CATALOG)
        else if TokenUpper = ('CHARACTER_SET_NAME') then result := Ord(_CHARACTER_SET_NAME)
        else if TokenUpper = ('CHARACTER_SET_SCHEMA') then result := Ord(_CHARACTER_SET_SCHEMA)
        else if TokenUpper = ('CHAR_LENGTH') then result := Ord(_CHAR_LENGTH)
        else if TokenUpper = ('CHECK') then result := Ord(_CHECK)
        else if TokenUpper = ('CLASS_ORIGIN') then result := Ord(_CLASS_ORIGIN)
        else if TokenUpper = ('CLOSE') then result := Ord(_CLOSE)
        else if TokenUpper = ('COALESCE') then result := Ord(_COALESCE)
        else if TokenUpper = ('COBOL') then result := Ord(_COBOL)
        else if TokenUpper = ('COLLATE') then result := Ord(_COLLATE)
        else if TokenUpper = ('COLLATION') then result := Ord(_COLLATION)
        else if TokenUpper = ('COLLATION_CATALOG') then result := Ord(_COLLATION_CATALOG)
        else if TokenUpper = ('COLLATION_NAME') then result := Ord(_COLLATION_NAME)
        else if TokenUpper = ('COLLATION_SCHEMA') then result := Ord(_COLLATION_SCHEMA)
        else if TokenUpper = ('COLUMN') then result := Ord(_COLUMN)
        else if TokenUpper = ('COLUMN_NAME') then result := Ord(_COLUMN_NAME)
        else if TokenUpper = ('COMMAND_FUNCTION') then result := Ord(_COMMAND_FUNCTION)
        else if TokenUpper = ('COMMIT') then result := Ord(_COMMIT)
        else if TokenUpper = ('COMMITTED') then result := Ord(_COMMITTED)
        else if TokenUpper = ('CONDITION_NUMBER') then result := Ord(_CONDITION_NUMBER)
        else if TokenUpper = ('CONNECT') then result := Ord(_CONNECT)
        else if TokenUpper = ('CONNECTION') then result := Ord(_CONNECTION)
        else if TokenUpper = ('CONNECTION_NAME') then result := Ord(_CONNECTION_NAME)
        else if TokenUpper = ('CONSTRAINT') then result := Ord(_CONSTRAINT)
        else if TokenUpper = ('CONSTRAINT_CATALOG') then result := Ord(_CONSTRAINT_CATALOG)
        else if TokenUpper = ('CONSTRAINT_NAME') then result := Ord(_CONSTRAINT_NAME)
        else if TokenUpper = ('CONSTRAINT_SCHEMA') then result := Ord(_CONSTRAINT_SCHEMA)
        else if TokenUpper = ('CONSTRAINTS') then result := Ord(_CONSTRAINTS)
        else if TokenUpper = ('CONTINUE') then result := Ord(_CONTINUE)
        else if TokenUpper = ('CONVERT') then result := Ord(_CONVERT)
        else if TokenUpper = ('CORRESPONDING') then result := Ord(_CORRESPONDING)
        else if TokenUpper = ('COUNT') then result := Ord(_COUNT)
        else if TokenUpper = ('CREATE') then result := Ord(_CREATE)
        else if TokenUpper = ('CROSS') then result := Ord(_CROSS)
        else if TokenUpper = ('CURRENT') then result := Ord(_CURRENT)
        else if TokenUpper = ('CURRENT_DATE') then result := Ord(_CURRENT_DATE)
        else if TokenUpper = ('CURRENT_TIME') then result := Ord(_CURRENT_TIME)
        else if TokenUpper = ('CURRENT_TIMESTAMP') then result := Ord(_CURRENT_TIMESTAMP)
        else if TokenUpper = ('CURRENT_USER') then result := Ord(_CURRENT_USER)
        else if TokenUpper = ('CURSOR') then result := Ord(_CURSOR)
        else if TokenUpper = ('CURSOR_NAME') then result := Ord(_CURSOR_NAME);
      'D':
        if TokenUpper = ('DATA') then result := Ord(_DATA)
        else if TokenUpper = ('DATE') then result := Ord(_DATE)
        else if TokenUpper = ('DATETIME_INTERVAL_CODE') then result := Ord(_DATETIME_INTERVAL_CODE)
        else if TokenUpper = ('DATETIME_INTERVAL_PRECISION') then result := Ord(_DATETIME_INTERVAL_PRECISION)
        else if TokenUpper = ('DAY') then result := Ord(_DAY)
        else if TokenUpper = ('DEALLOCATE') then result := Ord(_DEALLOCATE)
        else if TokenUpper = ('DEC') then result := Ord(_DEC)
        else if TokenUpper = ('DECIMAL') then result := Ord(_DECIMAL)
        else if TokenUpper = ('DECLARE') then result := Ord(_DECLARE)
        else if TokenUpper = ('DEFAULT') then result := Ord(_DEFAULT)
        else if TokenUpper = ('DEFERRABLE') then result := Ord(_DEFERRABLE)
        else if TokenUpper = ('DEFERRED') then result := Ord(_DEFERRED)
        else if TokenUpper = ('DELETE') then result := Ord(_DELETE)
        else if TokenUpper = ('DESC') then result := Ord(_DESC)
        else if TokenUpper = ('DESCRIBE') then result := Ord(_DESCRIBE)
        else if TokenUpper = ('DESCRIPTOR') then result := Ord(_DESCRIPTOR)
        else if TokenUpper = ('DIAGNOSTICS') then result := Ord(_DIAGNOSTICS)
        else if TokenUpper = ('DISCONNECT') then result := Ord(_DISCONNECT)
        else if TokenUpper = ('DISTINCT') then result := Ord(_DISTINCT)
        else if TokenUpper = ('DOMAIN') then result := Ord(_DOMAIN)
        else if TokenUpper = ('DOUBLE') then result := Ord(_DOUBLE)
        else if TokenUpper = ('DROP') then result := Ord(_DROP)
        else if TokenUpper = ('DYNAMIC_FUNCTION') then result := Ord(_DYNAMIC_FUNCTION);
      'E':
        if TokenUpper = ('E') then result := Ord(_E)
        else if TokenUpper = ('ELSE') then result := Ord(_ELSE)
        else if TokenUpper = ('END') then result := Ord(_END)
        else if TokenUpper = ('END_EXEC') then result := Ord(_END_EXEC)
        else if TokenUpper = ('ESCAPE') then result := Ord(_ESCAPE)
        else if TokenUpper = ('EXCEPT') then result := Ord(_EXCEPT)
        else if TokenUpper = ('EXCEPTION') then result := Ord(_EXCEPTION)
        else if TokenUpper = ('EXEC') then result := Ord(_EXEC)
        else if TokenUpper = ('EXECUTE') then result := Ord(_EXECUTE)
        else if TokenUpper = ('EXISTS') then result := Ord(_EXISTS)
        else if TokenUpper = ('EXTERNAL') then result := Ord(_EXTERNAL)
        else if TokenUpper = ('EXTRACT') then result := Ord(_EXTRACT);
      'F':
        if TokenUpper = ('FALSE') then result := Ord(_FALSE)
        else if TokenUpper = ('FETCH') then result := Ord(_FETCH)
        else if TokenUpper = ('FIRST') then result := Ord(_FIRST)
        else if TokenUpper = ('FLOAT') then result := Ord(_FLOAT)
        else if TokenUpper = ('FOR') then result := Ord(_FOR)
        else if TokenUpper = ('FORTRAN') then result := Ord(_FORTRAN)
        else if TokenUpper = ('FOREIGN') then result := Ord(_FOREIGN)
        else if TokenUpper = ('FOUND') then result := Ord(_FOUND)
        else if TokenUpper = ('FROM') then result := Ord(_FROM)
        else if TokenUpper = ('FULL') then result := Ord(_FULL);
      'G':
        if TokenUpper = ('GET') then result := Ord(_GET)
        else if TokenUpper = ('GLOBAL') then result := Ord(_GLOBAL)
        else if TokenUpper = ('GO') then result := Ord(_GO)
        else if TokenUpper = ('GOTO') then result := Ord(_GOTO)
        else if TokenUpper = ('GRANT') then result := Ord(_GRANT)
        else if TokenUpper = ('GROUP') then result := Ord(_GROUP);
      'H':
        if TokenUpper = ('HAVING') then result := Ord(_HAVING)
        else if TokenUpper = ('HOUR') then result := Ord(_HOUR);
      'I':
        if TokenUpper = ('IDENTITY') then result := Ord(_IDENTITY)
        else if TokenUpper = ('IMMEDIATE') then result := Ord(_IMMEDIATE)
        else if TokenUpper = ('IN') then result := Ord(_IN)
        else if TokenUpper = ('INDICATOR') then result := Ord(_INDICATOR)
        else if TokenUpper = ('INITIALLY') then result := Ord(_INITIALLY)
        else if TokenUpper = ('INNER') then result := Ord(_INNER)
        else if TokenUpper = ('INPUT') then result := Ord(_INPUT)
        else if TokenUpper = ('INSENSITIVE') then result := Ord(_INSENSITIVE)
        else if TokenUpper = ('INSERT') then result := Ord(_INSERT)
        else if TokenUpper = ('INT') then result := Ord(_INT)
        else if TokenUpper = ('INTEGER') then result := Ord(_INTEGER)
        else if TokenUpper = ('INTERSECT') then result := Ord(_INTERSECT)
        else if TokenUpper = ('INTERVAL') then result := Ord(_INTERVAL)
        else if TokenUpper = ('INTO') then result := Ord(_INTO)
        else if TokenUpper = ('IS') then result := Ord(_IS)
        else if TokenUpper = ('ISOLATION') then result := Ord(_ISOLATION);
      'J':
        if TokenUpper = ('JOIN') then result := Ord(_JOIN);
      'K':
        if TokenUpper = ('KEY') then result := Ord(_KEY);
      'L':
        if TokenUpper = ('LANGUAGE') then result := Ord(_LANGUAGE)
        else if TokenUpper = ('LAST') then result := Ord(_LAST)
        else if TokenUpper = ('LEADING') then result := Ord(_LEADING)
        else if TokenUpper = ('LEFT') then result := Ord(_LEFT)
        else if TokenUpper = ('LENGTH') then result := Ord(_LENGTH)
        else if TokenUpper = ('LEVEL') then result := Ord(_LEVEL)
        else if TokenUpper = ('LIKE') then result := Ord(_LIKE)
        else if TokenUpper = ('LOCAL') then result := Ord(_LOCAL)
        else if TokenUpper = ('LOWER') then result := Ord(_LOWER);
      'M':
        if TokenUpper = ('MATCH') then result := Ord(_MATCH)
        else if TokenUpper = ('MAX') then result := Ord(_MAX)
        else if TokenUpper = ('MESSAGE_LENGTH') then result := Ord(_MESSAGE_LENGTH)
        else if TokenUpper = ('MESSAGE_OCTET_LENGTH') then result := Ord(_MESSAGE_OCTET_LENGTH)
        else if TokenUpper = ('MESSAGE_TEXT') then result := Ord(_MESSAGE_TEXT)
        else if TokenUpper = ('MIN') then result := Ord(_MIN)
        else if TokenUpper = ('MINUTE') then result := Ord(_MINUTE)
        else if TokenUpper = ('MODULE') then result := Ord(_MODULE)
        else if TokenUpper = ('MONTH') then result := Ord(_MONTH)
        else if TokenUpper = ('MORE') then result := Ord(_MORE)
        else if TokenUpper = ('MUMPS') then result := Ord(_MUMPS);
      'N':
        if TokenUpper = ('NAME') then result := Ord(_NAME)
        else if TokenUpper = ('NAMES') then result := Ord(_NAMES)
        else if TokenUpper = ('NATIONAL') then result := Ord(_NATIONAL)
        else if TokenUpper = ('NATURAL') then result := Ord(_NATURAL)
        else if TokenUpper = ('NCHAR') then result := Ord(_NCHAR)
        else if TokenUpper = ('NEXT') then result := Ord(_NEXT)
        else if TokenUpper = ('NO') then result := Ord(_NO)
        else if TokenUpper = ('NOT') then result := Ord(_NOT)
        else if TokenUpper = ('NULL') then result := Ord(_NULL)
        else if TokenUpper = ('NULLABLE') then result := Ord(_NULLABLE)
        else if TokenUpper = ('NULLIF') then result := Ord(_NULLIF)
        else if TokenUpper = ('NUMBER') then result := Ord(_NUMBER)
        else if TokenUpper = ('NUMERIC') then result := Ord(_NUMERIC);
      'O':
        if TokenUpper = ('OCTET_LENGTH') then result := Ord(_OCTET_LENGTH)
        else if TokenUpper = ('OF') then result := Ord(_OF)
        else if TokenUpper = ('ON') then result := Ord(_ON)
        else if TokenUpper = ('ONLY') then result := Ord(_ONLY)
        else if TokenUpper = ('OPEN') then result := Ord(_OPEN)
        else if TokenUpper = ('OPTION') then result := Ord(_OPTION)
        else if TokenUpper = ('OR') then result := Ord(_OR)
        else if TokenUpper = ('ORDER') then result := Ord(_ORDER)
        else if TokenUpper = ('OUTER') then result := Ord(_OUTER)
        else if TokenUpper = ('OUTPUT') then result := Ord(_OUTPUT)
        else if TokenUpper = ('OVERLAPS') then result := Ord(_OVERLAPS);
      'P':
        if TokenUpper = ('PAD') then result := Ord(_PAD)
        else if TokenUpper = ('PARTIAL') then result := Ord(_PARTIAL)
        else if TokenUpper = ('PASCAL') then result := Ord(_PASCAL)
        else if TokenUpper = ('PLI') then result := Ord(_PLI)
        else if TokenUpper = ('POSITION') then result := Ord(_POSITION)
        else if TokenUpper = ('PRECISION') then result := Ord(_PRECISION)
        else if TokenUpper = ('PREPARE') then result := Ord(_PREPARE)
        else if TokenUpper = ('PRESERVE') then result := Ord(_PRESERVE)
        else if TokenUpper = ('PRIMARY') then result := Ord(_PRIMARY)
        else if TokenUpper = ('PRIOR') then result := Ord(_PRIOR)
        else if TokenUpper = ('PRIVILEGES') then result := Ord(_PRIVILEGES)
        else if TokenUpper = ('PROCEDURE') then result := Ord(_PROCEDURE)
        else if TokenUpper = ('PUBLIC') then result := Ord(_PUBLIC);
      'R':
        if TokenUpper = ('READ') then result := Ord(_READ)
        else if TokenUpper = ('REAL') then result := Ord(_REAL)
        else if TokenUpper = ('REFERENCES') then result := Ord(_REFERENCES)
        else if TokenUpper = ('RELATIVE') then result := Ord(_RELATIVE)
        else if TokenUpper = ('REPEATABLE') then result := Ord(_REPEATABLE)
        else if TokenUpper = ('RESTRICT') then result := Ord(_RESTRICT)
        else if TokenUpper = ('RETURNED_LENGTH') then result := Ord(_RETURNED_LENGTH)
        else if TokenUpper = ('RETURNED_OCTET_LENGTH') then result := Ord(_RETURNED_OCTET_LENGTH)
        else if TokenUpper = ('RETURNED_SQLSTATE') then result := Ord(_RETURNED_SQLSTATE)
        else if TokenUpper = ('REVOKE') then result := Ord(_REVOKE)
        else if TokenUpper = ('RIGHT') then result := Ord(_RIGHT)
        else if TokenUpper = ('ROLLBACK') then result := Ord(_ROLLBACK)
        else if TokenUpper = ('ROW_COUNT') then result := Ord(_ROW_COUNT)
        else if TokenUpper = ('ROWS') then result := Ord(_ROWS);
      'S':
        if TokenUpper = ('SCHEMA') then result := Ord(_SCHEMA)
        else if TokenUpper = ('SCHEMA_NAME') then result := Ord(_SCHEMA_NAME)
        else if TokenUpper = ('SCALE') then result := Ord(_SCALE)
        else if TokenUpper = ('SCROLL') then result := Ord(_SCROLL)
        else if TokenUpper = ('SECOND') then result := Ord(_SECOND)
        else if TokenUpper = ('SECTION') then result := Ord(_SECTION)
        else if TokenUpper = ('SELECT') then result := Ord(_SELECT)
        else if TokenUpper = ('SERIALIZABLE') then result := Ord(_SERIALIZABLE)
        else if TokenUpper = ('SERVER_NAME') then result := Ord(_SERVER_NAME)
        else if TokenUpper = ('SESSION') then result := Ord(_SESSION)
        else if TokenUpper = ('SESSION_USER') then result := Ord(_SESSION_USER)
        else if TokenUpper = ('SET') then result := Ord(_SET)
        else if TokenUpper = ('SIZE') then result := Ord(_SIZE)
        else if TokenUpper = ('SMALLINT') then result := Ord(_SMALLINT)
        else if TokenUpper = ('SNAPSHOT') then result := Ord(_SNAPSHOT)
        else if TokenUpper = ('SOME') then result := Ord(_SOME)
        else if TokenUpper = ('SPACE') then result := Ord(_SPACE)
        else if TokenUpper = ('SQL') then result := Ord(_SQL)
        else if TokenUpper = ('SQLCODE') then result := Ord(_SQLCODE)
        else if TokenUpper = ('SQLERROR') then result := Ord(_SQLERROR)
        else if TokenUpper = ('SQLSTATE') then result := Ord(_SQLSTATE)
        else if TokenUpper = ('SUBCLASS_ORIGIN') then result := Ord(_SUBCLASS_ORIGIN)
        else if TokenUpper = ('SUBSTRING') then result := Ord(_SUBSTRING)
        else if TokenUpper = ('SUM') then result := Ord(_SUM)
        else if TokenUpper = ('SYSTEM_USER') then result := Ord(_SYSTEM_USER);
      'T':
        if TokenUpper = ('TABLE') then result := Ord(_TABLE)
        else if TokenUpper = ('TABLE_NAME') then result := Ord(_TABLE_NAME)
        else if TokenUpper = ('TEMPORARY') then result := Ord(_TEMPORARY)
        else if TokenUpper = ('THEN') then result := Ord(_THEN)
        else if TokenUpper = ('TIME') then result := Ord(_TIME)
        else if TokenUpper = ('TIMESTAMP') then result := Ord(_TIMESTAMP)
        else if TokenUpper = ('TIMEZONE_HOUR') then result := Ord(_TIMEZONE_HOUR)
        else if TokenUpper = ('TIMEZONE_MINUTE') then result := Ord(_TIMEZONE_MINUTE)
        else if TokenUpper = ('TO') then result := Ord(_TO)
        else if TokenUpper = ('TRAILING') then result := Ord(_TRAILING)
        else if TokenUpper = ('TRANSACTION') then result := Ord(_TRANSACTION)
        else if TokenUpper = ('TRANSLATE') then result := Ord(_TRANSLATE)
        else if TokenUpper = ('TRANSLATION') then result := Ord(_TRANSLATION)
        else if TokenUpper = ('TRIM') then result := Ord(_TRIM)
        else if TokenUpper = ('TRUE') then result := Ord(_TRUE)
        else if TokenUpper = ('TYPE') then result := Ord(_TYPE);
      'U':
        if TokenUpper = ('UNION') then result := Ord(_UNION)
        else if TokenUpper = ('UNIQUE') then result := Ord(_UNIQUE)
        else if TokenUpper = ('UNCOMMITTED') then result := Ord(_UNCOMMITTED)
        else if TokenUpper = ('UNKNOWN') then result := Ord(_UNKNOWN)
        else if TokenUpper = ('UNNAMED') then result := Ord(_UNNAMED)
        else if TokenUpper = ('UPDATE') then result := Ord(_UPDATE)
        else if TokenUpper = ('UPPER') then result := Ord(_UPPER)
        else if TokenUpper = ('USAGE') then result := Ord(_USAGE)
        else if TokenUpper = ('USER') then result := Ord(_USER)
        else if TokenUpper = ('USING') then result := Ord(_USING);
      'V':
        if TokenUpper = ('VALUE') then result := Ord(_VALUE)
        else if TokenUpper = ('VALUES') then result := Ord(_VALUES)
        else if TokenUpper = ('VARCHAR') then result := Ord(_VARCHAR)
        else if TokenUpper = ('VARYING') then result := Ord(_VARYING)
        else if TokenUpper = ('VIEW') then result := Ord(_VIEW);
      'W':
        if TokenUpper = ('WHEN') then result := Ord(_WHEN)
        else if TokenUpper = ('WHENEVER') then result := Ord(_WHENEVER)
        else if TokenUpper = ('WHERE') then result := Ord(_WHERE)
        else if TokenUpper = ('WITH') then result := Ord(_WITH)
        else if TokenUpper = ('WORK') then result := Ord(_WORK)
        else if TokenUpper = ('WRITE') then result := Ord(_WRITE);
      'Y':
        if TokenUpper = ('YEAR') then result := Ord(_YEAR);
      'Z':
        if TokenUpper = ('ZONE') then result := Ord(_ZONE);
      else
    end;
  end;

end.


