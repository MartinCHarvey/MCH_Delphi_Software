
%{

unit SQL92Grammar_parser;

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

uses
{$IFDEF INCLYYSTYPE}
  YYSType_incl,
{$ENDIF}
  yacclib_oo, SQL92Grammar_lexer, Classes, SQL92Grammar_parser_debug,
  SysUtils, SQL92Nodes;

%}

/*
  TODO - Test and possibly amend

  0. Removed shift-reduce on period, by allowing
  a.b.c.* as a separate production.

  1. Current time / current timestamp shift/reduce,
  I think the shift (default) is OK.

*/

/*
  TODO - unsigned integers shouldn't be literal like, just
  string'y, and use exact and inexact numeric lierals as the classes?
*/

%classname SQL92GrammarParser

%classfunc  constructor Create;
%classfunc  destructor Destroy; override;
%classfunc  procedure yyerror ( msg : String ); override;
%classfunc  procedure yyaction_debug(State: integer; Action: integer); override;
%classfunc  function MakeClass(ClassType: TSQLSynNodeClass): TSQLSynNode;
%classfunc  function CheckContinuation(S: string; T: TSQLSynLiteralType): boolean;
%classfunc function HandleContinuation(S: string; T: TSQlSynLiteralType): string;
%classfunc  function CheckDelimIdent(S: string): boolean;
%classfunc function HandleDelimIdent(S: string): string;

%token

                identifier_body
                national_character_string_literal_start
                bit_string_literal_start
                string_literal_continuation
                hex_string_literal_start
                delimited_identifier
                digit
                not_equals_operator
                greater_than_or_equals_operator
                less_than_or_equals_operator
                concatenation_operator
                double_period
                space
                tab
                carriage_return
                line_feed
                double_quote
                percent
                ampersand
                quote
                left_paren
                right_paren
                left_bracket
                right_bracket
                asterisk
                plus_sign
                comma
                minus_sign
                period
                solidus
                colon
                semicolon
                less_than_operator
                equals_operator
                greater_than_operator
                question_mark
                underscore
                vertical_bar

		_ABSOLUTE
		_ACTION
		_ADD
		_ALL
		_ALLOCATE
		_ALTER
		_AND
		_ANY
		_ARE
		_AS
		_ASC
		_ASSERTION
		_AT
		_AUTHORIZATION
		_AVG
		_BEGIN
		_BETWEEN
                _BIGINT
		_BIT
		_BIT_LENGTH
		_BOTH
		_BY
		_CASCADE
		_CASCADED
		_CASE
		_CAST
		_CATALOG
		_CHAR
		_CHARACTER
		_CHARACTER_LENGTH
		_CHAR_LENGTH
		_CHECK
		_CLOSE
		_COALESCE
		_COLLATE
		_COLLATION
		_COLUMN
		_COMMIT
		_CONNECT
		_CONNECTION
		_CONSTRAINT
		_CONSTRAINTS
		_CONTINUE
		_CONVERT
		_CORRESPONDING
		_CREATE
		_CROSS
		_CURRENT
		_CURRENT_DATE
		_CURRENT_TIME
		_CURRENT_TIMESTAMP
		_CURRENT_USER
		_CURSOR
		_DATE
		_DAY
		_DEALLOCATE
		_DEC
		_DECIMAL
		_DECLARE
		_DEFAULT
		_DEFERRABLE
		_DEFERRED
		_DELETE
		_DESC
		_DESCRIBE
		_DESCRIPTOR
		_DIAGNOSTICS
		_DISCONNECT
		_DISTINCT
		_DOMAIN
		_DOUBLE
		_DROP
		_ELSE
		_END
		_END_EXEC
		_ESCAPE
		_EXCEPT
		_EXCEPTION
		_EXEC
		_EXECUTE
		_EXISTS
		_EXTERNAL
		_EXTRACT
		_FALSE
		_FETCH
		_FIRST
		_FLOAT
		_FOR
		_FOREIGN
		_FOUND
		_FROM
		_FULL
		_GET
		_GLOBAL
		_GO
		_GOTO
		_GRANT
		_GROUP
		_HAVING
		_HOUR
		_IDENTITY
		_IMMEDIATE
		_IN
		_INDICATOR
		_INITIALLY
		_INNER
		_INPUT
		_INSENSITIVE
		_INSERT
		_INT
		_INTEGER
		_INTERSECT
		_INTERVAL
		_INTO
		_IS
		_ISOLATION
		_JOIN
		_KEY
		_LANGUAGE
		_LAST
		_LEADING
		_LEFT
		_LEVEL
		_LIKE
		_LOCAL
		_LOWER
		_MATCH
		_MAX
		_MIN
		_MINUTE
		_MODULE
		_MONTH
		_NAMES
		_NATIONAL
		_NATURAL
		_NCHAR
		_NEXT
		_NO
		_NOT
		_NULL
		_NULLIF
		_NUMERIC
		_OCTET_LENGTH
		_OF
		_ON
		_ONLY
		_OPEN
		_OPTION
		_OR
		_ORDER
		_OUTER
		_OUTPUT
		_OVERLAPS
		_PAD
		_PARTIAL
		_POSITION
		_PRECISION
		_PREPARE
		_PRESERVE
		_PRIMARY
		_PRIOR
		_PRIVILEGES
		_PROCEDURE
		_PUBLIC
		_READ
		_REAL
		_REFERENCES
		_RELATIVE
		_RESTRICT
		_REVOKE
		_RIGHT
		_ROLLBACK
		_ROWS
		_SCHEMA
		_SCROLL
		_SECOND
		_SECTION
		_SELECT
		_SESSION
		_SESSION_USER
		_SET
		_SIZE
		_SMALLINT
		_SOME
		_SPACE
		_SQL
		_SQLCODE
		_SQLERROR
		_SQLSTATE
		_SUBSTRING
		_SUM
		_SYSTEM_USER
		_TABLE
		_TEMPORARY
		_THEN
		_TIME
		_TIMESTAMP
		_TIMEZONE_HOUR
		_TIMEZONE_MINUTE
		_TO
		_TRAILING
		_TRANSACTION
		_TRANSLATE
		_TRANSLATION
		_TRIM
		_TRUE
		_UNION
		_UNIQUE
		_UNKNOWN
		_UPDATE
		_UPPER
		_USAGE
		_USER
		_USING
		_VALUE
		_VALUES
		_VARCHAR
		_VARYING
		_VIEW
		_WHEN
		_WHENEVER
		_WHERE
		_WITH
		_WORK
		_WRITE
		_YEAR
		_ZONE

		 _ADA
		 _C
		 _CATALOG_NAME
		 _CHARACTER_SET_CATALOG
		 _CHARACTER_SET_NAME
		 _CHARACTER_SET_SCHEMA
		 _CLASS_ORIGIN
		 _COBOL
		 _COLLATION_CATALOG
		 _COLLATION_NAME
		 _COLLATION_SCHEMA
		 _COLUMN_NAME
		 _COMMAND_FUNCTION
		 _COMMITTED
		 _CONDITION_NUMBER
		 _CONNECTION_NAME
		 _CONSTRAINT_CATALOG
		 _CONSTRAINT_NAME
		 _CONSTRAINT_SCHEMA
		 _COUNT
		 _CURSOR_NAME
		 _DATA
		 _DATETIME_INTERVAL_CODE
		 _DATETIME_INTERVAL_PRECISION
		 _DYNAMIC_FUNCTION
		 _E
		 _FORTRAN
		 _LENGTH
		 _MESSAGE_LENGTH
		 _MESSAGE_OCTET_LENGTH
		 _MESSAGE_TEXT
		 _MORE
		 _MUMPS
		 _NAME
		 _NULLABLE
		 _NUMBER
		 _PASCAL
		 _PLI
		 _REPEATABLE
		 _RETURNED_LENGTH
		 _RETURNED_OCTET_LENGTH
		 _RETURNED_SQLSTATE
		 _ROW_COUNT
		 _SCALE
		 _SCHEMA_NAME
		 _SERIALIZABLE
		 _SERVER_NAME
		 _SNAPSHOT
		 _SUBCLASS_ORIGIN
		 _TABLE_NAME
		 _TYPE
		 _UNCOMMITTED
		 _UNNAMED
                 LEX_ERROR /* Does not appear anywhere in the grammar */

%< plus_sign minus_sign
%< concatenation_operator
%< asterisk solidus

%start SQL92Grammar

%%

/*
--h2 Basic Definitions of Characters Used, Tokens, Symbols, Etc.
--/h2
*/

%{
  { YYaction local vars here - after comment, before first production. }
  var
    TmpInt: integer;
    TmpClass, TmpClass2: TSqlSynNode;
%}

  regular_identifier : identifier_body          { $$.Text := UTF8ToString(Lexer.yytext);
                                                  $$.Obj := MakeClass(TSQLSynIdent);
                                                  with $$.Obj as TSQLSynIdent do
                                                    IdentName := $$.Text; }
        ;

/*
--hr
--h2 Literal Numbers, Strings, Dates and Times
--/h2
*/

  unsigned_numeric_literal : exact_numeric_literal
                                                { $$ := $1; }
	|	approximate_numeric_literal
                                                { $$ := $1; }
        ;

  exact_numeric_literal :
                unsigned_integer exact_numeric_literal_opt
                                                { $$ := $1;
                                                  if Length($2.Text) > 0 then
                                                  begin
                                                    with $$.Obj as TSQLSynLiteral do
                                                    begin
                                                      Assert($$.text = Text);
                                                      $$.text := $$.text + $2.text;
                                                      Text := $$.text;
                                                      LitType := sltExactNumeric;
                                                    end;
                                                  end; }
	|	period unsigned_integer         { $$ := $2;
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    LitType := sltExactNumeric;
                                                    Assert($$.text = Text);
                                                    $$.text := '.' + $$.text;
                                                    Text := $$.Text;
                                                  end; }
        ;

  exact_numeric_literal_opt :
                /* empty */                     { $$.text := ''; $$.Obj := nil; }
        |       period                          { $$.text := '.'; $$.Obj := nil; }
        |       period unsigned_integer         {
                                                  Assert($2.Text = ($2.Obj as TSQLSynLiteral).Text);
                                                  $$.text := '.' + $2.Text;
                                                  $2.Obj.Free;
                                                  $$.Obj := nil; }
        ;

  unsigned_integer : digit                      { $$.text := UTF8ToString(Lexer.yytext);
                                                  $$.Obj := MakeClass(TSQLSynLiteral);
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    LitType := sltUnsInt;
                                                    Text := $$.text;
                                                  end; }
        |            unsigned_integer  digit    { Assert($1.Text = ($1.Obj as TSQLSynLiteral).Text);
                                                  $$ := $1;
                                                  $$.Text := $1.Text + UTF8ToString(Lexer.yytext);
                                                  ($$.Obj as TSQLSynLiteral).Text := $$.Text; }
        ;

  approximate_numeric_literal : mantissa _E exponent
                                                {
                                                  Assert($1.Text = ($1.Obj as TSQLSynLiteral).Text);
                                                  Assert($3.Text = ($1.Obj as TSQLSynLiteral).Text);
                                                  $$ := $1;
                                                  $$.Text := $$.Text + 'E' + $3.Text;
                                                  with $$.Obj as TSqlSynLiteral do
                                                  begin
                                                    Text := $$.Text;
                                                    LitType := sltApproxNumeric;
                                                  end;
                                                  $3.Obj.Free; }
        ;

  mantissa : exact_numeric_literal
                                                { $$ := $1; }
        ;

  exponent : signed_integer
                                                { $$ := $1; }
        ;

  signed_integer :
                sign unsigned_integer
                                                { $$ := $2;
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    if $1.text <> '+' then
                                                    begin
                                                      Assert($$.Text = Text);
                                                      $$.Text := $1.Text + $$.Text;
                                                      Text := $$.Text;
                                                    end;
                                                    LitType := sltInt;
                                                  end; }
        |       unsigned_integer
                                                { $$ := $1;
                                                  with $$.Obj as TSQLSynLiteral do
                                                    LitType := sltInt; }
        ;

  sign : plus_sign
                                                { $$.Text := UTF8ToString(Lexer.yytext);
                                                  $$.Obj := nil; }
        | minus_sign
                                                { $$.Text := UTF8ToString(Lexer.yytext);
                                                  $$.Obj := nil; }
        ;

  _national_character_string_literal_start :
        national_character_string_literal_start
                                                { $$.Obj := nil;
                                                  $$.Text := UTF8ToString(Lexer.yytext);
                                                  Assert($$.Text[1] = 'N');
                                                  Assert($$.Text[2] = '''');
                                                  Assert($$.Text[Length($$.Text)] = '''');
                                                  TmpInt := Length($$.Text);
                                                  $$.Text := Copy($$.Text, 2, Length($$.Text) - 1);
                                                  Assert(Length($$.Text) = TmpInt - 1);
                                                  if CheckContinuation($$.Text, sltNatString) then
                                                    $$.Text := HandleContinuation($$.Text, sltNatString)
                                                  else
                                                    yyerror('Not a valid national character string.'); }
        ;

  national_character_string_literal :
        _national_character_string_literal_start
        national_character_string_literal_cont
                                                { yyinfo('National character string interpreted as plain UTF-8 string.');
                                                  $$.Text := $1.Text + $2.Text;
                                                  $$.Obj := MakeClass(TSQLSynLiteral);
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    Text := $$.Text;
                                                    LitType := sltNatString;
                                                  end; }
        ;

  national_character_string_literal_cont :
        /* empty */                             { $$.Text := '';
                                                  $$.Obj := nil; }
        | national_character_string_literal_cont string_literal_continuation
                                                { $$.Obj := nil;
                                                  if CheckContinuation(UTF8ToString(Lexer.yytext), sltNatString) then
                                                    $$.Text := $1.Text + HandleContinuation(UTF8ToString(Lexer.yytext), sltNatString)
                                                  else
                                                    yyerror('String continuation not a national character string'); }

        ;

  _bit_string_literal_start :
        bit_string_literal_start
                                                { $$.Obj := nil;
                                                  $$.Text := UTF8ToString(Lexer.yytext);
                                                  Assert($$.Text[1] = 'B');
                                                  Assert($$.Text[2] = '''');
                                                  Assert($$.Text[Length($$.Text)] = '''');
                                                  TmpInt := Length($$.Text);
                                                  $$.Text := Copy($$.Text, 3, Length($$.Text) - 3);
                                                  Assert(Length($$.Text) = TmpInt - 3); }
        ;

  bit_string_literal :
        _bit_string_literal_start
        bit_string_literal_cont
                                                { $$.Text := $1.Text + $2.Text;
                                                  $$.Obj := MakeClass(TSQLSynLiteral);
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    Text := $$.Text;
                                                    LitType := sltBitString;
                                                  end; }
        ;

  bit_string_literal_cont :
        /* empty */                             { $$.Text := ''; $$.Obj := nil; }
        | bit_string_literal_cont string_literal_continuation
                                                { $$.Obj := nil;
                                                  if CheckContinuation(UTF8ToString(Lexer.yytext), sltBitString) then
                                                    $$.Text := $1.Text + HandleContinuation(UTF8ToString(Lexer.yytext), sltBitString)
                                                  else
                                                    yyerror('String continuation not a bit string'); }
        ;

  _hex_string_literal_start :
        hex_string_literal_start
                                                { $$.Obj := nil;
                                                  $$.Text := UTF8ToString(Lexer.yytext);
                                                  Assert($$.Text[1] = 'X');
                                                  Assert($$.Text[2] = '''');
                                                  Assert($$.Text[Length($$.Text)] = '''');
                                                  TmpInt := Length($$.Text);
                                                  $$.Text := Copy($$.Text, 3, Length($$.Text) - 3);
                                                  Assert(Length($$.Text) = TmpInt - 3); }
        ;

  hex_string_literal:
        _hex_string_literal_start
        hex_string_literal_cont
                                                { $$.Text := $1.Text + $2.Text;
                                                  $$.Obj := MakeClass(TSQLSynLiteral);
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    Text := $$.Text;
                                                    LitType := sltHexString;
                                                  end; }
        ;

  hex_string_literal_cont :
        /* empty */                             { $$.Text := ''; $$.Obj := nil;}
        | hex_string_literal_cont string_literal_continuation
                                                { $$.Obj := nil;
                                                  if CheckContinuation(UTF8ToString(Lexer.yytext), sltHexString) then
                                                    $$.Text := $1.Text + HandleContinuation(UTF8ToString(Lexer.yytext), sltHexString)
                                                  else
                                                    yyerror('String continuation not a hex string'); }
        ;

  character_string_literal :
                introducer character_set_specification
                character_string_literal_main
                                                { yyinfo('Charset specification ignored. Interpreting as UTF-8 string');
                                                  $2.Obj.Free;
                                                  $$ := $3; }
        |       character_string_literal_main
                                                { $$ := $1; }
        ;

  character_string_literal_main :
                string_literal_continuation     { if CheckContinuation(UTF8ToString(Lexer.yytext), sltString) then
                                                  begin
                                                    $$.Text := HandleContinuation(UTF8ToString(Lexer.yyText), sltString);
                                                    $$.Obj := MakeClass(TSqlSynLiteral);
                                                    with $$.Obj as TSqlSynLiteral do
                                                    begin
                                                      Text := $$.Text;
                                                      LitType := sltString;
                                                    end;
                                                  end
                                                  else
                                                    yyerror('Not a valid character string'); }
        |       character_string_literal_main
                string_literal_continuation     { if CheckContinuation(UTF8ToString(Lexer.yytext), sltString) then
                                                  begin
                                                    $$.Text := HandleContinuation(UTF8ToString(Lexer.yyText), sltString);
                                                    //Now do the appending back to front.
                                                    Assert(($1.Obj as TSQLSynLiteral).Text = $1.Text);
                                                    $$.Text := $1.Text + $$.Text;
                                                    $$.Obj := $1.Obj;
                                                    ($$.Obj as TSQLSynLiteral).Text := $$.Text;
                                                  end
                                                  else
                                                    yyerror('Not a valid character string'); }
        ;

  introducer : underscore                       { $$.Obj := nil;
                                                  $$.Text := UTF8ToString(Lexer.yytext); }
        ;

  character_set_specification :
		character_set_name              { $$ := $1; }
        ;

  character_set_name :
                identifier period identifier period SQL_language_identifier
                                                { $$.Text := $1.Text + '.' +
                                                             $3.Text + '.' +
                                                             $5.Text;
                                                  $$.Obj := $5.Obj;
                                                  $1.Obj.Free;
                                                  $3.Obj.Free;
                                                  with $$.Obj as TSQLSynIdent do
                                                    IdentName := $$.Text; }
         |      identifier period SQL_language_identifier
                                                { $$.Text := $1.Text + '.' +
                                                             $3.Text;
                                                  $1.Obj.Free;
                                                  $$.Obj := $3.Obj;
                                                  with $$.Obj as TSQLSynIdent do
                                                    IdentName := $$.Text; }
         |      SQL_language_identifier
                                                { $$ := $1; }
         ;

  schema_name :
                identifier period identifier    { $$.Text := $1.Text + '.' + $3.Text;
                                                  $1.Obj.Free;
                                                  $$.Obj := $3.Obj;
                                                  with $$.Obj as TSQLSynIdent do
                                                    IdentName := $$.Text; }
        |       identifier
                                                { $$ := $1; }
        ;

  identifier :
                introducer character_set_specification actual_identifier
                                                { yyinfo('Charset specification ignored. Interpreting as UTF-8 identifier');
                                                  $2.Obj.Free;
                                                  $$ := $3; }
        |       actual_identifier
                                                { $$ := $1; }
        ;

  actual_identifier :
                regular_identifier              { $$ := $1; }
        |       delimited_identifier            { if CheckDelimIdent(UTF8ToString(Lexer.yytext)) then
                                                  begin
                                                    $$.Text := HandleDelimIdent(UTF8ToString(Lexer.yytext));
                                                    $$.Obj := MakeClass(TSQLSynIdent);
                                                    with $$.Obj as TSQLSynIdent do
                                                      IdentName := $$.Text;
                                                  end
                                                  else
                                                    yyerror('Not a valid delimited identifier.'); }
        ;

  SQL_language_identifier :
                regular_identifier              { $$ := $1; }
        ;

  date_string :
                quote date_value quote          { $$ := $2 }
        ;

  date_value :
                unsigned_integer minus_sign unsigned_integer minus_sign unsigned_integer
                                                { $$.Text := $1.Text + '-' +
                                                             $3.Text + '-' +
                                                             $5.Text;
                                                  $$.Obj := nil;
                                                  $1.Obj.Free;
                                                  $3.Obj.Free;
                                                  $5.Obj.Free; }
        ;

  time_string :
                quote time_value quote          { $$ := $2; }
                quote time_value time_zone_interval quote
                                                { $$.Text := $2.Text + $3.Text;
                                                  $$.Obj := nil; }
        ;

  time_value :
                unsigned_integer colon unsigned_integer colon seconds_value
                                                { $$.Text := $1.Text + ':'
                                                  + $3.Text + ':' + $5.Text;
                                                  $$.Obj := nil;
                                                  $1.Obj.Free;
                                                  $3.Obj.Free;
                                                  $5.Obj.Free; }
        ;

  seconds_value :
                unsigned_integer                { $$ := $1; }
        |       unsigned_integer period unsigned_integer
                                                { $$.Obj := nil;
                                                  $$.Text := $1.Text + '.' + $3.Text;
                                                  $1.Obj.Free;
                                                  $3.Obj.Free; }
        ;

  time_zone_interval : sign unsigned_integer colon unsigned_integer
                                                { $$.Obj := nil;
                                                  $$.Text := $1.Text + $2.Text +
                                                  ':' + $4.Text;
                                                  $2.Obj.Free;
                                                  $4.Obj.Free; }
        ;

  timestamp_string :
                quote date_value space time_value quote
                                                { $$.Obj := nil;
                                                  $$.Text := $2.Text + ' ' + $$.Text }
        |       quote date_value space time_value time_zone_interval quote
                                                { $$.Obj := nil;
                                                  $$.Text := $2.Text + ' ' + $$.Text;
                                                  yyinfo('Time zone ignored in in timestamp string.'); }
        ;

  interval_string :
                quote interval_string_literal quote { $$ := $1; }
        ;

  interval_string_literal :
                unsigned_integer                { $$ := $1;
                                                  $$.Obj.Free;
                                                  $$.Obj := MakeClass(TSQLSynIntervalStringLiteral);
                                                  with $$.Obj as TSQLSynIntervalStringLiteral do
                                                  begin
                                                    LitType := sltIntervalString;
                                                    IntervalStringType := istPlainInt;
                                                    Text := $$.Text;
                                                  end; }
                /* Year month interval */
        |       unsigned_integer minus_sign unsigned_integer
                                                { $$.Text := $1.Text + '-' + $3.Text;
                                                  $$.Obj := MakeClass(TSQLSynIntervalStringLiteral);
                                                  with $$.Obj as TSQLSynIntervalStringLiteral do
                                                  begin
                                                    LitType := sltIntervalString;
                                                    IntervalStringType := istYearMonth;
                                                    Text := $$.Text;
                                                  end;
                                                  $1.Obj.Free;
                                                  $3.Obj.Free; }
                /* Day time intervals */
        |       unsigned_integer space unsigned_integer
                                                { $$.Text := $1.Text + ' ' + $3.Text;
                                                  $$.Obj := MakeClass(TSQLSynIntervalStringLiteral);
                                                  with $$.Obj as TSQLSynIntervalStringLiteral do
                                                  begin
                                                    LitType := sltIntervalString;
                                                    IntervalStringType := istDayTime1;
                                                    Text := $$.Text;
                                                  end;
                                                  $1.Obj.Free;
                                                  $3.Obj.Free; }
        |       unsigned_integer space unsigned_integer colon unsigned_integer
                                                { $$.Text := $1.Text + ' ' + $3.Text
                                                  + ':' + $5.Text;
                                                  $$.Obj := MakeClass(TSQLSynIntervalStringLiteral);
                                                  with $$.Obj as TSQLSynIntervalStringLiteral do
                                                  begin
                                                    LitType := sltIntervalString;
                                                    IntervalStringType := istDayTime2;
                                                    Text := $$.Text;
                                                  end;
                                                  $1.Obj.Free;
                                                  $3.Obj.Free;
                                                  $5.Obj.Free; }
        |       unsigned_integer space unsigned_integer colon unsigned_integer colon seconds_value
                                                { $$.Text := $1.Text + ' ' + $3.Text
                                                  + ':' + $5.Text + ':' + $7.Text;
                                                  $$.Obj := MakeClass(TSQLSynIntervalStringLiteral);
                                                  with $$.Obj as TSQLSynIntervalStringLiteral do
                                                  begin
                                                    LitType := sltIntervalString;
                                                    IntervalStringType := istDayTime3;
                                                    Text := $$.Text;
                                                  end;
                                                  $1.Obj.Free;
                                                  $3.Obj.Free;
                                                  $5.Obj.Free;
                                                  $7.Obj.Free; }
                /* Time intervals */
	|	unsigned_integer period unsigned_integer
                                                { $$.Text := $1.Text + '.' + $3.Text;
                                                  $$.Obj := MakeClass(TSQLSynIntervalStringLiteral);
                                                  with $$.Obj as TSQLSynIntervalStringLiteral do
                                                  begin
                                                    LitType := sltIntervalString;
                                                    IntervalStringType := istTime1;
                                                    Text := $$.Text;
                                                  end;
                                                  $1.Obj.Free;
                                                  $3.Obj.Free; }
        |       unsigned_integer colon seconds_value
                                                { $$.Text := $1.Text + ':' + $3.Text;
                                                  $$.Obj := MakeClass(TSQLSynIntervalStringLiteral);
                                                  with $$.Obj as TSQLSynIntervalStringLiteral do
                                                  begin
                                                    LitType := sltIntervalString;
                                                    IntervalStringType := istTime2;
                                                    Text := $$.Text;
                                                  end;
                                                  $1.Obj.Free;
                                                  $3.Obj.Free; }
        |       unsigned_integer colon unsigned_integer colon seconds_value
                                                { $$.Text := $1.Text + ':' + $3.Text
                                                  + ':' + $5.Text;
                                                  $$.Obj := MakeClass(TSQLSynIntervalStringLiteral);
                                                  with $$.Obj as TSQLSynIntervalStringLiteral do
                                                  begin
                                                    LitType := sltIntervalString;
                                                    IntervalStringType := istTime3;
                                                    Text := $$.Text;
                                                  end;
                                                  $1.Obj.Free;
                                                  $3.Obj.Free;
                                                  $5.Obj.Free; }
        ;

/*
--hr
--h2 SQL Module
--/h2
*/


  module :
		module_name_clause
                language_clause
                module_authorization_clause
                module_contents
                                                {
                                                  $$ := $1; //Module and name
                                                  with $$.Obj as TSqlSynModule do
                                                  begin
                                                    // Language clause
                                                    InsertTailChild($2.Obj);
                                                    Language := $2.Obj as TSQLSynIdent;
                                                    // Module auth clause
                                                      //Schema.
                                                    if Assigned(($3.Obj as TTempTpl).T1) then
                                                    begin
                                                      ($3.Obj as TTempTpl).T1.RemoveFromTree;
                                                      InsertTailChild(($3.Obj as TTempTpl).T1);
                                                      Schema := ($3.Obj as TTempTpl).T1 as TSqlSynIdent;
                                                    end;
                                                      //Authorization
                                                    if Assigned(($3.Obj as TTempTpl).T2) then
                                                    begin
                                                      ($3.Obj as TTempTpl).T2.RemoveFromTree;
                                                      InsertTailChild(($3.Obj as TTempTpl).T2);
                                                      Authorization := ($3.Obj as TTempTpl).T2 as TSqlSynIdent;
                                                    end;
                                                    $3.Obj.Free;
                                                    // Module opt.
                                                    // Flatten tree, and keep a cpl of pointers as to
                                                    // locations of things.

                                                    //Module contents.
                                                    TmpClass := (($4.Obj) as TTempTpl);
                                                    while TmpClass.FirstChild <> nil do
                                                    begin
                                                      TmpClass2 := TmpClass.FirstChild;
                                                      TmpClass2.RemoveFromTree;
                                                      InsertTailChild(TmpClass2);
                                                      if not Assigned(FirstContents) then
                                                        FirstContents := TmpClass2;
                                                    end;
                                                    $4.Obj.Free;
                                                  end;
                                                }
        ;


  module_name_clause :
                _MODULE
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynModule);
                                                  with $$.Obj as TSQLSynNamedStructural do
                                                    StructuralType := sstModule; }
        |       _MODULE  module_name
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynModule);
                                                  with $$.Obj as TSQLSynNamedStructural do
                                                  begin
                                                    StructuralType := sstModule;
                                                    InsertTailChild($2.Obj);
                                                    Name := $2.Obj as TSqlSynIdent;
                                                  end; }
        |       _MODULE  module_character_set_specification
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynModule);
                                                  with $$.Obj as TSQLSynNamedStructural do
                                                    StructuralType := sstModule; }
        |       _MODULE  module_name module_character_set_specification
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynModule);
                                                  with $$.Obj as TSQLSynNamedStructural do
                                                  begin
                                                    StructuralType := sstModule;
                                                    InsertTailChild($2.Obj);
                                                    Name := $2.Obj as TSqlSynIdent;
                                                  end; }
        ;

  module_name :
                identifier
                                                { $$ := $1; }
        ;

  module_character_set_specification : _NAMES _ARE character_set_specification
                                                { yyinfo('Charset specification ignored in module decl.'); }
        ;

  language_clause :
        _LANGUAGE language_name
                                                { $$.Text := $2.Text;
                                                  $$.Obj := MakeClass(TSqlSynIdent);
                                                  with $$.Obj as TSqlSynIdent do
                                                  begin
                                                    IdentName := $$.Text;
                                                    Wildcard := false;
                                                  end; }
        ;

  language_name :
                _ADA
                                                { $$.Text := (Lexer as SQL92GrammarLexer).TokenName(yychar); $$.Obj := nil; }
        |       _C
                                                { $$.Text := (Lexer as SQL92GrammarLexer).TokenName(yychar); $$.Obj := nil; }
        |       _COBOL
                                                { $$.Text := (Lexer as SQL92GrammarLexer).TokenName(yychar); $$.Obj := nil; }
        |       _FORTRAN
                                                { $$.Text := (Lexer as SQL92GrammarLexer).TokenName(yychar); $$.Obj := nil; }
        |       _MUMPS
                                                { $$.Text := (Lexer as SQL92GrammarLexer).TokenName(yychar); $$.Obj := nil; }
        |       _PASCAL
                                                { $$.Text := (Lexer as SQL92GrammarLexer).TokenName(yychar); $$.Obj := nil; }
        |       _PLI
                                                { $$.Text := (Lexer as SQL92GrammarLexer).TokenName(yychar); $$.Obj := nil; }
        ;

  module_authorization_clause :
		_SCHEMA schema_name
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($2.Obj);
                                                    T1 := $2.Obj;
                                                  end; }
	|	_AUTHORIZATION module_authorization_identifier
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($2.Obj);
                                                    T2 := $2.Obj;
                                                  end; }
	|	_SCHEMA schema_name _AUTHORIZATION module_authorization_identifier
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($2.Obj);
                                                    T1 := $2.Obj;
                                                    InsertTailChild($4.Obj);
                                                    T2 := $4.Obj;
                                                  end; }
        ;

  module_authorization_identifier :
                authorization_identifier
                                                { $$ := $1; }
        ;

  authorization_identifier :
                identifier
                                                { $$ := $1; }
        ;


  /* TODO - Continue from here. Make somewhat similar to other
     create table constructs. */

  temporary_table_declaration :
                _DECLARE _LOCAL _TEMPORARY _TABLE
                qualified_local_table_name table_element_list
                temporary_table_declaration_opt
        ;

  temporary_table_declaration_opt :
        /* Empty */
        |       _ON _COMMIT _PRESERVE _ROWS
        |       _ON _COMMIT _DELETE _ROWS
        ;

  qualified_local_table_name : _MODULE period local_table_name ;

  local_table_name : identifier ;

  table_element_list : left_paren table_element table_element_list_opt right_paren ;

  table_element_list_opt :
        /* Empty */
        |       table_element_list_opt comma table_element
        ;

  table_element :
                column_definition
        |       table_constraint_definition
        ;

  column_definition :
        column_name column_definition_sel default_clause_opt
        column_constraint_definition_opt collate_clause_opt;

  column_definition_sel :
                data_type
        |       domain_name
        ;

  default_clause_opt :
        /* Empty */
        |       default_clause
        ;

  column_constraint_definition_opt :
        /* Empty */
        |       column_constraint_definition
        ;

  collate_clause_opt :
        /* Empty */
        |       collate_clause
        ;

  column_name : identifier ;

/*
--hr
--h2 Data Types
--/h2
*/

  data_type :
		character_string_type data_type_opt
                                                { $$ := $1; }
	|	national_character_string_type
                                                { $$ := $1; }
	|	bit_string_type
                                                { $$ := $1; }
	|	numeric_type
                                                { $$ := $1; }
	|	datetime_type
                                                { $$ := $1; }
	|	interval_type
                                                { $$ := $1; }
        ;

  data_type_opt :
        /* Empty */
                                                { $$.Text := '';
                                                  $$.Obj := nil; }
        |       _CHARACTER _SET character_set_specification
                                                { $$.Text := '';
                                                  $$.Obj := nil;
                                                  yyinfo('Character set specification ignored in type decl. UTF-8 strings please.');
                                                  $3.Obj.Free; }
        ;

  character_string_type :
		_CHARACTER character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtString; }
	|	_CHAR character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtString; }
	|	_CHARACTER _VARYING character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtString; }
	|	_CHAR _VARYING character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtString; }
	|	_VARCHAR character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtString; }
	|	_CHARACTER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtString;
                                                  yyinfo('Representing single chars as strings.');}
	|	_CHAR
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtString;
                                                  yyinfo('Representing single chars as strings.');}
	|	_CHARACTER _VARYING
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtString; }
	|	_CHAR _VARYING
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtString; }
	|	_VARCHAR
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtString; }
        ;

  character_string_type_len :
                left_paren length right_paren
                                                { yyinfo('Character string type length declaration ignored.');
                                                  $$.Text := ''; $$.Obj := nil;
                                                  $2.Obj.Free; }
        ;

  length : unsigned_integer
                                                { $$ := $1; }
        ;

  national_character_string_type :
		_NATIONAL _CHARACTER character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtNatString; }
	|	_NATIONAL _CHAR character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtNatString; }
	|	_NCHAR character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtNatString; }
	|	_NATIONAL _CHARACTER _VARYING character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtNatString; }
	|	_NATIONAL _CHAR _VARYING character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtNatString; }
	|	_NCHAR _VARYING character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtNatString; }
	|	_NATIONAL _CHARACTER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtNatString;
                                                  yyinfo('Representing single chars as strings.');}
	|	_NATIONAL _CHAR
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtNatString;
                                                  yyinfo('Representing single chars as strings.');}
	|	_NCHAR
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtNatString;
                                                  yyinfo('Representing single chars as strings.');}
	|	_NATIONAL _CHARACTER _VARYING
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtNatString; }
	|	_NATIONAL _CHAR _VARYING
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtNatString; }
	|	_NCHAR _VARYING
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtNatString; }
        ;

  bit_string_type :
		_BIT character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtBitString; }
	|	_BIT _VARYING character_string_type_len
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtBitString; }
	|	_BIT
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtBitString;
                                                  yyinfo('Representing single bits as bit strings.');}
	|	_BIT _VARYING
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtBitString; }
        ;

  numeric_type :
		exact_numeric_type              { $$ := $1; }
	|	approximate_numeric_type        { $$ := $1; }
        ;

  exact_numeric_type :
	 	_NUMERIC numeric_precision_scale_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtExactNumeric; }
	| 	_DECIMAL numeric_precision_scale_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtExactNumeric; }
	| 	_DEC numeric_precision_scale_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtExactNumeric; }
	|	_INTEGER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtExactNumeric; }
	|	_INT
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtExactNumeric; }
	|	_SMALLINT
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtExactNumeric; }
        |       _BIGINT
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtExactNumeric; }
        ;

  numeric_precision_scale_opt :
        /* Empty */
        |       left_paren precision comma scale right_paren
                                                { yyerror('Integer scaling not supported at the moment. E-mail the author.'); }
        |       left_paren precision right_paren
                                                { yyinfo('Integer precision ignored in type.');
                                                  $1.Obj.Free; }
        ;

  precision : unsigned_integer
                                                { $$ := $1; }
        ;

  scale : unsigned_integer
                                                { $$ := $1; }
        ;

  approximate_numeric_type :
	 	_FLOAT
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtApproxNumeric; }
        |       _FLOAT left_paren precision right_paren
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtApproxNumeric;
                                                  yyinfo('Floating point precision ignored in type.');
                                                  $3.Obj.Free; }
	|	_REAL
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtApproxNumeric; }
	|	_DOUBLE _PRECISION
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtApproxNumeric; }
        ;

  datetime_type :
		_DATE
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtDate; }
	|       _TIME time_precision_opt tz_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtTime; }
	|       _TIMESTAMP timestamp_precision_opt tz_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynType);
                                                  with $$.Obj as TSQLSynType do
                                                    GeneralType := sgtTimestamp; }
        ;

  timestamp_precision_opt :
        /* Empty */
        |       left_paren timestamp_precision right_paren
                                                { yyinfo('Timestamp precision ignored.');
                                                  $$.Text := ''; $$.obj := nil;
                                                  $2.Obj.Free; }
        ;

  time_precision_opt :
        /* Empty */
        |       left_paren time_precision right_paren
                                                { yyinfo('Time precision ignored.');
                                                  $$.Text := ''; $$.obj := nil;
                                                  $2.Obj.Free; }
        ;

  tz_opt :
        /* Empty */
        |        _WITH _TIME _ZONE
                                                { yyerror('Time zones not supported.');}
        ;


  time_precision : time_fractional_seconds_precision
                                                { $$ := $1; }
        ;

  time_fractional_seconds_precision : unsigned_integer
                                                { $$ := $1; }
        ;

  timestamp_precision : time_fractional_seconds_precision
                                                { $$ := $1; }
        ;

  interval_type : _INTERVAL interval_qualifier
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynIntervalType);
                                                  with $$.Obj as TSQLSynIntervalType do
                                                  begin
                                                    InsertTailChild($2.Obj);
                                                    Qualifier := $2.Obj as TSQLSynIntervalQualifier;
                                                  end; }
        ;

  interval_qualifier :
                start_field
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynIntervalQualifier);
                                                  with $$.Obj as TSQlSynIntervalQualifier do
                                                  begin
                                                    Start := TSQLSynQualField($1.Obj);
                                                    _End := TSQLSynQualField($1.Obj);
                                                  end; }
	|	start_field _TO end_field
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynIntervalQualifier);
                                                  with $$.Obj as TSQlSynIntervalQualifier do
                                                  begin
                                                    Start := TSQLSynQualField($1.Obj);
                                                    _End := TSQLSynQualField($3.Obj);
                                                  end; }
        |       _SECOND single_datetime_field_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynIntervalQualifier);
                                                  with $$.Obj as TSQlSynIntervalQualifier do
                                                  begin
                                                    Start := sqfSecond;
                                                    _End := sqfSecond;
                                                  end; }
        ;

  start_field :
		non_second_datetime_field
                                                { $$ := $1; }
        |       non_second_datetime_field left_paren precision right_paren
                                                { $$ := $1;
                                                  yyinfo('Datetime field precision ignored.');
                                                  $2.Obj.Free; }
        ;

  non_second_datetime_field :
        _YEAR
                                                { $$.Text := ''; $$.Obj := TSQLSynNode(sqfYear); }
        | _MONTH
                                                { $$.Text := ''; $$.Obj := TSQLSynNode(sqfMonth); }
        | _DAY
                                                { $$.Text := ''; $$.Obj := TSQLSynNode(sqfDay); }
        | _HOUR
                                                { $$.Text := ''; $$.Obj := TSQLSynNode(sqfHour); }
        | _MINUTE
                                                { $$.Text := ''; $$.Obj := TSQLSynNode(sqfMinute); }
        ;

  interval_leading_field_precision : unsigned_integer
                                                { $$ := $1; }
        ;

  end_field :
		non_second_datetime_field
                                                { $$ := $1; }
	|       _SECOND
                                                { $$.Text := ''; $$.Obj := TSqlSynNode(sqfSecond); }
        |       _SECOND left_paren precision right_paren
                                                { $$.Text := ''; $$.Obj := TSqlSYnNode(sqfSecond);
                                                  yyinfo('Datetime field precision ignored.');
                                                  $3.Obj.Free; }
        ;

  interval_fractional_seconds_precision : unsigned_integer
                                                { $$ := $1; }
        ;

  single_datetime_field_opt :
        /* Empty */
                                                { $$.Text := ''; $$.obj := nil; }
        |       left_paren interval_leading_field_precision single_datetime_field_opt2 right_paren
                                                { $$.Text := ''; $$.obj := nil;
                                                  yyinfo('Leading field precision ignored.');
                                                  $2.Obj.Free; }
        ;

  single_datetime_field_opt2 :
        /* Empty */
                                                { $$.Text := ''; $$.obj := nil; }
        |       comma interval_fractional_seconds_precision
                                                { $$.Text := ''; $$.obj := nil;
                                                  yyinfo('Fractional seconds precision ignored.');
                                                  $2.Obj.Free; }
        ;


  domain_name :
                qualified_name
                                                { $$ := $1; }
        ;

  qualified_name :
                identifier
                                                { $$ := $1; }
        |       identifier period identifier
                                                { $$ := $1;
                                                  $$.Text := $$.Text + '.' + $3.Text;
                                                  ($$.Obj as TSqlSynIdent).IdentName := $$.Text;
                                                  $3.Obj.Free; }
        |       identifier period identifier period identifier
                                                { $$ := $1;
                                                  $$.Text := $$.Text + '.' + $3.Text + '.' + $5.Text;
                                                  ($$.Obj as TSqlSynIdent).IdentName := $$.Text;
                                                  $3.Obj.Free;
                                                  $5.Obj.Free; }
        ;

  qualified_name_trail_asterisk :
                identifier period asterisk
                                                { $$ := $1;
                                                  $$.Text := $1.Text + '.*';
                                                  with $$.Obj as TSqlsynIdent do
                                                  begin
                                                    IdentName := $$.Text;
                                                    Wildcard := True;
                                                  end; }
        |       identifier period identifier period asterisk
                                                { $$ := $1;
                                                  $$.Text := $1.Text +
                                                  '.' + $3.Text + '.*';
                                                  with $$.Obj as TSqlsynIdent do
                                                  begin
                                                    IdentName := $$.Text;
                                                    Wildcard := True;
                                                  end;
                                                  $3.Obj.Free; }
        |       identifier period identifier period identifier period asterisk
                                                { $$ := $1;
                                                  $$.Text := $1.Text +
                                                  '.' + $3.Text +
                                                  '.' + $5.Text + '.*';
                                                  with $$.Obj as TSqlsynIdent do
                                                  begin
                                                    IdentName := $$.Text;
                                                    Wildcard := True;
                                                  end;
                                                  $3.Obj.Free;
                                                  $5.Obj.Free; }
        ;

  default_clause :
                _DEFAULT default_option
                                                { $$ := $1; }
        ;

  default_option :
		literal
                                                { $$ := $1; }
	|	datetime_value_function
                                                { $$ := $1; }
	|	_USER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                    BuiltInType := sftUser; }
	|	_CURRENT_USER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                    BuiltInType := sftCurrentUser; }
	|	_SESSION_USER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                    BuiltInType := sftSessionUser; }
	|	_SYSTEM_USER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                    BuiltInType := sftSystemUser; }
	|	_NULL
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                    BuiltInType := sftNull; }
        ;

/*
--hr
--h2 Literals
--/h2
*/

  literal :
                signed_numeric_literal          { $$ := $1; }
        |       general_literal                 { $$ := $1; }
        ;

  signed_numeric_literal :
                sign unsigned_numeric_literal
                                                { $$ := $2;
                                                  Assert($2.Text = ($2.Obj as TSQLSynLiteral).Text);
                                                  $$.Text := $1.Text + $2.Text;
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    Text := $$.Text;
                                                    if $1.Text <> '+' then
                                                    begin
                                                      case LitType of
                                                        sltUnsInt: LitType := sltInt;
                                                        sltInt: (* No change *);
                                                        sltExactNumeric: LitType := sltSignedExactNumeric;
                                                        sltApproxNumeric: LitType := sltSignedApproxNumeric;
                                                      else
                                                        Assert(false);
                                                      end;
                                                    end;
                                                  end; }
        |       unsigned_numeric_literal
                                                { $$ := $1; }
        ;

  general_literal :
		character_string_literal        { $$ := $1; }
	|	national_character_string_literal
                                                { $$ := $1; }
	|	bit_string_literal
                                                { $$ := $1; }
	|	hex_string_literal
                                                { $$ := $1; }
	|	datetime_literal
                                                { $$ := $1; }
	|	interval_literal
                                                { $$ := $1; }
        ;

  datetime_literal :
		date_literal
                                                { $$ := $1; }
	|	time_literal
                                                { $$ := $1; }
	|	timestamp_literal
                                                { $$ := $1; }
        ;

  date_literal : _DATE date_string
                                                { $$.Text := $1.Text;
                                                  Assert(not Assigned($1.Obj));
                                                  $$.Obj := MakeClass(TSQlSynLiteral);
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    LitType := sltDate;
                                                    Text := $$.Text;
                                                  end; }
        ;

  time_literal : _TIME time_string
                                                { $$.Text := $1.Text;
                                                  Assert(not Assigned($1.Obj));
                                                  $$.Obj := MakeClass(TSQlSynLiteral);
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    LitType := sltTime;
                                                    Text := $$.Text;
                                                  end; }
        ;

  timestamp_literal : _TIMESTAMP timestamp_string
                                                { $$.Text := $1.Text;
                                                  Assert(not Assigned($1.Obj));
                                                  $$.Obj := MakeClass(TSQlSynLiteral);
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    LitType := sltTimestamp;
                                                    Text := $$.Text;
                                                  end; }
        ;

  interval_literal :
                _INTERVAL interval_string interval_qualifier
                                                { $$.Text := $2.Text + ' ' + $3.Text;
                                                  $$.Obj := MakeClass(TSQLSynIntervalLiteral);
                                                  with $$.Obj as TSQLSynIntervalLiteral do
                                                  begin
                                                    LitType := sltInterval;
                                                    Text := $$.Text;
                                                    Assert(Assigned($2.Obj));
                                                    Assert(Assigned($3.Obj));
                                                    InsertTailChild($2.Obj);
                                                    InsertTailChild($3.Obj);
                                                    Interval := $2.Obj as TSQLSynIntervalStringLiteral;
                                                    Qualifier := $3.Obj as TSQLSynIntervalQualifier;
                                                  end; }
        |       _INTERVAL sign  interval_string interval_qualifier
                                                { $$.Text := $3.Text + ' ' + $4.Text;
                                                  $$.Obj := MakeClass(TSQLSynIntervalLiteral);
                                                  with $$.Obj as TSQLSynIntervalLiteral do
                                                  begin
                                                    LitType := sltInterval;
                                                    Text := $$.Text;
                                                    if $2.Text <> '+' then
                                                      Negated := True;
                                                    Assert(Assigned($3.Obj));
                                                    Assert(Assigned($4.Obj));
                                                    InsertTailChild($3.Obj);
                                                    InsertTailChild($4.Obj);
                                                    Interval := $3.Obj as TSQLSynIntervalStringLiteral;
                                                    Qualifier := $4.Obj as TSQLSynIntervalQualifier;
                                                  end; }
        ;

  datetime_value_function :
		current_date_value_function     { $$ := $1; }
	|	current_time_value_function     { $$ := $1; }
	|	current_timestamp_value_function
                                                { $$ := $1; }
        ;

  current_date_value_function : _CURRENT_DATE
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltin do
                                                    BuiltInType := sftCurrentDate; }
        ;

  current_time_value_function :
                _CURRENT_TIME
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltin do
                                                    BuiltInType := sftCurrentTime; }
        |       _CURRENT_TIME left_paren time_precision right_paren
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltin do
                                                    BuiltInType := sftCurrentTime;
                                                  $3.Obj.Free;
                                                  yyinfo('Time precision ignored in current time function.'); }
        ;

  current_timestamp_value_function :
                _CURRENT_TIMESTAMP
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltin do
                                                    BuiltInType := sftCurrentTimestamp; }
        |       _CURRENT_TIMESTAMP left_paren timestamp_precision right_paren
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltin do
                                                    BuiltInType := sftCurrentTimestamp;
                                                  $3.Obj.Free;
                                                  yyinfo('Timestamp precision ignored in current timestamp function.'); }
        ;

/*
--hr
--h2 Constraints
--/h2
*/

  column_constraint_definition :
		constraint_name_definition_opt column_constraint constraint_attributes_opt ;

  constraint_name_definition :
               _CONSTRAINT constraint_name
        ;

  constraint_name_definition_opt :
        /* Empty */
        |       constraint_name_definition
        ;

  constraint_name : qualified_name ;

  column_constraint :
		_NOT _NULL
	|	unique_specification
	|	references_specification
	|	check_constraint_definition
        ;

  unique_specification :
                _UNIQUE
        |       _PRIMARY _KEY
        ;

  references_specification :
		_REFERENCES referenced_table_and_columns
                match_type_opt
                referential_triggered_action_opt ;

  match_type_opt :
        /* Empty */
        |       _MATCH match_type
        ;

  referential_triggered_action_opt :
        /* Empty */
        |       referential_triggered_action
        ;

  referenced_table_and_columns :
                table_name reference_column_list_opt
        ;

  reference_column_list_opt :
        /* Empty */
        |       left_paren reference_column_list right_paren
        ;

  table_name :
                qualified_name
        |       qualified_local_table_name
        ;

  reference_column_list :
                column_name_list
        ;

  column_name_list :
                column_name
        |       column_name_list comma column_name
        ;

  match_type : _FULL | _PARTIAL ;

  referential_triggered_action :
		update_rule delete_rule_opt
	|	delete_rule update_rule_opt
        ;

  update_rule_opt :
        /* Empty */
        |       update_rule
        ;

  delete_rule_opt :
        /* Empty */
        |       delete_rule
        ;

  update_rule :
                _ON _UPDATE referential_action
        ;

  referential_action :
                _CASCADE | _SET _NULL | _SET _DEFAULT | _NO _ACTION
        ;

  delete_rule :
                _ON _DELETE referential_action
        ;

  check_constraint_definition :
        _CHECK left_paren search_condition right_paren
        ;

/*
--hr
--h2 Search Condition
--/h2
*/

  search_condition :
                boolean_term
	|       search_condition _OR boolean_term
        ;

  boolean_term :
		boolean_factor
	|       boolean_term _AND boolean_factor
        ;

  boolean_factor :
                boolean_test
        |       _NOT boolean_test
        ;

  boolean_test :
                boolean_primary
        |       boolean_primary _IS truth_value
        |       boolean_primary _IS _NOT truth_value
        ;

  boolean_primary :
                predicate
        |       left_paren search_condition right_paren
        ;

  predicate :
	    comparison_predicate
	|   between_predicate
	|   in_predicate
	|   like_predicate
	|   null_predicate
	|   quantified_comparison_predicate
	|   exists_predicate
        |   unique_predicate
	|   match_predicate
	|   overlaps_predicate ;

  comparison_predicate : row_value_constructor comp_op row_value_constructor
        ;

  row_value_constructor :
                expression
        |       left_paren row_value_constructor_list right_paren
        ;

  primary_expression :
                unsigned_value_specification
    |           column_reference
    |           set_function_specification
    |           scalar_subquery
    |           case_expression
    |           cast_specification
    |           numeric_value_function
    |           string_value_function
    |           datetime_value_function
    |           null_specification
    |           default_specification
    |           left_paren expression right_paren
    ;

  postfix_expression:
                primary_expression
      |         primary_expression postfix_op
    ;

  postfix_op :
                time_zone
        |       interval_qualifier
        |       collate_clause
        ;

  unary_expression :
        plus_sign postfix_expression
    |   minus_sign postfix_expression
    |   postfix_expression
    ;

  multiplicative_expression :
      unary_expression
    | multiplicative_expression asterisk unary_expression
    | multiplicative_expression solidus unary_expression
    ;

  /* numeric or string, or times or intervals, resolved later */
  expression :
      multiplicative_expression
    | expression plus_sign multiplicative_expression
    | expression minus_sign multiplicative_expression
    | expression concatenation_operator multiplicative_expression
    ;

  string_value_function :
      character_value_function
;

  scalar_subquery :
        left_paren subquery right_paren
        ;

  subquery :
        query_expression
        ;

  unsigned_value_specification :
                unsigned_literal
        |       general_value_specification
        ;

  unsigned_literal :
                unsigned_numeric_literal
        |       general_literal ;

  general_value_specification :
	    parameter_specification
	|   _USER
	|   _CURRENT_USER
	|   _SESSION_USER
	|   _SYSTEM_USER
	|   _VALUE
        ;

  parameter_specification :
                parameter_name indicator_parameter_opt
        ;

  parameter_name : colon identifier ;

  indicator_parameter_opt :
        /* Empty */
        |       _INDICATOR parameter_name
        |       parameter_name
        ;

  column_reference :
                qualified_name
/*
                qualifier period column_name
        |       column_name
*/
        ;

/*
  qualifier :
                table_name
        |       correlation_name
        ;
*/

  correlation_name : identifier ;

  /* TODO - I have generalised the args, need to check
     for the specific set function */

  set_function_specification :
	        general_set_function
        ;

  general_set_function :
		set_function_type left_paren set_quantifier_args right_paren
        ;

  set_quantifier_args :
        /* Empty */
        |       asterisk
        |       set_quantifier expression
        ;

  set_function_type : _AVG | _MAX | _MIN | _SUM | _COUNT ;

  set_quantifier : _DISTINCT | _ALL;

  set_quantifier_opt :
        /* Empty */
        |       set_quantifier
        ;

/*
--hr
--h2 Queries
--/h2
*/

  query_expression :
		non_join_query_term
	|	query_expression _UNION all_opt corresponding_spec_opt query_term
	|	query_expression _EXCEPT all_opt corresponding_spec_opt query_term
        ;

  non_join_query_term :
		non_join_query_primary
	|	query_term _INTERSECT all_opt corresponding_spec_opt query_primary
        ;

  all_opt :
        /* Empty */
        |       _ALL
        ;

  corresponding_spec_opt :
        /* Empty */
        |       corresponding_spec
        ;

  non_join_query_primary :
                simple_table
        |       table_subquery
        ;

  simple_table :
		query_specification
	|	table_value_constructor
	|	explicit_table
        ;

  query_specification :
		_SELECT set_quantifier_opt select_list table_expression ;

  select_list :
		asterisk
	|	select_list_opt ;

  select_list_opt :
                select_sublist
        |       select_list_opt comma select_sublist
        ;

  select_sublist :
                derived_column
        |       qualified_name_trail_asterisk
        ;

  derived_column :
                expression
        |       expression as_clause
        ;

  as_clause :
                column_name
        |       _AS column_name
        ;


  table_expression :
                from_clause
		where_clause_opt
		group_by_clause_opt
		having_clause_opt
        ;

  where_clause_opt :
        /* Empty */
        |       where_clause
        ;

  group_by_clause_opt :
        /* Empty */
        |       group_by_clause
        ;

  having_clause_opt :
        /* Empty */
        |       having_clause
        ;

  from_clause : _FROM from_clause_list
        ;

  from_clause_list :
                table_reference
        |       from_clause_list comma table_reference
        ;

/*
--small
--i
Note that <correlation specification> does not appear in the ISO/IEC grammar;
The notation is written out longhand several times, instead;
--/i
--/small
*/

  table_reference :
                joined_table
        |       table_factor
        ;

  table_factor :
                table_name
        |       table_name correlation_specification
        |       derived_table correlation_specification
        |       table_name _AS correlation_specification
        |       derived_table _AS correlation_specification
        ;

  correlation_specification :
		correlation_name derived_column_list_opt
        ;

  as_opt :
        /* Empty */
        | _AS
        ;

  derived_column_list_opt :
        /* Empty */
        |       left_paren derived_column_list right_paren
        ;

  derived_column_list :
                column_name_list
        ;

  derived_table :
                table_subquery
        ;

  table_subquery :
                left_paren query_expression right_paren
        ;

  joined_table :
		cross_join
	|       qualified_join
	|       left_paren joined_table right_paren
        ;

  cross_join :
        table_reference _CROSS _JOIN table_factor
        ;

  qualified_join :
                table_reference _JOIN table_factor join_specification
        |       table_reference _INNER _JOIN table_factor join_specification
        |       table_reference _LEFT outer_opt _JOIN table_factor join_specification
        |       table_reference _RIGHT outer_opt _JOIN table_factor join_specification
        |       table_reference _FULL outer_opt _JOIN table_factor join_specification
        |       table_reference _NATURAL _JOIN table_factor
        |       table_reference _NATURAL _INNER _JOIN table_factor
        |       table_reference _NATURAL _LEFT outer_opt _JOIN table_factor
        |       table_reference _NATURAL _RIGHT outer_opt _JOIN table_factor
        |       table_reference _NATURAL _FULL outer_opt _JOIN table_factor
        |       table_reference _NATURAL _UNION _JOIN table_factor
        ;

  outer_opt:
        /* Empty */
        |       _OUTER
        ;

  join_specification :
                join_condition
        |       named_columns_join
        ;

  join_condition :
                _ON search_condition
        ;

  named_columns_join :
                _USING left_paren join_column_list right_paren
        ;

  join_column_list :
                column_name_list
        ;

  where_clause :
                _WHERE search_condition
        ;

  group_by_clause :
                _GROUP _BY grouping_column_reference_list
        ;

  grouping_column_reference_list :
                grouping_column_reference
        |       grouping_column_reference_list comma grouping_column_reference
        ;

  grouping_column_reference :
                column_reference collate_clause_opt
        ;

  collate_clause :
                _COLLATE collation_name
        ;

  collation_name :
                qualified_name
        ;

  having_clause :
                _HAVING search_condition
        ;

  table_value_constructor :
                _VALUES table_value_constructor_list
        ;

  table_value_constructor_list :
                row_value_constructor
        |       table_value_constructor_list  comma row_value_constructor
        ;

  explicit_table : _TABLE table_name
        ;

  query_term :
                non_join_query_term
        ;

  corresponding_spec : _CORRESPONDING corresponding_column_list_opt
        ;

  corresponding_column_list_opt :
        /* Empty */
        |       _BY left_paren corresponding_column_list right_paren
        ;

  corresponding_column_list : column_name_list ;

  query_primary :
                non_join_query_primary
        ;

/*
--hr
--h2 Query expression components
--/h2
*/

  case_expression :
                case_abbreviation
        |       case_specification
        ;

  case_abbreviation :
		_NULLIF left_paren expression comma expression right_paren
	|	_COALESCE left_paren expression_list right_paren
        ;

  expression_list :
                expression
        |       expression_list comma expression
        ;

  case_specification :
                simple_case
        |       searched_case
        ;

  simple_case :
		_CASE case_operand
			simple_when_clause
                        else_clause_opt
		_END
        ;

  else_clause_opt :
        /* Empty */
        |       else_clause
        ;

  case_operand :
                expression
        ;

  simple_when_clause :
                _WHEN when_operand _THEN result
        ;

  when_operand :
                expression
        ;

  result :
                expression /* Which can be NULL specification */
        ;

  else_clause :
                _ELSE result
        ;

  searched_case :
		_CASE
                searched_when_clause
                else_clause_opt
		_END
        ;

  searched_when_clause :
                _WHEN search_condition _THEN result
        ;

  cast_specification :
                _CAST left_paren cast_operand _AS cast_target right_paren
        ;

  cast_operand :
                expression      /* Which can be null specification */
        ;

  cast_target :
                domain_name
        |       data_type
        ;

  numeric_value_function :
                position_expression
        |       extract_expression
        |       length_expression
        ;

  position_expression :
	        _POSITION left_paren
                expression /* TODO - check char-ness / bit-ness */
                _IN
                expression right_paren
        ;


  character_value_function :
	    character_bit_substring_function
	|   fold
	|   form_of_use_conversion
	|   character_translation
	|   trim_function
        ;

  character_bit_substring_function :
		_SUBSTRING left_paren expression
                _FROM start_position for_strlength_opt right_paren
        ;

  for_strlength_opt :
        /* Empty */
        |       _FOR string_length
        ;

  start_position : expression ;

  string_length : expression ;

  fold :
                _UPPER left_paren expression right_paren
        |       _LOWER left_paren expression right_paren
        ;

  form_of_use_conversion :
		_CONVERT left_paren expression
                _USING form_of_use_conversion_name right_paren
        ;

  form_of_use_conversion_name : qualified_name ;

  character_translation :
		_TRANSLATE left_paren expression _USING translation_name right_paren ;

  translation_name : qualified_name ;

  trim_function :
                _TRIM left_paren trim_operands right_paren
        ;

  trim_operands :
                trim_source
        |       trim_specification _FROM trim_source
        |       trim_character _FROM trim_source
        |       trim_specification trim_character _FROM trim_source
  ;

  trim_specification :
                _LEADING
        |       _TRAILING
        |       _BOTH
  ;

  trim_character :
                expression /* Check char/bit */
        ;

  trim_source :
                expression /* Check char/bit */
        ;

  extract_expression :
                _EXTRACT left_paren extract_field _FROM extract_source right_paren
        ;

  extract_field :
                datetime_field
        |       time_zone_field
        ;

  datetime_field :
                non_second_datetime_field
        |       _SECOND
        ;

  time_zone_field :
                _TIMEZONE_HOUR
        |       _TIMEZONE_MINUTE
        ;

  extract_source :
                expression
        ;

  time_zone :
                _AT time_zone_specifier
        ;

  time_zone_specifier :
                _LOCAL
        |       _TIME _ZONE expression
        ;

  length_expression :
                char_length_expression
        |       octet_length_expression
        |       bit_length_expression
        ;

  char_length_expression :
                char_length_specifier left_paren expression right_paren
        ;

  char_length_specifier :
                _CHAR_LENGTH
        |       _CHARACTER_LENGTH
        ;

  octet_length_expression :
                _OCTET_LENGTH left_paren expression right_paren
        ;

  bit_length_expression :
                _BIT_LENGTH left_paren expression right_paren
        ;

  null_specification : _NULL ;

  default_specification : _DEFAULT ;

  row_value_constructor_list :
                expression
        |       row_value_constructor_list comma expression
        ;

  comp_op :
	    equals_operator
	|   not_equals_operator
	|   less_than_operator
	|   greater_than_operator
	|   less_than_or_equals_operator
	|   greater_than_or_equals_operator ;

  between_predicate :
		row_value_constructor _BETWEEN row_value_constructor _AND row_value_constructor
	|	row_value_constructor _NOT _BETWEEN row_value_constructor _AND row_value_constructor
        ;

  in_predicate :
                row_value_constructor _IN in_predicate_value
        |        row_value_constructor _NOT _IN in_predicate_value
        ;

  in_predicate_value : table_subquery | left_paren in_value_list right_paren ;

  in_value_list :
                expression
        |       in_value_list comma expression
        ;

  like_predicate : /* Check char-bit expressions */
                expression _LIKE pattern like_predicate_escape_opt
        |       expression _NOT _LIKE pattern like_predicate_escape_opt
        ;

  like_predicate_escape_opt :
        /* Empty */
        |       _ESCAPE escape_character
        ;

  pattern : expression /* Check char-bit */;

  escape_character : expression /* Check char-bit */;

  null_predicate :
                row_value_constructor _IS _NULL
        |       row_value_constructor _IS _NOT _NULL
        ;

  quantified_comparison_predicate : row_value_constructor comp_op quantifier table_subquery ;

  quantifier : all | some ;

  all : _ALL ;

  some : _SOME | _ANY ;

  exists_predicate : _EXISTS table_subquery ;

  unique_predicate : _UNIQUE table_subquery ;

  match_predicate : row_value_constructor _MATCH unique_opt partial_full_opt table_subquery ;

  unique_opt :
        /* Empty */
        |       _UNIQUE
        ;

  partial_full_opt:
        /* Empty */
        |       _PARTIAL
        |       _FULL
        ;

  overlaps_predicate : row_value_constructor_1 _OVERLAPS row_value_constructor_2 ;

  row_value_constructor_1 : row_value_constructor ;

  row_value_constructor_2 : row_value_constructor ;

  truth_value :
                _TRUE
        |       _FALSE
        |       _UNKNOWN
        ;

/*
--hr
--h2 More about constraints
--/h2
*/

  constraint_attributes_opt :
        /* Empty */
        | constraint_attributes
        ;

  constraint_attributes :
		constraint_check_time deferrable_opt
	|	_DEFERRABLE constraint_check_time_opt
        ;

  deferrable_opt :
        /* Empty */
        |       _DEFERRABLE
        |       _NOT _DEFERRABLE
        ;

  constraint_check_time_opt :
        /* Empty */
        |       constraint_check_time
        ;

  constraint_check_time :
                _INITIALLY _DEFERRED
        |       _INITIALLY _IMMEDIATE
        ;

  table_constraint_definition :
                constraint_name_definition_opt
                table_constraint constraint_check_time_opt
        ;

  table_constraint :
		unique_constraint_definition
	|	referential_constraint_definition
	|	check_constraint_definition ;

  unique_constraint_definition :
                unique_specification left_paren unique_column_list right_paren
        ;

  unique_column_list :
                column_name_list
        ;

  referential_constraint_definition :
		_FOREIGN _KEY left_paren referencing_columns right_paren references_specification
        ;

  referencing_columns :
                        reference_column_list
        ;

/*
--hr
--h2 Module contents
--/h2
*/

  module_contents :
        /* Empty */                             { $$.Obj := MakeClass(TTempTpl); $$.Text := ''; }
        |       module_contents module_content
                                                { $$ := $1;
                                                  $$.Obj.InsertTailChild($2.Obj); }
        ;

  module_content :
		declare_cursor
                                                { $$ := $1; }
	|	procedure
                                                { $$ := $1; }
        |       temporary_table_declaration
                                                { $$ := $1; }
        ;

  declare_cursor :
		_DECLARE cursor_name insensitive_opt scroll_opt _CURSOR _FOR cursor_specification ;

  insensitive_opt :
        /* Empty */
        |       _INSENSITIVE
        ;

  scroll_opt :
        /* Empty */
        |       _SCROLL
        ;

  cursor_name : identifier ;

  cursor_specification : query_expression order_by_clause_opt updatability_clause_opt ;

  order_by_clause_opt :
        /* Empty */
        |       _ORDER _BY sort_specification_list
        ;

  sort_specification_list :
                sort_specification
        |       sort_specification_list comma sort_specification
        ;

  sort_specification : sort_key collate_clause_opt ordering_specification_opt ;

  sort_key : column_name | unsigned_integer ;

  ordering_specification_opt :
        /* Empty */
        |       _ASC
        |       _DESC
        ;

  updatability_clause_opt :
        /* Empty */
        |       _FOR _READ _ONLY
        |       _FOR _UPDATE updatability_column_opt
        ;

  updatability_column_opt:
        /* Empty */
        |       _OF column_name_list
        ;

/*
--hr
--h2 SQL Procedures
--/h2
*/

  procedure :
	_PROCEDURE procedure_name parameter_declaration_list
        semicolon SQL_procedure_statement semicolon ;

  procedure_name : identifier ;

  parameter_declaration_list :
		left_paren parameter_declarations right_paren
        ;

  parameter_declarations :
                parameter_declaration
        |       parameter_declarations comma parameter_declaration
        ;

  parameter_declaration :
                parameter_name data_type
        |       status_parameter
        ;

  status_parameter :
                _SQLCODE
        |       _SQLSTATE
        ;

  SQL_procedure_statement :
		SQL_schema_statement
	|	SQL_data_statement
	|	SQL_transaction_statement
	|	SQL_connection_statement
	|	SQL_session_statement
        ;

/*
--hr
--h2 SQL Schema Definition Statements
--/h2
*/

  SQL_schema_statement :
		SQL_schema_definition_statement
	|	SQL_schema_manipulation_statement
        ;

  SQL_schema_definition_statement :
		schema_definition
	|	table_definition
	|	view_definition
	|	grant_statement
	|	domain_definition
	|	character_set_definition
	|	collation_definition
	|	translation_definition
	|	assertion_definition ;

  schema_definition :
		_CREATE _SCHEMA schema_name_clause
			schema_character_set_specification_opt
			schema_elements ;

  schema_character_set_specification_opt :
        /* Empty */
        |       schema_character_set_specification
        ;

  schema_elements :
                schema_element
        |       schema_elements schema_element
        ;

  schema_name_clause :
		schema_name
	|	_AUTHORIZATION schema_authorization_identifier
	|	schema_name _AUTHORIZATION schema_authorization_identifier
        ;

  schema_authorization_identifier : authorization_identifier ;

  schema_character_set_specification : _DEFAULT _CHARACTER _SET character_set_specification ;

  schema_element :
		domain_definition
	|	table_definition
	|	view_definition
	|	grant_statement
	|	assertion_definition
	|	character_set_definition
	|	collation_definition
	|	translation_definition ;

  domain_definition :
		_CREATE _DOMAIN domain_name as_opt data_type
			default_clause_opt domain_constraint_opt collate_clause_opt ;

  domain_constraint_opt :
        /* Empty */
        |       domain_constraint
        ;

  domain_constraint :
		constraint_name_definition_opt check_constraint_definition constraint_attributes_opt
        ;

  table_definition :
		_CREATE table_definition_opts _TABLE
                table_name table_element_list table_commit_opts ;

  table_definition_opts :
        /* Empty */
        |       _GLOBAL _TEMPORARY
        |       _LOCAL _TEMPORARY
        ;

  table_commit_opts :
        /* Empty */
        |       _ON _COMMIT _DELETE _ROWS
        |       _ON _COMMIT _PRESERVE _ROWS
        ;

  view_definition :
		_CREATE _VIEW table_name view_column_list_opt
			_AS query_expression view_check_opt ;

  view_column_list_opt :
        /* Empty */
        |       left_paren view_column_list right_paren
        ;

  view_check_opt :
        /* Empty */
        |       _WITH _CHECK _OPTION
        |       _WITH _CASCADED _CHECK _OPTION
        |       _WITH _LOCAL _CHECK _OPTION
        ;

  view_column_list : column_name_list ;

  grant_statement :
		_GRANT privileges _ON object_name _TO grantee_list grant_option ;

  grantee_list :
                grantee
        |       grantee_list comma grantee
        ;

  grant_option :
        /* Empty */
        |       _WITH _GRANT _OPTION
        ;

  privileges : _ALL _PRIVILEGES | action_list ;

  action_list :
                action
        |       action_list  comma action
        ;

  action :
		_SELECT
	|	_DELETE
	|	_INSERT privilege_column_list_opt
	|	_UPDATE privilege_column_list_opt
	|	_REFERENCES privilege_column_list_opt
	|	_USAGE
        ;

  privilege_column_list_opt :
        /* Empty */
        |       left_paren privilege_column_list right_paren
        ;

  privilege_column_list : column_name_list ;

  object_name :
		table_opt table_name
	|	_DOMAIN domain_name
	|	_COLLATION collation_name
	|	_CHARACTER _SET character_set_name
	|	_TRANSLATION translation_name ;

  table_opt :
        /* Empty */
        |       _TABLE
        ;

  grantee : _PUBLIC | authorization_identifier ;

  assertion_definition :
		_CREATE _ASSERTION constraint_name assertion_check constraint_attributes_opt
        ;

  assertion_check : _CHECK left_paren search_condition right_paren
        ;

  character_set_definition :
		_CREATE _CHARACTER _SET character_set_name as_opt character_set_source
		charset_collation_opt ;

  charset_collation_opt :
        /* Empty */
        |       collate_clause
        |       limited_collation_definition
        ;

  character_set_source : _GET existing_character_set_name ;

  existing_character_set_name :
                character_set_name
        ;

  limited_collation_definition :
		_COLLATION _FROM collation_source ;

  collation_source : collating_sequence_definition | translation_collation ;

  collating_sequence_definition :
		external_collation
	|	schema_collation_name
	|	_DESC left_paren collation_name right_paren
	|	_DEFAULT ;

  external_collation :
	_EXTERNAL left_paren quote external_collation_name quote right_paren ;

  external_collation_name : collation_name ;

  schema_collation_name : collation_name ;

  translation_collation : _TRANSLATION translation_name translation_collation_opt ;

  translation_collation_opt :
        /* Empty */
        |       _THEN _COLLATION collation_name
        ;

  collation_definition :
		_CREATE _COLLATION collation_name _FOR character_set_specification
			_FROM collation_source pad_attribute_opt
        ;

  pad_attribute_opt :
        /* Empty */
        |       _NO _PAD
        |       _PAD _SPACE
        ;

  translation_definition :
		_CREATE _TRANSLATION translation_name
			_FOR source_character_set_specification
			_TO target_character_set_specification
			_FROM translation_source
        ;

  source_character_set_specification : character_set_specification ;

  target_character_set_specification : character_set_specification ;

  translation_source : translation_specification ;

  translation_specification :
		external_translation
	|	_IDENTITY
	|	schema_translation_name ;

  external_translation :
		_EXTERNAL left_paren quote external_translation_name quote right_paren ;

  external_translation_name :
		translation_name ;

  schema_translation_name : translation_name ;

  SQL_schema_manipulation_statement :
		drop_schema_statement
	|	alter_table_statement
	|	drop_table_statement
	|	drop_view_statement
	|	revoke_statement
	|	alter_domain_statement
	|	drop_domain_statement
	|	drop_character_set_statement
	|	drop_collation_statement
	|	drop_translation_statement
	|	drop_assertion_statement
        ;

  drop_schema_statement : _DROP _SCHEMA schema_name drop_behaviour
        ;

  drop_behaviour : _CASCADE | _RESTRICT
        ;

  alter_table_statement : _ALTER _TABLE table_name alter_table_action
        ;

  alter_table_action :
		add_column_definition
	|	alter_column_definition
	|	drop_column_definition
	|	add_table_constraint_definition
	|	drop_table_constraint_definition
        ;

  column_opt :
        /* Empty */
        |       _COLUMN
        ;

  add_column_definition :
                _ADD column_opt column_definition
        ;

  alter_column_definition :
                _ALTER column_opt column_name alter_column_action
        ;

  alter_column_action :
                set_column_default_clause
        |       drop_column_default_clause
        ;

  set_column_default_clause :
                _SET default_clause
        ;

  drop_column_default_clause :
                _DROP _DEFAULT
        ;

  drop_column_definition :
                _DROP column_opt column_name drop_behaviour
        ;

  add_table_constraint_definition :
                _ADD table_constraint_definition
        ;

  drop_table_constraint_definition :
                _DROP _CONSTRAINT constraint_name drop_behaviour
        ;

  drop_table_statement :
                _DROP _TABLE table_name drop_behaviour
        ;

  drop_view_statement :
                _DROP _VIEW table_name drop_behaviour
        ;

  revoke_statement :
		_REVOKE grant_option_for_opt privileges _ON object_name
			_FROM grantee_list drop_behaviour
        ;

  grant_option_for_opt :
        /* Empty */
        |        _GRANT _OPTION _FOR
        ;

  alter_domain_statement : _ALTER _DOMAIN domain_name alter_domain_action
        ;

  alter_domain_action :
		set_domain_default_clause
	|	drop_domain_default_clause
	|	add_domain_constraint_definition
	|	drop_domain_constraint_definition
        ;

  set_domain_default_clause :
                _SET default_clause
        ;

  drop_domain_default_clause :
                _DROP _DEFAULT
        ;

  add_domain_constraint_definition :
                _ADD domain_constraint
        ;

  drop_domain_constraint_definition :
                _DROP _CONSTRAINT constraint_name
        ;

  drop_domain_statement :
                _DROP _DOMAIN domain_name drop_behaviour
        ;

  drop_character_set_statement :
                _DROP _CHARACTER _SET character_set_name
        ;

  drop_collation_statement :
                _DROP _COLLATION collation_name
        ;

  drop_translation_statement :
                _DROP _TRANSLATION translation_name
        ;

  drop_assertion_statement :
                _DROP _ASSERTION constraint_name
        ;

/*
--hr
--h2 SQL Data Manipulation Statements
--/h2
*/

  SQL_data_statement :
		open_statement
	|	fetch_statement
	|	close_statement
	|	select_statement__single_row
	|	SQL_data_change_statement ;

  open_statement : _OPEN cursor_name ;

  fetch_statement :
		_FETCH fetch_orientation_opt cursor_name _INTO fetch_target_list ;

  fetch_orientation_opt :
        /* Empty */
        |       _FROM
        |       fetch_orientation _FROM
        ;

  fetch_orientation :
		_NEXT
	|	_PRIOR
	|	_FIRST
	|	_LAST
	|	_ABSOLUTE simple_value_specification
        |       _RELATIVE simple_value_specification
        ;

  simple_value_specification :
                parameter_name
        |       literal
        ;

  fetch_target_list :
                target_specification
        |       fetch_target_list comma target_specification
        ;

  target_specification :
		parameter_specification
        ;

  close_statement :
                _CLOSE cursor_name
        ;

  select_statement__single_row :
	        _SELECT set_quantifier_opt select_list
                _INTO select_target_list table_expression
        ;

  select_target_list :
                target_specification
        |       select_target_list comma target_specification
        ;

  SQL_data_change_statement :
		delete_statement__positioned
	|	delete_statement__searched
	|	insert_statement
	|	update_statement__positioned
	|	update_statement__searched ;

  delete_statement__positioned :
                _DELETE _FROM table_name
                _WHERE _CURRENT _OF cursor_name
        ;

  delete_statement__searched :
                _DELETE _FROM table_name where_clause_opt ;

  insert_statement : _INSERT _INTO table_name insert_columns_and_source ;

  insert_columns_and_source :
                left_paren insert_column_list right_paren query_expression
        |       query_expression
        |       _DEFAULT _VALUES
        ;

  insert_column_list : column_name_list ;

  update_statement__positioned :
		_UPDATE table_name _SET set_clause_list _WHERE _CURRENT _OF cursor_name ;

  set_clause_list :
                set_clause
        |       set_clause_list comma set_clause
        ;

  set_clause :
                object_column equals_operator update_source
        ;

  object_column : column_name ;

  update_source :
                expression
                /* Expression includes NULL and DEFAULT specifications */
        ;

  update_statement__searched :
		_UPDATE table_name _SET set_clause_list where_clause_opt
        ;

  SQL_transaction_statement :
		set_transaction_statement
	|	set_constraints_mode_statement
	|	commit_statement
	|	rollback_statement
        ;

  set_transaction_statement :
		_SET _TRANSACTION transaction_mode_list ;

  transaction_mode_list :
                transaction_mode
        |       transaction_mode_list comma transaction_mode
        ;

  transaction_mode :
		isolation_level
	|	transaction_access_mode
	|	diagnostics_size
        ;

  isolation_level : _ISOLATION _LEVEL level_of_isolation ;

  level_of_isolation :
		_READ _UNCOMMITTED
	|	_READ _COMMITTED
	|	_REPEATABLE _READ
	|	_SERIALIZABLE
        |       _SNAPSHOT
        ;

  transaction_access_mode :
                _READ _ONLY
        |       _READ _WRITE
        ;

  diagnostics_size : _DIAGNOSTICS _SIZE number_of_conditions ;

  number_of_conditions : simple_value_specification ;

  set_constraints_mode_statement :
                _SET _CONSTRAINTS constraint_name_list _DEFERRED
         |      _SET _CONSTRAINTS constraint_name_list _IMMEDIATE
         ;

  constraint_name_list :
                _ALL
        |       constraint_name_list_some
        ;

  constraint_name_list_some:
                constraint_name
        |       constraint_name_list_some comma constraint_name
        ;

  commit_statement :
                _COMMIT
        |       _COMMIT _WORK
        ;

  rollback_statement :
                _ROLLBACK
        |       _ROLLBACK _WORK
        ;

/*
--hr
--h2 Connection Management
--/h2
*/

  SQL_connection_statement :
		connect_statement
	|	set_connection_statement
	|	disconnect_statement ;

  connect_statement : _CONNECT _TO connection_target ;

  connection_target :
		SQL_server_name connection_name_opt user_name_opt
	|	_DEFAULT ;

  connection_name_opt :
        /* Empty */
        |       _AS connection_name
        ;

  user_name_opt :
        /* Empty */
        |       _USER user_name
        ;

  SQL_server_name : simple_value_specification ;

  connection_name : simple_value_specification ;

  user_name : simple_value_specification ;

  set_connection_statement : _SET _CONNECTION connection_object ;

  connection_object : _DEFAULT | connection_name ;

  disconnect_statement : _DISCONNECT disconnect_object ;

  disconnect_object : connection_object | _ALL | _CURRENT ;

/*
--hr
--h2 Session Attributes
--/h2
*/

  SQL_session_statement :
		set_catalog_statement
	|	set_schema_statement
	|	set_names_statement
	|	set_session_authorization_identifier_statement
	|	set_local_time_zone_statement ;

  set_catalog_statement : _SET _CATALOG value_specification ;

  value_specification : literal | general_value_specification ;

  set_schema_statement : _SET _SCHEMA value_specification ;

  set_names_statement : _SET _NAMES value_specification ;

  set_session_authorization_identifier_statement : _SET _SESSION _AUTHORIZATION value_specification ;

  set_local_time_zone_statement : _SET _TIME _ZONE set_time_zone_value ;

  set_time_zone_value : expression /* Check interval */; | _LOCAL ;

/*
--hr
--h2 Dynamic SQL
--/h2
*/

/* Omitted, not doing dynamic SQL, or diagnostics */

/*
--small
--i
Note that <colon> is written as a literal colon in the ANSI grammar;
--/i
--/small
*/

  direct_SQL_statement :
		direct_SQL_data_statement
	|	SQL_schema_statement
	|	SQL_transaction_statement
	|	SQL_connection_statement
	|	SQL_session_statement
	|	direct_implementation_defined_statement ;

  direct_SQL_data_statement :
		delete_statement__searched
	|	direct_select_statement__multiple_rows
	|	insert_statement
	|	update_statement__searched
	|	temporary_table_declaration ;

  direct_select_statement__multiple_rows : query_expression order_by_clause_opt ;

  direct_implementation_defined_statement : identifier ;
                                          /* TODO - SynError; */

/*
--hr
--h2 Top-Level_reachable in standalone SQL statements
--   (no embedding inside host program, no host program embedded in &c;);
--   This you might modify depending on whether you want an interactive
--   sql prompt or something else;
--/h2
*/

sql_statement :
      direct_SQL_statement
    ;

sql_script :
                sql_statement
    |           sql_script sql_statement
    ;

sql_input :
      sql_script
    | module
    ;

SQL92Grammar :

  sql_input ;

%%

constructor SQL92GrammarParser.Create;
begin
  inherited;
  Lexer := SQL92GrammarLexer.Create;
end;

destructor SQL92GrammarParser.Destroy;
begin
  Lexer.Free;
  inherited;
end;

procedure SQL92GrammarParser.yyerror ( msg : String );
var
  Debug: TStringList;
  i: integer;
begin
  inherited;
  Debug := GetStateDebug(yystate);
  if Assigned(Debug) then
  begin
    Lexer.YYOutWriteLn('Parser state debug: ');
    for i := 0 to Pred(Debug.Count) do
      Lexer.YYOutWriteLn(Debug[i]);
    Debug.Free;
  end;
end;

procedure SQL92GrammarParser.yyaction_debug(State: integer; Action: integer);
var
  S: string;
begin
  if not (yydebug or yyactiondebug) then exit;
  inherited;
  S := GetStateActionString(State, Action);
  if Length(S) > 0 then
    Lexer.YYOutWriteLn(S);
end;

function SQL92GrammarParser.MakeClass(ClassType: TSQLSynNodeClass): TSQLSynNode;
begin
  result := ClassType.Create;
  result.Line := Lexer.yylineno;
  result.Col := Lexer.yycolno;
  //TODO - State and action as debug?
end;

function SQL92GrammarParser.CheckContinuation(S: string; T: TSQLSynLiteralType): boolean;
begin
  Assert(false);
  //TODO - Write this.
  result := false;
end;

function SQL92GrammarParser.HandleContinuation(S: string; T: TSQLSynLiteralType): string;
begin
  Assert(false);
  //TODO - Write this.
  result := '';
end;

function SQL92GrammarParser.CheckDelimIdent(S: string): boolean;
begin
  Assert(false);
  //TODO - Write this.
  result := false;
end;

function SQL92GrammarParser.HandleDelimIdent(S: string): string;
begin
  Assert(false);
  //TODO - Write this.
  result := '';
end;


end.
