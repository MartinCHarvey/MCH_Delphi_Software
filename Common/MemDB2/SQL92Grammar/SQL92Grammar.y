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
  //TODO - Remove tmp decls if not required.
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
                                                    ExprType := setLiteral;
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
                                                    ExprType := setLiteral;
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
                                                    ExprType := setLiteral;
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
                                                    ExprType := setLiteral;
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
                                                      ExprType := setLiteral;
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
        |       quote time_value time_zone_interval quote
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
                                                  $$.Text := $2.Text + ' ' + $4.Text }
        |       quote date_value space time_value time_zone_interval quote
                                                { $$.Obj := nil;
                                                  $$.Text := $2.Text + ' ' + $4.Text;
                                                  yyinfo('Time zone ignored in in timestamp string.'); }
        ;

  interval_string :
                quote interval_string_literal quote { $$ := $2; }
        ;

  interval_string_literal :
                unsigned_integer                { $$ := $1;
                                                  $$.Obj.Free;
                                                  $$.Obj := MakeClass(TSQLSynIntervalStringLiteral);
                                                  with $$.Obj as TSQLSynIntervalStringLiteral do
                                                  begin
                                                    ExprType := setLiteral;
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
                                                    ExprType := setLiteral;
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
                                                    ExprType := setLiteral;
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
                                                    ExprType := setLiteral;
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
                                                    ExprType := setLiteral;
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
                                                    ExprType := setLiteral;
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
                                                    ExprType := setLiteral;
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
                                                    ExprType := setLiteral;
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
                                                    FlattenModuleContents(($4.Obj) as TTempTpl);
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
                                                { $$.Text := ''; $$.Obj := nil;
                                                  yyinfo('Charset specification ignored in module decl.');
                                                  $3.Obj.Free; }
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

  /* TODO - Check with the AI about whether this is consistent with creations
    elsewhere. */
  temporary_table_declaration :
                _DECLARE _LOCAL _TEMPORARY _TABLE
                qualified_local_table_name
                table_element_list
                temporary_table_declaration_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynCreateOrDeclTable);
                                                  with $$.Obj as TSqlSynCreateOrDeclTable do
                                                  begin
                                                    StructuralType := sstCreateOrDecl;
                                                    CreateOrDeclType := codTable;
                                                    TempOpts := dtoLocalTemporary;
                                                    InsertTailChild($5.Obj);
                                                    Name := $5.Obj as TSqlSynIdent;
                                                    FlattenColDefsConstraints($6.Obj as TTempTpl);
                                                    $6.Obj.Free;
                                                    RowCommitAction := TSqlSynRowCommitAction($7.Obj);
                                                  end;}
        ;

  temporary_table_declaration_opt :
        /* Empty */
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(rcaUnspecified); }
        |       _ON _COMMIT _PRESERVE _ROWS
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(rcaCommitRows); }
        |       _ON _COMMIT _DELETE _ROWS
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(rcaDeleteRows); }
        ;

  qualified_local_table_name :
                _MODULE period local_table_name
                                                { //Module localness should be implicit in type recognition
                                                  //algorithm
                                                  $$ := $3; }
        ;

  local_table_name :
                identifier
                                                { $$ := $1;
                                                  ($$.Obj as TSqlSynIdent).LocalName := True; }
        ;

  table_element_list :
        left_paren table_element table_element_list_opt right_paren
                                                { $$ := $3;
                                                  with $$.Obj as TTempTpl do
                                                    InsertHeadChild($2.Obj); }
        ;

  table_element_list_opt :
        /* Empty */
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl); }
        |       table_element_list_opt comma table_element
                                                { $$ := $1;
                                                  if Assigned($3.Obj) then
                                                  begin
                                                    //Check constraints may be omitted / NIL.
                                                    with $$.Obj as TTempTpl do
                                                      InsertTailChild($3.Obj);
                                                  end;}
        ;

  table_element :
                column_definition
                                                { $$ := $1; }
        |       table_constraint_definition
                                                { $$ := $1; } /* Might be NIL if check constraint ignored.*/
        ;

  column_definition :
                column_name
                column_definition_sel
                default_clause_opt
                column_constraint_definition_opt
                collate_clause_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynColDef);
                                                  with $$.Obj as TSqlSynColDef do
                                                  begin
                                                    StructuralType := sstColDef;
                                                    InsertTailChild($1.Obj);
                                                    Name := $1.Obj as TSqlSynIdent;
                                                    InsertTailChild($2.Obj);
                                                    DataType := $2.Obj;
                                                    if Assigned($3.Obj) then
                                                    begin
                                                      InsertTailChild($3.Obj);
                                                      _Default := $3.Obj as TSqlSynLiteral;
                                                    end;
                                                    if Assigned($4.Obj) then
                                                    begin
                                                      InsertTailChild($4.Obj);
                                                      Constraint := $4.Obj as TSqlSynConstraint;
                                                    end;
                                                    Assert(not Assigned($5.Obj)); //No collations yet.
                                                  end; }
        ;

  column_definition_sel :
                data_type
                                                { $$ := $1; }
        |       domain_name
                                                { $$ := $1; }
        ;

  default_clause_opt :
        /* Empty */
                                                { $$.Text := ''; $$.Obj := nil; }
        |       default_clause
                                                { $$ := $1; }
        ;

  column_constraint_definition_opt :
        /* Empty */
                                                { $$.Text := ''; $$.Obj := nil; }
        |       column_constraint_definition
                                                { $$ := $1; }
        ;

  collate_clause_opt :
        /* Empty */
                                                { $$.Text := ''; $$.Obj := nil; }
        |       collate_clause
                                                { $$ := $1; }
        ;

  column_name :
                identifier
                                                { $$ := $1; }
        ;

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
        /* Empty */                             { $$.Text := ''; $$.Obj := nil; }
        |       left_paren precision comma scale right_paren
                                                { $$.Text := ''; $$.Obj := nil;
                                                  yyerror('Integer scaling not supported at the moment. E-mail the author.'); }
        |       left_paren precision right_paren
                                                { $$.Text := ''; $$.Obj := nil;
                                                  yyinfo('Integer precision ignored in type.');
                                                  $2.Obj.Free; }
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
        /* Empty */                             { $$.Text := ''; $$.Obj := nil; }
        |       left_paren timestamp_precision right_paren
                                                { yyinfo('Timestamp precision ignored.');
                                                  $$.Text := ''; $$.obj := nil;
                                                  $2.Obj.Free; }
        ;

  time_precision_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := nil; }
        |       left_paren time_precision right_paren
                                                { yyinfo('Time precision ignored.');
                                                  $$.Text := ''; $$.obj := nil;
                                                  $2.Obj.Free; }
        ;

  tz_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := nil; }
        |        _WITH _TIME _ZONE
                                                { $$.Text := ''; $$.Obj := nil;
                                                  yyerror('Time zones not supported.');}
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
                                                  $3.Obj.Free; }
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
                                                { $$ := $2; }
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
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftUser;
                                                  end;}
	|	_CURRENT_USER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftCurrentUser;
                                                  end; }
	|	_SESSION_USER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftSessionUser;
                                                  end; }
	|	_SYSTEM_USER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftSystemUser;
                                                  end; }
	|	_NULL
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftNull;
                                                  end;}
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
                                                { $$.Text := $2.Text;
                                                  Assert(not Assigned($2.Obj));
                                                  $$.Obj := MakeClass(TSQlSynLiteral);
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltDate;
                                                    Text := $$.Text;
                                                  end; }
        ;

  time_literal : _TIME time_string
                                                { $$.Text := $2.Text;
                                                  Assert(not Assigned($2.Obj));
                                                  $$.Obj := MakeClass(TSQlSynLiteral);
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltTime;
                                                    Text := $$.Text;
                                                  end; }
        ;

  timestamp_literal : _TIMESTAMP timestamp_string
                                                { $$.Text := $2.Text;
                                                  Assert(not Assigned($2.Obj));
                                                  $$.Obj := MakeClass(TSQlSynLiteral);
                                                  with $$.Obj as TSQLSynLiteral do
                                                  begin
                                                    ExprType := setLiteral;
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
                                                    ExprType := setLiteral;
                                                    LitType := sltInterval;
                                                    Text := $$.Text;
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
                                                    ExprType := setLiteral;
                                                    LitType := sltInterval;
                                                    Text := $$.Text;
                                                    if $2.Text <> '+' then
                                                      Negated := True;
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
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftCurrentDate;
                                                  end;}
        ;

  current_time_value_function :
                _CURRENT_TIME
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftCurrentTime;
                                                  end; }
        |       _CURRENT_TIME left_paren time_precision right_paren
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftCurrentTime;
                                                  end;
                                                  $3.Obj.Free;
                                                  yyinfo('Time precision ignored in current time function.'); }
        ;

  current_timestamp_value_function :
                _CURRENT_TIMESTAMP
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftCurrentTimestamp;
                                                  end;}
        |       _CURRENT_TIMESTAMP left_paren timestamp_precision right_paren
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftCurrentTimestamp;
                                                  end;
                                                  $3.Obj.Free;
                                                  yyinfo('Timestamp precision ignored in current timestamp function.'); }
        ;

/*
--hr
--h2 Constraints
--/h2
*/

  column_constraint_definition :
		constraint_name_definition_opt
                column_constraint
                constraint_attributes_opt
                                                { //NB. Arbitary check constraints not supported
                                                  //at this time, so building this part of the tree is
                                                  //optional...

                                                  //TODO - Always create relational constraint so we can
                                                  //fix up columns later.
                                                  if Assigned($2.Obj) then
                                                  begin
                                                    $$.Text := '';
                                                    $$.Obj := MakeClass(TSqlSynConstraint);
                                                    with $$.Obj as TSqlSynConstraint do
                                                    begin
                                                      StructuralType := sstConstraint;
                                                      ConstraintType := sctColumn;
                                                      if Assigned($1.Obj) then
                                                      begin
                                                        InsertTailChild($1.Obj);
                                                        Name := $1.Obj as TSqlSynIdent;
                                                      end;
                                                      //For relational column constraints, reffing rows fixed up
                                                      //later.
                                                      InsertTailChild($2.Obj);
                                                      Detail := $1.Obj as TSqlSynConstraintDetail;
                                                      if Assigned($3.Obj) then
                                                      begin
                                                        InsertTailChild($3.Obj);
                                                        Attributes := $3.Obj as TSqlSynConstraintAttributes;
                                                      end;
                                                    end;
                                                  end
                                                  else
                                                  begin
                                                    $$.Text := '';
                                                    $$.Obj := nil;
                                                    $1.Obj.Free;
                                                    $3.Obj.Free;
                                                  end;}
        ;

  constraint_name_definition :
               _CONSTRAINT constraint_name
                                                { $$ := $2; }
        ;

  constraint_name_definition_opt :
        /* Empty */
                                                { $$.Text := ''; $$.Obj := nil; }
        |       constraint_name_definition
                                                { $$ := $1; }
        ;

  constraint_name : qualified_name
                                                { $$ := $1; }
        ;

  column_constraint :
		_NOT _NULL
                                                { $$.Obj := MakeClass(TSqlSynConstraintDetail);
                                                  with $$.Obj as TSqlSynConstraintDetail do
                                                  begin
                                                    StructuralType := sstConstraintDetail;
                                                    DetailType := cdtNotNull;
                                                  end; }
	|	unique_specification
                                                { $$ := $1; }
	|	references_specification
                                                { $$ := $1; }
	|	check_constraint_definition
                                                { $$.Text := '';
                                                  $$.Obj := nil;
                                                  yyinfo('Arbitrary check constraints not supported at this time');
                                                  $1.Obj.Free; }
        ;

  unique_specification :
                _UNIQUE
                                                { $$.Obj := MakeClass(TSqlSynConstraintDetail);
                                                  with $$.Obj as TSqlSynConstraintDetail do
                                                  begin
                                                    StructuralType := sstConstraintDetail;
                                                    DetailType := cdtUnique;
                                                  end; }
        |       _PRIMARY _KEY
                                                { $$.Obj := MakeClass(TSqlSynConstraintDetail);
                                                  with $$.Obj as TSqlSynConstraintDetail do
                                                  begin
                                                    StructuralType := sstConstraintDetail;
                                                    DetailType := cdtPrimaryKey;
                                                  end; }
        ;

  references_specification :
		_REFERENCES referenced_table_and_columns
                match_type_opt
                referential_triggered_action_opt
                                                { $$.Obj := MakeClass(TSqlSynReferencesConstraintDetail);
                                                  with $$.Obj as TSqlSynReferencesConstraintDetail do
                                                  begin
                                                    StructuralType := sstConstraintDetail;
                                                    DetailType := cdtReferences;
                                                    MatchType := TSqlSynMatchType($3.Obj);
                                                    FlattenReffedDetails($2.Obj as TTempTpl);
                                                    if Assigned($4.Obj) then
                                                    begin
                                                      InsertTailChild($4.Obj);
                                                      RefAction := $4.Obj as TSqlSynRefAction;
                                                    end;
                                                  end; }
        ;

  match_type_opt :
        /* Empty */
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(mtUnspec); }
        |       _MATCH match_type
                                                { $$ := $2; }
        ;

  referential_triggered_action_opt :
        /* Empty */
                                                { $$.Text := ''; $$.Obj := nil; }
        |       referential_triggered_action
                                                { $$ := $1; }
        ;

  referenced_table_and_columns :
                table_name reference_column_list_opt
                                                { $$.Text := '';
                                                  if Assigned($2.Obj) then
                                                    $$.Obj := $2.Obj
                                                  else
                                                    $$.Obj := MakeClass(TTempTpl);
                                                  ($$.Obj as TTempTpl).InsertHeadChild($1.Obj); }
        ;

  reference_column_list_opt :
        /* Empty */
                                                { $$.Text := ''; $$.Obj := nil; }
        |       left_paren reference_column_list right_paren
                                                { $$ := $2; }
        ;

  table_name :
                qualified_name
                                                { $$ := $1; }
        |       qualified_local_table_name
                                                { $$ := $1; }
        ;

  reference_column_list :
                column_name_list
                                                { $$ := $1; }
        ;

  column_name_list :
                column_name
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  ($$.Obj as TTempTpl).InsertTailChild($1.Obj); }
        |       column_name_list comma column_name
                                                { $$ := $1;
                                                  ($$.Obj as TTempTpl).InsertTailChild($3.Obj); }
        ;

  match_type :
                _FULL
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(mtFull); }
        |       _PARTIAL
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(mtPartial); }
        ;

  referential_triggered_action :
		update_rule delete_rule_opt
                                                { $$ := $1;
                                                  if Assigned($2.Obj) then
                                                    ($$.Obj as TSqlSynRefAction)
                                                      .MergeWith($2.Obj as TSqlSynRefAction);
                                                  $2.Obj.Free;}
	|	delete_rule update_rule_opt
                                                { $$ := $1;
                                                  if Assigned($2.Obj) then
                                                    ($$.Obj as TSqlSynRefAction)
                                                      .MergeWith($2.Obj as TSqlSynRefAction);
                                                  $2.Obj.Free; }
        ;

  update_rule_opt :
        /* Empty */
                                                { $$.Text := ''; $$.Obj := nil; }
        |       update_rule
                                                { $$ := $1; }
        ;

  delete_rule_opt :
        /* Empty */
                                                { $$.Text := ''; $$.Obj := nil; }
        |       delete_rule
                                                { $$ := $1; }
        ;

  update_rule :
                _ON _UPDATE referential_action
                                                { $$.Obj := MakeClass(TSqlSynRefAction);
                                                  with $$.Obj as TSqlSynRefAction do
                                                  begin
                                                    StructuralType := sstRefAction;
                                                    UpdateAction := TSqlSynRefDoAction($3.Obj);
                                                  end; }
        ;

  referential_action :
                _CASCADE
                                                { $$.Text := ''; $$.Obj := TSqlSynNode(rdaCascade); }
        |       _SET _NULL
                                                { $$.Text := ''; $$.Obj := TSqlSynNode(rdaSetNull); }
        |       _SET _DEFAULT
                                                { $$.Text := ''; $$.Obj := TSqlSynNode(rdaSetDefault); }
        |       _NO _ACTION
                                                { $$.Text := ''; $$.Obj := TSqlSynNode(rdaNone); }
        ;

  delete_rule :
                _ON _DELETE referential_action
                                                { $$.Obj := MakeClass(TSqlSynRefAction);
                                                  with $$.Obj as TSqlSynRefAction do
                                                  begin
                                                    StructuralType := sstRefAction;
                                                    DeleteAction := TSqlSynRefDoAction($3.Obj);
                                                  end; }
        ;

  check_constraint_definition :
        _CHECK left_paren search_condition right_paren
                                                { $$ := $3; }
        ;

/*
--hr
--h2 Search Condition
--/h2
*/

  search_condition :
                boolean_term
                                                { $$ := $1; }
	|       search_condition _OR boolean_term
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBinaryOperator);
                                                  with $$.Obj as TSqlSynBinaryOperator do
                                                  begin
                                                    ExprType := setBinaryOr;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    Right := $3.Obj as TSqlSynExpr;
                                                  end; }
        ;

  boolean_term :
		boolean_factor
                                                { $$ := $1; }
	|       boolean_term _AND boolean_factor
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBinaryOperator);
                                                  with $$.Obj as TSqlSynBinaryOperator do
                                                  begin
                                                    ExprType := setBinaryAnd;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    Right := $3.Obj as TSqlSynExpr;
                                                  end; }
        ;

  boolean_factor :
                boolean_test
                                                { $$ := $1; }
        |       _NOT boolean_test
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynUnaryOperator);
                                                  with $$.Obj as TSqlSynUnaryOperator do
                                                  begin
                                                    ExprType := setUnaryNot;
                                                    InsertTailChild($2.Obj);
                                                    Expr := $2.Obj as TSqlSynExpr;
                                                  end; }
        ;

  boolean_test :
                boolean_primary
                                                { $$ := $1; }
        |       boolean_primary _IS truth_value
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBinaryOperator);
                                                  with $$.Obj as TSqlSynBinaryOperator do
                                                  begin
                                                    ExprType := setBinaryTest;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    Right := $3.Obj as TSqlSynExpr;
                                                  end; }
        |       boolean_primary _IS _NOT truth_value
                                                { $$.Text := '';
                                                  TmpClass := MakeClass(TSqlSynBinaryOperator);
                                                  with TmpClass as TSqlSynBinaryOperator do
                                                  begin
                                                    ExprType := setBinaryTest;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($4.Obj);
                                                    Right := $4.Obj as TSqlSynExpr;
                                                  end;
                                                  $$.Obj := MakeClass(TSqlSynUnaryOperator);
                                                  with $$.Obj as TSqlSynUnaryOperator do
                                                  begin
                                                    ExprType := setUnaryNot;
                                                    InsertTailChild(TmpClass);
                                                    Expr := TmpClass as TSqlSynExpr;
                                                  end; }
        ;

  boolean_primary :
                predicate
                                                { $$ := $1; }
        |       left_paren search_condition right_paren
                                                { $$ := $2; }
        ;

  predicate :
	    comparison_predicate
                                                { $$ := $1; }
	|   between_predicate
                                                { $$ := $1; }
	|   in_predicate
                                                { $$ := $1; }
	|   like_predicate
                                                { $$ := $1; }
	|   null_predicate
                                                { $$ := $1; }
	|   quantified_comparison_predicate
                                                { $$ := $1; }
	|   exists_predicate
                                                { $$ := $1; }
        |   unique_predicate
                                                { $$ := $1; }
	|   match_predicate
                                                { $$ := $1; }
	|   overlaps_predicate
                                                { $$ := $1; }
        ;

  comparison_predicate :
                row_value_constructor comp_op row_value_constructor
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynComparisonPredicate);
                                                  with $$.Obj as TSqlSynComparisonPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptComparisonPredicate;
                                                    CompOp := TSqlSynCompOp($2.Obj);
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    Right := $3.Obj as TSqlSynExpr;
                                                  end; }
        ;

  row_value_constructor :
                expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlRowValConstructor);
                                                  with $$.Obj as TSqlRowValConstructor do
                                                  begin
                                                    ExprType := setRValConstructor;
                                                    InsertTailChild($1.Obj);
                                                  end; }
        |       left_paren row_value_constructor_list right_paren
                                                { $$ := $2; }
        ;

  primary_expression :
                unsigned_value_specification
                                                { $$ := $1; }
    |           column_reference
                                                { $$ := $1; }
    |           set_function_specification
                                                { $$ := $1; }
    |           scalar_subquery
                                                { $$ := $1; }
    |           case_expression
                                                { $$ := $1; }
    |           cast_specification
                                                { $$ := $1; }
    |           numeric_value_function
                                                { $$ := $1; }
    |           string_value_function
                                                { $$ := $1; }
    |           datetime_value_function
                                                { $$ := $1; }
    |           null_specification
                                                { $$ := $1; }
    |           default_specification
                                                { $$ := $1; }
    |           left_paren expression right_paren
                                                { $$ := $2; }
    ;

  postfix_expression:
                primary_expression
                                                { $$ := $1; }
      |         primary_expression postfix_op
    ;

  postfix_op :
                time_zone
                                                { $$.Text := ''; $$.Obj := nil;
                                                  yyerror('Time zones not supported');
                                                  $1.Obj.Free; }
        |       interval_qualifier
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynUnaryOperator);
                                                  with $$.Obj as TSqlSynUnaryOperator do
                                                  begin
                                                    ExprType := setUnaryPostfixIntervalQualifier;
                                                    InsertTailChild($1.Obj);
                                                    Expr := $1.Obj as TSqlSynExpr;
                                                  end; }
        |       collate_clause
                                                { $$.Text := ''; $$.Obj := nil;
                                                  yyerror('Collations not supported');
                                                  $1.Obj.Free; }        ;

  unary_expression :
        plus_sign postfix_expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynUnaryOperator);
                                                  with $$.Obj as TSqlSynUnaryOperator do
                                                  begin
                                                    ExprType := setUnaryPlus;
                                                    InsertTailChild($2.Obj);
                                                    Expr := $2.Obj as TSqlSynExpr;
                                                  end; }
    |   minus_sign postfix_expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynUnaryOperator);
                                                  with $$.Obj as TSqlSynUnaryOperator do
                                                  begin
                                                    ExprType := setUnaryMinus;
                                                    InsertTailChild($2.Obj);
                                                    Expr := $2.Obj as TSqlSynExpr;
                                                  end; }
    |   postfix_expression
                                                { $$ := $1; }
    ;

  multiplicative_expression :
        unary_expression
                                                { $$ := $1; }
    |   multiplicative_expression asterisk unary_expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBinaryOperator);
                                                  with $$.Obj as TSqlSynBinaryOperator do
                                                  begin
                                                    ExprType := setBinaryMul;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    Right := $3.Obj as TSqlSynExpr;
                                                  end; }
    |   multiplicative_expression solidus unary_expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBinaryOperator);
                                                  with $$.Obj as TSqlSynBinaryOperator do
                                                  begin
                                                    ExprType := setBinaryDiv;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    Right := $3.Obj as TSqlSynExpr;
                                                  end; }
    ;

  /* numeric or string, or times or intervals, resolved later */
  expression :
        multiplicative_expression
                                                { $$ := $1; }
    |   expression plus_sign multiplicative_expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBinaryOperator);
                                                  with $$.Obj as TSqlSynBinaryOperator do
                                                  begin
                                                    ExprType := setBinaryAdd;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    Right := $3.Obj as TSqlSynExpr;
                                                  end; }
    |   expression minus_sign multiplicative_expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBinaryOperator);
                                                  with $$.Obj as TSqlSynBinaryOperator do
                                                  begin
                                                    ExprType := setBinarySub;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    Right := $3.Obj as TSqlSynExpr;
                                                  end; }
    |   expression concatenation_operator multiplicative_expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBinaryOperator);
                                                  with $$.Obj as TSqlSynBinaryOperator do
                                                  begin
                                                    ExprType := setBinaryCat;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    Right := $3.Obj as TSqlSynExpr;
                                                  end; }
    ;

  string_value_function :
        character_value_function
                                                { $$ := $1 }
        ;

  scalar_subquery :
        left_paren subquery right_paren
                                                { $$ := $2; }
        ;

  subquery :
        query_expression
                                                { $$ := $1; }
        ;

  unsigned_value_specification :
                unsigned_literal
                                                { $$ := $1; }
        |       general_value_specification
                                                { $$ := $1; }
        ;

  unsigned_literal :
                unsigned_numeric_literal
                                                { $$ := $1; }
        |       general_literal
                                                { $$ := $1; }
        ;

  general_value_specification :
                parameter_specification
                                                { $$ := $1; }
	|	_USER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftUser;
                                                  end;}
	|	_CURRENT_USER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftCurrentUser;
                                                  end; }
	|	_SESSION_USER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftSessionUser;
                                                  end; }
	|	_SYSTEM_USER
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftSystemUser;
                                                  end; }
	|   _VALUE
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBuiltin);
                                                  with $$.Obj as TSqlSynBuiltIn do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltBuiltIn;
                                                    BuiltInType := sftValue;
                                                  end; }
        ;

  parameter_specification :
                parameter_name indicator_parameter_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynParameterSpec);
                                                  with $$.Obj as TSqlSynParameterSpec do
                                                  begin
                                                    ExprType := setParameter;
                                                    InsertTailChild($1.Obj);
                                                    ParamName := $1.Obj as TSqlSynIdent;
                                                    if Assigned($2.Obj) then
                                                    begin
                                                      InsertTailChild($2.Obj);
                                                      Indicator := $2.Obj as TSqlSynIdent;
                                                    end;
                                                  end;}
        ;

  parameter_name :
                colon identifier
                                                { $$ := $2; }
        ;

  indicator_parameter_opt :
        /* Empty */
                                                { $$.Text := ''; $$.Obj := nil; }
        |       _INDICATOR parameter_name
                                                { $$ := $2; }
        |       parameter_name
                                                { $$ := $1; }
        ;

  column_reference :
                /* Table Name or Correlation name qualified
                   or just bare column name */
                qualified_name
                                                { $$ := $1; }
        ;

  correlation_name : identifier
                                                { $$ := $1; }
        ;

  /* TODO - I have generalised the args, need to check
     for the specific set function */

  set_function_specification :
	        general_set_function
                                                { $$ := $1; }
        ;

  general_set_function :
		set_function_type left_paren set_quantifier_args right_paren
                                                { $$ := $3;
                                                  with $$.Obj as TSqlSynSetFunction do
                                                    SetFunctionType := TSqlSynSetFunctionType($1.Obj); }
        ;

  set_quantifier_args :
        |       asterisk
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSetFunction);
                                                  with $$.Obj as TSqlSynSetFunction do
                                                  begin
                                                    ExprType := setSetFunction;
                                                    Quantifier := ssqWildcard;
                                                  end; }
        |       set_quantifier expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSetFunction);
                                                  with $$.Obj as TSqlSynSetFunction do
                                                  begin
                                                    ExprType := setSetFunction;
                                                    Quantifier := TSqlSynSetFunctionQuantifier($1.Obj);
                                                    InsertTailChild($2.Obj);
                                                    Expr := $2.Obj as TSqlSynExpr;
                                                  end; }
        |       expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSetFunction);
                                                  with $$.Obj as TSqlSynSetFunction do
                                                  begin
                                                    ExprType := setSetFunction;
                                                    Quantifier := ssqNone;
                                                    InsertTailChild($1.Obj);
                                                    Expr := $1.Obj as TSqlSynExpr;
                                                  end; }
        ;

  set_function_type :
                _AVG
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(ssfAvg); }
        |       _MAX
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(ssfMax); }
        |       _MIN
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(ssfMin); }
        |       _SUM
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(ssfSum); }
        |       _COUNT
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(ssfCount); }
        ;

  set_quantifier :
                _DISTINCT
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(ssqDistinct); }
        |       _ALL
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(ssqAll); }
        ;

  set_quantifier_opt :
        /* Empty */
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(ssqNone); }
        |       set_quantifier
                                                { $$ := $1; }
        ;

/*
--hr
--h2 Queries
--/h2
*/

  query_expression :
		non_join_query_term
                                                { $$ := $1; }
	|	query_expression _UNION all_opt corresponding_spec_opt query_term
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynQueryBinary);
                                                  with $$.Obj as TSqlSynQueryBinary do
                                                  begin
                                                    ExprType := setQueryUnion;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    All := Boolean($3.Obj);
                                                    if Assigned($4.Obj) then
                                                    begin
                                                      InsertTailChild($4.Obj);
                                                      Corresponding := $4.Obj;
                                                    end;
                                                    InsertTailChild($5.Obj);
                                                    Right := $5.Obj as TSqlSynExpr;
                                                  end; }
	|	query_expression _EXCEPT all_opt corresponding_spec_opt query_term
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynQueryBinary);
                                                  with $$.Obj as TSqlSynQueryBinary do
                                                  begin
                                                    ExprType := setQueryExcept;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    All := Boolean($3.Obj);
                                                    if Assigned($4.Obj) then
                                                    begin
                                                      InsertTailChild($4.Obj);
                                                      Corresponding := $4.Obj;
                                                    end;
                                                    InsertTailChild($5.Obj);
                                                    Right := $5.Obj as TSqlSynExpr;
                                                  end; }
        ;

  non_join_query_term :
		non_join_query_primary
                                                { $$ := $1; }
	|	query_term _INTERSECT all_opt corresponding_spec_opt query_primary
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynQueryBinary);
                                                  with $$.Obj as TSqlSynQueryBinary do
                                                  begin
                                                    ExprType := setQueryIntersect;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    All := Boolean($3.Obj);
                                                    if Assigned($4.Obj) then
                                                    begin
                                                      InsertTailChild($4.Obj);
                                                      Corresponding := $4.Obj;
                                                    end;
                                                    InsertTailChild($5.Obj);
                                                    Right := $5.Obj as TSqlSynExpr;
                                                  end; }
        ;

  all_opt :
        /* Empty */
                                                { $$.Text := ''; $$.Obj := TSqlSynNode(false); }
        |       _ALL
                                                { $$.Text := ''; $$.Obj := TSqlSynNode(true); }
        ;

  corresponding_spec_opt :
        /* Empty */
                                                { $$.Text := ''; $$.Obj := nil; }
        |       corresponding_spec
                                                { $$ := $1; }
        ;

  non_join_query_primary :
                simple_table
                                                { $$ := $1; }
        |       table_subquery
                                                { $$ := $1; }
        ;

  simple_table :
		query_specification
                                                { $$ := $1; }
	|	table_value_constructor
                                                { $$ := $1; }
	|	explicit_table
                                                { $$ := $1; }
        ;

  query_specification :
		_SELECT set_quantifier_opt select_list table_expression
                                                { $$ := $4;
                                                  with $$.Obj as TSqlSynQuerySelect do
                                                  begin
                                                    InsertHeadChild($3.Obj);
                                                    SelectList := $3.Obj as TSqlSynSelectList;
                                                    Quantifier := TSqlSynSetFunctionQuantifier($2.Obj);
                                                  end; }
        ;

  select_list :
		asterisk
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSelectList);
                                                  ($$.Obj as TSqlSynSelectList).Wildcard := true; }
	|	select_list_opt
                                                { $$ := $1; }
        ;

  select_list_opt :
                select_sublist
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSelectList);
                                                  $$.Obj.InsertTailChild($1.Obj);}
        |       select_list_opt comma select_sublist
                                                { $$ := $1;
                                                  $$.Obj.InsertTailChild($3.Obj); }
        ;

  select_sublist :
                derived_column
                                                { $$ := $1; }
        |       qualified_name_trail_asterisk
                                                { $$ := $1; }
        ;

  /* TODO - Expressions must evaluate to a value expression */
  derived_column :
                expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynDerivedColumn);
                                                  with $$.Obj as TSqlSynDerivedColumn do
                                                  begin
                                                    StructuralType := sstDerivedColumn;
                                                    InsertTailChild($1.Obj);
                                                    Expr := $1.Obj as TSqlSynExpr;
                                                  end; }
        |       expression as_clause
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynDerivedColumn);
                                                  with $$.Obj as TSqlSynDerivedColumn do
                                                  begin
                                                    StructuralType := sstDerivedColumn;
                                                    InsertTailChild($1.Obj);
                                                    Expr := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($2.Obj);
                                                    AsClause := $2.Obj as TSqlSynIdent;
                                                  end; }
        ;

  as_clause :
                column_name
                                                { $$ := $1; }
        |       _AS column_name
                                                { $$ := $2; }
        ;


  table_expression :
                from_clause
		where_clause_opt
		group_by_clause_opt
		having_clause_opt
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynQuerySelect);
                                          with $$.Obj as TSqlSynQuerySelect do
                                          begin
                                            InsertTailChild($1.Obj);
                                            From := $1.Obj;
                                            if Assigned($2.Obj) then
                                            begin
                                              InsertTailChild($2.Obj);
                                              Where := $2.Obj as TSqlSynExpr;
                                            end;
                                            if Assigned($3.Obj) then
                                            begin
                                              InsertTailChild($3.Obj);
                                              GroupBy := $3.Obj;
                                            end;
                                            if Assigned($4.Obj) then
                                            begin
                                              InsertTailChild($4.Obj);
                                              Having := $4.Obj as TSqlSynExpr;
                                            end;
                                          end; }
        ;

  where_clause_opt :
        /* Empty */
                                        { $$.Text := ''; $$.Obj := nil; }
        |       where_clause
                                        { $$ := $1; }
        ;

  group_by_clause_opt :
        /* Empty */
                                        { $$.Text := ''; $$.Obj := nil; }
        |       group_by_clause
                                        { $$ := $1; }
        ;

  having_clause_opt :
        /* Empty */
                                        { $$.Text := ''; $$.Obj := nil; }
        |       having_clause
                                        { $$ := $1; }
        ;

  from_clause : _FROM from_clause_list
                                        { $$ := $2; }
        ;

  from_clause_list :
                table_reference
                                        { $$.Obj := MakeClass(TTempTpl);
                                          $$.Text := '';
                                          $$.Obj.InsertTailChild($1.Obj); }

        |       from_clause_list comma table_reference
                                        { $$ := $1;
                                          $$.Obj.InsertTailChild($3.Obj); }
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
                                        { $$ := $1; }
        |       table_factor
                                        { $$ := $1; }
        ;

  table_factor :
                table_name
                                        { $$ := $1; }
        |       table_name correlation_specification
                                        { $$ := $2;
                                          $$.Obj.InsertHeadChild($1.Obj);
                                          ($$.Obj as TSqlSynCorrelation).TableLike := $1.Obj as TSqlSynExpr };
        |       derived_table correlation_specification
                                        { $$ := $2;
                                          $$.Obj.InsertHeadChild($1.Obj);
                                          ($$.Obj as TSqlSynCorrelation).TableLike := $1.Obj as TSqlSynExpr };
        |       table_name _AS correlation_specification
                                        { $$ := $3;
                                          $$.Obj.InsertHeadChild($1.Obj);
                                          ($$.Obj as TSqlSynCorrelation).TableLike := $1.Obj as TSqlSynExpr };
        |       derived_table _AS correlation_specification
                                        { $$ := $3;
                                          $$.Obj.InsertHeadChild($1.Obj);
                                          ($$.Obj as TSqlSynCorrelation).TableLike := $1.Obj as TSqlSynExpr };
        ;

  correlation_specification :
		correlation_name derived_column_list_opt
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynCorrelation);
                                          with $$.Obj as TSqlSynCorrelation do
                                          begin
                                            ExprType := setCorrelation;
                                            InsertHeadChild($1.Obj);
                                            CorrelationName := $1.Obj as TSqlSynIdent;
                                            if Assigned($2.Obj) then
                                              FlattenDerivedColList($2.Obj as TTempTpl);
                                          end; }
        ;

  /* No semantic change with this _AS being omitted... */
  as_opt :
        /* Empty */
                                        { $$.Text := ''; $$.Obj := nil; }
        | _AS
                                        { $$.Text := ''; $$.Obj := nil; }
        ;

  derived_column_list_opt :
        /* Empty */
                                        { $$.Text := ''; $$.Obj := nil; }
        |       left_paren derived_column_list right_paren
                                        { $$ := $2; }
        ;

  derived_column_list :
                column_name_list
                                        { $$ := $1; }
        ;

  derived_table :
                table_subquery
                                        { $$ := $1; }
        ;

  table_subquery :
                left_paren query_expression right_paren
                                        { $$ := $2; }
        ;

  joined_table :
		cross_join
                                        { $$ := $1; }
	|       qualified_join
                                        { $$ := $1; }
	|       left_paren joined_table right_paren
                                        { $$ := $2; }
        ;

  cross_join :
        table_reference _CROSS _JOIN table_factor
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinBinary);
                                          with $$.Obj as TSqlSynJoinBinary do
                                          begin
                                            ExprType := setJoin;
                                            JoinType := jtCross;
                                            InsertHeadChild($1.Obj);
                                            Left := $1.Obj as TSqlSynExpr;
                                            InsertTailChild($4.Obj);
                                            Right := $4.Obj as TSqlSynExpr;
                                          end; }
        ;

  qualified_join :
                table_reference _JOIN table_factor join_specification
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinBinary);
                                          with $$.Obj as TSqlSynJoinBinary do
                                          begin
                                            ExprType := setJoin;
                                            JoinType := jtInner;
                                            InsertHeadChild($1.Obj);
                                            Left := $1.Obj as TSqlSynExpr;
                                            InsertTailChild($3.Obj);
                                            Right := $3.Obj as TSqlSynExpr;
                                            InsertTailChild($4.Obj);
                                            Spec := $4.Obj as TSqlSynJoinSpec;
                                          end; }
        |       table_reference _INNER _JOIN table_factor join_specification
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinBinary);
                                          with $$.Obj as TSqlSynJoinBinary do
                                          begin
                                            ExprType := setJoin;
                                            JoinType := jtInner;
                                            InsertHeadChild($1.Obj);
                                            Left := $1.Obj as TSqlSynExpr;
                                            InsertTailChild($4.Obj);
                                            Right := $4.Obj as TSqlSynExpr;
                                            InsertTailChild($5.Obj);
                                            Spec := $5.Obj as TSqlSynJoinSpec;
                                          end; }
        |       table_reference _LEFT outer_opt _JOIN table_factor join_specification
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinBinary);
                                          with $$.Obj as TSqlSynJoinBinary do
                                          begin
                                            ExprType := setJoin;
                                            JoinType := jtLeft;
                                            InsertHeadChild($1.Obj);
                                            Left := $1.Obj as TSqlSynExpr;
                                            InsertTailChild($5.Obj);
                                            Right := $5.Obj as TSqlSynExpr;
                                            InsertTailChild($6.Obj);
                                            Spec := $6.Obj as TSqlSynJoinSpec;
                                            Outer := Boolean($3.Obj);
                                          end; }
        |       table_reference _RIGHT outer_opt _JOIN table_factor join_specification
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinBinary);
                                          with $$.Obj as TSqlSynJoinBinary do
                                          begin
                                            ExprType := setJoin;
                                            JoinType := jtRight;
                                            InsertHeadChild($1.Obj);
                                            Left := $1.Obj as TSqlSynExpr;
                                            InsertTailChild($5.Obj);
                                            Right := $5.Obj as TSqlSynExpr;
                                            InsertTailChild($6.Obj);
                                            Spec := $6.Obj as TSqlSynJoinSpec;
                                            Outer := Boolean($3.Obj);
                                          end; }
        |       table_reference _FULL outer_opt _JOIN table_factor join_specification
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinBinary);
                                          with $$.Obj as TSqlSynJoinBinary do
                                          begin
                                            ExprType := setJoin;
                                            JoinType := jtFull;
                                            InsertHeadChild($1.Obj);
                                            Left := $1.Obj as TSqlSynExpr;
                                            InsertTailChild($5.Obj);
                                            Right := $5.Obj as TSqlSynExpr;
                                            InsertTailChild($6.Obj);
                                            Spec := $6.Obj as TSqlSynJoinSpec;
                                            Outer := Boolean($3.Obj);
                                          end; }
        /* TODO |       table_reference _UNION _JOIN table_factor
           without adding any more conflicts.
        */
        |       table_reference _NATURAL _JOIN table_factor
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinBinary);
                                          with $$.Obj as TSqlSynJoinBinary do
                                          begin
                                            ExprType := setJoin;
                                            JoinType := jtInner;
                                            InsertHeadChild($1.Obj);
                                            Left := $1.Obj as TSqlSynExpr;
                                            InsertTailChild($4.Obj);
                                            Right := $4.Obj as TSqlSynExpr;
                                            Natural := true;
                                          end; }
        |       table_reference _NATURAL _INNER _JOIN table_factor
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinBinary);
                                          with $$.Obj as TSqlSynJoinBinary do
                                          begin
                                            ExprType := setJoin;
                                            JoinType := jtInner;
                                            InsertHeadChild($1.Obj);
                                            Left := $1.Obj as TSqlSynExpr;
                                            InsertTailChild($5.Obj);
                                            Right := $5.Obj as TSqlSynExpr;
                                            Natural := true;
                                          end; }
        |       table_reference _NATURAL _LEFT outer_opt _JOIN table_factor
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinBinary);
                                          with $$.Obj as TSqlSynJoinBinary do
                                          begin
                                            ExprType := setJoin;
                                            JoinType := jtLeft;
                                            InsertHeadChild($1.Obj);
                                            Left := $1.Obj as TSqlSynExpr;
                                            InsertTailChild($6.Obj);
                                            Right := $6.Obj as TSqlSynExpr;
                                            Natural := true;
                                            Outer := Boolean($4.Obj);
                                          end; }
        |       table_reference _NATURAL _RIGHT outer_opt _JOIN table_factor
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinBinary);
                                          with $$.Obj as TSqlSynJoinBinary do
                                          begin
                                            ExprType := setJoin;
                                            JoinType := jtRight;
                                            InsertHeadChild($1.Obj);
                                            Left := $1.Obj as TSqlSynExpr;
                                            InsertTailChild($6.Obj);
                                            Right := $6.Obj as TSqlSynExpr;
                                            Natural := true;
                                            Outer := Boolean($4.Obj);
                                          end; }
        |       table_reference _NATURAL _FULL outer_opt _JOIN table_factor
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinBinary);
                                          with $$.Obj as TSqlSynJoinBinary do
                                          begin
                                            ExprType := setJoin;
                                            JoinType := jtFull;
                                            InsertHeadChild($1.Obj);
                                            Left := $1.Obj as TSqlSynExpr;
                                            InsertTailChild($6.Obj);
                                            Right := $6.Obj as TSqlSynExpr;
                                            Natural := true;
                                            Outer := Boolean($4.Obj);
                                          end; }
        /* TODO |       table_reference _NATURAL _UNION _JOIN table_factor
           without adding any more conflicts.  */
        ;

  outer_opt:
        /* Empty */
                                        { $$.Text := ''; $$.Obj := TSqlSynNode(false); }
        |       _OUTER
                                        { $$.Text := ''; $$.Obj := TSqlSynNode(true); }
        ;

  join_specification :
                join_condition
                                        { $$ := $1; }
        |       named_columns_join
                                        { $$ := $1; }
        ;

  join_condition :
                _ON search_condition
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinSpec);
                                          with $$.Obj as TSqlSynJoinSpec do
                                          begin
                                            StructuralType := sstJoinSpec;
                                            SpecType := stJoinCond;
                                            InsertTailChild($2.Obj);
                                            JoinCondition := $2.Obj as TSqlSynExpr;
                                          end; }
        ;

  named_columns_join :
                _USING left_paren join_column_list right_paren
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynJoinSpec);
                                          with $$.Obj as TSqlSynJoinSpec do
                                          begin
                                            StructuralType := sstJoinSpec;
                                            SpecType := stNamedColsJoin;
                                            FlattenColList($3.Obj as TTempTpl);
                                          end; }
        ;

  join_column_list :
                column_name_list
                                        { $$ := $1; }
        ;

  where_clause :
                _WHERE search_condition
                                        { $$ := $2; }
        ;

  group_by_clause :
                _GROUP _BY grouping_column_reference_list
                                        { $$ := $3; }
        ;

  grouping_column_reference_list :
                grouping_column_reference
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TTempTpl);
                                          $$.Obj.InsertTailChild($1.Obj); }
        |       grouping_column_reference_list comma grouping_column_reference
                                        { $$ := $1;
                                          $$.Obj.InsertTailChild($3.Obj); }
        ;

  grouping_column_reference :
                column_reference collate_clause_opt
                                        { $$ := $1;
                                          Assert(not Assigned($2.Obj)); }
        ;

  collate_clause :
                _COLLATE collation_name
                                        { $$.Text := ''; $$.Obj := nil;
                                          yyerror('Collations not supported'); }
        ;

  collation_name :
                qualified_name
                                        { $$ := $1; }
        ;

  having_clause :
                _HAVING search_condition
                                        { $$ := $2; }
        ;

  table_value_constructor :
                _VALUES table_value_constructor_list
                                        { $$ := $2; }
        ;

  table_value_constructor_list :
                row_value_constructor
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlTableValConstructor);
                                          with $$.Obj as TSqlTableValConstructor do
                                          begin
                                            ExprType := setTValConstructor;
                                            InsertTailChild($1.Obj);
                                          end; }
        |       table_value_constructor_list  comma row_value_constructor
                                        { $$ := $1;
                                          with $$.Obj as TSqlTableValConstructor do
                                          begin
                                            InsertTailChild($3.Obj);
                                          end; }
        ;

  explicit_table : _TABLE table_name
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynExplicitTable);
                                          with $$.Obj as TSqlSynExplicitTable do
                                          begin
                                            ExprType := setExplicitTable;
                                            InsertTailChild($2.Obj);
                                            TableName := $2.Obj as TSqlSynIdent;
                                          end; }
        ;

  query_term :
                non_join_query_term
                                        { $$ := $1; }
        ;

  corresponding_spec : _CORRESPONDING corresponding_column_list_opt
                                        { $$ := $2; }
        ;

  corresponding_column_list_opt :
        /* Empty */
                                        { $$.Text := ''; $$.Obj := nil; }
        |       _BY left_paren corresponding_column_list right_paren
                                        { $$ := $3; }
        ;

  corresponding_column_list :
                column_name_list
                                        { $$ := $1; }
        ;

  query_primary :
                non_join_query_primary
                                        { $$ := $1; }
        ;

/*
--hr
--h2 Query expression components
--/h2
*/

  case_expression :
                case_abbreviation
                                        { $$ := $1; }
        |       case_specification
                                        { $$ := $1; }
        ;

  case_abbreviation :
		_NULLIF left_paren expression comma expression right_paren
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynCaseAbbr);
                                          with $$.Obj as TSqlSynCaseAbbr do
                                          begin
                                            ExprType := setCaseAbbr;
                                            CaseType := scetNullif;
                                            InsertTailChild($3.Obj);
                                            InsertTailChild($5.Obj);
                                            FirstExpr := $3.Obj as TSqlSynExpr;
                                            LastExpr := $5.Obj as TSqlSynExpr;
                                          end; }
	|	_COALESCE left_paren expression_list right_paren
                                        { $$.Text := '';
                                          $$.Obj := MakeClass(TSqlSynCaseAbbr);
                                          with $$.Obj as TSqlSynCaseAbbr do
                                          begin
                                            ExprType := setCaseAbbr;
                                            CaseType := scetCoalesce;
                                            InsertTailChild($3.Obj);
                                            List := $3.Obj;
                                          end; }
        ;

  expression_list :
                expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  ($$.Obj as TTempTpl).InsertTailChild($1.Obj); }
        |       expression_list comma expression
                                                { $$ := $1;
                                                  ($$.Obj as TTempTpl).InsertTailChild($3.Obj); }
        ;

  case_specification :
                simple_case
                                                { $$ := $1; }
        |       searched_case
                                                { $$ := $1; }
        ;

  simple_case :
		_CASE case_operand simple_when_clause else_clause_opt _END
                                                { $$ := $3;
                                                  with $$.Obj as TSqlSynSimpleSearchedCase do
                                                  begin
                                                    InsertHeadChild($2.Obj);
                                                    Operand := $2.Obj as TSqlSynExpr;
                                                    if Assigned($4.Obj) then
                                                    begin
                                                      InsertTailChild($4.Obj);
                                                      _Else := $4.Obj as TSqlSynExpr;
                                                    end;
                                                  end; }
        ;

  else_clause_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := nil; }
        |       else_clause
                                                { $$ := $1; }
        ;

  case_operand :
                expression                      { $$ := $1; }
        ;

  simple_when_clause :
                _WHEN when_operand _THEN result
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSimpleSearchedCase);
                                                  with $$.Obj as TSqlSynSimpleSearchedCase do
                                                  begin
                                                    ExprType := setSimpleSearchedCase;
                                                    SSCaseType := sscetSimple;
                                                    InsertHeadChild($2.Obj);
                                                    Comparator := $2.Obj as TSqlSynExpr;
                                                    InsertTailChild($4.Obj);
                                                    _When := $4.Obj as TSqlSynExpr;
                                                  end; }
        ;

  when_operand :
                expression
                                                { $$ := $1; }
        ;

  result :
                expression /* Which can be NULL specification */
                                                { $$ := $1; }
        ;

  else_clause :
                _ELSE result
                                                { $$ := $2; }
        ;

  searched_case :
		_CASE searched_when_clause else_clause_opt _END
                                                { $$ := $2;
                                                  with $$.Obj as TSqlSynSimpleSearchedCase do
                                                  begin
                                                    if Assigned($3.Obj) then
                                                    begin
                                                      InsertTailChild($3.Obj);
                                                      _Else := $3.Obj as TSqlSynExpr;
                                                    end;
                                                  end; }
        ;

  searched_when_clause :
                _WHEN search_condition _THEN result
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSimpleSearchedCase);
                                                  with $$.Obj as TSqlSynSimpleSearchedCase do
                                                  begin
                                                    ExprType := setSimpleSearchedCase;
                                                    SSCaseType := sscetSearched;
                                                    InsertHeadChild($2.Obj);
                                                    Comparator := $2.Obj as TSqlSynExpr;
                                                    InsertTailChild($4.Obj);
                                                    _When := $4.Obj as TSqlSynExpr;
                                                  end; }
        ;

  cast_specification :
                _CAST left_paren cast_operand _AS cast_target right_paren
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynCastOperator);
                                                  with $$.Obj as TSqlSynCastOperator do
                                                  begin
                                                    ExprType := setCast;
                                                    InsertTailChild($3.Obj);
                                                    Expr := $3.Obj as TSqlSynExpr;
                                                    InsertTailChild($5.Obj);
                                                    CastType := $5.Obj;
                                                  end; }
        ;

  cast_operand :
                expression      /* Which can be null specification */
                                                {$$ := $1; }
        ;

  cast_target :
                domain_name
                                                { $$ := $1; }
        |       data_type
                                                { $$ := $1; }
        ;

  numeric_value_function :
                position_expression
                                                { $$ := $1; }
        |       extract_expression
                                                { $$ := $1; }
        |       length_expression
                                                { $$ := $1; }
        ;

  position_expression :
	        _POSITION left_paren expression _IN expression right_paren
                /* TODO - check char-ness / bit-ness */
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynBinaryNumericFunction);
                                                  with $$.Obj as TSqlSynBinaryNumericFunction do
                                                  begin
                                                    ExprType := setNumericFunction;
                                                    FuncType:= nfPosition;
                                                    InsertTailChild($3.Obj);
                                                    Expr := $3.Obj as TSqlSynExpr;
                                                    InsertTailChild($5.Obj);
                                                    SecondExpr := $5.Obj as TSqlSynExpr;
                                                  end; }
        ;


  character_value_function :
	    character_bit_substring_function
                                                { $$ := $1; }
	|   fold
                                                { $$ := $1; }
	|   form_of_use_conversion
                                                { $$ := $1; }
	|   character_translation
                                                { $$ := $1; }
	|   trim_function
                                                { $$ := $1; }
        ;

  character_bit_substring_function :
		_SUBSTRING left_paren expression
                _FROM start_position for_strlength_opt right_paren
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynCharacterFunction);
                                                  with $$.Obj as TSqlSynCharacterFunction do
                                                  begin
                                                    ExprType := setCharacterFunction;
                                                    FuncType := cfsSubString;
                                                    InsertTailChild($3.Obj);
                                                    First := $3.Obj as TSqlSynExpr;
                                                    InsertTailChild($5.Obj);
                                                    if Assigned($6.Obj) then
                                                    begin
                                                      Second := $6.Obj as TSqlSynExpr;
                                                      InsertTailChild($6.Obj);
                                                    end;
                                                  end; }
        ;

  for_strlength_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := nil; }
        |       _FOR string_length              { $$ := $2; }
        ;

  start_position : expression
                                                { $$ := $1; }
        ;

  string_length : expression
                                                { $$ := $1; }
        ;

  fold :
                _UPPER left_paren expression right_paren
                                                {  $$.Text := '';
                                                   $$.Obj := MakeClass(TSqlSynCharacterFunction);
                                                   with $$.Obj as TSqlSynCharacterFunction do
                                                   begin
                                                     ExprType := setCharacterFunction;
                                                     FuncType := cfsFold;
                                                     FoldType := sftUpper;
                                                     InsertTailChild($3.Obj);
                                                     First := $3.Obj as TSqlSynExpr;
                                                   end; }
        |       _LOWER left_paren expression right_paren
                                                {  $$.Text := '';
                                                   $$.Obj := MakeClass(TSqlSynCharacterFunction);
                                                   with $$.Obj as TSqlSynCharacterFunction do
                                                   begin
                                                     ExprType := setCharacterFunction;
                                                     FuncType := cfsFold;
                                                     FoldType := sftLower;
                                                     InsertTailChild($3.Obj);
                                                     First := $3.Obj as TSqlSynExpr;
                                                   end; }
        ;

  form_of_use_conversion :
		_CONVERT left_paren expression
                _USING form_of_use_conversion_name right_paren
                                                { $$.Text := ''; $$.Obj := nil;
                                                  yyerror('Form of use conversions not supported.'); }
        ;

  form_of_use_conversion_name : qualified_name
                                                { $$ := $1; }
        ;

  character_translation :
		_TRANSLATE left_paren expression _USING translation_name right_paren
                                                { $$.Text := ''; $$.Obj := nil;
                                                  yyerror('Character translations not supported.'); }
        ;

  translation_name : qualified_name
                                                { $$ := $1; }
        ;

  trim_function :
                _TRIM left_paren trim_operands right_paren
                                                {  $$ := $3; }
        ;

  trim_operands :
                trim_source
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynCharacterFunction);
                                                  with $$.Obj as TSqlSynCharacterFunction do
                                                  begin
                                                     ExprType := setCharacterFunction;
                                                     FuncType := cfsTrim;
                                                     InsertTailChild($1.Obj);
                                                     First := $1.Obj as TSqlSynExpr;
                                                  end; }
        |       trim_specification _FROM trim_source
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynCharacterFunction);
                                                  with $$.Obj as TSqlSynCharacterFunction do
                                                  begin
                                                     ExprType := setCharacterFunction;
                                                     FuncType := cfsTrimSpec;
                                                     InsertTailChild($1.Obj);
                                                     First := $1.Obj;
                                                     InsertTailChild($3.Obj);
                                                     Second := $3.Obj as TSqlSynExpr;
                                                  end; }
        |       trim_character _FROM trim_source
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynCharacterFunction);
                                                  with $$.Obj as TSqlSynCharacterFunction do
                                                  begin
                                                     ExprType := setCharacterFunction;
                                                     FuncType := cfsTrimChar;
                                                     InsertTailChild($1.Obj);
                                                     First := $1.Obj as TSqlSynExpr;
                                                     InsertTailChild($3.Obj);
                                                     Second := $3.Obj as TSqlSynExpr;
                                                  end; }
        |       trim_specification trim_character _FROM trim_source
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynCharacterFunction);
                                                  with $$.Obj as TSqlSynCharacterFunction do
                                                  begin
                                                     ExprType := setCharacterFunction;
                                                     FuncType := cfsTrim;
                                                     InsertTailChild($1.Obj);
                                                     First := $1.Obj;
                                                     InsertTailChild($2.Obj);
                                                     Second := $2.Obj as TSqlSynExpr;
                                                     InsertTailChild($4.Obj);
                                                     Third := $4.Obj as TSqlSynExpr;
                                                  end; }
  ;

  trim_specification :
                _LEADING
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynTrimSpecification);
                                                  with $$.Obj as TSqlSynTrimSpecification do
                                                  begin
                                                    SpecType := sttLeading;
                                                  end; }
        |       _TRAILING
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynTrimSpecification);
                                                  with $$.Obj as TSqlSynTrimSpecification do
                                                  begin
                                                    SpecType := sttTrailing;
                                                  end; }
        |       _BOTH
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynTrimSpecification);
                                                  with $$.Obj as TSqlSynTrimSpecification do
                                                  begin
                                                    SpecType := sttBoth;
                                                  end; }
  ;

  trim_character :
                expression /* Check char/bit */ { $$ := $1; }
        ;

  trim_source :
                expression /* Check char/bit */ { $$ := $1; }
        ;

  extract_expression :
                _EXTRACT left_paren extract_field _FROM extract_source right_paren
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynExtractFunction);
                                                  with $$.Obj as TSqlSynExtractFunction do
                                                  begin
                                                    ExprType := setNumericFunction;
                                                    FuncType:= nfExtract;
                                                    ExtractField := TSQLSynQualField($3.Obj);
                                                    InsertTailChild($5.Obj);
                                                    Expr := $5.Obj as TSqlSynExpr;
                                                  end; }
        ;

  extract_field :
                datetime_field
                                                { $$ := $1; }
        |       time_zone_field
                                                { $$ := $1; }
        ;

  datetime_field :
                non_second_datetime_field
                                                { $$ := $1; }
        |       _SECOND
                                                { $$.Text := ''; $$.Obj := TSQLSynNode(sqfSecond); }
        ;

  time_zone_field :
                _TIMEZONE_HOUR
                                                { $$.Text := ''; $$.Obj := TSQLSynNode(sqfHour); }
        |       _TIMEZONE_MINUTE
                                                { $$.Text := ''; $$.Obj := TSQLSynNode(sqfMinute); }
        ;

  extract_source :
                expression
                                                { $$ := $1; }
        ;

  time_zone :
                _AT time_zone_specifier
                                                { $$ := $2; //Not impl. }
        ;

  time_zone_specifier :
                _LOCAL
                                                { $$.Text := ''; $$.Obj := nil; //Not impl. }
        |       _TIME _ZONE expression
                                                { $$ := $3; //Not impl. }
        ;

  length_expression :
                char_length_expression
                                                { $$ := $1; }
        |       octet_length_expression
                                                { $$ := $1; }
        |       bit_length_expression
                                                { $$ := $1; }
        ;

  char_length_expression :
                char_length_specifier left_paren expression right_paren
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynUnaryNumericFunction);
                                                  with $$.Obj as TSQLSynUnaryNumericFunction do
                                                  begin
                                                    ExprType := setNumericFunction;
                                                    FuncType := nfLength;
                                                    LengthSubtype := lfsChar;
                                                    InsertTailChild($3.Obj);
                                                    Expr := $3.Obj as TSqlSynExpr;
                                                  end; }
        ;

  char_length_specifier :
                _CHAR_LENGTH
                                                { $$.Text := ''; $$.Obj := nil; }
        |       _CHARACTER_LENGTH
                                                { $$.Text := ''; $$.Obj := nil; }
        ;

  octet_length_expression :
                _OCTET_LENGTH left_paren expression right_paren
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynUnaryNumericFunction);
                                                  with $$.Obj as TSQLSynUnaryNumericFunction do
                                                  begin
                                                    ExprType := setNumericFunction;
                                                    FuncType := nfLength;
                                                    LengthSubtype := lfsOctet;
                                                    InsertTailChild($3.Obj);
                                                    Expr := $3.Obj as TSqlSynExpr;
                                                  end; }
        ;

  bit_length_expression :
                _BIT_LENGTH left_paren expression right_paren
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSQLSynUnaryNumericFunction);
                                                  with $$.Obj as TSQLSynUnaryNumericFunction do
                                                  begin
                                                    ExprType := setNumericFunction;
                                                    FuncType := nfLength;
                                                    LengthSubtype := lfsBit;
                                                    InsertTailChild($3.Obj);
                                                    Expr := $3.Obj as TSqlSynExpr;
                                                  end; }
        ;

  null_specification : _NULL
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSpecification);
                                                  with $$.Obj as TSqlSynSpecification do
                                                  begin
                                                    ExprType := setSpecification;
                                                    SpecType := sstNull;
                                                  end; }
        ;

  default_specification : _DEFAULT
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSpecification);
                                                  with $$.Obj as TSqlSynSpecification do
                                                  begin
                                                    ExprType := setSpecification;
                                                    SpecType := sstDefault;
                                                  end; }
        ;

  row_value_constructor_list :
                expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlRowValConstructor);
                                                  with $$.Obj as TSqlRowValConstructor do
                                                  begin
                                                    ExprType := setRValConstructor;
                                                    InsertTailChild($1.Obj);
                                                  end; }
        |       row_value_constructor_list comma expression
                                                { $$ := $1;
                                                  with $$.Obj as TSqlRowValConstructor do
                                                    InsertTailChild($3.Obj); }
        ;

  comp_op :
	    equals_operator
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(coEquals); }
	|   not_equals_operator
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(coNotEquals); }
	|   less_than_operator
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(coLessThan); }
	|   greater_than_operator
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(coGreaterThan); }
	|   less_than_or_equals_operator
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(coLessThanOrEq); }
	|   greater_than_or_equals_operator
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(coGreaterThanOrEq); }
        ;

  between_predicate :
		row_value_constructor _BETWEEN row_value_constructor _AND row_value_constructor
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSyn3VPredicate);
                                                  with $$.Obj as TSqlSyn3VPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptBetweenPredicate;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    Middle := $3.Obj as TSqlSynExpr;
                                                    InsertTailChild($5.Obj);
                                                    Right := $5.Obj as TSqlSynExpr;
                                                  end; }

	|	row_value_constructor _NOT _BETWEEN row_value_constructor _AND row_value_constructor
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSyn3VPredicate);
                                                  with $$.Obj as TSqlSyn3VPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptNotBetweenPredicate;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($4.Obj);
                                                    Middle := $4.Obj as TSqlSynExpr;
                                                    InsertTailChild($6.Obj);
                                                    Right := $6.Obj as TSqlSynExpr;
                                                  end; }
        ;

  in_predicate :
                row_value_constructor _IN in_predicate_value
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynInPredicate);
                                                  with $$.Obj as TSqlSynInPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptInPredicate;
                                                    InsertTailChild($1.Obj);
                                                    Expr := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    InList := $3.Obj;
                                                  end; }
        |        row_value_constructor _NOT _IN in_predicate_value
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynInPredicate);
                                                  with $$.Obj as TSqlSynInPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptNotInPredicate;
                                                    InsertTailChild($1.Obj);
                                                    Expr := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($4.Obj);
                                                    InList := $4.Obj;
                                                  end; }
        ;

  in_predicate_value :
                table_subquery
                                                { $$ := $1; }
        |       left_paren in_value_list right_paren
                                                { $$ := $2; }
        ;

  in_value_list :
                expression                      { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                        InsertTailChild($1.Obj); }
        |       in_value_list comma expression
                                                { $$ := $1;
                                                  with $$.Obj as TTempTpl do
                                                        InsertTailChild($3.Obj); }
        ;

  like_predicate : /* Check char-bit expressions */
                expression _LIKE pattern like_predicate_escape_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSyn3VPredicate);
                                                  with $$.Obj as TSqlSyn3VPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptLikePredicate;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    Middle := $3.Obj as TSqlSynExpr;
                                                    if Assigned($4.obj) then
                                                    begin
                                                      InsertTailChild($4.Obj);
                                                      Right := $4.Obj as TSqlSynExpr;
                                                    end;
                                                  end; }
        |       expression _NOT _LIKE pattern like_predicate_escape_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSyn3VPredicate);
                                                  with $$.Obj as TSqlSyn3VPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptNotLikePredicate;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($4.Obj);
                                                    Middle := $4.Obj as TSqlSynExpr;
                                                    if Assigned($5.obj) then
                                                    begin
                                                      InsertTailChild($5.Obj);
                                                      Right := $5.Obj as TSqlSynExpr;
                                                    end;
                                                  end; }
        ;

  like_predicate_escape_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := nil; }
        |       _ESCAPE escape_character
                                                { $$ := $2; }
        ;

  pattern : expression /* Check char-bit */
                                                { $$ := $1; }
        ;

  escape_character : expression /* Check char-bit */
                                                { $$ := $1; }
        ;

  null_predicate :
                row_value_constructor _IS _NULL
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSyn1VPredicate);
                                                  with $$.Obj as TSqlSyn1VPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptNullPredicate;
                                                    InsertTailChild($1.Obj);
                                                    Expr := $1.Obj as TSqlSynExpr;
                                                  end; }
        |       row_value_constructor _IS _NOT _NULL
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSyn1VPredicate);
                                                  with $$.Obj as TSqlSyn1VPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptNotNullPredicate;
                                                    InsertTailChild($1.Obj);
                                                    Expr := $1.Obj as TSqlSynExpr;
                                                  end; }
        ;

  quantified_comparison_predicate : row_value_constructor comp_op quantifier table_subquery
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynQuantComparisonPredicate);
                                                  with $$.Obj as TSqlSynQuantComparisonPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptQuantifiedComparisonPredicate;
                                                    CompOp := TSqlSynCompOp($2.Obj);
                                                    Quantifier := TSqlQuantifierType($3.Obj);
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($4.Obj);
                                                    Right :=$4.Obj as TSqlSynExpr;
                                                  end; }
        ;

  quantifier :
                all                             { $$ := $1; }
        |       some                            { $$ := $1; }
        ;

  all : _ALL                                    { $$.Text := ''; $$.Obj := TSqlSynNode(sqtAll); }
        ;

  some : _SOME                                  { $$.Text := ''; $$.Obj := TSqlSynNode(sqtSome); }
        | _ANY                                  { $$.Text := ''; $$.Obj := TSqlSynNode(sqtAny); }
        ;

  exists_predicate : _EXISTS table_subquery     { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSyn1VPredicate);
                                                  with $$.Obj as TSqlSyn1VPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptExistsPredicate;
                                                    InsertTailChild($2.Obj);
                                                    Expr := $2.Obj as TSqlSynExpr;
                                                  end; }
        ;

  unique_predicate : _UNIQUE table_subquery     { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSyn1VPredicate);
                                                  with $$.Obj as TSqlSyn1VPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptUniquePredicate;
                                                    InsertTailChild($2.Obj);
                                                    Expr := $2.Obj as TSqlSynExpr;
                                                  end; }
        ;

  match_predicate : row_value_constructor _MATCH unique_opt partial_full_opt table_subquery
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynMatchPredicate);
                                                  with $$.Obj as TSqlSynMatchPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptMatchPredicate;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($5.Obj);
                                                    Right := $5.Obj as TSqlSynExpr;
                                                    UniqueOpt := TSqlSynUniqueOpt($3.Obj);
                                                    PartialOpt := TSqlSynPartialOpt($4.Obj);
                                                  end; }
        ;

  unique_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := TSqlSynNode(suoNone); }
        |       _UNIQUE                         { $$.Text := ''; $$.Obj := TSqlSynNode(suoUnique); }
        ;

  partial_full_opt:
        /* Empty */                             { $$.Text := ''; $$.Obj := TSqlSynNode(spoNone); }
        |       _PARTIAL                        { $$.Text := ''; $$.Obj := TSqlSynNode(spoPartial); }
        |       _FULL                           { $$.Text := ''; $$.Obj := TSqlSynNode(spoFull); }
        ;

  overlaps_predicate : row_value_constructor_1 _OVERLAPS row_value_constructor_2
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSyn2VPredicate);
                                                  with $$.Obj as TSqlSyn2VPredicate do
                                                  begin
                                                    ExprType := setPredicate;
                                                    PredicateType := sptOverlapsPredicate;
                                                    InsertTailChild($1.Obj);
                                                    Left := $1.Obj as TSqlSynExpr;
                                                    InsertTailChild($3.Obj);
                                                    Right := $3.Obj as TSqlSynExpr;
                                                  end; }
        ;

  row_value_constructor_1 : row_value_constructor
                                                { $$ := $1; }
        ;

  row_value_constructor_2 : row_value_constructor
                                                { $$ := $1; }
        ;

  truth_value :
                _TRUE
                                                { $$.Text := UpperCase(UTF8ToString(Lexer.yytext));
                                                  $$.Obj := MakeClass(TSqlSynTruthValue);
                                                  with $$.Obj as TSqlSynTruthValue do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltTruthValue;
                                                    TruthValue := tvtTrue;
                                                    Text := $$.text;
                                                  end; }
        |       _FALSE
                                                { $$.Text := UpperCase(UTF8ToString(Lexer.yytext));
                                                  $$.Obj := MakeClass(TSqlSynTruthValue);
                                                  with $$.Obj as TSqlSynTruthValue do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltTruthValue;
                                                    TruthValue := tvtTrue;
                                                    Text := $$.text;
                                                  end; }
        |       _UNKNOWN
                                                { $$.Text := UpperCase(UTF8ToString(Lexer.yytext));
                                                  $$.Obj := MakeClass(TSqlSynTruthValue);
                                                  with $$.Obj as TSqlSynTruthValue do
                                                  begin
                                                    ExprType := setLiteral;
                                                    LitType := sltTruthValue;
                                                    TruthValue := tvtTrue;
                                                    Text := $$.text;
                                                  end; }
        ;

/*
--hr
--h2 More about constraints
--/h2
*/

  constraint_attributes_opt :
        /* Empty */
                                        { $$.Text := ''; $$.Obj := nil; }
        | constraint_attributes
                                        { $$ := $1; }
        ;

  constraint_attributes :
		constraint_check_time deferrable_opt
                                        {  $$.Text := '';
                                           $$.Obj := MakeClass(TSqlSynConstraintAttributes);
                                           with ($$.Obj as TSqlSynConstraintAttributes) do
                                           begin
                                             StructuralType := sstConstraintAttributes;
                                             InitDeferred := TSqlSynInitDeferred($1.Obj);
                                             Deferrable := TSqlSynDeferrable($2.Obj);
                                           end; }
	|	_DEFERRABLE constraint_check_time_opt
                                        {  $$.Text := '';
                                           $$.Obj := MakeClass(TSqlSynConstraintAttributes);
                                           with ($$.Obj as TSqlSynConstraintAttributes) do
                                           begin
                                             StructuralType := sstConstraintAttributes;
                                             Deferrable := ssdDeferrable;
                                             InitDeferred := TSqlSynInitDeferred($2.Obj);
                                           end; }
        ;

  deferrable_opt :
        /* Empty */
                                        { $$.Text := ''; $$.Obj := TSqlSynNode(ssdUnspec); }
        |       _DEFERRABLE
                                        { $$.Text := ''; $$.Obj := TSqlSynNode(ssdDeferrable); }
        |       _NOT _DEFERRABLE
                                        { $$.Text := ''; $$.Obj := TSqlSynNode(ssdNotDeferrable); }
        ;

  constraint_check_time_opt :
        /* Empty */
                                        { $$.Text := ''; $$.Obj := TSqlSynNode(sidUnspec); }
        |       constraint_check_time
                                        { $$ := $1; }
        ;

  constraint_check_time :
                _INITIALLY _DEFERRED
                                        { $$.Text := ''; $$.Obj := TSqlSynNode(sidInitDeferred); }
        |       _INITIALLY _IMMEDIATE
                                        { $$.Text := ''; $$.Obj := TSqlSynNode(sidNotInitDeferred); }
        ;


  table_constraint_definition :
                constraint_name_definition_opt
                table_constraint
                table_constraint_attributes_opt
                                                { //NB. Arbitary check constraints not supported
                                                  //at this time, so building this part of the tree is
                                                  //optional...
                                                  if Assigned($2.Obj) then
                                                  begin
                                                    $$.Text := '';
                                                    //Always relational constraint, always provided column list.
                                                    $$.Obj := MakeClass(TSqlSynConstraint);
                                                    with $$.Obj as TSqlSynConstraint do
                                                    begin
                                                      StructuralType := sstConstraint;
                                                      ConstraintType := sctTable;
                                                      if Assigned($1.Obj) then
                                                      begin
                                                        InsertTailChild($1.Obj);
                                                        Name := $1.Obj as TSqlSynIdent;
                                                      end;
                                                      //This is a table constraint so at this point,
                                                      //can take columns from column list, instead of later
                                                      //fixup.
                                                      TmpClass := $2.Obj.FirstChild; // Column list.
                                                      FlattenReffingCols(TmpClass as TTempTpl);
                                                      TmpClass := $2.Obj.LastChild; // Constraint details.
                                                      TmpClass.RemoveFromTree;
                                                      InsertTailChild(TmpClass);
                                                      Detail := TmpClass as TSqlSynConstraintDetail;
                                                      if Assigned($3.Obj) then
                                                      begin
                                                        InsertTailChild($3.Obj);
                                                        Attributes := $3.Obj as TSqlSynConstraintAttributes;
                                                      end;
                                                    end;
                                                    $2.Obj.Free;
                                                  end
                                                  else
                                                  begin
                                                    $$.Text := '';
                                                    $$.Obj := nil;
                                                    $1.Obj.Free;
                                                    $3.Obj.Free;
                                                  end; }
        ;

  table_constraint_attributes_opt :
                constraint_check_time_opt
                                        {  $$.Text := '';
                                           if $1.Obj <> TSqlSynNode(sidUnspec) then
                                           begin
                                             $$.Obj := MakeClass(TSqlSynConstraintAttributes);
                                             with ($$.Obj as TSqlSynConstraintAttributes) do
                                             begin
                                               StructuralType := sstConstraintAttributes;
                                               InitDeferred := TSqlSynInitDeferred($1.Obj);
                                             end;
                                           end
                                           else
                                             $1.Obj := nil; }
        ;

  table_constraint :
		unique_constraint_definition            { $$ := $1; }
	|	referential_constraint_definition       { $$ := $1; }
	|	check_constraint_definition
                                                { $$.Text := '';
                                                  $$.Obj := nil;
                                                  yyinfo('Arbitrary check constraints not supported at thhis time');
                                                  $1.Obj.Free;}
        ;

  unique_constraint_definition :
                unique_specification left_paren unique_column_list right_paren
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($3.Obj); //Column list.
                                                    InsertTailChild($1.Obj); //Unique specification.
                                                  end; }
        ;

  unique_column_list :
                column_name_list
                                                { $$ := $1; }
        ;

  referential_constraint_definition :
		_FOREIGN _KEY left_paren referencing_columns right_paren references_specification
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($4.Obj); //Column list.
                                                    InsertTailChild($6.Obj); //References specification.
                                                  end; }
        ;

  referencing_columns :
                        reference_column_list
                                                { $$ := $1; }
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
		_DECLARE cursor_name insensitive_opt scroll_opt _CURSOR _FOR cursor_specification
                                                { $$ := $7;
                                                  with $$.Obj as TSqlSynDeclCursor do
                                                  begin
                                                    InsertHeadChild($2.Obj);
                                                    Name := $2.Obj as TSqlSynIdent;
                                                    Insensitive := Boolean($3.Obj);
                                                    Scroll := Boolean($4.Obj);
                                                  end; }
        ;

  insensitive_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := TSqlSynNode(false); }
        |       _INSENSITIVE                    { $$.Text := ''; $$.Obj := TSqlSynNode(true); }
        ;

  scroll_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := TSqlSynNode(false); }
        |       _SCROLL                         { $$.Text := ''; $$.Obj := TSqlSynNode(true); }
        ;

  cursor_name : identifier
                                                { $$ := $1; }
        ;

  cursor_specification : query_expression order_by_clause_opt updatability_clause_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynDeclCursor);
                                                  with $$.Obj as TSqlSynDeclCursor do
                                                  begin
                                                    StructuralType := sstCreateOrDecl;
                                                    CreateorDeclType := codCursor;
                                                    InsertTailChild($1.Obj);
                                                    Query := $1.Obj as TSqlSynExpr;
                                                    if Assigned($2.Obj) then
                                                    begin
                                                      InsertTailChild($2.Obj);
                                                      OrderBy := $2.Obj;
                                                    end;
                                                    if Assigned($3.Obj) then
                                                    begin
                                                      InsertTailChild($3.Obj);
                                                      Updatability := $3.Obj;
                                                    end;
                                                  end; }
        ;

  order_by_clause_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := nil; }
        |       _ORDER _BY sort_specification_list
                                                { $$ := $3; }
        ;

  sort_specification_list :
                sort_specification              { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($1.Obj);
                                                  end; }
        |       sort_specification_list comma sort_specification
                                                { $$ := $1;
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($3.Obj);
                                                  end; }
        ;

  sort_specification : sort_key collate_clause_opt ordering_specification_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSortSpecification);
                                                  with $$.Obj as TSqlSynSortSpecification do
                                                  begin
                                                    InsertTailChild($1.Obj);
                                                    SortKey := $1.Obj;
                                                    Assert(not Assigned($2.Obj));
                                                    //No collations yet.
                                                    OrderingSpecType :=
                                                        TSqlSynOrderingSpecType($3.Obj);
                                                  end; }
        ;

  sort_key :
                column_name                     { $$ := $1; }
        |       unsigned_integer                { $$ := $1; }
        ;

  ordering_specification_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := TSqlSynNode(ostNone); }
        |       _ASC                            { $$.Text := ''; $$.Obj := TSqlSynNode(ostAscending); }
        |       _DESC                           { $$.Text := ''; $$.Obj := TSqlSynNode(ostDescending); }
        ;

  updatability_clause_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := nil; }
        |       _FOR _READ _ONLY                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynUpdatability);
                                                  with $$.Obj as TSqlSynUpdatability do
                                                  begin
                                                    StructuralType := sstUpdatability;
                                                    ForUpdate := false;
                                                  end; }
        |       _FOR _UPDATE updatability_column_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynUpdatability);
                                                  with $$.Obj as TSqlSynUpdatability do
                                                  begin
                                                    StructuralType := sstUpdatability;
                                                    ForUpdate := true;
                                                    if Assigned($3.Obj) then
                                                    begin
                                                      InsertTailChild($3.Obj);
                                                      Columns := $3.Obj;
                                                    end;
                                                  end; }
        ;

  updatability_column_opt:
        /* Empty */                             { $$.Text := ''; $$.Obj := nil; }
        |       _OF column_name_list            { $$ := $2; }
        ;

/*
--hr
--h2 SQL Procedures
--/h2
*/

  procedure :
	_PROCEDURE procedure_name parameter_declaration_list
        semicolon SQL_procedure_statement semicolon
                                                {  $$.Text := '';
                                                   $$.Obj := MakeClass(TSqlSynProcedure);
                                                   with $$.Obj as TSqlSynProcedure do
                                                   begin
                                                     StructuralType := sstProcedure;
                                                     InsertHeadChild($2.Obj);
                                                     Name := $2.Obj as TSqlSynIdent;
                                                     InsertTailChild($3.Obj);
                                                     ParamDecls := $3.Obj;
                                                     InsertTailChild($5.Obj);
                                                     Statement := $5.Obj;
                                                   end; }
        ;

  procedure_name : identifier                   { $$ := $1; }
        ;

  parameter_declaration_list :
		left_paren parameter_declarations right_paren
                                                { $$ := $2; }
        ;

  parameter_declarations :
                parameter_declaration           { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($1.Obj);
                                                  end; }
        |       parameter_declarations comma parameter_declaration
                                                { $$ := $1;
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($3.Obj);
                                                  end; }
        ;

  parameter_declaration :
                parameter_name data_type
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynParameterDecl);
                                                  with $$.Obj as TSqlSynParameterDecl do
                                                  begin
                                                    StructuralType := sstParameterDecl;
                                                    InsertTailChild($1.Obj);
                                                    Name := $1.Obj as TSqlSynIdent;
                                                    InsertTailChild($2.Obj);
                                                    DataType := $2.Obj as TSQLSynType;
                                                  end; }
        |       status_parameter                { $$ := $1; }
        ;

  status_parameter :
                _SQLCODE                        { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynStatusParamDecl);
                                                  with $$.Obj as TSqlSynStatusParamDecl do
                                                  begin
                                                    StructuralType := sstStatusParamDecl;
                                                    SqlCode := true;
                                                  end; }
        |       _SQLSTATE                       { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynStatusParamDecl);
                                                  with $$.Obj as TSqlSynStatusParamDecl do
                                                  begin
                                                    StructuralType := sstStatusParamDecl;
                                                    SqlState := true;
                                                  end; }
        ;

  SQL_procedure_statement :
		SQL_schema_statement            { $$ := $1; }
	|	SQL_data_statement              { $$ := $1; }
	|	SQL_transaction_statement       { $$ := $1; }
	|	SQL_connection_statement        { $$ := $1; }
	|	SQL_session_statement           { $$ := $1; }
        ;

/*
--hr
--h2 SQL Schema Definition Statements
--/h2
*/

  SQL_schema_statement :
		SQL_schema_definition_statement         { $$ := $1; }
	|	SQL_schema_manipulation_statement       { $$ := $1; }
        ;

  /* TODO - Ask The AI about where nil's here might percolate to,
     and in fact more generally cases where ignored code generate NIL's
     in the tree. */

  SQL_schema_definition_statement :
		schema_definition                       { $$ := $1; }
	|	table_definition                        { $$ := $1; }
	|	view_definition                         { $$ := $1; }
	|	grant_statement                         { $$ := $1; }
	|	domain_definition                       { $$ := $1; }
	|	character_set_definition                { $$.Text := ''; $$.Obj := nil;
                                                          yyinfo('Character set definition in schema definition ignored.'); }
	|	collation_definition                    { $$.Text := ''; $$.Obj := nil;
                                                          yyerror('Collation definitions not supported in schema definition'); }
	|	translation_definition                  { $$.Text := ''; $$.Obj := nil;
                                                          yyerror('Translation definitions not supported in schema definition'); }
	|	assertion_definition                    { $$ := $1; }
        ;

  schema_definition :
		_CREATE _SCHEMA schema_name_clause
			schema_character_set_specification_opt
			schema_elements
                                                        { $$ := $3;
                                                          with $$.Obj as TSqlSynSchema do
                                                          begin
                                                            InsertTailChild($5.Obj);
                                                            Elements := $5.Obj;
                                                          end; }
        ;

  schema_character_set_specification_opt :
        /* Empty */                                     { $$.Text := ''; $$.Obj := nil; }
        |       schema_character_set_specification
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          yyinfo('Character set specification ignored in schema definition.');
                                                          $1.Obj.Free; }
        ;

  /* TODO - Handle NIL Schema elements. */
  schema_elements :
                schema_element                          { $$.Text := '';
                                                          $$.Obj := MakeClass(TTempTpl);
                                                          with $$.Obj as TTempTpl do
                                                          begin
                                                            InsertTailChild($1.Obj);
                                                          end; }
        |       schema_elements schema_element          { $$ := $1;
                                                          with $$.Obj as TTempTpl do
                                                          begin
                                                            InsertTailChild($2.Obj);
                                                          end; }
        ;

  schema_name_clause :
		schema_name
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSchema);
                                                  with $$.Obj as TSqlSynSchema do
                                                  begin
                                                    StructuralType := sstCreateorDecl;
                                                    CreateOrDeclType := codSchema;
                                                    InsertHeadChild($1.Obj);
                                                    Name := $1.Obj as TSqlSynIdent;
                                                  end; }
	|	_AUTHORIZATION schema_authorization_identifier
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSchema);
                                                  with $$.Obj as TSqlSynSchema do
                                                  begin
                                                    StructuralType := sstCreateorDecl;
                                                    CreateOrDeclType := codSchema;
                                                    InsertTailChild($2.Obj);
                                                    Authorization := $2.Obj;
                                                  end; }
	|	schema_name _AUTHORIZATION schema_authorization_identifier
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSchema);
                                                  with $$.Obj as TSqlSynSchema do
                                                  begin
                                                    StructuralType := sstCreateorDecl;
                                                    CreateOrDeclType := codSchema;
                                                    InsertHeadChild($1.Obj);
                                                    Name := $1.Obj as TSqlSynIdent;
                                                    InsertTailChild($3.Obj);
                                                    Authorization := $3.Obj;
                                                  end; }
        ;

  schema_authorization_identifier : authorization_identifier
                                                { $$ := $1; }
        ;

  schema_character_set_specification : _DEFAULT _CHARACTER _SET character_set_specification
                                                { $$ := $4; }
        ;

  /* TODO - Ask The AI about where nil's here might percolate to. */
  schema_element :
		domain_definition                       { $$ := $1; }
	|	table_definition                        { $$ := $1; }
	|	view_definition                         { $$ := $1; }
	|	grant_statement                         { $$ := $1; }
	|	assertion_definition                    { $$ := $1; }
	|	character_set_definition                { $$.Text := ''; $$.Obj := nil;
                                                          yyinfo('Character set definition in schema ignored.'); }
	|	collation_definition                    { $$.Text := ''; $$.Obj := nil;
                                                          yyerror('Collation definitions not supported in schema'); }
	|	translation_definition                  { $$.Text := ''; $$.Obj := nil;
                                                          yyerror('Translation definitions not supported in schema'); }
        ;

  domain_definition :
		_CREATE _DOMAIN domain_name as_opt data_type
			default_clause_opt domain_constraint_opt collate_clause_opt
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynDomain);
                                                          with $$.Obj as TSqlSynDomain do
                                                          begin
                                                            StructuralType := sstCreateorDecl;
                                                            CreateOrDeclType := codDomain;
                                                            InsertTailChild($3.Obj);
                                                            Name := $3.Obj as TSqlSynIdent;
                                                            InsertTailChild($5.Obj);
                                                            DataType := $5.Obj;
                                                            if Assigned($6.Obj) then
                                                            begin
                                                              InsertTailChild($6.Obj);
                                                              _Default := $6.Obj;
                                                            end;
                                                            if Assigned($7.Obj) then
                                                            begin
                                                              InsertTailChild($7.obj);
                                                              Constraint := $7.Obj;
                                                              //TODO - Do we support any of the contraints
                                                              //you could impose on a domain at the moment?
                                                            end;
                                                            Assert(not Assigned($8.Obj));
                                                          end; }
        ;

  domain_constraint_opt :
        /* Empty */
                                                { $$.Text := ''; $$.Obj := nil; }
        |       domain_constraint
                                                { $$ := $1; }
        ;

  domain_constraint :
		constraint_name_definition_opt check_constraint_definition constraint_attributes_opt
                                                { $$.Text := ''; $$.Obj := nil;
                                                  yyinfo('Arbitrary check constraints not supported at this time');
                                                  $1.Obj.Free;
                                                  $2.Obj.Free;
                                                  $3.Obj.Free; }
        ;

  table_definition :
		_CREATE table_definition_opts _TABLE
                table_name
                table_element_list
                table_commit_opts
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynCreateOrDeclTable);
                                                  with $$.Obj as TSqlSynCreateOrDeclTable do
                                                  begin
                                                    StructuralType := sstCreateOrDecl;
                                                    CreateOrDeclType := codTable;
                                                    TempOpts := TSqlSynCreateOrDeclTableOpts($2.Obj);
                                                    InsertTailChild($4.Obj);
                                                    Name := $4.Obj as TSqlSynIdent;
                                                    FlattenColDefsConstraints($5.Obj as TTempTpl);
                                                    $5.Obj.Free;
                                                    RowCommitAction := TSqlSynRowCommitAction($6.Obj);
                                                  end;}
        ;

  table_definition_opts :
        /* Empty */                             { $$.Text := ''; $$.Obj := TSqlSynNode(dtoNone); }
        |       _GLOBAL _TEMPORARY              { $$.Text := ''; $$.Obj := TSqlSynNode(dtoGlobalTemporary); }
        |       _LOCAL _TEMPORARY               { $$.Text := ''; $$.Obj := TSqlSynNode(dtoLocalTemporary); }
        ;

  table_commit_opts :
        /* Empty */
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(rcaUnspecified); }
        |       _ON _COMMIT _PRESERVE _ROWS
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(rcaCommitRows); }
        |       _ON _COMMIT _DELETE _ROWS
                                                { $$.Text := '';
                                                  $$.Obj := TSqlSynNode(rcaDeleteRows); }
        ;

  view_definition :
		_CREATE _VIEW table_name view_column_list_opt
			_AS query_expression view_check_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynView);
                                                  with $$.Obj as TSqlSynView do
                                                  begin
                                                    InsertTailChild($3.Obj);
                                                    Name := $3.Obj as TSqlSynIdent;
                                                    InsertTailChild($4.Obj);
                                                    ColumnList := $4.Obj;
                                                    InsertTailChild($6.Obj);
                                                    QueryExpr := $6.Obj as TSqlSynExpr;
                                                    ViewCheck := TSqlSynViewCheckOption($7.Obj);
                                                  end; }
        ;

  view_column_list_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := nil; }
        |       left_paren view_column_list right_paren
                                                { $$ := $2; }
        ;

  view_check_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := TSqlSynNode(vcoNone); }
        |       _WITH _CHECK _OPTION            { $$.Text := ''; $$.Obj := TSqlSynNode(vcoCheck); }
        |       _WITH _CASCADED _CHECK _OPTION  { $$.Text := ''; $$.Obj := TSqlSynNode(vcoCascadedCheck); }
        |       _WITH _LOCAL _CHECK _OPTION     { $$.Text := ''; $$.Obj := TSqlSynNode(vcoLocalCheck); }
        ;

  view_column_list : column_name_list
                                                { $$ := $1; }
        ;

  grant_statement :
		_GRANT privileges _ON object_name _TO grantee_list grant_option
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynGrantStatement);
                                                  with $$.Obj as TSqlSynGrantStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstGrant;
                                                    InsertTailChild($2.Obj);
                                                    Privs := $2.Obj;
                                                    InsertTailChild($4.Obj);
                                                    ObjectName := $4.Obj as TSqlSynIdent;
                                                    InsertTailChild($6.Obj);
                                                    GranteeList := $6.Obj;
                                                    GrantOption := Boolean($7.Obj);
                                                  end; }
        ;

  grantee_list :
                grantee                         { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($1.Obj);
                                                  end; }
        |       grantee_list comma grantee
                                                { $$ := $1;
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($3.Obj);
                                                  end; }
        ;

  grant_option :
        /* Empty */                             { $$.Text := ''; $$.Obj := TSqlSynNode(false); }
        |       _WITH _GRANT _OPTION            { $$.Text := ''; $$.Obj := TSqlSynNode(true); }
        ;

  privileges : _ALL _PRIVILEGES
                                                { $$.Text := ''; $$.Obj := MakeClass(TSqlSynPriv);
                                                  with $$.Obj as TSqlSynPriv do
                                                  begin
                                                    StructuralType := sstPrivilege;
                                                    Priv := ssptAll;
                                                  end; }
        | action_list
                                                { $$ := $1; }
        ;

  action_list :
                action                          { $$.Text := ''; $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($1.Obj);
                                                  end; }
        |       action_list  comma action       { $$ := $1;
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($3.Obj);
                                                  end; }
        ;

  action :
		_SELECT
                                                { $$.Text := ''; $$.Obj := MakeClass(TSqlSynPriv);
                                                  with $$.Obj as TSqlSynPriv do
                                                  begin
                                                    StructuralType := sstPrivilege;
                                                    Priv := ssptSelect;
                                                  end; }
	|	_DELETE
                                                { $$.Text := ''; $$.Obj := MakeClass(TSqlSynPriv);
                                                  with $$.Obj as TSqlSynPriv do
                                                  begin
                                                    StructuralType := sstPrivilege;
                                                    Priv := ssptDelete;
                                                  end; }
	|	_INSERT privilege_column_list_opt
                                                { $$.Text := ''; $$.Obj := MakeClass(TSqlSynPriv);
                                                  with $$.Obj as TSqlSynPriv do
                                                  begin
                                                    StructuralType := sstPrivilege;
                                                    Priv := ssptInsert;
                                                    if Assigned($2.Obj) then
                                                    begin
                                                      InsertTailChild($2.Obj);
                                                      ColumnList := $2.Obj;
                                                    end;
                                                  end; }
	|	_UPDATE privilege_column_list_opt
                                                { $$.Text := ''; $$.Obj := MakeClass(TSqlSynPriv);
                                                  with $$.Obj as TSqlSynPriv do
                                                  begin
                                                    StructuralType := sstPrivilege;
                                                    Priv := ssptUpdate;
                                                    if Assigned($2.Obj) then
                                                    begin
                                                      InsertTailChild($2.Obj);
                                                      ColumnList := $2.Obj;
                                                    end;
                                                  end; }
	|	_REFERENCES privilege_column_list_opt
                                                { $$.Text := ''; $$.Obj := MakeClass(TSqlSynPriv);
                                                  with $$.Obj as TSqlSynPriv do
                                                  begin
                                                    StructuralType := sstPrivilege;
                                                    Priv := ssptReferences;
                                                    if Assigned($2.Obj) then
                                                    begin
                                                      InsertTailChild($2.Obj);
                                                      ColumnList := $2.Obj;
                                                    end;
                                                  end; }
	|	_USAGE
                                                { $$.Text := ''; $$.Obj := MakeClass(TSqlSynPriv);
                                                  with $$.Obj as TSqlSynPriv do
                                                  begin
                                                    StructuralType := sstPrivilege;
                                                    Priv := ssptUsage;
                                                  end; }
        ;

  privilege_column_list_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := nil; }
        |       left_paren privilege_column_list right_paren
                                                { $$ := $2; }
        ;

  privilege_column_list : column_name_list      { $$ := $1; }
        ;

  object_name :
		table_opt table_name                    { $$ := $2; }
	|	_DOMAIN domain_name                     { $$ := $2; }
	|	_COLLATION collation_name               { $$ := $2; }
	|	_CHARACTER _SET character_set_name      { $$ := $3; }
	|	_TRANSLATION translation_name           { $$ := $2; }
        ;

  table_opt :
        /* Empty */                                     { $$.Text := ''; $$.Obj := nil; }
        |       _TABLE                                  { $$.Text := ''; $$.Obj := nil; }
        ;

  grantee :
        _PUBLIC
                                                        { $$.Text := UTF8ToString(Lexer.yytext);
                                                          $$.Obj := MakeClass(TSQLSynIdent);
                                                          with $$.Obj as TSQLSynIdent do
                                                            IdentName := $$.Text; }
        | authorization_identifier
                                                        { $$ := $1; }
        ;

  assertion_definition :
		_CREATE _ASSERTION constraint_name assertion_check constraint_attributes_opt
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynAssertion);
                                                          with $$.Obj as TSqlSynAssertion do
                                                          begin
                                                            StructuralType := sstCreateOrDecl;
                                                            CreateorDeclType := codAssertion;
                                                            InsertTailChild($3.Obj);
                                                            Name := $3.Obj as TSqlSynIdent;
                                                            InsertTailChild($4.Obj);
                                                            SearchCondition := $4.Obj as TSqlSynExpr;
                                                            if Assigned($5.Obj) then
                                                            begin
                                                              InsertTailChild($5.Obj);
                                                              Attributes := $5.Obj as TSqlSynConstraintAttributes;
                                                            end;
                                                          end; }

        ;

  assertion_check : _CHECK left_paren search_condition right_paren
                                                        { $$ := $3; }
        ;

  character_set_definition :
		_CREATE _CHARACTER _SET character_set_name as_opt character_set_source
		charset_collation_opt
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          yyinfo('Create character set ignored.');
                                                          //Character set definitions ignored.
                                                          $4.Obj.Free;
                                                          $6.Obj.Free;
                                                          $7.Obj.Free; }
        ;

  charset_collation_opt :
        /* Empty */                                     { $$.Text := ''; $$.Obj := nil; }
        |       collate_clause
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          $1.Obj.Free; }
        |       limited_collation_definition
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          $1.Obj.Free; }
        ;

  character_set_source : _GET existing_character_set_name
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          $2.Obj.Free; }
        ;

  existing_character_set_name :
                character_set_name
                                                        { $$ := $1; }
        ;

  limited_collation_definition :
		_COLLATION _FROM collation_source
                                                        { $$ := $3;}
        ;

  collation_source :
                collating_sequence_definition           { $$ := $1; }
        |       translation_collation                   { $$ := $1; }
        ;

  collating_sequence_definition :
		external_collation
                                                        { $$ := $1; }
	|	schema_collation_name
                                                        { $$ := $1; }
	|	_DESC left_paren collation_name right_paren
                                                        { $$ := $3; }
	|	_DEFAULT
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //TODO - Some marker here if/when collations supported.
                                                        }
        ;

  external_collation :
	_EXTERNAL left_paren quote external_collation_name quote right_paren
                                                        { $$ := $4; }
        ;

  external_collation_name : collation_name
                                                        { $$ := $1; }
        ;

  schema_collation_name : collation_name
                                                        { $$ := $1; }
        ;

  translation_collation : _TRANSLATION translation_name translation_collation_opt
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //Translation collations ignored.
                                                        }
        ;

  translation_collation_opt :
        /* Empty */
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //Translation collations ignored.
                                                        }
        |       _THEN _COLLATION collation_name
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //Translation collations ignored.
                                                        }
        ;

  collation_definition :
		_CREATE _COLLATION collation_name _FOR character_set_specification
			_FROM collation_source pad_attribute_opt
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //Collation definitions are an error.
                                                        }

        ;

  pad_attribute_opt :
        /* Empty */
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //Collation definitions are an error.
                                                        }
        |       _NO _PAD
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //Collation definitions are an error.
                                                        }
        |       _PAD _SPACE
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //Collation definitions are an error.
                                                        }
        ;

  translation_definition :
		_CREATE _TRANSLATION translation_name
			_FOR source_character_set_specification
			_TO target_character_set_specification
			_FROM translation_source
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //Translation definitions are an error.
                                                          $3.Obj.Free;
                                                          $5.Obj.Free;
                                                          $7.Obj.Free;
                                                          $9.Obj.Free;
                                                        }
        ;

  source_character_set_specification : character_set_specification
                                                        { $$ := $1;}
        ;

  target_character_set_specification : character_set_specification
                                                        { $$ := $1;}
        ;

  translation_source : translation_specification
                                                        { $$ := $1; }
        ;

  translation_specification :
		external_translation                    { $$ := $1; }
	|	_IDENTITY
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //Some marker here if translations supported.
                                                        }
	|	schema_translation_name
                                                        { $$ := $1; }
        ;

  external_translation :
		_EXTERNAL left_paren quote external_translation_name quote right_paren
                                                        { $$ := $4; }
        ;

  external_translation_name :
		translation_name
                                                        { $$ := $1; }
        ;

  schema_translation_name : translation_name
                                                        { $$ := $1; }
        ;

  SQL_schema_manipulation_statement :
		drop_schema_statement                   { $$ := $1; }
	|	alter_table_statement                   { $$ := $1; }
	|	drop_table_statement                    { $$ := $1; }
	|	drop_view_statement                     { $$ := $1; }
	|	revoke_statement                        { $$ := $1; }
	|	alter_domain_statement                  { $$ := $1; }
	|	drop_domain_statement                   { $$ := $1; }
	|	drop_character_set_statement            { $$ := $1;
                                                          yyinfo('Drop character set ignored.');}
	|	drop_collation_statement                { $$.Text := ''; $$.Obj := nil;
                                                          yyerror('Drop collation not supported in schema'); }
	|	drop_translation_statement              { $$.Text := ''; $$.Obj := nil;
                                                          yyerror('Drop translation not supported in schema'); }
	|	drop_assertion_statement                { $$ := $1; }
        ;

  drop_schema_statement : _DROP _SCHEMA schema_name drop_behaviour
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynDropEntity);
                                                          with $$.Obj as TSqlSynDropEntity do
                                                          begin
                                                            StructuralType := sstDrop;
                                                            DropType := codSchema;
                                                            InsertTailChild($3.Obj);
                                                            Name := $3.Obj as TSqlSynIdent;
                                                            Propagation := TSqlSynDropPropagation($4.Obj);
                                                          end; }
        ;

  drop_behaviour :
                _CASCADE
                                                        { $$.Text := ''; $$.Obj := TSqlSynNode(sdpCascade); }
        |       _RESTRICT
                                                        { $$.Text := ''; $$.Obj := TSqlSynNode(sdpRestrict); }
        ;

  alter_table_statement : _ALTER _TABLE table_name alter_table_action
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynAlterTable);
                                                          with $$.Obj as TSqlSynAlterTable do
                                                          begin
                                                            StructuralType := sstAlter;
                                                            AlterType := codTable;
                                                            InsertTailChild($3.Obj);
                                                            Name := $3.Obj as TSqlSynIdent;
                                                            InsertTailChild($4.Obj);
                                                            Action := $4.Obj as TSqlSynAlterTableAction;
                                                          end; }
        ;

  alter_table_action :
		add_column_definition                   { $$ := $1; }
	|	alter_column_definition                 { $$ := $1; }
	|	drop_column_definition                  { $$ := $1; }
	|	add_table_constraint_definition         { $$ := $1; }
	|	drop_table_constraint_definition        { $$ := $1; }
        ;

  column_opt :
        /* Empty */                                     { $$.Text := ''; $$.Obj := nil; }
        |       _COLUMN                                 { $$.Text := ''; $$.Obj := nil; }
        ;

  add_column_definition :
                _ADD column_opt column_definition
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynAlterColDef);
                                                          with $$.Obj as TSqlSynAlterColDef do
                                                          begin
                                                            StructuralType := sstAlterTableAction;
                                                            ActionType := atatAddCol;
                                                            InsertTailChild($3.Obj);
                                                            ColDef := $3.Obj as TSqlSynColDef;
                                                            PropagateNameUp;
                                                          end; }
        ;

  alter_column_definition :
                _ALTER column_opt column_name alter_column_action
                                                        { $$ := $4;
                                                          with $$.Obj as TSqlSynAlterColDef do
                                                          begin
                                                            InsertHeadChild($3.Obj);
                                                            ColName := $3.Obj as TSqlSynIdent;
                                                          end; };
        ;

  alter_column_action :
                set_column_default_clause
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynAlterColDef);
                                                          with $$.Obj as TSqlSynAlterColDef do
                                                          begin
                                                            StructuralType := sstAlterTableAction;
                                                            ActionType := atatAlterColSetDefault;
                                                            InsertTailChild($1.Obj);
                                                            DefaultClause := $1.Obj as TSqlSynExpr;
                                                          end; }
        |       drop_column_default_clause
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynAlterColDef);
                                                          with $$.Obj as TSqlSynAlterColDef do
                                                          begin
                                                            StructuralType := sstAlterTableAction;
                                                            ActionType := atatAlterColDropDefault;
                                                          end; }
        ;

  set_column_default_clause :
                _SET default_clause
                                                        { $$ := $2; }
        ;

  drop_column_default_clause :
                _DROP _DEFAULT
                                                        { $$.Text := ''; $$.Obj := nil; }
        ;

  drop_column_definition :
                _DROP column_opt column_name drop_behaviour
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynDropTableComponent);
                                                          with $$.Obj as TSqlSynDropTableComponent do
                                                          begin
                                                            StructuralType := sstAlterTableAction;
                                                            ActionType := atatDropCol;
                                                            InsertHeadChild($3.Obj);
                                                            Name := $3.Obj as TSqlSynIdent;
                                                            Propagation := TSqlSynDropPropagation($4.Obj);
                                                          end; }
        ;

  add_table_constraint_definition :
                _ADD table_constraint_definition
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynAddTableConstraint);
                                                          with $$.Obj as TSqlSynAddTableConstraint do
                                                          begin
                                                            StructuralType := sstAlterTableAction;
                                                            ActionType := atatAddCons;
                                                            InsertTailChild($2.Obj);
                                                            Constraint := $2.Obj as TSqlSynConstraint;
                                                            PropagateNameUp;
                                                          end; }
        ;

  drop_table_constraint_definition :
                _DROP _CONSTRAINT constraint_name drop_behaviour
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynDropTableComponent);
                                                          with $$.Obj as TSqlSynDropTableComponent do
                                                          begin
                                                            StructuralType := sstAlterTableAction;
                                                            ActionType := atatDropCons;
                                                            InsertHeadChild($3.Obj);
                                                            Name := $3.Obj as TSqlSynIdent;
                                                            Propagation := TSqlSynDropPropagation($4.Obj);
                                                          end; }
        ;

  drop_table_statement :
                _DROP _TABLE table_name drop_behaviour
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynDropEntity);
                                                          with $$.Obj as TSqlSynDropEntity do
                                                          begin
                                                            StructuralType := sstDrop;
                                                            DropType := codTable;
                                                            InsertTailChild($3.Obj);
                                                            Name := $3.Obj as TSqlSynIdent;
                                                            Propagation := TSqlSynDropPropagation($4.Obj);
                                                          end; }
        ;

  drop_view_statement :
                _DROP _VIEW table_name drop_behaviour
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynDropEntity);
                                                          with $$.Obj as TSqlSynDropEntity do
                                                          begin
                                                            StructuralType := sstDrop;
                                                            DropType := codView;
                                                            InsertTailChild($3.Obj);
                                                            Name := $3.Obj as TSqlSynIdent;
                                                            Propagation := TSqlSynDropPropagation($4.Obj);
                                                          end; }
        ;

  revoke_statement :
		_REVOKE grant_option_for_opt privileges _ON object_name
			_FROM grantee_list drop_behaviour
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynRevokeStatement);
                                                  with $$.Obj as TSqlSynRevokeStatement do
                                                  begin
                                                    Structuraltype := sstMiscStatement;
                                                    MiscStatementType := smstRevoke;
                                                    GrantOption := Boolean($2.Obj);
                                                    InsertTailChild($3.obj);
                                                    Privs := $3.Obj;
                                                    InsertTailChild($5.Obj);
                                                    ObjectName := $5.Obj as TSqlSynIdent;
                                                    InsertTailChild($7.Obj);
                                                    GranteeList := $7.Obj;
                                                    Propagation := TSqlSynDropPropagation($8.Obj);
                                                  end; }
        ;

  grant_option_for_opt :
        /* Empty */                                     { $$.Text := ''; $$.Obj := TSqlSynNode(false); }
        |        _GRANT _OPTION _FOR                    { $$.Text := ''; $$.obj := TSqlSynNode(true); }
        ;

  alter_domain_statement : _ALTER _DOMAIN domain_name alter_domain_action
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynAlterDomain);
                                                  with $$.Obj as TSqlSynAlterDomain do
                                                  begin
                                                    StructuralType := sstAlter;
                                                    AlterType := codDomain;
                                                    InsertTailChild($3.Obj);
                                                    Name := $3.Obj as TSqlSynIdent;
                                                    InsertTailChild($4.Obj);
                                                    Action := $4.Obj as TSqlSynAlterDomainAction;
                                                  end; }
        ;

  alter_domain_action :
		set_domain_default_clause               { $$ := $1; }
	|	drop_domain_default_clause              { $$ := $1; }
	|	add_domain_constraint_definition        { $$ := $1; }
	|	drop_domain_constraint_definition       { $$ := $1; }
        ;

  set_domain_default_clause :
                _SET default_clause
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynSetDomainDefaultAction);
                                                          with $$.Obj as TSqlSynSetDomainDefaultAction do
                                                          begin
                                                            StructuralType := sstAlterDomainAction;
                                                            ActionType := adatAlterSetDefault;
                                                            InsertTailChild($2.Obj);
                                                            DefaultClause := $2.Obj as TSqlSynExpr;
                                                          end; }
        ;

  drop_domain_default_clause :
                _DROP _DEFAULT
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynAlterDomainAction);
                                                          with $$.Obj as TSqlSynAlterDomainAction do
                                                          begin
                                                            StructuralType := sstAlterDomainAction;
                                                            ActionType := adatAlterDropDefault;
                                                          end; }
        ;

  add_domain_constraint_definition :
                _ADD domain_constraint
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynAddDomainConstraintAction);
                                                          with $$.Obj as TSqlSynAddDomainConstraintAction do
                                                          begin
                                                            StructuralType := sstAlterDomainAction;
                                                            ActionType := adatAddCons;
                                                            InsertTailChild($2.Obj);
                                                            Constraint := $2.Obj as TSqlSynConstraint;
                                                            PropagateNameUp;
                                                          end; }
        ;

  drop_domain_constraint_definition :
                _DROP _CONSTRAINT constraint_name
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynDropDomainConstraint);
                                                          with $$.Obj as TSqlSynDropDomainConstraint do
                                                          begin
                                                            StructuralType := sstAlterDomainAction;
                                                            ActionType := adatDropCons;
                                                            InsertTailChild($3.Obj);
                                                            Name := $3.Obj as TSqlSynIdent;
                                                          end; }
        ;

  drop_domain_statement :
                _DROP _DOMAIN domain_name drop_behaviour
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynDropEntity);
                                                          with $$.Obj as TSqlSynDropEntity do
                                                          begin
                                                            StructuralType := sstDrop;
                                                            DropType := codDomain;
                                                            InsertTailChild($3.Obj);
                                                            Name := $3.Obj as TSqlSynIdent;
                                                            Propagation := TSqlSynDropPropagation($4.Obj);
                                                          end; }
        ;

  drop_character_set_statement :
                _DROP _CHARACTER _SET character_set_name
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //Character set definitions ignored.
                                                          $4.Obj.Free; }
        ;

  drop_collation_statement :
                _DROP _COLLATION collation_name
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //Collation drops are an error.
                                                          $3.Obj.Free; }
        ;

  drop_translation_statement :
                _DROP _TRANSLATION translation_name
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          //Translation drops are an error.
                                                          $3.Obj.Free; }
        ;

  drop_assertion_statement :
                _DROP _ASSERTION constraint_name
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynDropEntity);
                                                          with $$.Obj as TSqlSynDropEntity do
                                                          begin
                                                            StructuralType := sstDrop;
                                                            DropType := codAssertion;
                                                            InsertTailChild($3.Obj);
                                                            Name := $3.Obj as TSqlSynIdent;
                                                          end; }
        ;

/*
--hr
--h2 SQL Data Manipulation Statements
--/h2
*/

  SQL_data_statement :
		open_statement                          { $$ := $1; }
	|	fetch_statement                         { $$ := $1; }
	|	close_statement                         { $$ := $1; }
	|	select_statement__single_row            { $$ := $1; }
	|	SQL_data_change_statement               { $$ := $1; }
        ;

  open_statement : _OPEN cursor_name
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynDmlCursor);
                                                          with $$.Obj as TSqlSynDmlCursor do
                                                          begin
                                                            StructuralType := sstDmlWithCursor;
                                                            DmlCursorType := sdcOpen;
                                                            InsertTailChild($2.Obj);
                                                            CursorName := $2.obj as TSqlSynIdent;
                                                          end; }
        ;

  fetch_statement :
		_FETCH fetch_orientation_opt cursor_name _INTO fetch_target_list
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynDmlCursorFetch);
                                                          with $$.Obj as TSqlSynDmlCursorFetch do
                                                          begin
                                                            StructuralType := sstDmlWithCursor;
                                                            DmlCursorType := sdcFetch;
                                                            InsertTailChild($3.Obj);
                                                            CursorName := $3.Obj as TSqlSynIdent;
                                                            if Assigned($2.Obj) then
                                                            begin
                                                              InsertHeadChild($2.Obj);
                                                              Orientation := $2.Obj;
                                                            end;
                                                            InsertTailChild($5.Obj);
                                                            FetchTarget := $5.Obj;
                                                          end; }
        ;

  fetch_orientation_opt :
        /* Empty */                                     { $$.Text := ''; $$.Obj := nil; }
        |       _FROM                                   { $$.Text := ''; $$.Obj := nil; }
        |       fetch_orientation _FROM                 { $$ := $1; }
        ;

  fetch_orientation :
		_NEXT                                   { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynFetchOrientation);
                                                          with $$.Obj as TSqlSynFetchOrientation do
                                                          begin
                                                            StructuralType := sstFetchOrientation;
                                                            FetchOrientDir := sfdNext;
                                                          end; }
	|	_PRIOR                                  { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynFetchOrientation);
                                                          with $$.Obj as TSqlSynFetchOrientation do
                                                          begin
                                                            StructuralType := sstFetchOrientation;
                                                            FetchOrientDir := sfdPrior;
                                                          end; }
	|	_FIRST                                  { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynFetchOrientation);
                                                          with $$.Obj as TSqlSynFetchOrientation do
                                                          begin
                                                            StructuralType := sstFetchOrientation;
                                                            FetchOrientDir := sfdFirst;
                                                          end; }
	|	_LAST                                   { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynFetchOrientation);
                                                          with $$.Obj as TSqlSynFetchOrientation do
                                                          begin
                                                            StructuralType := sstFetchOrientation;
                                                            FetchOrientDir := sfdLast;
                                                          end; }
	|	_ABSOLUTE simple_value_specification    { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynFetchOrientation);
                                                          with $$.Obj as TSqlSynFetchOrientation do
                                                          begin
                                                            StructuralType := sstFetchOrientation;
                                                            FetchOrientDir := sfdAbsolute;
                                                            InsertTailChild($2.Obj);
                                                            ValSpec := $2.Obj;
                                                          end; }
        |       _RELATIVE simple_value_specification    { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynFetchOrientation);
                                                          with $$.Obj as TSqlSynFetchOrientation do
                                                          begin
                                                            StructuralType := sstFetchOrientation;
                                                            FetchOrientDir := sfdRelative;
                                                            InsertTailChild($2.Obj);
                                                            ValSpec := $2.Obj;
                                                          end; }
        ;

  simple_value_specification :
                parameter_name                          { $$ := $1; }
        |       literal                                 { $$ := $1; }
        ;

  fetch_target_list :
                target_specification                    { $$.Text := '';
                                                          $$.Obj := MakeClass(TTempTpl);
                                                          with $$.Obj as TTempTpl do
                                                          begin
                                                            InsertTailChild($1.Obj);
                                                          end; }

        |       fetch_target_list comma target_specification
                                                        { $$ := $1;
                                                          with $$.Obj as TTempTpl do
                                                          begin
                                                            InsertTailChild($3.Obj);
                                                          end; }
        ;

  target_specification :
		parameter_specification                 { $$ := $1; }
        ;

  close_statement :
                _CLOSE cursor_name                      {  $$.Text := '';
                                                           $$.Obj := MakeClass(TSqlSynDmlCursor);
                                                          with $$.Obj as TSqlSynDmlCursor do
                                                          begin
                                                            StructuralType := sstDmlWithCursor;
                                                            DmlCursorType := sdcClose;
                                                            InsertTailChild($2.Obj);
                                                            CursorName := $2.obj as TSqlSynIdent;
                                                          end; }
        ;

  select_statement__single_row :
	        _SELECT set_quantifier_opt select_list
                _INTO select_target_list table_expression
                                                {  $$.Text := '';
                                                   $$.Obj := MakeClass(TSqlSynDmlSelectToTarget);
                                                   with $$.Obj as TSqlSynDmlSelectToTarget do
                                                   begin
                                                     StructuralType := sstDmlNoCursor;
                                                     DmlNoCursorType := sdnSelectTotarget;
                                                     SelectExpr := $6.Obj as TSqlSynExpr;
                                                     InsertTailChild($6.Obj);
                                                     with SelectExpr as TSqlSynQuerySelect do
                                                     begin
                                                       InsertHeadChild($3.Obj);
                                                       SelectList := $3.Obj as TSqlSynSelectList;
                                                       Quantifier := TSqlSynSetFunctionQuantifier($2.Obj);
                                                     end;
                                                     InsertHeadChild($5.Obj);
                                                     TargetList:= $5.Obj;
                                                   end; }
        ;

  select_target_list :
                target_specification            { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($1.Obj);
                                                  end; }

        |       select_target_list comma target_specification
                                                { $$ := $1;
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($3.Obj);
                                                  end; }
        ;

  SQL_data_change_statement :
		delete_statement__positioned    { $$ := $1; }
	|	delete_statement__searched      { $$ := $1; }
	|	insert_statement                { $$ := $1; }
	|	update_statement__positioned    { $$ := $1; }
	|	update_statement__searched      { $$ := $1; }
        ;

  delete_statement__positioned :
                _DELETE _FROM table_name _WHERE _CURRENT _OF cursor_name
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynDmlDeletePositioned);
                                                  with $$.Obj as TSqlSynDmlDeletePositioned do
                                                  begin
                                                    StructuralType := sstDmlWithCursor;
                                                    DmlCursorType := sdcDeletePositioned;
                                                    InsertTailChild($7.Obj);
                                                    CursorName := $7.Obj as TSqlSynIdent;
                                                    InsertTailChild($3.Obj);
                                                    Table := $3.Obj as TSqlSynIdent;
                                                  end; }
        ;

  delete_statement__searched :
                _DELETE _FROM table_name where_clause_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynDmlDeleteSearched);
                                                  with $$.Obj as TSqlSynDmlDeleteSearched do
                                                  begin
                                                    StructuralType := sstDmlNoCursor;
                                                    DmlNoCursorType := sdnDeleteSearched;
                                                    InsertTailChild($3.Obj);
                                                    Table := $3.obj as TSqlSynIdent;
                                                    if Assigned($4.Obj) then
                                                    begin
                                                      InsertTailChild($4.Obj);
                                                      Where := $4.Obj as TSqlSynExpr;
                                                    end;
                                                  end; }
        ;

  insert_statement : _INSERT _INTO table_name insert_columns_and_source
                                                { $$ := $4;
                                                  with $$.Obj as TSqlSynDmlInsert do
                                                  begin
                                                    InsertHeadChild($3.Obj);
                                                    Table := $3.Obj as TSqlSynIdent;
                                                  end; }
        ;

  insert_columns_and_source :
                left_paren insert_column_list right_paren query_expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynDmlInsert);
                                                  with $$.Obj as TSqlSynDmlInsert do
                                                  begin
                                                    StructuralType := sstDmlNoCursor;
                                                    DmlNoCursorType := sdnInsert;
                                                    InsertTailChild($2.Obj);
                                                    ColList := $2.Obj;
                                                    InsertTailChild($4.Obj);
                                                    Query := $4.Obj as TSqlSynExpr;
                                                  end; }
        |       query_expression
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynDmlInsert);
                                                  with $$.Obj as TSqlSynDmlInsert do
                                                  begin
                                                    StructuralType := sstDmlNoCursor;
                                                    DmlNoCursorType := sdnInsert;
                                                    InsertTailChild($1.Obj);
                                                    Query := $1.Obj as TSqlSynExpr;
                                                  end; }
        |       _DEFAULT _VALUES
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynDmlInsert);
                                                  with $$.Obj as TSqlSynDmlInsert do
                                                  begin
                                                    StructuralType := sstDmlNoCursor;
                                                    DmlNoCursorType := sdnInsert;
                                                  end; }
        ;

  insert_column_list :
                column_name_list                { $$ := $1; }
        ;

  update_statement__positioned :
		_UPDATE table_name _SET set_clause_list _WHERE _CURRENT _OF cursor_name
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynDmlCursorUpdate);
                                                  with $$.Obj as TSqlSynDmlCursorUpdate do
                                                  begin
                                                    StructuralType := sstDmlWithCursor;
                                                    DmlCursorType := sdcUpdatePositioned;
                                                    InsertTailChild($2.Obj);
                                                    Table := $2.Obj as TSqlSynIdent;
                                                    InsertTailChild($4.Obj);
                                                    SetClauseList := $4.Obj;
                                                    InsertTailChild($8.Obj);
                                                    CursorName := $8.Obj as TSqlSynIdent;
                                                  end; }
        ;

  set_clause_list :
                set_clause                      { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($1.Obj);
                                                  end; }
        |       set_clause_list comma set_clause
                                                { $$ := $1;
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($3.Obj);
                                                  end; }
        ;

  set_clause :
                object_column equals_operator update_source
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynDmlSetClause);
                                                  with $$.Obj as TSqlSynDmlSetClause do
                                                  begin
                                                    StructuralType := sstSetClause;
                                                    InsertTailChild($1.Obj);
                                                    ColName := $1.Obj as TSqlSynIdent;
                                                    InsertTailChild($3.Obj);
                                                    UpdateSource := $3.Obj as TSqlSynExpr;
                                                  end; }
        ;

  object_column : column_name                   { $$ := $1; }
        ;

  update_source :
                expression                      { $$ := $1; }
                /* Expression includes NULL and DEFAULT specifications */
        ;

  update_statement__searched :
		_UPDATE table_name _SET set_clause_list where_clause_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSymDmlUpdateSearched);
                                                  with $$.Obj as TSqlSymDmlUpdateSearched do
                                                  begin
                                                    StructuralType := sstDmlNoCursor;
                                                    DmlNoCursorType := sdnUpdateSearched;
                                                    InsertTailChild($2.Obj);
                                                    Table := $2.Obj as TSqlSynIdent;
                                                    InsertTailChild($4.Obj);
                                                    SetClauseList := $4.Obj;
                                                    if Assigned($5.Obj) then
                                                    begin
                                                      InsertTailChild($5.Obj);
                                                      WhereClause := $5.Obj as TSqlSynExpr;
                                                    end;
                                                  end;  }
        ;

  SQL_transaction_statement :
		set_transaction_statement       { $$ := $1; }
	|	set_constraints_mode_statement  { $$ := $1; }
	|	commit_statement                { $$ := $1; }
	|	rollback_statement              { $$ := $1; }
        ;

  set_transaction_statement :
		_SET _TRANSACTION transaction_mode_list
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSetTransactionStatement);
                                                  with $$.Obj as TSqlSynSetTransactionStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstSetTransaction;
                                                    InsertTailChild($3.Obj);
                                                    ModeList := $3.Obj;
                                                  end; }

        ;

  transaction_mode_list :
                transaction_mode                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                     InsertTailChild($1.Obj);
                                                  end; }
        |       transaction_mode_list comma transaction_mode
                                                { $$ := $1;
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($3.Obj);
                                                  end; }
        ;

  transaction_mode :
		isolation_level                 { $$ := $1; }
	|	transaction_access_mode         { $$ := $1; }
	|	diagnostics_size                { $$ := $1; }
        ;

  isolation_level : _ISOLATION _LEVEL level_of_isolation
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTransactionMode);
                                                  with $$.Obj as TTransactionMode do
                                                  begin
                                                    StructuralType := sstTransactionMode;
                                                    ModeType := tmtIsolation;
                                                    IsoVal := TTransactionIsoVal($3.Obj);
                                                  end; }
        ;

  level_of_isolation :
		_READ _UNCOMMITTED              { $$.Text := ''; $$.Obj := TSqlSynNode(tivReadUncommited); }
	|	_READ _COMMITTED                { $$.Text := ''; $$.Obj := TSqlSynNode(tivReadComitted); }
	|	_REPEATABLE _READ               { $$.Text := ''; $$.Obj := TSqlSynNode(tivReadRepeatable); }
	|	_SERIALIZABLE                   { $$.Text := ''; $$.Obj := TSqlSynNode(tivSerializable); }
        |       _SNAPSHOT                       { $$.Text := ''; $$.Obj := TSqlSynNode(tivSnapshot); }
        ;

  transaction_access_mode :
                _READ _ONLY
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTransactionMode);
                                                  with $$.Obj as TTransactionMode do
                                                  begin
                                                    StructuralType := sstTransactionMode;
                                                    ModeType := tmtAccessType;
                                                    RWVal := trwReadOnly;
                                                  end; }
        |       _READ _WRITE
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTransactionMode);
                                                  with $$.Obj as TTransactionMode do
                                                  begin
                                                    StructuralType := sstTransactionMode;
                                                    ModeType := tmtAccessType;
                                                    RWVal := trwReadWrite;
                                                  end; }
        ;

  diagnostics_size : _DIAGNOSTICS _SIZE number_of_conditions
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TTransactionMode);
                                                  with $$.Obj as TTransactionMode do
                                                  begin
                                                    StructuralType := sstTransactionMode;
                                                    ModeType := tmtDiagnosticSize;
                                                    InsertTailChild($3.Obj);
                                                    DiagSize := $3.Obj;
                                                  end; }
        ;

  number_of_conditions :
                simple_value_specification      { $$ := $1; }
        ;

  set_constraints_mode_statement :
                _SET _CONSTRAINTS constraint_name_list _DEFERRED
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSetConstraintsModeStatement);
                                                  with $$.Obj as TSqlSynSetConstraintsModeStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstSetConstraintsMode;
                                                    Immediate := false;
                                                    if Assigned($3.Obj) then
                                                    begin
                                                      InsertTailChild($3.Obj);
                                                      ConstraintNameList := $3.Obj;
                                                    end;
                                                  end; }

         |      _SET _CONSTRAINTS constraint_name_list _IMMEDIATE
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSetConstraintsModeStatement);
                                                  with $$.Obj as TSqlSynSetConstraintsModeStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstSetConstraintsMode;
                                                    Immediate := true;
                                                    if Assigned($3.Obj) then
                                                    begin
                                                      InsertTailChild($3.Obj);
                                                      ConstraintNameList := $3.Obj;
                                                    end;
                                                  end; }
         ;

  constraint_name_list :
                _ALL                            { $$.Text := ''; $$.Obj := nil; }
        |       constraint_name_list_some
                                                { $$ := $1; }
        ;

  constraint_name_list_some:
                constraint_name
                                                { $$.Text := ''; $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($1.Obj);
                                                  end; }
        |       constraint_name_list_some comma constraint_name
                                                { $$ := $1;
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($3.Obj);
                                                  end; }
        ;

  commit_statement :
                _COMMIT
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynCommitRollbackStatement);
                                                  with $$.Obj as TSqlSynCommitRollbackStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstCommit;
                                                    Commit := true;
                                                  end; }
        |       _COMMIT _WORK
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynCommitRollbackStatement);
                                                  with $$.Obj as TSqlSynCommitRollbackStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstCommit;
                                                    Commit := true;
                                                  end; }
        ;

  rollback_statement :
                _ROLLBACK
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynCommitRollbackStatement);
                                                  with $$.Obj as TSqlSynCommitRollbackStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstRollback;
                                                    Commit := false;
                                                  end; }
        |       _ROLLBACK _WORK
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynCommitRollbackStatement);
                                                  with $$.Obj as TSqlSynCommitRollbackStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstRollback;
                                                    Commit := false;
                                                  end; }
        ;

/*
--hr
--h2 Connection Management
--/h2
*/

  SQL_connection_statement :
		connect_statement               { $$ := $1; }
	|	set_connection_statement        { $$ := $1; }
	|	disconnect_statement            { $$ := $1; }
        ;

  connect_statement : _CONNECT _TO connection_target
                                                { $$ := $3; }
        ;

  connection_target :
		SQL_server_name connection_name_opt user_name_opt
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynConnectStatement);
                                                  with $$.Obj as TSqlSynConnectStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstConnect;
                                                    InsertTailChild($1.Obj);
                                                    ServerName := $1.Obj;
                                                    if Assigned($2.Obj) then
                                                    begin
                                                      InsertTailChild($2.Obj);
                                                      ConnectionName := $2.Obj;
                                                    end;
                                                    if Assigned($3.Obj) then
                                                    begin
                                                      InsertTailChild($3.Obj);
                                                      UserName := $3.Obj;
                                                    end;
                                                  end; }
	|	_DEFAULT
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynConnectStatement);
                                                  with $$.Obj as TSqlSynConnectStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstConnect;
                                                  end; }
        ;

  connection_name_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := nil }
        |       _AS connection_name             { $$ := $2; }
        ;

  user_name_opt :
        /* Empty */                             { $$.Text := ''; $$.Obj := nil }
        |       _USER user_name                 { $$ := $2; }
        ;

  SQL_server_name : simple_value_specification  { $$ := $1; }
        ;

  connection_name : simple_value_specification  { $$ := $1; }
        ;

  user_name : simple_value_specification        { $$ := $1; }
        ;

  set_connection_statement : _SET _CONNECTION connection_object
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynConnectStatement);
                                                  with $$.Obj as TSqlSynConnectStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstSetConnection;
                                                    if Assigned($3.Obj) then
                                                    begin
                                                      InsertTailChild($3.Obj);
                                                      ConnectionName := $3.Obj;
                                                    end;
                                                  end; }
        ;

  connection_object :
          _DEFAULT                              { $$.Text := ''; $$.Obj := nil }
        | connection_name                       { $$ := $1; }
        ;

  disconnect_statement : _DISCONNECT disconnect_object
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynDisconnectStatement);
                                                  with $$.Obj as TSqlSynDisconnectStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstDisconnect;
                                                    if $2.Obj = TSqlSynNode(ssdAll) then
                                                      Conns := TSqlSynDisconnectContext($2.Obj)
                                                    else if $2.Obj = TSqlSynNode(ssdCurrent) then
                                                      Conns := TSqlSynDisconnectContext($2.Obj)
                                                    else if Assigned($2.Obj) then
                                                    begin
                                                      Conns := ssdSpecified;
                                                      InsertTailChild($2.Obj);
                                                      ConnectionName := $2.Obj;
                                                    end;
                                                  end; }
        ;

  disconnect_object :
          connection_object                     { $$ := $1; }
        | _ALL                                  { $$.Text := ''; $$.Obj := TSqlSynNode(ssdAll); }
        | _CURRENT                              { $$.Text := ''; $$.Obj := TSqlSynNode(ssdCurrent); }

/*
--hr
--h2 Session Attributes
--/h2
*/

  SQL_session_statement :
		set_catalog_statement           { $$ := $1; }
	|	set_schema_statement            { $$ := $1; }
	|	set_names_statement             { $$ := $1; }
	|	set_session_authorization_identifier_statement  { $$ := $1; }
	|	set_local_time_zone_statement   { $$ := $1; }
        ;

  set_catalog_statement : _SET _CATALOG value_specification
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSessionSetStatement);
                                                  with $$.Obj as TSqlSynSessionSetStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstSessionSet;
                                                    SessionSetType := ssstCatalog;
                                                    InsertTailChild($3.Obj);
                                                    ValueSpec := $3.Obj;
                                                  end; }
        ;

  value_specification :
                literal                         { $$ := $1; }
        |       general_value_specification     { $$ := $1; }
        ;

  set_schema_statement : _SET _SCHEMA value_specification
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSessionSetStatement);
                                                  with $$.Obj as TSqlSynSessionSetStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstSessionSet;
                                                    SessionSetType := ssstSchema;
                                                    InsertTailChild($3.Obj);
                                                    ValueSpec := $3.Obj;
                                                  end; }
        ;

  set_names_statement : _SET _NAMES value_specification
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSessionSetStatement);
                                                  with $$.Obj as TSqlSynSessionSetStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstSessionSet;
                                                    SessionSetType := ssstNames;
                                                    InsertTailChild($3.Obj);
                                                    ValueSpec := $3.Obj;
                                                  end; }
        ;

  set_session_authorization_identifier_statement : _SET _SESSION _AUTHORIZATION value_specification
                                                { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynSessionSetStatement);
                                                  with $$.Obj as TSqlSynSessionSetStatement do
                                                  begin
                                                    StructuralType := sstMiscStatement;
                                                    MiscStatementType := smstSessionSet;
                                                    SessionSetType := ssstAuth;
                                                    InsertTailChild($4.Obj);
                                                    ValueSpec := $4.Obj;
                                                  end; }
        ;

  set_local_time_zone_statement : _SET _TIME _ZONE set_time_zone_value
                                                { $$.Text := ''; $$.Obj := nil;
                                                  yyerror('Time zones not supported.'); }
        ;

  set_time_zone_value :
          expression                            { $$.Text := ''; $$.Obj := nil; $1.Obj.Free; }
        | _LOCAL                                { $$.Text := ''; $$.Obj := nil; }
        ;

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
		direct_SQL_data_statement               { $$ := $1; }
	|	SQL_schema_statement                    { $$ := $1; }
	|	SQL_transaction_statement               { $$ := $1; }
	|	SQL_connection_statement                { $$ := $1; }
	|	SQL_session_statement                   { $$ := $1; }
	|	direct_implementation_defined_statement { $$ := $1; }
        ;

  direct_SQL_data_statement :
		delete_statement__searched              { $$ := $1; }
	|	direct_select_statement__multiple_rows  { $$ := $1; }
	|	insert_statement                        { $$ := $1; }
	|	update_statement__searched              { $$ := $1; }
	|	temporary_table_declaration             { $$ := $1; }
        ;

  direct_select_statement__multiple_rows : query_expression order_by_clause_opt
                                                        { $$.Text := '';
                                                          $$.Obj := MakeClass(TSqlSynDirectQueryWrapper);
                                                          with $$.Obj as TSqlSynDirectQueryWrapper do
                                                          begin
                                                            
                                                            ExprType := setDirectQueryWrapper;
                                                            InsertTailChild($1.Obj);
                                                            Query := $1.Obj as TSqlSynExpr;
                                                            if Assigned($2.Obj) then
                                                            begin
                                                              InsertTailChild($2.Obj);
                                                              OrderBy := $2.Obj;
                                                            end;
                                                          end; }
        ;

  direct_implementation_defined_statement : identifier
                                                        { $$.Text := ''; $$.Obj := nil;
                                                          yyerror('Unrecognised statement.'); }
        ;

/*
--hr
--h2 Top-Level_reachable in standalone SQL statements
--   (no embedding inside host program, no host program embedded in &c;);
--   This you might modify depending on whether you want an interactive
--   sql prompt or something else;
--/h2
*/

sql_statement :
      direct_SQL_statement                      { $$ := $1; }
    ;

sql_script :
                sql_statement                   { $$.Text := '';
                                                  $$.Obj := MakeClass(TTempTpl);
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($1.Obj);
                                                  end; }
    |           sql_script sql_statement        { $$ := $1;
                                                  with $$.Obj as TTempTpl do
                                                  begin
                                                    InsertTailChild($1.Obj);
                                                  end; }
    ;

sql_input :
      sql_script                                { $$ := $1; }
    | module                                    { $$ := $1; }
    ;

SQL92Grammar :

  sql_input                                     { $$.Text := '';
                                                  $$.Obj := MakeClass(TSqlSynTopLevel);
                                                  with $$.Obj as TSqlSynTopLevel do
                                                  begin
                                                    InsertTailChild($1.Obj);
                                                  end;
                                                  //TODO - connect top level parse to rest of program.
                                                }
  ;

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
