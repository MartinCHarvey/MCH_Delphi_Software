
(* lexical analyzer template (TP Lex V3.0), V1.0 3-2-91 AG *)

(* global definitions: *)

 (* Don't use macros, they appear to be broken in TP Lex / yacc
    TODO - Fix them one day when i get a moment *)

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

  uses LexLib;

  type
    SQL_Token = (
                INVALID_TOKEN = 0,
                identifier_body,
                national_character_string_literal_start,
                bit_string_literal_start,
                string_literal_continuation,
                hex_string_literal_start,
                delimited_identifier,
                digit,
                not_equals_operator,
                greater_than_or_equals_operator,
                less_than_or_equals_operator,
                concatenation_operator,
                double_period,
                space,
                tab,
                carriage_return,
                line_feed,
                double_quote,
                percent,
                ampersand,
                quote,
                left_paren,
                right_paren,
                left_bracket,
                right_bracket,
                asterisk,
                plus_sign,
                comma,
                minus_sign,
                period,
                solidus,
                colon,
                semicolon,
                less_than_operator,
                equals_operator,
                greater_than_operator,
                question_mark,
                underscore,
                vertical_bar,

		_ABSOLUTE,
		_ACTION,
		_ADD,
		_ALL,
		_ALLOCATE,
		_ALTER,
		_AND,
		_ANY,
		_ARE,
		_AS,
		_ASC,
		_ASSERTION,
		_AT,
		_AUTHORIZATION,
		_AVG,
		_BEGIN,
		_BETWEEN,
		_BIT,
		_BIT_LENGTH,
		_BOTH,
		_BY,
		_CASCADE,
		_CASCADED,
		_CASE,
		_CAST,
		_CATALOG,
		_CHAR,
		_CHARACTER,
		_CHARACTER_LENGTH,
		_CHAR_LENGTH,
		_CHECK,
		_CLOSE,
		_COALESCE,
		_COLLATE,
		_COLLATION,
		_COLUMN,
		_COMMIT,
		_CONNECT,
		_CONNECTION,
		_CONSTRAINT,
		_CONSTRAINTS,
		_CONTINUE,
		_CONVERT,
		_CORRESPONDING,
		_CREATE,
		_CROSS,
		_CURRENT,
		_CURRENT_DATE,
		_CURRENT_TIME,
		_CURRENT_TIMESTAMP,
		_CURRENT_USER,
		_CURSOR,
		_DATE,
		_DAY,
		_DEALLOCATE,
		_DEC,
		_DECIMAL,
		_DECLARE,
		_DEFAULT,
		_DEFERRABLE,
		_DEFERRED,
		_DELETE,
		_DESC,
		_DESCRIBE,
		_DESCRIPTOR,
		_DIAGNOSTICS,
		_DISCONNECT,
		_DISTINCT,
		_DOMAIN,
		_DOUBLE,
		_DROP,
		_ELSE,
		_END,
		_END_EXEC,
		_ESCAPE,
		_EXCEPT,
		_EXCEPTION,
		_EXEC,
		_EXECUTE,
		_EXISTS,
		_EXTERNAL,
		_EXTRACT,
		_FALSE,
		_FETCH,
		_FIRST,
		_FLOAT,
		_FOR,
		_FOREIGN,
		_FOUND,
		_FROM,
		_FULL,
		_GET,
		_GLOBAL,
		_GO,
		_GOTO,
		_GRANT,
		_GROUP,
		_HAVING,
		_HOUR,
		_IDENTITY,
		_IMMEDIATE,
		_IN,
		_INDICATOR,
		_INITIALLY,
		_INNER,
		_INPUT,
		_INSENSITIVE,
		_INSERT,
		_INT,
		_INTEGER,
		_INTERSECT,
		_INTERVAL,
		_INTO,
		_IS,
		_ISOLATION,
		_JOIN,
		_KEY,
		_LANGUAGE,
		_LAST,
		_LEADING,
		_LEFT,
		_LEVEL,
		_LIKE,
		_LOCAL,
		_LOWER,
		_MATCH,
		_MAX,
		_MIN,
		_MINUTE,
		_MODULE,
		_MONTH,
		_NAMES,
		_NATIONAL,
		_NATURAL,
		_NCHAR,
		_NEXT,
		_NO,
		_NOT,
		_NULL,
		_NULLIF,
		_NUMERIC,
		_OCTET_LENGTH,
		_OF,
		_ON,
		_ONLY,
		_OPEN,
		_OPTION,
		_OR,
		_ORDER,
		_OUTER,
		_OUTPUT,
		_OVERLAPS,
		_PAD,
		_PARTIAL,
		_POSITION,
		_PRECISION,
		_PREPARE,
		_PRESERVE,
		_PRIMARY,
		_PRIOR,
		_PRIVILEGES,
		_PROCEDURE,
		_PUBLIC,
		_READ,
		_REAL,
		_REFERENCES,
		_RELATIVE,
		_RESTRICT,
		_REVOKE,
		_RIGHT,
		_ROLLBACK,
		_ROWS,
		_SCHEMA,
		_SCROLL,
		_SECOND,
		_SECTION,
		_SELECT,
		_SESSION,
		_SESSION_USER,
		_SET,
		_SIZE,
		_SMALLINT,
		_SOME,
		_SPACE,
		_SQL,
		_SQLCODE,
		_SQLERROR,
		_SQLSTATE,
		_SUBSTRING,
		_SUM,
		_SYSTEM_USER,
		_TABLE,
		_TEMPORARY,
		_THEN,
		_TIME,
		_TIMESTAMP,
		_TIMEZONE_HOUR,
		_TIMEZONE_MINUTE,
		_TO,
		_TRAILING,
		_TRANSACTION,
		_TRANSLATE,
		_TRANSLATION,
		_TRIM,
		_TRUE,
		_UNION,
		_UNIQUE,
		_UNKNOWN,
		_UPDATE,
		_UPPER,
		_USAGE,
		_USER,
		_USING,
		_VALUE,
		_VALUES,
		_VARCHAR,
		_VARYING,
		_VIEW,
		_WHEN,
		_WHENEVER,
		_WHERE,
		_WITH,
		_WORK,
		_WRITE,
		_YEAR,
		_ZONE,
		LAST_TOKEN );

  const
    token_names: array [0..Ord(LAST_TOKEN)] of string =
      ( 'invalid / eof token',
        'identifier_body' ,
        'national_character_string_literal_start' ,
        'bit_string_literal_start' ,
        'string_literal_continuation' ,
        'hex_string_literal_start' ,
        'delimited_identifier' ,
        'digit' ,
        'not_equals_operator' ,
        'greater_than_or_equals_operator' ,
        'less_than_or_equals_operator' ,
        'concatenation_operator' ,
        'double_period' ,
        'space' ,
        'tab' ,
        'carriage_return' ,
        'line_feed' ,
        'double_quote' ,
        'percent' ,
        'ampersand' ,
        'quote' ,
        'left_paren' ,
        'right_paren' ,
        'left_bracket' ,
        'right_bracket' ,
        'asterisk' ,
        'plus_sign' ,
        'comma' ,
        'minus_sign' ,
        'period' ,
        'solidus' ,
        'colon' ,
        'semicolon' ,
        'less_than_operator' ,
        'equals_operator' ,
        'greater_than_operator' ,
        'question_mark' ,
        'underscore' ,
        'vertical_bar',

	'_ABSOLUTE',
	'_ACTION',
	'_ADD',
	'_ALL',
	'_ALLOCATE',
	'_ALTER',
	'_AND',
	'_ANY',
	'_ARE',
	'_AS',
	'_ASC',
	'_ASSERTION',
	'_AT',
	'_AUTHORIZATION',
	'_AVG',
	'_BEGIN',
	'_BETWEEN',
	'_BIT',
	'_BIT_LENGTH',
	'_BOTH',
	'_BY',
	'_CASCADE',
	'_CASCADED',
	'_CASE',
	'_CAST',
	'_CATALOG',
	'_CHAR',
	'_CHARACTER',
	'_CHARACTER_LENGTH',
	'_CHAR_LENGTH',
	'_CHECK',
	'_CLOSE',
	'_COALESCE',
	'_COLLATE',
	'_COLLATION',
	'_COLUMN',
	'_COMMIT',
	'_CONNECT',
	'_CONNECTION',
	'_CONSTRAINT',
	'_CONSTRAINTS',
	'_CONTINUE',
	'_CONVERT',
	'_CORRESPONDING',
	'_CREATE',
	'_CROSS',
	'_CURRENT',
	'_CURRENT_DATE',
	'_CURRENT_TIME',
	'_CURRENT_TIMESTAMP',
	'_CURRENT_USER',
	'_CURSOR',
	'_DATE',
	'_DAY',
	'_DEALLOCATE',
	'_DEC',
	'_DECIMAL',
	'_DECLARE',
	'_DEFAULT',
	'_DEFERRABLE',
	'_DEFERRED',
	'_DELETE',
	'_DESC',
	'_DESCRIBE',
	'_DESCRIPTOR',
	'_DIAGNOSTICS',
	'_DISCONNECT',
	'_DISTINCT',
	'_DOMAIN',
	'_DOUBLE',
	'_DROP',
	'_ELSE',
	'_END',
	'_END-EXEC',
	'_ESCAPE',
	'_EXCEPT',
	'_EXCEPTION',
	'_EXEC',
	'_EXECUTE',
	'_EXISTS',
	'_EXTERNAL',
	'_EXTRACT',
	'_FALSE',
	'_FETCH',
	'_FIRST',
	'_FLOAT',
	'_FOR',
	'_FOREIGN',
	'_FOUND',
	'_FROM',
	'_FULL',
	'_GET',
	'_GLOBAL',
	'_GO',
	'_GOTO',
	'_GRANT',
	'_GROUP',
	'_HAVING',
	'_HOUR',
	'_IDENTITY',
	'_IMMEDIATE',
	'_IN',
	'_INDICATOR',
	'_INITIALLY',
	'_INNER',
	'_INPUT',
	'_INSENSITIVE',
	'_INSERT',
	'_INT',
	'_INTEGER',
	'_INTERSECT',
	'_INTERVAL',
	'_INTO',
	'_IS',
	'_ISOLATION',
	'_JOIN',
	'_KEY',
	'_LANGUAGE',
	'_LAST',
	'_LEADING',
	'_LEFT',
	'_LEVEL',
	'_LIKE',
	'_LOCAL',
	'_LOWER',
	'_MATCH',
	'_MAX',
	'_MIN',
	'_MINUTE',
	'_MODULE',
	'_MONTH',
	'_NAMES',
	'_NATIONAL',
	'_NATURAL',
	'_NCHAR',
	'_NEXT',
	'_NO',
	'_NOT',
	'_NULL',
	'_NULLIF',
	'_NUMERIC',
	'_OCTET_LENGTH',
	'_OF',
	'_ON',
	'_ONLY',
	'_OPEN',
	'_OPTION',
	'_OR',
	'_ORDER',
	'_OUTER',
	'_OUTPUT',
	'_OVERLAPS',
	'_PAD',
	'_PARTIAL',
	'_POSITION',
	'_PRECISION',
	'_PREPARE',
	'_PRESERVE',
	'_PRIMARY',
	'_PRIOR',
	'_PRIVILEGES',
	'_PROCEDURE',
	'_PUBLIC',
	'_READ',
	'_REAL',
	'_REFERENCES',
	'_RELATIVE',
	'_RESTRICT',
	'_REVOKE',
	'_RIGHT',
	'_ROLLBACK',
	'_ROWS',
	'_SCHEMA',
	'_SCROLL',
	'_SECOND',
	'_SECTION',
	'_SELECT',
	'_SESSION',
	'_SESSION_USER',
	'_SET',
	'_SIZE',
	'_SMALLINT',
	'_SOME',
	'_SPACE',
	'_SQL',
	'_SQLCODE',
	'_SQLERROR',
	'_SQLSTATE',
	'_SUBSTRING',
	'_SUM',
	'_SYSTEM_USER',
	'_TABLE',
	'_TEMPORARY',
	'_THEN',
	'_TIME',
	'_TIMESTAMP',
	'_TIMEZONE_HOUR',
	'_TIMEZONE_MINUTE',
	'_TO',
	'_TRAILING',
	'_TRANSACTION',
	'_TRANSLATE',
	'_TRANSLATION',
	'_TRIM',
	'_TRUE',
	'_UNION',
	'_UNIQUE',
	'_UNKNOWN',
	'_UPDATE',
	'_UPPER',
	'_USAGE',
	'_USER',
	'_USING',
	'_VALUE',
	'_VALUES',
	'_VARCHAR',
	'_VARYING',
	'_VIEW',
	'_WHEN',
	'_WHENEVER',
	'_WHERE',
	'_WITH',
	'_WORK',
	'_WRITE',
	'_YEAR',
	'_ZONE',
	'LAST_TOKEN'
        );

  function yylex: integer;

  implementation

  uses SysUtils;

  function BestIdentifierOrLiteral(const TokenText: AnsiString): integer;
  var
    TokenUpper: AnsiString;
  begin
    TokenUpper := yytoken_text;
    //Assert(Length(TokenUpper)>0);
    TokenUpper := UpperCase(TokenUpper);
    result := Ord(identifier_body);
    case TokenUpper[1] of
      'A' :
        if TokenUpper = ('ABSOLUTE') then result := Ord(_ABSOLUTE)
        else if TokenUpper = ('ACTION') then result := Ord(_ACTION)
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
        if TokenUpper = ('CASCADE') then result := Ord(_CASCADE)
        else if TokenUpper = ('CASCADED') then result := Ord(_CASCADED)
        else if TokenUpper = ('CASE') then result := Ord(_CASE)
        else if TokenUpper = ('CAST') then result := Ord(_CAST)
        else if TokenUpper = ('CATALOG') then result := Ord(_CATALOG)
        else if TokenUpper = ('CHAR') then result := Ord(_CHAR)
        else if TokenUpper = ('CHARACTER') then result := Ord(_CHARACTER)
        else if TokenUpper = ('CHARACTER_LENGTH') then result := Ord(_CHARACTER_LENGTH)
        else if TokenUpper = ('CHAR_LENGTH') then result := Ord(_CHAR_LENGTH)
        else if TokenUpper = ('CHECK') then result := Ord(_CHECK)
        else if TokenUpper = ('CLOSE') then result := Ord(_CLOSE)
        else if TokenUpper = ('COALESCE') then result := Ord(_COALESCE)
        else if TokenUpper = ('COLLATE') then result := Ord(_COLLATE)
        else if TokenUpper = ('COLLATION') then result := Ord(_COLLATION)
        else if TokenUpper = ('COLUMN') then result := Ord(_COLUMN)
        else if TokenUpper = ('COMMIT') then result := Ord(_COMMIT)
        else if TokenUpper = ('CONNECT') then result := Ord(_CONNECT)
        else if TokenUpper = ('CONNECTION') then result := Ord(_CONNECTION)
        else if TokenUpper = ('CONSTRAINT') then result := Ord(_CONSTRAINT)
        else if TokenUpper = ('CONSTRAINTS') then result := Ord(_CONSTRAINTS)
        else if TokenUpper = ('CONTINUE') then result := Ord(_CONTINUE)
        else if TokenUpper = ('CONVERT') then result := Ord(_CONVERT)
        else if TokenUpper = ('CORRESPONDING') then result := Ord(_CORRESPONDING)
        else if TokenUpper = ('CREATE') then result := Ord(_CREATE)
        else if TokenUpper = ('CROSS') then result := Ord(_CROSS)
        else if TokenUpper = ('CURRENT') then result := Ord(_CURRENT)
        else if TokenUpper = ('CURRENT_DATE') then result := Ord(_CURRENT_DATE)
        else if TokenUpper = ('CURRENT_TIME') then result := Ord(_CURRENT_TIME)
        else if TokenUpper = ('CURRENT_TIMESTAMP') then result := Ord(_CURRENT_TIMESTAMP)
        else if TokenUpper = ('CURRENT_USER') then result := Ord(_CURRENT_USER)
        else if TokenUpper = ('CURSOR') then result := Ord(_CURSOR);
      'D':
        if TokenUpper = ('DATE') then result := Ord(_DATE)
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
        else if TokenUpper = ('DROP') then result := Ord(_DROP);
      'E':
        if TokenUpper = ('ELSE') then result := Ord(_ELSE)
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
        else if TokenUpper = ('LEVEL') then result := Ord(_LEVEL)
        else if TokenUpper = ('LIKE') then result := Ord(_LIKE)
        else if TokenUpper = ('LOCAL') then result := Ord(_LOCAL)
        else if TokenUpper = ('LOWER') then result := Ord(_LOWER);
      'M':
        if TokenUpper = ('MATCH') then result := Ord(_MATCH)
        else if TokenUpper = ('MAX') then result := Ord(_MAX)
        else if TokenUpper = ('MIN') then result := Ord(_MIN)
        else if TokenUpper = ('MINUTE') then result := Ord(_MINUTE)
        else if TokenUpper = ('MODULE') then result := Ord(_MODULE)
        else if TokenUpper = ('MONTH') then result := Ord(_MONTH);
      'N':
        if TokenUpper = ('NAMES') then result := Ord(_NAMES)
        else if TokenUpper = ('NATIONAL') then result := Ord(_NATIONAL)
        else if TokenUpper = ('NATURAL') then result := Ord(_NATURAL)
        else if TokenUpper = ('NCHAR') then result := Ord(_NCHAR)
        else if TokenUpper = ('NEXT') then result := Ord(_NEXT)
        else if TokenUpper = ('NO') then result := Ord(_NO)
        else if TokenUpper = ('NOT') then result := Ord(_NOT)
        else if TokenUpper = ('NULL') then result := Ord(_NULL)
        else if TokenUpper = ('NULLIF') then result := Ord(_NULLIF)
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
        else if TokenUpper = ('RESTRICT') then result := Ord(_RESTRICT)
        else if TokenUpper = ('REVOKE') then result := Ord(_REVOKE)
        else if TokenUpper = ('RIGHT') then result := Ord(_RIGHT)
        else if TokenUpper = ('ROLLBACK') then result := Ord(_ROLLBACK)
        else if TokenUpper = ('ROWS') then result := Ord(_ROWS);
      'S':
        if TokenUpper = ('SCHEMA') then result := Ord(_SCHEMA)
        else if TokenUpper = ('SCROLL') then result := Ord(_SCROLL)
        else if TokenUpper = ('SECOND') then result := Ord(_SECOND)
        else if TokenUpper = ('SECTION') then result := Ord(_SECTION)
        else if TokenUpper = ('SELECT') then result := Ord(_SELECT)
        else if TokenUpper = ('SESSION') then result := Ord(_SESSION)
        else if TokenUpper = ('SESSION_USER') then result := Ord(_SESSION_USER)
        else if TokenUpper = ('SET') then result := Ord(_SET)
        else if TokenUpper = ('SIZE') then result := Ord(_SIZE)
        else if TokenUpper = ('SMALLINT') then result := Ord(_SMALLINT)
        else if TokenUpper = ('SOME') then result := Ord(_SOME)
        else if TokenUpper = ('SPACE') then result := Ord(_SPACE)
        else if TokenUpper = ('SQL') then result := Ord(_SQL)
        else if TokenUpper = ('SQLCODE') then result := Ord(_SQLCODE)
        else if TokenUpper = ('SQLERROR') then result := Ord(_SQLERROR)
        else if TokenUpper = ('SQLSTATE') then result := Ord(_SQLSTATE)
        else if TokenUpper = ('SUBSTRING') then result := Ord(_SUBSTRING)
        else if TokenUpper = ('SUM') then result := Ord(_SUM)
        else if TokenUpper = ('SYSTEM_USER') then result := Ord(_SYSTEM_USER);
      'T':
        if TokenUpper = ('TABLE') then result := Ord(_TABLE)
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
        else if TokenUpper = ('TRUE') then result := Ord(_TRUE);
      'U':
        if TokenUpper = ('UNION') then result := Ord(_UNION)
        else if TokenUpper = ('UNIQUE') then result := Ord(_UNIQUE)
        else if TokenUpper = ('UNKNOWN') then result := Ord(_UNKNOWN)
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


function yylex : Integer;

procedure yyaction ( yyruleno : Integer );
  (* local definitions: *)

begin
  (* actions: *)
  case yyruleno of
  1:
                        return (BestIdentifierOrLiteral(yytext));
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

  end;
end(*yyaction*);

(* DFA table: *)

type YYTRec = record
                cc : set of AnsiChar;
                s  : Integer;
              end;

const

yynmarks   = 41;
yynmatches = 41;
yyntrans   = 91;
yynstates  = 48;

yyk : array [1..yynmarks] of Integer = (
  { 0: }
  { 1: }
  { 2: }
  1,
  { 3: }
  1,
  { 4: }
  1,
  { 5: }
  20,
  { 6: }
  1,
  { 7: }
  17,
  { 8: }
  7,
  { 9: }
  33,
  { 10: }
  35,
  { 11: }
  38,
  { 12: }
  29,
  { 13: }
  13,
  { 14: }
  14,
  { 15: }
  15,
  { 16: }
  16,
  { 17: }
  18,
  { 18: }
  19,
  { 19: }
  21,
  { 20: }
  22,
  { 21: }
  23,
  { 22: }
  24,
  { 23: }
  25,
  { 24: }
  26,
  { 25: }
  27,
  { 26: }
  28,
  { 27: }
  30,
  { 28: }
  31,
  { 29: }
  32,
  { 30: }
  34,
  { 31: }
  36,
  { 32: }
  37,
  { 33: }
  { 34: }
  { 35: }
  { 36: }
  4,
  { 37: }
  { 38: }
  { 39: }
  6,
  { 40: }
  8,
  { 41: }
  10,
  { 42: }
  9,
  { 43: }
  11,
  { 44: }
  12,
  { 45: }
  2,
  { 46: }
  3,
  { 47: }
  5
);

yym : array [1..yynmatches] of Integer = (
{ 0: }
{ 1: }
{ 2: }
  1,
{ 3: }
  1,
{ 4: }
  1,
{ 5: }
  20,
{ 6: }
  1,
{ 7: }
  17,
{ 8: }
  7,
{ 9: }
  33,
{ 10: }
  35,
{ 11: }
  38,
{ 12: }
  29,
{ 13: }
  13,
{ 14: }
  14,
{ 15: }
  15,
{ 16: }
  16,
{ 17: }
  18,
{ 18: }
  19,
{ 19: }
  21,
{ 20: }
  22,
{ 21: }
  23,
{ 22: }
  24,
{ 23: }
  25,
{ 24: }
  26,
{ 25: }
  27,
{ 26: }
  28,
{ 27: }
  30,
{ 28: }
  31,
{ 29: }
  32,
{ 30: }
  34,
{ 31: }
  36,
{ 32: }
  37,
{ 33: }
{ 34: }
{ 35: }
{ 36: }
  4,
{ 37: }
{ 38: }
{ 39: }
  6,
{ 40: }
  8,
{ 41: }
  10,
{ 42: }
  9,
{ 43: }
  11,
{ 44: }
  12,
{ 45: }
  2,
{ 46: }
  3,
{ 47: }
  5
);

yyt : array [1..yyntrans] of YYTrec = (
{ 0: }
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
  ( cc: [ '0'..'9','A'..'Z','_','a'..'z' ]; s: 2),
{ 3: }
  ( cc: [ '''' ]; s: 33),
  ( cc: [ '0'..'9','A'..'Z','_','a'..'z' ]; s: 2),
{ 4: }
  ( cc: [ '''' ]; s: 34),
  ( cc: [ '0'..'9','A'..'Z','_','a'..'z' ]; s: 2),
{ 5: }
  ( cc: [ #1..'&','('..#255 ]; s: 35),
  ( cc: [ '''' ]; s: 36),
{ 6: }
  ( cc: [ '''' ]; s: 37),
  ( cc: [ '0'..'9','A'..'Z','_','a'..'z' ]; s: 2),
{ 7: }
  ( cc: [ #1..'!','#'..#255 ]; s: 38),
  ( cc: [ '"' ]; s: 39),
{ 8: }
{ 9: }
  ( cc: [ '=' ]; s: 41),
  ( cc: [ '>' ]; s: 40),
{ 10: }
  ( cc: [ '=' ]; s: 42),
{ 11: }
  ( cc: [ '|' ]; s: 43),
{ 12: }
  ( cc: [ '.' ]; s: 44),
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
  ( cc: [ #1..'&','('..#255 ]; s: 33),
  ( cc: [ '''' ]; s: 45),
{ 34: }
  ( cc: [ '''' ]; s: 46),
  ( cc: [ '0','1' ]; s: 34),
{ 35: }
  ( cc: [ #1..'&','('..#255 ]; s: 35),
  ( cc: [ '''' ]; s: 36),
{ 36: }
  ( cc: [ '''' ]; s: 35),
{ 37: }
  ( cc: [ '''' ]; s: 47),
  ( cc: [ '0'..'9','A'..'F','a'..'f' ]; s: 37),
{ 38: }
  ( cc: [ #1..'!','#'..#255 ]; s: 38),
  ( cc: [ '"' ]; s: 39),
{ 39: }
  ( cc: [ '"' ]; s: 38),
{ 40: }
{ 41: }
{ 42: }
{ 43: }
{ 44: }
{ 45: }
  ( cc: [ '''' ]; s: 33)
{ 46: }
{ 47: }
);

yykl : array [0..yynstates-1] of Integer = (
{ 0: } 1,
{ 1: } 1,
{ 2: } 1,
{ 3: } 2,
{ 4: } 3,
{ 5: } 4,
{ 6: } 5,
{ 7: } 6,
{ 8: } 7,
{ 9: } 8,
{ 10: } 9,
{ 11: } 10,
{ 12: } 11,
{ 13: } 12,
{ 14: } 13,
{ 15: } 14,
{ 16: } 15,
{ 17: } 16,
{ 18: } 17,
{ 19: } 18,
{ 20: } 19,
{ 21: } 20,
{ 22: } 21,
{ 23: } 22,
{ 24: } 23,
{ 25: } 24,
{ 26: } 25,
{ 27: } 26,
{ 28: } 27,
{ 29: } 28,
{ 30: } 29,
{ 31: } 30,
{ 32: } 31,
{ 33: } 32,
{ 34: } 32,
{ 35: } 32,
{ 36: } 32,
{ 37: } 33,
{ 38: } 33,
{ 39: } 33,
{ 40: } 34,
{ 41: } 35,
{ 42: } 36,
{ 43: } 37,
{ 44: } 38,
{ 45: } 39,
{ 46: } 40,
{ 47: } 41
);

yykh : array [0..yynstates-1] of Integer = (
{ 0: } 0,
{ 1: } 0,
{ 2: } 1,
{ 3: } 2,
{ 4: } 3,
{ 5: } 4,
{ 6: } 5,
{ 7: } 6,
{ 8: } 7,
{ 9: } 8,
{ 10: } 9,
{ 11: } 10,
{ 12: } 11,
{ 13: } 12,
{ 14: } 13,
{ 15: } 14,
{ 16: } 15,
{ 17: } 16,
{ 18: } 17,
{ 19: } 18,
{ 20: } 19,
{ 21: } 20,
{ 22: } 21,
{ 23: } 22,
{ 24: } 23,
{ 25: } 24,
{ 26: } 25,
{ 27: } 26,
{ 28: } 27,
{ 29: } 28,
{ 30: } 29,
{ 31: } 30,
{ 32: } 31,
{ 33: } 31,
{ 34: } 31,
{ 35: } 31,
{ 36: } 32,
{ 37: } 32,
{ 38: } 32,
{ 39: } 33,
{ 40: } 34,
{ 41: } 35,
{ 42: } 36,
{ 43: } 37,
{ 44: } 38,
{ 45: } 39,
{ 46: } 40,
{ 47: } 41
);

yyml : array [0..yynstates-1] of Integer = (
{ 0: } 1,
{ 1: } 1,
{ 2: } 1,
{ 3: } 2,
{ 4: } 3,
{ 5: } 4,
{ 6: } 5,
{ 7: } 6,
{ 8: } 7,
{ 9: } 8,
{ 10: } 9,
{ 11: } 10,
{ 12: } 11,
{ 13: } 12,
{ 14: } 13,
{ 15: } 14,
{ 16: } 15,
{ 17: } 16,
{ 18: } 17,
{ 19: } 18,
{ 20: } 19,
{ 21: } 20,
{ 22: } 21,
{ 23: } 22,
{ 24: } 23,
{ 25: } 24,
{ 26: } 25,
{ 27: } 26,
{ 28: } 27,
{ 29: } 28,
{ 30: } 29,
{ 31: } 30,
{ 32: } 31,
{ 33: } 32,
{ 34: } 32,
{ 35: } 32,
{ 36: } 32,
{ 37: } 33,
{ 38: } 33,
{ 39: } 33,
{ 40: } 34,
{ 41: } 35,
{ 42: } 36,
{ 43: } 37,
{ 44: } 38,
{ 45: } 39,
{ 46: } 40,
{ 47: } 41
);

yymh : array [0..yynstates-1] of Integer = (
{ 0: } 0,
{ 1: } 0,
{ 2: } 1,
{ 3: } 2,
{ 4: } 3,
{ 5: } 4,
{ 6: } 5,
{ 7: } 6,
{ 8: } 7,
{ 9: } 8,
{ 10: } 9,
{ 11: } 10,
{ 12: } 11,
{ 13: } 12,
{ 14: } 13,
{ 15: } 14,
{ 16: } 15,
{ 17: } 16,
{ 18: } 17,
{ 19: } 18,
{ 20: } 19,
{ 21: } 20,
{ 22: } 21,
{ 23: } 22,
{ 24: } 23,
{ 25: } 24,
{ 26: } 25,
{ 27: } 26,
{ 28: } 27,
{ 29: } 28,
{ 30: } 29,
{ 31: } 30,
{ 32: } 31,
{ 33: } 31,
{ 34: } 31,
{ 35: } 31,
{ 36: } 32,
{ 37: } 32,
{ 38: } 32,
{ 39: } 33,
{ 40: } 34,
{ 41: } 35,
{ 42: } 36,
{ 43: } 37,
{ 44: } 38,
{ 45: } 39,
{ 46: } 40,
{ 47: } 41
);

yytl : array [0..yynstates-1] of Integer = (
{ 0: } 1,
{ 1: } 32,
{ 2: } 63,
{ 3: } 64,
{ 4: } 66,
{ 5: } 68,
{ 6: } 70,
{ 7: } 72,
{ 8: } 74,
{ 9: } 74,
{ 10: } 76,
{ 11: } 77,
{ 12: } 78,
{ 13: } 79,
{ 14: } 79,
{ 15: } 79,
{ 16: } 79,
{ 17: } 79,
{ 18: } 79,
{ 19: } 79,
{ 20: } 79,
{ 21: } 79,
{ 22: } 79,
{ 23: } 79,
{ 24: } 79,
{ 25: } 79,
{ 26: } 79,
{ 27: } 79,
{ 28: } 79,
{ 29: } 79,
{ 30: } 79,
{ 31: } 79,
{ 32: } 79,
{ 33: } 79,
{ 34: } 81,
{ 35: } 83,
{ 36: } 85,
{ 37: } 86,
{ 38: } 88,
{ 39: } 90,
{ 40: } 91,
{ 41: } 91,
{ 42: } 91,
{ 43: } 91,
{ 44: } 91,
{ 45: } 91,
{ 46: } 92,
{ 47: } 92
);

yyth : array [0..yynstates-1] of Integer = (
{ 0: } 31,
{ 1: } 62,
{ 2: } 63,
{ 3: } 65,
{ 4: } 67,
{ 5: } 69,
{ 6: } 71,
{ 7: } 73,
{ 8: } 73,
{ 9: } 75,
{ 10: } 76,
{ 11: } 77,
{ 12: } 78,
{ 13: } 78,
{ 14: } 78,
{ 15: } 78,
{ 16: } 78,
{ 17: } 78,
{ 18: } 78,
{ 19: } 78,
{ 20: } 78,
{ 21: } 78,
{ 22: } 78,
{ 23: } 78,
{ 24: } 78,
{ 25: } 78,
{ 26: } 78,
{ 27: } 78,
{ 28: } 78,
{ 29: } 78,
{ 30: } 78,
{ 31: } 78,
{ 32: } 78,
{ 33: } 80,
{ 34: } 82,
{ 35: } 84,
{ 36: } 85,
{ 37: } 87,
{ 38: } 89,
{ 39: } 90,
{ 40: } 90,
{ 41: } 90,
{ 42: } 90,
{ 43: } 90,
{ 44: } 90,
{ 45: } 91,
{ 46: } 91,
{ 47: } 91
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

  yylex := yyretval;

end(*yylex*);



end.

