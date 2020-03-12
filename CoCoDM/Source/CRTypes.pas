unit CRTypes;
{$INCLUDE CocoCD.inc}

interface

uses
  Classes,
  Sets;

const
  MAX_MULTIPLIER = 10;

  maxClasses   =  400 * MAX_MULTIPLIER; // max number of character classes
  maxList      =  150 * MAX_MULTIPLIER; // max array size in FindCircularProductions
  maxLiterals  =  130 * MAX_MULTIPLIER; // max number of literal terminals
  maxNodes     = 1500 * MAX_MULTIPLIER; // max number of top-down graph nodes
  maxTerminals =  256 * MAX_MULTIPLIER; // max number of terminals
  maxNt        =  128 * MAX_MULTIPLIER; // max number of nonterminals
  maxSymbols   = maxTerminals;          // max number of symbols (terminals+nonterminals+pragmas)

  maxSemLen = 64000; // max length of a semantic text
  symSetSize = 100; { max.number of symbol sets of the generated parser }

  {Error types}
  etLL1 = 2;
  etGrammar = 3;
  etWarning = 4;
  etGeneration = 5;
  etMetaError = 6;
  etMetaWarning = 7;

  normTrans = 0; // DFA transition during normal scanning
  contextTrans = 1; // DFA transition during scanning of right context

  CRTunknown = 0;
  CRTt = 1; // terminal symbol
  CRTpr = 2; // pragma
  CRTnt = 3; // nonterminal symbol
  CRTchrclass = 4; // character class
  CRTchart = 5; // single character
  CRTwt = 6; // weak terminal symbol
  CRTany = 7; // symbol ANY
  CRTeps = 8; // empty alternative
  CRTsync = 9; // symbol SYNC
  CRTsem = 10; // semantic action
  CRTalt = 11; // alternative
  CRTiter = 12; // iteration
  CRTopt = 13; // option
  CRTif = 14; // if
  CRTelse = 15; // else
  CRTendsc = 16; // end with semi-colon
  CRTend = 17; // end without semi-colon

  CRTnoSym = -1;
  CRTeofSy = 0;

  { token kinds }
  CRTclassToken = 0; // token class
  CRTlitToken = 1; // literal (e.g. keyword) not recognized by DFA
  CRTclassLitToken = 2; // token class that can also match a literal

  CRTMaxNameLength = 255; // max length of token name

type
  TProcedureStreamLine = procedure (s : AnsiString; const AddEndOfLine : boolean) of object;
  TProcedureString = procedure (const S : AnsiString) of object;
  TProcedure = procedure of object;
  TErrorStr = function (const ErrorCode : integer; const Data : AnsiString) : AnsiString of object;
  TSetGrammarNameEvent = procedure(const GrammarName : AnsiString) of object;
  TGetGrammarNameEvent = function : AnsiString of object;
  TFunctionGetErrorData = function(const Token1 : integer; const Token2 : integer) : AnsiString of object;
  TInDistinguishedErrorEvent = procedure (const Token : integer; const Data : AnsiString) of object;
  TAbortErrorMessageEvent = procedure(const AbortErrorMsg : AnsiString) of object;
  TExecuteFrameParser = function (const aFileName : AnsiString;
      const aGrammarName : AnsiString; const aFrameFileName : AnsiString) : boolean of object;

  { node types }
  TCocoNodeType = (cntUnknown, cntTerminal, cntPragma, cntNonTerminal,
      cntCharacterClass, cntCharacter, cntWeakTerminal, cntAny,
      cntEmptyAlternative, cntSync, cntSymanticAction, cntAlternative,
      cntIteration, cntOption);

  CRTName = ShortString;

  TCRTPosition = class(TObject)
  private
    fTextPresent: boolean;
    fTextLength: integer;
    fColumn: integer;
    fStartText: longint;
  public
    procedure Clear;

    property TextPresent : boolean read fTextPresent write fTextPresent;
    property StartText : longint read fStartText write fStartText; // start relative to beginning of file
    property TextLength : integer read fTextLength write fTextLength; // length
    property Column : integer read fColumn write fColumn; // column number of start position
  end;

  CRTPosition = record // position of stretch of source text
    beg : longint; // start relative to beginning of file
    len : longint; // length
    col : integer; // column number of start position
  end;

  THomographType = (htNone, htHomograph, htDefaultIdent);
  CRTSymbolNode = record // node of symbol table
//    SymbolType : TCocoNodeType; // nt, t, pr, unknown, etc
    typ : integer;
    HomographType : THomographType;
    ErrorDesc : AnsiString;
    name : CRTName; // symbol name
    constant : CRTName; // named constant of symbol
    struct : integer; // typ = nt: index of first node of syntax graph
                                        // typ = t: token kind: literal, class, ...
    deletable : boolean; // typ = nt: TRUE, if nonterminal is deletable
    attrPos : CRTPosition; // position of attributes in source text
    semPos : CRTPosition; // typ = pr: pos of sem action in source text
                                        // typ = nt: pos of local decls in source text
    line : integer; // source text line number of symbol in this node
  end;

  CRTGraphNode = record // node of top-down graph
    typ : integer; // nt,sts,wts,chart,class,any,eps,sem,sync,alt, iter,opt
    next : integer; // to successor node
                  // next < 0: to successor of enclosing structure
    p1 : integer; // typ IN {nt, t, wt}: index to symbol table
                  // typ = any: index to anyset
                  // typ = sync: index to syncset
                  // typ = alt: index of first node of first alternative
                  // typ IN {iter, opt}: first node in subexpression
                  // typ = chart: ordinal character value
                  // typ = class: index of character class
    p2 : integer; // typ = alt: index of first node of second alternative
                  // typ IN {chart, class}: transition code
    pos : CRTPosition; // typ IN {nt, t, wt}: source pos of actual attributes
                  // typ = sem: source pos of sem action
    line : integer; // source text line number of item in this node
    BooleanFunction : shortstring;
  end;

// The next is nasty - we really want different sizes of sets
//  CRTSet   = ARRAY [0 .. maxTerminals DIV 16 {Sets.size} ] OF BITSET;
  CRTSet = Sets.BITARRAY;

// This means some hacking later on from the original Modula stuff
  MarkList = array[0..maxNodes div Sets.size] of BITSET;

function ErrorDesc(const ErrorType : integer) : AnsiString;

implementation

uses
  SysUtils;

{ TCRTPosition }

procedure TCRTPosition.Clear;
begin
  fTextPresent := FALSE;
  fTextLength := 0;
  fColumn := 0;
  fStartText := 0;
end; {Clear}

function ErrorDesc(const ErrorType : integer) : AnsiString;
begin
  case ErrorType of
    0 : Result := 'syntax error';
    1 : Result := 'symantic error';
    2 : Result := 'LL(1) error';
    3 : Result := 'grammar error';
    4 : Result := 'warning';
    5 : Result := 'generation error';
  end; // case
end;

end.

