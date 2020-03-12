Unit CRT;
{$INCLUDE CocoCD.inc}

{ CRT       Table Handler
  =======   =============
  (1) handles a symbol table for terminals, pragmas and nonterminals
  (2) handles a table for character classes (for scanner generation)
  (3) handles a top-down graph for productions
  (4) computes various sets (start symbols, followers, any sets)
  (5) contains procedures for grammar tests
  --------------------------------------------------------------------}

interface

uses
  CocoSwitch, Classes, CRTypes, Sets, CocoBase;

type
  TTableHandler = class(TObject)
  private
    fHasWeak: boolean;
    fCocoAborted: boolean;
    fGenScanner: boolean;
    fIgnoreCase: boolean;
    fSymNames: boolean;
    fFirstNt: integer;
    fMaxP: integer;
    fLastNt: integer;
    fShowWarnings : boolean;
    fShowLL1StartSucc : boolean;
    fNumWarnings: integer;
    fnNodes: integer;
    fMaxT: integer;
    fMaxC: integer;
    fRoot: integer;
    fSrcDirectory: AnsiString;
    fSemDeclPos: TCRTPosition;
    fDestroyDeclPos: TCRTPosition;
    fCreateDeclPos: TCRTPosition;
    fPublicDeclPos: TCRTPosition;
    fErrorsDeclPos: TCRTPosition;
    fConstDeclPos: TCRTPosition;
    fPrivateDeclPos: TCRTPosition;
    fTypeDeclPos: TCRTPosition;
    fProtectedDeclPos: TCRTPosition;
    fPublishedDeclPos: TCRTPosition;
    FImplementationUses: TStringList;
    FErrorList: TStringList;
    FInterfaceUses: TStringList;
    fLL1Acceptable: boolean;
    fHomographCount: integer;
    fMaxAlternates: integer;
    fSwitchSet: TCocoSwitchSet;
    fOnStreamLn: TProcedureStreamLine;
    fOnPrintDivider: TProcedure;
    fOnScannerError: TErrorProc;
    fOnErrorStr: TErrorStr;
    fOnAbortErrorMessage: TAbortErrorMessageEvent;
    procedure BuildName(var oldName, newName: CRTName);
    procedure CompAnySets;
    procedure CompFirstSets(doPrint: boolean);
    procedure CompFollowSets;
    procedure CompSyncSets;
    procedure MovePragmas;
    procedure PrintFirstSet(gp: integer; fs: CRTSet);
    procedure PrintSet(s: BITARRAY; indent: integer);
    procedure SymName(symn: CRTName; var conn: CRTName);
    procedure WriteText(s : AnsiString; w : integer);
    procedure SetErrorList(const Value: TStringList);
    procedure SetImplementationUses(const Value: TStringList);
    procedure SetInterfaceUses(const Value: TStringList);
    function GetHasLiterals: boolean;
    function GetHasLiteralTokens: boolean;
    function IfStatementCheckProduction(gp: integer;
      checked: CRTSet): integer;
    (*function TestIfStatements: integer;*)
  public
    IgnoredCharSet : CRTSet; { characters ignored by the scanner }
    function NextPointerLeadsToIfStatement(
        const gp: integer;
        var HasSemantic : boolean): boolean;

    property maxT : integer read fMaxT write fMaxT; { terminals stored from 0 .. maxT in symbol table }
    property maxP : integer read fMaxP write fMaxP; { pragmas stored from maxT+1..maxP in symbol table }
    property firstNt : integer read fFirstNt write fFirstNt; { index of first nt: available after CompSymbolSets }
    property lastNt : integer read fLastNt write fLastNt; { index of last nt: available after CompSymbolSets }
    property maxC : integer read fMaxC write fMaxC; { index of last character class }
    property nNodes : integer read fnNodes write fnNodes; { index of last top-down graph node }
    property Root : integer read fRoot write fRoot; { index of root node, filled by ATG }

    property InterfaceUses : TStringList read FInterfaceUses write SetInterfaceUses;
    property ImplementationUses : TStringList read FImplementationUses write SetImplementationUses;

    property ErrorList : TStringList read FErrorList write SetErrorList;

    property ConstDeclPos : TCRTPosition read fConstDeclPos write fConstDeclPos;
    property TypeDeclPos : TCRTPosition read fTypeDeclPos write fTypeDeclPos;
    property PrivateDeclPos : TCRTPosition read fPrivateDeclPos write fPrivateDeclPos;
    property ProtectedDeclPos : TCRTPosition read fProtectedDeclPos write fProtectedDeclPos;
    property PublicDeclPos : TCRTPosition read fPublicDeclPos write fPublicDeclPos;
    property PublishedDeclPos : TCRTPosition read fPublishedDeclPos write fPublishedDeclPos;
    property ErrorsDeclPos : TCRTPosition read fErrorsDeclPos write fErrorsDeclPos;
    property CreateDeclPos : TCRTPosition read fCreateDeclPos write fCreateDeclPos;
    property DestroyDeclPos : TCRTPosition read fDestroyDeclPos write fDestroyDeclPos;

    property SemDeclPos : TCRTPosition read fSemDeclPos write fSemDeclPos; { position of global semantic declarations }

    property GenScanner : boolean read fGenScanner write fGenScanner; { TRUE: a scanner shall be generated }
    property IgnoreCase : boolean read fIgnoreCase write fIgnoreCase; { TRUE: scanner treats lower case as upper case }
    property SymNames : boolean read fSymNames write fSymNames; { TRUE: symbol names have to be assigned }
    property CocoAborted : boolean read fCocoAborted write fCocoAborted;
    property ShowWarnings : boolean read fShowWarnings write fShowWarnings;
    property ShowLL1StartSucc : boolean read fShowLL1StartSucc write fShowLL1StartSucc;
    property NumWarnings : integer read fNumWarnings write fNumWarnings;
    property LL1Acceptable : boolean read fLL1Acceptable;

    property HasWeak : boolean read fHasWeak write fHasWeak;
    property SrcDirectory : AnsiString read fSrcDirectory write fSrcDirectory;
    property HomographCount : integer read fHomographCount write fHomographCount;

    constructor Create;
    destructor Destroy; override;

    function GetDefaultSymbol : CRTSymbolNode;
    procedure AssignSymNames;
    function Alternatives(gp: integer): integer;
    function ClassWithName(n : CRTName) : integer; { Searches for a class with the given name.  Returns its index or -1 }
    function ClassWithSet(s : CRTSet) : integer; { Searches for a class with the given set. Returns its index or -1 }
    function CompDeletableSymbols(doPrint : boolean) : integer; { Marks deletable nonterminals and prints them. }
    procedure Clear;
    procedure ClearMarkList(var m : MarkList);  { Clears all elements of m }
    procedure CompExpected(gp, sp : integer; var exp : CRTSet); { Computes all symbols expected at location gp in graph of symbol sp. }
    procedure CompFirstSet(gp : integer; var fs : CRTSet; doPrint : boolean); { Computes start symbols of graph gp. }
    procedure CompleteGraph(gp : integer); { Lets right ends of graph gp be 0 }
    procedure CompSymbolSets(doPrint : boolean); { Collects first-sets, follow-sets, any-sets, and sync-sets. }
    procedure ConcatAlt(var gL1, gR1 : integer; gL2, gR2 : integer); { Makes (gL2, gR2) an alternative of the graph (gL1, gR1). The resulting graph is identified by (gL1, gR1). }
    procedure ConcatSeq(var gL1, gR1 : integer; gL2, gR2 : integer); { Concatenates graph (gL1, gR1) with graph (gL2, gR2) via next-chain. The resulting graph is identified by (gL1, gR1). }
    function DelGraph(gp : integer) : boolean; { TRUE, if (sub) graph with root gp is deletable. }
    function DelNode(gn : CRTGraphNode) : boolean; { TRUE, if graph node gn is deletable, i.e. can be derived into the empty AnsiString. }
    function FindCircularProductions : integer; { Finds and prints the circular part of the grammar. ok = TRUE means no circular part. }
    function FindSym(n : CRTName) : integer; { Gets symbol index for identifier with name n. }
    procedure GetClass(n : integer; var s : CRTSet); { Returns character class n }
    procedure GetClassName(n : integer; var name : CRTName); { Returns the name of class n }
    procedure GetNode(gp : integer; var n : CRTGraphNode); { Gets graph node with index gp in gn. }
    procedure GetSet(nr : integer; var s : CRTSet); { Gives access to precomputed symbol sets }
    procedure GetSym(sp : integer; var sn : CRTSymbolNode); { Gets symbol node with index sp in sn. }
    function IsInMarkList(var s : MarkList; x : integer) : boolean; { Returns x IN s }
    procedure InclMarkList(var s : MarkList; x : integer); { s.INCL(x) }
    procedure Initialize;
    function LL1Test : integer; { Checks if the grammar satisfies the LL(1) conditions. LL1 = 0 means no LL(1)-conflicts. }
    procedure MakeFirstAlt(var gL, gR : integer); { Generates an alt-node with (gL, gR) as its first and only alternative }
    procedure MakeIteration(var gL, gR : integer); { Encloses the graph (gL, gR) into an iteration construct. The resulting graph is identified by (gL, gR). }
    procedure MakeOption(var gL, gR : integer); { Encloses the graph (gL, gR) into an option construct. The resulting graph is identified by (gL, gR). }
    procedure NewName(n : CRTName; s : AnsiString); { Inserts the pair (n, s) in the token symbol name table }
    function NewSym(typ : integer; n : CRTName; line : integer) : integer; { Generates a new symbol with type t and name n and returns its index }
    function NewSet(s : CRTSet) : integer; { Stores s as a new set and returns its index. }
    function NewClass(n : CRTName; cset : CRTSet) : integer; { Defines a new character class and returns its index }
    function NewNode(typ, p1, line : integer) : integer; { Generates a new graph node with typ, p1, and source line number line and returns its index. }
    procedure PutNode(gp : integer; n : CRTGraphNode); { Replaces graph node with index gp by gn. }
    procedure PutSym(sp : integer; sn : CRTSymbolNode); { Replaces symbol node with index sp by sn. }
    procedure PrintSymbolTable; { Prints the symbol table (for tracing). }
    procedure PrintGraph; { Prints the graph (for tracing). }
    procedure PrintStartFollowerSets;
    procedure WriteStatistics(const MaxSS : integer);
    procedure Restriction(n, limit : integer); { Signal compiler restriction and abort program }
    procedure StrToGraph(s : AnsiString; var gL, gR : integer); { Generates linear graph from characters in s }
    function TestCompleteness : integer; { ok = TRUE, if all nonterminals have productions. }
    function TestIfAllNtReached : integer; { ok = TRUE, if all nonterminals can be reached from the start symbol. }
    function TestIfNtToTerm : integer; { ok = TRUE, if all nonterminals can be reduced to terminals. }
    function TestHomographs : integer;
    procedure XRef; { Produces a cross reference listing of all symbols. }

    property HasLiterals : boolean read GetHasLiterals;
    property HasLiteralTokens : boolean read GetHasLiteralTokens;
    property MaxAlternates : integer read fMaxAlternates write fMaxAlternates;
    property SwitchSet : TCocoSwitchSet read fSwitchSet write fSwitchSet;

    property OnStreamLn : TProcedureStreamLine read fOnStreamLn write fOnStreamLn;
    property OnPrintDivider : TProcedure read fOnPrintDivider write fOnPrintDivider;
    property OnScannerError : TErrorProc read fOnScannerError write fOnScannerError;
    property OnErrorStr : TErrorStr read fOnErrorStr write fOnErrorStr;
    property OnAbortErrorMessage : TAbortErrorMessageEvent read fOnAbortErrorMessage write fOnAbortErrorMessage;
  end; {TTableHandler}

implementation

uses
  CocoTools, SysUtils;

const
  maxSetNr = 200;  { max. number of symbol sets }
  maxNames = 100; { max. number of declared token names }

type
  FirstSets = array[0..maxNt] of record
    ts : CRTSet; { terminal symbols }
    ready : boolean; { TRUE = ts is complete }
  end;
  FollowSets = array[0..maxNt] of record
    ts : CRTSet; { terminal symbols }
    nts : CRTSet; { nts whose start set is to be included in ts }
  end;
  CharClass = record
    name : CRTName; { class name }
    cset : integer { ptr to set representing the class }
  end;
  SymbolTable = array[0..maxSymbols] of CRTSymbolNode;
  ClassTable = array[0..maxClasses] of CharClass;
  GraphList = array[0..maxNodes] of CRTGraphNode;
  SymbolSet = array[0..maxSetNr] of CRTSet;
  NameTable = array[1..maxNames] of record
    name : CRTName;
    definition : CRTName
  end;

var
  { moved symbol table to the heap Fri  08-20-1993 to allow larger one }
  st : ^SymbolTable; { symbol table for terminals, pragmas, and nonterminals }
  gn : ^GraphList; { top-down graph }
  tt : NameTable; { table of token name declarations }
  first : FirstSets; { first[i]  = first symbols of st[i+firstNt] }
  follow : FollowSets; { follow[i] = followers of st[i+firstNt] }
  chClass : ClassTable; { character classes }
  cset : SymbolSet; { cset[0] = all SYNC symbols }
  maxSet : integer; { index of last symbol set }
  lastName, dummyName : integer; { for unnamed character classes }
  curSy : integer; { symbol whose production is currently being checked for if statement violations }

procedure TTableHandler.WriteText(s : AnsiString; w : integer);
begin
  fOnStreamLn(PadR(S, ' ', W), FALSE);
end;

{ Restriction          Implementation restriction
----------------------------------------------------------------------}

procedure TTableHandler.Restriction(n, limit : integer);
var
  AbortStr : AnsiString;
begin
  AbortStr := 'Restriction ' + IntToStr(n) + ': ';
  case n of
    1 : AbortStr := AbortStr + 'Too many graph nodes';
    2 : AbortStr := AbortStr + 'Too many symbols';
    3 : AbortStr := AbortStr + 'Too many sets';
    4 : AbortStr := AbortStr + 'Too many character classes';
    5 : AbortStr := AbortStr + 'Too many symbol sets';
    6 : AbortStr := AbortStr + 'Too many token names';
    7 : AbortStr := AbortStr + 'Too many states';
    8 : AbortStr := AbortStr + 'Semantic text buffer overflow';
    9 : AbortStr := AbortStr + 'Circular check buffer overflow';
    10 : AbortStr := AbortStr + 'Too many literal terminals';
    11 : AbortStr := AbortStr + 'Too many non-terminals';
    - 1 : AbortStr := AbortStr + 'Compiler error';
  end;
  if n > 0 then
    AbortStr := AbortStr + ' - limited to: ' + IntToStr(Limit);
    { maybe we want CRX.WriteStatistics; }
  fOnAbortErrorMessage(AbortStr);
  fCocoAborted := true;
end;

{ MovePragmas          Move pragmas after terminals
----------------------------------------------------------------------}

procedure TTableHandler.MovePragmas;
var
  i : integer;
begin
  if fMaxP > fFirstNt then
  begin
    i := MaxSymbols - 1;
    fMaxP := fMaxT;
    while i > fLastNt do
    begin
      inc(fMaxP);
      if fMaxP >= fFirstNt then
        Restriction(2, MaxSymbols);
      st^[MaxP] := st^[i];
      dec(i)
    end;
  end
end;

{ ClearMarkList        Clear mark list m
----------------------------------------------------------------------}

procedure TTableHandler.ClearMarkList(var m : MarkList);
var
  i : integer;
begin
  i := 0;
  while i < MaxNodes div Sets.size do
  begin m[i] := [];
    inc(i)
  end;
end;

function TTableHandler.IsInMarkList(var s : MarkList; x : integer) : boolean;
begin
  Result := x mod size in s[x div size]
end;

procedure TTableHandler.InclMarkList(var s : MarkList; x : integer);
begin
  s[x div size] := s[x div size] + [x mod size]
end;

{ GetNode              Get node with index gp in n
----------------------------------------------------------------------}

procedure TTableHandler.GetNode(gp : integer; var n : CRTGraphNode);
begin
  n := gn^[gp]
end;

{ PutNode              Replace node with index gp by n
----------------------------------------------------------------------}

procedure TTableHandler.PutNode(gp : integer; n : CRTGraphNode);
begin
  gn^[gp] := n
end;

{ NewName              Collects a user defined token name
----------------------------------------------------------------------}

procedure TTableHandler.NewName(n : CRTName; s : AnsiString);
begin
  if lastName = maxNames then
    Restriction(6, maxNames)
  else
  begin
    inc(lastName);
    fSymNames := true;
    tt[LastName].name := n;
    tt[LastName].definition := s;
  end;
end;

{ NewSym               Generate a new symbol and return its index
----------------------------------------------------------------------}

function TTableHandler.NewSym(typ : integer; n : CRTName; line : integer) : integer;
var
  i : integer;
begin
  i := 0;
  if fMaxT + 1 = fFirstNt then
    Restriction(2, MaxSymbols)
  else
  begin
    case typ of
      CRTt :
        begin
          inc(fMaxT);
          i := fMaxT;
        end;
      CRTpr :
        begin
          dec(fMaxP);
          dec(fFirstNt);
          dec(fLastNt);
          i := fMaxP;
        end;
      CRTnt, CRTunknown :
        begin
          dec(fFirstNt);
          i := fFirstNt;
        end;
    end;
    if fMaxT + 1 >= fFirstNt then
      Restriction(2, MaxSymbols);
    st^[i].typ := typ;
    st^[i].HomographType := htNone;
    st^[i].name := n;
    st^[i].constant := ''; { Bug fix - PDT }
    st^[i].struct := 0;
    st^[i].deletable := false;
    st^[i].attrPos.beg := -1;
    st^[i].semPos.beg := -1;
    st^[i].line := line;
  end;
  Result := i;
end;

{ GetSym               Get symbol sp in sn
----------------------------------------------------------------------}

procedure TTableHandler.GetSym(sp : integer; var sn : CRTSymbolNode);
begin
  sn := st^[sp]
end;

{ PutSym               Replace symbol with index snix by sn
----------------------------------------------------------------------}

procedure TTableHandler.PutSym(sp : integer; sn : CRTSymbolNode);
begin
  st^[sp] := sn
end;

{ FindSym              Find index of symbol with name n
----------------------------------------------------------------------}

function TTableHandler.FindSym(n : CRTName) : integer;
var
  i : integer;

begin
  i := 0;
    {search in terminal list}
  while (i <= fMaxT) and (st^[i].name <> n) do
    inc(i);
  if i <= fMaxT then
  begin
    Result := i;
    Exit;
  end;
  i := fFirstNt;
    {search in nonterminal/pragma list}
  while (i < MaxSymbols) and (st^[i].name <> n) do
    inc(i);
  if i < MaxSymbols then
    Result := i
  else
    Result := CRTnoSym
end;

{ PrintSet             Print sets
----------------------------------------------------------------------}

procedure TTableHandler.PrintSet(s : BITARRAY; indent : integer);
const
  maxLineLen = 80;
var
  col, i, len : integer;
  empty : boolean;
  sn : CRTSymbolNode;
begin
  i := 0;
  col := indent;
  empty := true;
  while i <= fMaxT do
  begin
    if Sets.IsIn(s, i) then
    begin
      empty := false;
      GetSym(i, sn);
      len := Length(sn.name);
      if col + len + 2 > maxLineLen then
      begin
        fOnStreamLn('', TRUE);
        col := 1;
        while col < indent do
        begin
          fOnStreamLn(' ', FALSE);
          inc(col)
        end
      end;
      fOnStreamLn(sn.name + '  ', FALSE);
      inc(col, len + 2)
    end;
    inc(i)
  end;
  if empty then
    fOnStreamLn('-- empty set --', FALSE);
  fOnStreamLn('', TRUE);
end;

{ NewSet               Stores s as a new set and return its index
----------------------------------------------------------------------}

function TTableHandler.NewSet(s : CRTSet) : integer;
{any-set computation requires not to search if s is already in set}
begin
  inc(maxSet);
  if maxSet > maxSetNr then
    Restriction(3, maxSetNr)
  else
    cset[maxSet] := s;
  Result := maxSet
end;

{ CompFirstSet         Compute first symbols of (sub) graph at gp
----------------------------------------------------------------------}

procedure TTableHandler.PrintFirstSet(gp : integer; fs : CRTSet);
begin
  if crsTraceStartSets IN fSwitchSet then
  begin
    fOnStreamLn('', TRUE);
    fOnStreamLn('ComputeFirstSet: gp = ' + IntToStr(gp), TRUE);
    PrintSet(fs, 0);
  end;
end;

procedure TTableHandler.CompFirstSet(gp : integer; var fs : CRTSet; doPrint : boolean);
var
  visited : MarkList;

  procedure CompFirst(gp : integer; var fs : CRTSet);
  var
    s : CRTSet;
    gn : CRTGraphNode;
    sn : CRTSymbolNode;
  begin
    Sets.Clear(fs);
    while (gp <> 0) and not IsInMarkList(visited, gp) do
    begin
      GetNode(gp, gn);
      InclMarkList(visited, gp);
      case gn.typ of
        CRTnt :
          begin
            if first[gn.p1 - fFirstNt].ready then
              Sets.Unite(fs, first[gn.p1 - fFirstNt].ts)
            else
            begin
              GetSym(gn.p1, sn);
              CompFirst(sn.struct, s);
              Sets.Unite(fs, s);
            end;
          end;
        CRTt, CRTwt :
          begin Sets.Incl(fs, gn.p1)
          end;
        CRTany :
          begin Sets.Unite(fs, cset[gn.p1])
          end;
        CRTalt, CRTiter, CRTopt :
          begin
            CompFirst(gn.p1, s);
            Sets.Unite(fs, s);
            if gn.typ = CRTalt then
            begin
              CompFirst(gn.p2, s);
              Sets.Unite(fs, s)
            end
          end;
      else
          { eps, sem, sync, ind: nothing }
      end;
      if not DelNode(gn) then
        EXIT;
      gp := ABS(gn.next)
    end
  end;

begin { ComputeFirstSet }
  ClearMarkList(visited);
  CompFirst(gp, fs);
  if doPrint then
    PrintFirstSet(gp, fs);
end;

{ CompFirstSets        Compute first symbols of nonterminals
----------------------------------------------------------------------}

procedure TTableHandler.CompFirstSets(doPrint : boolean);
var
  i : integer;
  sn : CRTSymbolNode;
begin
  i := fFirstNt;
  if lastNt - firstNt > maxNt then
    Restriction(11, maxNt);
  while i <= fLastNt do
  begin
    first[i - fFirstNt].ready := false;
    inc(i)
  end;
  i := fFirstNt;
  while i <= fLastNt do
  begin { for all nonterminals }
    GetSym(i, sn);
    CompFirstSet(sn.struct, first[i - fFirstNt].ts, doPrint);
    first[i - fFirstNt].ready := true;
    inc(i)
  end;
end;

{ CompExpected     Compute symbols expected at location gp in Symbol sp
----------------------------------------------------------------------}

procedure TTableHandler.CompExpected(gp, sp : integer; var exp : CRTSet);
begin
  CompFirstSet(gp, exp, false);
  if DelGraph(gp) then
    Sets.Unite(exp, follow[sp - fFirstNt].ts)
end;

{ CompFollowSets       Get follow symbols of nonterminals
----------------------------------------------------------------------}

procedure TTableHandler.CompFollowSets;
var
  sn : CRTSymbolNode;
  curSy, i, size : integer;
  visited : MarkList;

  procedure CompFol(gp : integer);
  var
    s : CRTSet;
    gn : CRTGraphNode;
  begin
    while (gp > 0) and not IsInMarkList(visited, gp) do
    begin
      GetNode(gp, gn);
      InclMarkList(visited, gp);
      if gn.typ = CRTnt then
      begin
        CompFirstSet(ABS(gn.next), s, false);
        Sets.Unite(follow[gn.p1 - fFirstNt].ts, s);
        if DelGraph(ABS(gn.next)) then
          Sets.Incl(follow[gn.p1 - fFirstNt].nts, curSy - fFirstNt)
      end
      else if (gn.typ = CRTopt) or (gn.typ = CRTiter) then
        CompFol(gn.p1)
      else if gn.typ = CRTalt then
      begin
        CompFol(gn.p1);
        CompFol(gn.p2)
      end;
      gp := gn.next
    end
  end;

  procedure Complete(i : integer);
  var
    j : integer;
  begin
    if IsInMarkList(visited, i) then
      EXIT;
    InclMarkList(visited, i);
    j := 0;
    while j <= fLastNt - fFirstNt do
    begin { for all nonterminals }
      if Sets.IsIn(follow[i].nts, j) then
      begin
        Complete(j);
        Sets.Unite(follow[i].ts, follow[j].ts);
            { fix 1.42 } if i = curSy then
          Sets.Excl(follow[i].nts, j)
      end;
      inc(j)
    end;
  end;

begin { GetFollowSets }
  size := (fLastNT - fFirstNT + 2) div Sets.size;
  curSy := fFirstNT;
  while curSy <= fLastNT do
  begin
    Sets.Clear(follow[curSy - fFirstNT].ts);
    i := 0;
    while i <= size do
    begin
      follow[curSy - fFirstNT].nts[i] := [];
      inc(i)
    end;
    inc(curSy)
  end;

  ClearMarkList(visited);
  curSy := fFirstNT; {get direct successors of nonterminals}
  while curSy <= fLastNT do
  begin
    GetSym(curSy, sn);
    CompFol(sn.struct);
    inc(curSy)
  end;

  curSy := 0; {add indirect successors to follow.ts}
  while curSy <= fLastNT - fFirstNT do
  begin
    ClearMarkList(visited);
    Complete(curSy);
    inc(curSy);
  end;
end;

{ CompAnySets          Compute all any-sets
----------------------------------------------------------------------}

procedure TTableHandler.CompAnySets;
var
  curSy : integer;
  sn : CRTSymbolNode;

  function LeadingAny(gp : integer; var a : CRTGraphNode) : boolean;
  var
    gn : CRTGraphNode;
  begin
    if gp <= 0 then
    begin
      Result := false;
      EXIT
    end;
    GetNode(gp, gn);
    if (gn.typ = CRTany) then
    begin
      a := gn;
      Result := true
    end
    else
      LeadingAny := (gn.typ = CRTalt) and (LeadingAny(gn.p1, a)
        or LeadingAny(gn.p2, a)) or ((gn.typ = CRTopt)
        or (gn.typ = CRTiter)) and LeadingAny(gn.p1, a)
        or DelNode(gn) and LeadingAny(gn.next, a)
  end;

  procedure FindAS(gp : integer);
  var
    gn, gn2, a : CRTGraphNode;
    s1, s2 : CRTSet;
    p : integer;
  begin
    while gp > 0 do
    begin
      GetNode(gp, gn);
      if (gn.typ = CRTopt) or (gn.typ = CRTiter) then
      begin
        FindAS(gn.p1);
        if LeadingAny(gn.p1, a) then
        begin
          CompExpected(ABS(gn.next), curSy, s1);
          Sets.Differ(cset[a.p1], s1)
        end
      end
      else if gn.typ = CRTalt then
      begin
        p := gp;
        Sets.Clear(s1);
        while p <> 0 do
        begin
          GetNode(p, gn2);
          FindAS(gn2.p1);
          if LeadingAny(gn2.p1, a) then
          begin
            CompExpected(gn2.p2, curSy, s2);
            Sets.Unite(s2, s1);
            Sets.Differ(cset[a.p1], s2)
          end
          else
          begin
            CompFirstSet(gn2.p1, s2, false);
            Sets.Unite(s1, s2)
          end;
          p := gn2.p2
        end
      end;
      gp := gn.next
    end
  end;

begin
  curSy := fFirstNT;
  while curSy <= fLastNT do
  begin
    { for all nonterminals }
    GetSym(curSy, sn);
    FindAS(sn.struct);
    inc(curSy)
  end
end;

{ CompSyncSets         Compute follow symbols of sync-nodes
----------------------------------------------------------------------}

procedure TTableHandler.CompSyncSets;
var
  curSy : integer;
  sn : CRTSymbolNode;
  visited : MarkList;

  procedure CompSync(gp : integer);
  var
    s : CRTSet;
    gn : CRTGraphNode;
  begin
    while (gp > 0) and not IsInMarkList(visited, gp) do
    begin
      GetNode(gp, gn);
      InclMarkList(visited, gp);
      if gn.typ = CRTsync then
      begin
        CompExpected(ABS(gn.next), curSy, s);
        Sets.Incl(s, CRTeofSy);
        Sets.Unite(cset[0], s);
        gn.p1 := NewSet(s);
        PutNode(gp, gn)
      end
      else if gn.typ = CRTalt then
      begin
        CompSync(gn.p1);
        CompSync(gn.p2)
      end
      else if (gn.typ = CRTopt) or (gn.typ = CRTiter) then
      begin
        CompSync(gn.p1)
      end;
      gp := gn.next
    end
  end;

begin
  curSy := fFirstNT;
  ClearMarkList(visited);
  while curSy <= fLastNT do
  begin
    GetSym(curSy, sn);
    CompSync(sn.struct);
    inc(curSy);
  end
end;

{ CompDeletableSymbols Compute all deletable symbols and print them
----------------------------------------------------------------------}

function TTableHandler.CompDeletableSymbols(doPrint : boolean) : integer;
var
  changed : boolean;
  i : integer;
  sn : CRTSymbolNode;
  Count : integer;
  Symbol : TSymbolPosition;
begin
  repeat
    changed := false;
    i := fFirstNT;
    while i <= fLastNT do
    begin {for all nonterminals}
      GetSym(i, sn);
      if not sn.deletable and DelGraph(sn.struct) then
      begin
        sn.deletable := true;
        PutSym(i, sn);
        changed := true
      end;
      inc(i)
    end;
  until not changed;
  if DoPrint then
    fOnStreamLn('  Deletable symbols:', FALSE);
  i := fFirstNT;
  Count := 0;
  while i <= fLastNT do
  begin
    GetSym(i, sn);
    if sn.deletable then
    begin
      if DoPrint AND fShowWarnings then
      begin
        Count := Count + 1;
        fOnStreamLn('', TRUE);
        fOnStreamLn('     ' + sn.name, FALSE);
        Symbol := TSymbolPosition.Create;
        try
          Symbol.Line := sn.Line;
          fOnScannerError(200, Symbol, sn.Name, etWarning);
        finally
          Symbol.Free;
        end;
      end;
    end;
    inc(i);
  end;
  if (Count = 0) and DoPrint then
    fOnStreamLn('        -- none --', FALSE);
  fOnStreamLn('', TRUE);
  Result := Count;
end;

{ CompSymbolSets       Get first-sets, follow-sets, and sync-set
----------------------------------------------------------------------}

procedure TTableHandler.PrintStartFollowerSets;
var
  i : integer;
  sn : CRTSymbolNode;
begin
  if {CRTddt['F']} crsStartFollowSets IN fSwitchSet then
  begin
    fOnPrintDivider;
    i := fFirstNT;
    fOnStreamLn('List of first & follow symbols:', TRUE);
    fOnStreamLn('', TRUE);
    while i <= fLastNT do
    begin { for all nonterminals }
      GetSym(i, sn);
      fOnStreamLn(sn.name, TRUE);
      fOnStreamLn('first:   ', FALSE);
      PrintSet(first[i - fFirstNT].ts, 10);
      fOnStreamLn('follow:  ', FALSE);
      PrintSet(follow[i - fFirstNT].ts, 10);
      fOnStreamLn('', TRUE);
      inc(i);
    end;
    i := 0;
    fOnStreamLn('', TRUE);
    fOnStreamLn('', TRUE);
    fOnStreamLn('List of sets (ANY, SYNC): ', FALSE);
    if maxSet < 0 then
      fOnStreamLn('        -- none --', FALSE)
    else
      fOnStreamLn('', TRUE);
    while i <= maxSet do
    begin
      fOnStreamLn('     set[' + PadL(IntToStr(i), ' ', 2) + '] = ', FALSE);
      PrintSet(cset[i], 16);
      inc(i)
    end;
    fOnStreamLn('', TRUE);
  end;
end;

procedure TTableHandler.CompSymbolSets(doPrint : boolean);
begin
  MovePragmas;
  fNumWarnings := CompDeletableSymbols(not doPrint);
  if doPrint and {CRTddt['I']} (crsTraceStartSets IN fSwitchSet) then
  begin
    fOnPrintDivider;
    fOnStreamLn('Start set computations:', TRUE);
  end;
  CompFirstSets(doPrint);
  CompFollowSets;
  CompAnySets;
  CompSyncSets;
end;

(*  GetFirstSet and GetFollowSet where in the original TurboPascal version
    but are not used.  Save them for posterity.

{ GetFirstSet          Get precomputed first-set for nonterminal sp
----------------------------------------------------------------------}

procedure TTableHandler.GetFirstSet(sp : integer; var s : CRTSet);
begin
  s := first[sp - FirstNT].ts
end;

{ GetFollowSet         Get precomputed follow-set for nonterminal snix
----------------------------------------------------------------------}

procedure TTableHandler.GetFollowSet(sp : integer; var s : CRTSet);
begin
  s := follow[sp - FirstNT].ts
end;
*)

{ GetSet               Get set with index nr
----------------------------------------------------------------------}

procedure TTableHandler.GetSet(nr : integer; var s : CRTSet);
begin
  s := cset[nr]
end;

function TTableHandler.GetDefaultSymbol : CRTSymbolNode;
var
  i : integer;
  Found : boolean;
begin
  Result := st^[0];
  i := 0;
  Found := false;
  while (i < MaxSymbols) AND NOT Found do
  begin
    if st^[i].HomographType = htDefaultIdent then
    begin
      Result := st^[i];
      Found := True;
    end
    else
      inc(i);
  end;
end; {GetDeafultSymIndex}

{ PrintSymbolTable     Print symbol table
----------------------------------------------------------------------}
procedure TTableHandler.PrintSymbolTable;
var
  i : integer;

  procedure WriteBool(b : boolean);
  begin
    if b then
      fOnStreamLn('  TRUE ', FALSE)
    else
      fOnStreamLn('  FALSE', FALSE);
  end;

  procedure WriteTyp1(typ : integer);
  begin
    case typ of
      CRTunknown : fOnStreamLn(' unknown', FALSE);
      CRTt : fOnStreamLn(' t      ', FALSE);
      CRTpr : fOnStreamLn(' pr     ', FALSE);
      CRTnt : fOnStreamLn(' nt     ', FALSE);
    end;
  end;

begin { PrintSymbolTable }
  fOnStreamLn('SymbolTable:', TRUE);
  fOnStreamLn('nr     definition                ', FALSE);
  fOnStreamLn('constant        ', FALSE);
  fOnStreamLn('typ    hasAttrs struct del  line', TRUE);
  fOnStreamLn('', TRUE);
  i := 0;
  while i < MaxSymbols do
  begin
    fOnStreamLn(PadL(IntToStr(i), ' ', 4) + PadR('', ' ', 3), FALSE);

    if st^[i].constant = 'NOSYMB' then
      WriteText('', 26)
    else
      WriteText(st^[i].name, 26);
    if i <= fMaxT then
      WriteText(st^[i].constant, 16)
    else
      fOnStreamLn(PadR('', ' ', 16), FALSE);
    WriteTyp1(st^[i].typ);
    WriteBool(st^[i].attrPos.beg >= 0);
    fOnStreamLn(PadL(IntToStr(st^[i].struct), ' ', 5), FALSE);
    WriteBool(st^[i].deletable);
    fOnStreamLn(PadL(IntToStr(st^[i].line), ' ', 5), TRUE);
    if i = fMaxT then
      i := fFirstNT
    else
      inc(i);
  end;
end;

{ NewClass             Define a new character class
----------------------------------------------------------------------}

function TTableHandler.NewClass(n : CRTName; cset : CRTSet) : integer;
begin
  inc(fMaxC);
  if fMaxC > MaxClasses then
    Restriction(4, MaxClasses);
  if n[1] = '#' then
  begin
    n[2] := AnsiChar(ORD('A') + dummyName);
    inc(dummyName)
  end;
  chClass[fMaxC].name := n;
  chClass[fMaxC].cset := NewSet(cset);
  Result := fMaxC
end;

{ ClassWithName        Return index of class with name n
----------------------------------------------------------------------}

function TTableHandler.ClassWithName(n : CRTName) : integer;
var
  i : integer;
begin
  i := fMaxC;
  while (i >= 0) and (chClass[i].name <> n) do
    dec(i);
  Result := i
end;

{ ClassWithSet        Return index of class with the specified set
----------------------------------------------------------------------}

function TTableHandler.ClassWithSet(s : CRTSet) : integer;
var
  i : integer;
begin
  i := fMaxC;
  while (i >= 0) and not Sets.Equal(cset[chClass[i].cset], s) do
    dec(i);
  Result := i
end;

{ GetClass             Return character class n
----------------------------------------------------------------------}

procedure TTableHandler.GetClass(n : integer; var s : CRTSet);
begin
  GetSet(chClass[n].cset, s);
end;

{ GetClassName         Get the name of class n
----------------------------------------------------------------------}

procedure TTableHandler.GetClassName(n : integer; var name : CRTName);
begin
  name := chClass[n].name
end;

{ XRef                 Produce a cross reference listing of all symbols
----------------------------------------------------------------------}

procedure TTableHandler.XRef;
const
  maxLineLen = 80;
type
  ListPtr = ^ListNode;
  ListNode =
    record
    next : ListPtr;
    line : integer;
  end;
  ListHdr =
    record
    name : CRTName;
    lptr : ListPtr;
  end;
var
  gn : CRTGraphNode;
  col, i : integer;
  l, p, q : ListPtr;
  sn : CRTSymbolNode;
  xList : array[0..MaxSymbols] of ListHdr;
begin { XRef }
  if fMaxT <= 0 then
    EXIT;
  FillChar(xList,sizeof(xList),0);
  MovePragmas;
  { initialize cross reference list };
  i := 0;
  while i <= fLastNT do
  begin { for all symbols }
    GetSym(i, sn);
    xList[i].name := sn.name;
    xList[i].lptr := nil;
    if i = MaxP then
      i := fFirstNT
    else
      inc(i)
  end;
    { search lines where symbol has been referenced }
  i := 1;
  while i <= fnNodes do
  begin { for all graph nodes }
    GetNode(i, gn);
    if (gn.typ = CRTt) or (gn.typ = CRTwt) or (gn.typ = CRTnt) then
    begin
      NEW(l);
      l^.next := xList[gn.p1].lptr;
      l^.line := gn.line;
      xList[gn.p1].lptr := l
    end;
    inc(i);
  end;
    { search lines where symbol has been defined and insert in order }
  i := 1;
  while i <= fLastNT do
  begin {for all symbols}
    GetSym(i, sn);
    p := xList[i].lptr;
    q := nil;
    while (p <> nil) and (p^.line > sn.line) do
    begin
      q := p;
      p := p^.next
    end;
    NEW(l);
    l^.next := p;
    l^.line := -sn.line;
    if q <> nil then
      q^.next := l
    else
      xList[i].lptr := l;
    if i = MaxP then
      i := fFirstNT
    else
      inc(i)
  end;
    { print cross reference listing }
  fOnPrintDivider;
  fOnStreamLn('Cross reference list:', TRUE);
  fOnStreamLn('', TRUE);
  fOnStreamLn('Terminals:', TRUE);
  fOnStreamLn('  0  EOF', TRUE);
  i := 1;
  while i <= fLastNT do
  begin { for all symbols }
    if i = fMaxT then
    begin
      fOnStreamLn('', TRUE);
      fOnStreamLn('Pragmas:', TRUE);
    end
    else
    begin
      fOnStreamLn(PadL(IntToStr(i), ' ', 3) + '  ', FALSE);
      WriteText(xList[i].name, 25);
      l := xList[i].lptr;
      col := 35;
      while l <> nil do
      begin
        if col + 5 > maxLineLen then
        begin
          fOnStreamLn('', TRUE);
          fOnStreamLn(PadR('', ' ', 30), FALSE);
          col := 35
        end;
        if l^.line = 0 then
          fOnStreamLn('undef', FALSE)
        else
          fOnStreamLn(PadL(IntToStr(l^.line), ' ', 5), FALSE);
        inc(col, 5);
        l := l^.next
      end;
      fOnStreamLn('', TRUE);
    end;
    if i = MaxP then
    begin
      fOnStreamLn('', TRUE);
      fOnStreamLn('Nonterminals:', TRUE);
      i := fFirstNT
    end
    else
      inc(i)
  end;

  for i := 0 to MaxSymbols do
  begin
    while xList[i].lptr <> nil do
    begin
      l := xList[i].lptr;
      xList[i].lptr := xList[i].lptr.Next;
      dispose(l);
    end;
  end;
end;

{ NewNode              Generate a new graph node and return its index gp
----------------------------------------------------------------------}

function TTableHandler.NewNode(typ, p1, line : integer) : integer;
begin
  inc(fnNodes);
  if fnNodes > MaxNodes then
    Restriction(1, MaxNodes);
  gn^[fnNodes].typ := typ;
  gn^[fnNodes].next := 0;
  gn^[fnNodes].p1 := p1;
  gn^[fnNodes].p2 := 0;
  gn^[fnNodes].pos.beg := -1;
  gn^[fnNodes].pos.len := 0;
  gn^[fnNodes].pos.col := 0;
  gn^[fnNodes].line := line;
  Result := fnNodes;
end;

{ CompleteGraph        Set right ends of graph gp to 0
----------------------------------------------------------------------}

procedure TTableHandler.CompleteGraph(gp : integer);
var
  p : integer;
begin
  while gp <> 0 do
  begin
    p := gn^[gp].next;
    gn^[gp].next := 0;
    gp := p
  end
end;

{ ConcatAlt            Make (gL2, gR2) an alternative of (gL1, gR1)
----------------------------------------------------------------------}

procedure TTableHandler.ConcatAlt(var gL1, gR1 : integer; gL2, gR2 : integer);
var
  p : integer;
begin
  gL2 := NewNode(CRTalt, gL2, 0);
  p := gL1;
  while gn^[p].p2 <> 0 do
    p := gn^[p].p2;
  gn^[p].p2 := gL2;
  p := gR1;
  while gn^[p].next <> 0 do
    p := gn^[p].next;
  gn^[p].next := gR2
end;

{ ConcatSeq            Make (gL2, gR2) a successor of (gL1, gR1)
----------------------------------------------------------------------}

procedure TTableHandler.ConcatSeq(var gL1, gR1 : integer; gL2, gR2 : integer);
var
  p, q : integer;
begin
  p := gn^[gR1].next;
  gn^[gR1].next := gL2; {head node}
  while p <> 0 do
  begin {substructure}
    q := gn^[p].next;
    gn^[p].next := -gL2;
    p := q
  end;
  gR1 := gR2
end;

{ MakeFirstAlt         Generate alt-node with (gL,gR) as only alternative
----------------------------------------------------------------------}

procedure TTableHandler.MakeFirstAlt(var gL, gR : integer);
begin
  gL := NewNode(CRTalt, gL, 0);
  gn^[gL].next := gR;
  gR := gL
end;

{ MakeIteration        Enclose (gL, gR) into iteration node
----------------------------------------------------------------------}

procedure TTableHandler.MakeIteration(var gL, gR : integer);
var
  p, q : integer;
begin
  gL := NewNode(CRTiter, gL, 0);
  p := gR;
  gR := gL;
  while p <> 0 do
  begin
    q := gn^[p].next;
    gn^[p].next := -gL;
    p := q
  end
end;

{ MakeOption           Enclose (gL, gR) into option node
----------------------------------------------------------------------}

procedure TTableHandler.MakeOption(var gL, gR : integer);
begin
  gL := NewNode(CRTopt, gL, 0);
  gn^[gL].next := gR;
  gR := gL
end;

{ StrToGraph           Generate node chain from characters in s
----------------------------------------------------------------------}

procedure TTableHandler.StrToGraph(s : AnsiString; var gL, gR : integer);
var
  i, len : integer;
begin
  gR := 0;
  i := 2;
  len := Length(s); {strip quotes}
  while i < len do
  begin
    gn^[gR].next := NewNode(CRTchart, ORD(s[i]), 0);
    gR := gn^[gR].next;
    inc(i)
  end;
  gL := gn^[0].next;
  gn^[0].next := 0
end;

{ DelGraph             Check if graph starting with index gp is deletable
----------------------------------------------------------------------}

function TTableHandler.DelGraph(gp : integer) : boolean;
var
  gn : CRTGraphNode;
begin
  if gp = 0 then
  begin
    Result := true;
    EXIT
  end; {end of graph found}
  GetNode(gp, gn);
  Result := DelNode(gn) and DelGraph(ABS(gn.next));
end;

{ DelNode              Check if graph node gn is deletable
----------------------------------------------------------------------}

function TTableHandler.DelNode(gn : CRTGraphNode) : boolean;
var
  sn : CRTSymbolNode;

  function DelAlt(gp : integer) : boolean;
  var
    gn : CRTGraphNode;
  begin
    if gp <= 0 then
    begin
      Result := true;
      EXIT
    end; {end of graph found}
    GetNode(gp, gn);
    Result := DelNode(gn) and DelAlt(gn.next);
  end;

begin
  if gn.typ = CRTnt then
  begin
    GetSym(gn.p1, sn);
    Result := sn.deletable
  end
  else if gn.typ = CRTalt then
    Result := DelAlt(gn.p1) or (gn.p2 <> 0) and DelAlt(gn.p2)
  else
    Result := (gn.typ = CRTeps) or (gn.typ = CRTiter) or (gn.typ = CRTopt)
        or (gn.typ = CRTsem) or (gn.typ = CRTsync); 
end;

{ PrintGraph           Print the graph
----------------------------------------------------------------------}

procedure TTableHandler.PrintGraph;
var
  i : integer;

  procedure WriteTyp2(typ : integer);
  begin
    case typ of
      CRTnt    : fOnStreamLn('nt   ', FALSE);
      CRTt     : fOnStreamLn('t    ', FALSE);
      CRTwt    : fOnStreamLn('wt   ', FALSE);
      CRTany   : fOnStreamLn('any  ', FALSE);
      CRTeps   : fOnStreamLn('eps  ', FALSE);
      CRTsem   : fOnStreamLn('sem  ', FALSE);
      CRTsync  : fOnStreamLn('sync ', FALSE);
      CRTalt   : fOnStreamLn('alt  ', FALSE);
      CRTiter  : fOnStreamLn('iter ', FALSE);
      CRTopt   : fOnStreamLn('opt  ', FALSE);
      CRTif    : fOnStreamLn('if   ', FALSE);
      CRTend   : fOnStreamLn('end  ', FALSE);
      CRTendsc : fOnStreamLn('end; ', FALSE);
      CRTelse  : fOnStreamLn('else ', FALSE);
    else
      fOnStreamLn('     ', FALSE);
    end;
  end;

  procedure WriteDirType(gn : CRTGraphNode);
  var
    sn : CRTSymbolNode;
  begin
    GetSym(gn.p1, sn);
    case sn.HomographType of
      htNone : fOnStreamLn(' none      ', FALSE);
      htHomograph : fOnStreamLn(' homograph ', FALSE);
      htDefaultIdent : fOnStreamLn(' ident     ', FALSE);
    else
      fOnStreamLn(' --------- ', FALSE);
    end;
  end;

  procedure WriteName(gn : CRTGraphNode);
  var
    sn : CRTSymbolNode;
    Info : AnsiString;
  begin
    if gn.typ = CRTif then
      Info := gn.BooleanFunction
    else if (gn.typ IN [CRTt, CRTnt, CRTwt, CRTpr]) then
    begin
      GetSym(gn.p1, sn);
      Info := sn.name;
      if sn.constant > '' then
        Info := Info + ' [' + sn.constant + ']';
    end;
    if gn.pos.beg > 0 then
    begin
      Info := Info + ' pos:' + IntToStr(gn.pos.beg)
          + ' len:' + IntToStr(gn.pos.len)
          + ' col:' + IntToStr(gn.pos.col);
    end;
    fOnStreamLn(' ' + Trim(Info), FALSE);
  end;

begin { PrintGraph }
  fOnStreamLn('Top-down graph listing:', TRUE);
  fOnStreamLn('', TRUE);
  fOnStreamLn(' nr typ      next     p1     p2   line', FALSE);
  if fHomographCount > 0 then
    fOnStreamLn(' homograph', FALSE);
  fOnStreamLn(' other info', FALSE);
  fOnStreamLn('', TRUE);
  fOnStreamLn('=== ===== ======= ====== ====== ====== ', FALSE);
  if fHomographCount > 0 then
    fOnStreamLn('========= ', FALSE);
  fOnStreamLn('===============================', TRUE);
  i := 0;
  while i <= fnNodes do
  begin
    fOnStreamLn(PadL(IntToStr(i), ' ', 3), FALSE);
    fOnStreamLn('  ', FALSE);
    WriteTyp2(gn^[i].typ);
    fOnStreamLn(PadL(IntToStr(gn^[i].next), ' ', 7), FALSE);
    fOnStreamLn(PadL(IntToStr(gn^[i].p1), ' ', 7), FALSE);
    fOnStreamLn(PadL(IntToStr(gn^[i].p2), ' ', 7), FALSE);
    fOnStreamLn(PadL(IntToStr(gn^[i].line), ' ', 7), FALSE);
    if fHomographCount > 0 then
    WriteDirType(gn^[i]);
    WriteName(gn^[i]);
      {  }
    fOnStreamLn('', TRUE);
    inc(i);
  end; 
end;

{ FindCircularProductions      Test grammar for circular derivations
----------------------------------------------------------------------}

function TTableHandler.FindCircularProductions : integer;
type
  ListEntry =
    record
    left : integer;
    right : integer;
    deleted : boolean;
  end;
var
  changed, onLeftSide, onRightSide : boolean;
  i, j, listLength : integer;
  list : array[0..MaxList] of ListEntry;
  singles : MarkList;
  sn : CRTSymbolNode;
  Count : integer;
  Symbol : TSymbolPosition;
  s : AnsiString;

  procedure GetSingles(gp : integer; var singles : MarkList);
  var
    gn : CRTGraphNode;
  begin
    if gp <= 0 then
      EXIT;
    { end of graph found }
    GetNode(gp, gn);
    if gn.typ = CRTnt then
      if DelGraph(ABS(gn.next)) then
        InclMarkList(singles, gn.p1)
      else if (gn.typ = CRTalt) or (gn.typ = CRTiter) or (gn.typ = CRTopt) then
        if DelGraph(ABS(gn.next)) then
        begin
          GetSingles(gn.p1, singles);
          if gn.typ = CRTalt then
            GetSingles(gn.p2, singles)
        end;
    if DelNode(gn) then
      GetSingles(gn.next, singles)
  end;

begin { FindCircularProductions }
  i := fFirstNT;
  listLength := 0;
  while i <= fLastNT do
  begin { for all nonterminals i }
    ClearMarkList(singles);
    GetSym(i, sn);
    GetSingles(sn.struct, singles);
    { get nt's j such that i-->j }
    j := fFirstNT;
    while j <= fLastNT do
    begin { for all nonterminals j }
      if IsInMarkList(singles, j) then
      begin
        list[listLength].left := i;
        list[listLength].right := j;
        list[listLength].deleted := false;
        inc(listLength);
        if listLength > MaxList then
          Restriction(9, listLength)
      end;
      inc(j)
    end;
    inc(i)
  end;
  repeat
    i := 0;
    changed := false;
    while i < listLength do
    begin
      if not list[i].deleted then
      begin
        j := 0;
        onLeftSide := false;
        onRightSide := false;
        while j < listLength do
        begin
          if not list[j].deleted then
          begin
            if list[i].left = list[j].right then
              onRightSide := true;
            if list[j].left = list[i].right then
              onLeftSide := true
          end;
          inc(j)
        end;
        if not onRightSide or not onLeftSide then
        begin
          list[i].deleted := true;
          changed := true
        end
      end;
      inc(i)
    end
  until not changed;
  fOnStreamLn('  Circular derivations:    ', FALSE);
  i := 0;
  Count := 0;
  while i < listLength do
  begin
    if not list[i].deleted then
    begin
      Count := Count + 1;
      fOnStreamLn('', TRUE);
      fOnStreamLn('     ', FALSE);
      GetSym(list[i].left, sn);
      s := sn.Name + ' --> ';
      fOnStreamLn(PadR(sn.name, ' ', 20) + ' --> ', FALSE);
      GetSym(list[i].right, sn);
      s := s + sn.Name;
      fOnStreamLn(PadR(sn.name, ' ', 20), FALSE);
      Symbol := TSymbolPosition.Create;
      try
        Symbol.Line := sn.Line;
        fOnScannerError(207, Symbol, s, etGrammar);
      finally
        Symbol.Free;
      end;
    end;
    inc(i)
  end;
  if Count = 0 then
    fOnStreamLn(' -- none --', FALSE);
  fOnStreamLn('', TRUE);
  Result := Count;
end;

{ LL1Test              Collect terminal sets and checks LL(1) conditions
----------------------------------------------------------------------}

function TTableHandler.LL1Test : integer;
var
  sn : CRTSymbolNode;
  curSy : integer;
  Count : integer;

  procedure LL1Error(cond, ts : integer);
  var
    sn : CRTSymbolNode;
    s : AnsiString;
    Symbol : TSymbolPosition;
  begin
    fLL1Acceptable := FALSE;
    Symbol := TSymbolPosition.Create;
    try
      fOnStreamLn('', TRUE);
      fOnStreamLn('     LL(1) error in ', FALSE);
      GetSym(curSy, sn);
      fOnStreamLn(sn.name + ': ', FALSE);
      s := ' in ' + sn.name + ': ';
      if ts > 0 then
      begin
        GetSym(ts, sn);
        fOnStreamLn(sn.name, FALSE);
        s := s + ' ' + sn.name;
      end;
      Symbol.Line := sn.Line;
      case cond of
        1 :
          begin
            Count := Count + 1;
            fOnScannerError(201, Symbol, s, etLL1);
            fOnStreamLn(fOnErrorStr(201, ''), FALSE);
          end;
        2 : if fShowLL1StartSucc then
          begin
            Count := Count + 1;
            fOnScannerError(202, Symbol, s, etLL1);
            fOnStreamLn(fOnErrorStr(202, ''), FALSE);
          end;
        3 :
          begin
            Count := Count + 1;
            fOnScannerError(203, Symbol, s, etLL1);
            fOnStreamLn(fOnErrorStr(203, ''), FALSE);
          end;
      end;
    finally
      Symbol.Free;
    end;
  end;

  procedure Check(cond : integer; var s1, s2 : CRTSet);
  var
    i : integer;
  begin
    i := 0;
    while i <= fMaxT do
    begin
      if Sets.IsIn(s1, i) and Sets.IsIn(s2, i) then
        LL1Error(cond, i);
      inc(i)
    end
  end;

  procedure CheckAlternatives(gp : integer);
  var
    gn, gn1 : CRTGraphNode;
    s1, s2 : CRTSet;
    p : integer;
  begin
    while gp > 0 do
    begin
      GetNode(gp, gn);
      if gn.typ = CRTalt then
      begin
        p := gp;
        Sets.Clear(s1);
        while p <> 0 do
        begin {for all alternatives}
          GetNode(p, gn1);
          CompExpected(gn1.p1, curSy, s2);
          Check(1, s1, s2);
          Sets.Unite(s1, s2);
          CheckAlternatives(gn1.p1);
          p := gn1.p2
        end
      end
      else if (gn.typ = CRTopt) or (gn.typ = CRTiter) then
      begin
        CompExpected(gn.p1, curSy, s1);
        CompExpected(ABS(gn.next), curSy, s2);
        Check(2, s1, s2);
        CheckAlternatives(gn.p1)
      end
      else if gn.typ = CRTany then
      begin
        GetSet(gn.p1, s1);
        if Sets.Empty(s1) then
          LL1Error(3, 0)
      end;  // e.g. {ANY} ANY or [ANY] ANY
      gp := gn.next;
    end;
  end;

begin { LL1Test }
  fLL1Acceptable := TRUE;
  fOnStreamLn('  LL(1) conditions:', FALSE);
  curSy := fFirstNT;
  Count := 0;
  while curSy <= fLastNT do
  begin {for all nonterminals}
    GetSym(curSy, sn);
    CheckAlternatives(sn.struct);
    inc(curSy)
  end;
  if Count = 0 then
    fOnStreamLn('         --  ok  --', FALSE);
  fOnStreamLn('', TRUE);
  Result := Count;
end;

function TTableHandler.TestHomographs: integer;
var
  i : integer;
  DefaultCount : integer;
begin
  Result := 0;
  fHomographCount := 0;
  DefaultCount := 0;
  for i := 0 to maxSymbols do
  begin
    if st^[i].HomographType = htHomograph then
      fHomographCount := fHomographCount + 1
    else if st^[i].HomographType = htDefaultIdent then
      DefaultCount := DefaultCount + 1;
  end;
  if ((fHomographCount > 0) OR (DefaultCount > 0))
      AND NOT (crsUseHashFunctions IN fSwitchSet) then
  begin
    fOnScannerError(157, NIL, '', etGrammar);
    Result := Result + 1;
  end;
  if (fHomographCount > 0) AND (DefaultCount = 0) then
  begin
    fOnScannerError(156, NIL, '', etGrammar);
    Result := Result + 1;
  end;
  if (DefaultCount > 1) then
  begin
    fOnScannerError(155, NIL, '', etGrammar);
    Result := Result + 1;
  end;
end; {TestHomographs}

{ TestCompleteness     Test if all nonterminals have productions
----------------------------------------------------------------------}

function TTableHandler.TestCompleteness : integer;
var
  sp : integer;
  sn : CRTSymbolNode;
  Count : integer;
  Symbol : TSymbolPosition;
begin
  fOnStreamLn('  Undefined nonterminals:  ', FALSE);
  sp := fFirstNT;
  Count := 0;
  while sp <= fLastNT do
  begin {for all nonterminals}
    GetSym(sp, sn);
    if sn.struct = 0 then
    begin
      Count := Count + 1;
      fOnStreamLn('', TRUE);
      fOnStreamLn('     ' + sn.name, FALSE);
      Symbol := TSymbolPosition.Create;
      try
        Symbol.Line := sn.Line;
        fOnScannerError(204, Symbol, sn.name, etGrammar);
      finally
        Symbol.Free;
      end;
    end;
    inc(sp)
  end;
  if Count = 0 then
    fOnStreamLn(' -- none --', FALSE);
  fOnStreamLn('', TRUE);
  Result := Count;
end;

{ TestIfAllNtReached   Test if all nonterminals can be reached
----------------------------------------------------------------------}

function TTableHandler.TestIfAllNtReached : integer;
var
  gn : CRTGraphNode;
  sp : integer;
  reached : MarkList;
  sn : CRTSymbolNode;
  Count : integer;
  Symbol : TSymbolPosition;

  procedure MarkReachedNts(gp : integer);
  var
    gn : CRTGraphNode;
    sn : CRTSymbolNode;
  begin
    while gp > 0 do
    begin
      GetNode(gp, gn);
      if gn.typ = CRTnt then
      begin
        if not IsInMarkList(reached, gn.p1) then {new nt reached}
        begin
          InclMarkList(reached, gn.p1);
          GetSym(gn.p1, sn);
          MarkReachedNts(sn.struct)
        end
      end
      else if (gn.typ = CRTalt) or (gn.typ = CRTiter) or (gn.typ = CRTopt) then
      begin
        MarkReachedNts(gn.p1);
        if gn.typ = CRTalt then
          MarkReachedNts(gn.p2)
      end;
      gp := gn.next
    end
  end;

begin { TestIfAllNtReached }
  ClearMarkList(reached);
  GetNode(fRoot, gn);
  InclMarkList(reached, gn.p1);
  GetSym(gn.p1, sn);
  MarkReachedNts(sn.struct);
  fOnStreamLn('  Unreachable nonterminals:', FALSE);
  sp := fFirstNT;
  Count := 0;
  while sp <= fLastNT do
  begin {for all nonterminals}
    if not IsInMarkList(reached, sp) then
    begin
      Count := Count + 1;
      GetSym(sp, sn);
      fOnStreamLn('', TRUE);
      fOnStreamLn('     ' + sn.name, FALSE);
      Symbol := TSymbolPosition.Create;
      try
        Symbol.Line := sn.Line;
        fOnScannerError(205, Symbol, sn.name, etGrammar);
      finally
        Symbol.Free;
      end;
    end;
    inc(sp)
  end;
  if Count = 0 then
    fOnStreamLn(' -- none --', FALSE);
  fOnStreamLn('', TRUE);
  Result := Count;
end;

{ TestIfNtToTerm   Test if all nonterminals can be derived to terminals
----------------------------------------------------------------------}

function TTableHandler.TestIfNtToTerm : integer;
var
  changed : boolean;
  sp : integer;
  sn : CRTSymbolNode;
  termList : MarkList;
  Count : integer;
  Symbol : TSymbolPosition;

  function IsTerm(gp : integer) : boolean;
  var
    gn : CRTGraphNode;
  begin
    while gp > 0 do
    begin
      GetNode(gp, gn);
      if (gn.typ = CRTnt) and not IsInMarkList(termList, gn.p1)
        or (gn.typ = CRTalt) and not IsTerm(gn.p1)
        and ((gn.p2 = 0) or not IsTerm(gn.p2)) then
      begin
        IsTerm := false;
        EXIT
      end;
      gp := gn.next
    end;
    Result := true
  end;

begin { TestIfNtToTerm }
  ClearMarkList(termList);
  repeat
    sp := fFirstNT;
    changed := false;
    while sp <= fLastNT do
    begin
      if not IsInMarkList(termList, sp) then
      begin
        GetSym(sp, sn);
        if IsTerm(sn.struct) then
        begin
          InclMarkList(termList, sp);
          changed := true
        end
      end;
      inc(sp)
    end
  until not changed;
  fOnStreamLn('  Underivable nonterminals:', FALSE);
  sp := fFirstNT;
  Count := 0;
  while sp <= fLastNT do
  begin
    if not IsInMarkList(termList, sp) then
    begin
      Count := Count + 1;
      GetSym(sp, sn);
      fOnStreamLn('', TRUE);
      fOnStreamLn('     ' + sn.name, FALSE);
      Symbol := TSymbolPosition.Create;
      try
        Symbol.Line := sn.Line;
        fOnScannerError(206, Symbol, sn.name, etGrammar);
      finally
        Symbol.Free;
      end;
    end;
    inc(sp)
  end;
  if Count = 0 then
    fOnStreamLn(' -- none --', FALSE);
  fOnStreamLn('', TRUE);
  Result := Count;
end;

function TTableHandler.NextPointerLeadsToIfStatement(
    const gp : integer;
    var HasSemantic : boolean) : boolean;
var
  gn : CRTGraphNode;
begin
  HasSemantic := FALSE;
  GetNode(gp, gn);
  while (gn.typ IN [CRTeps, CRTsync, CRTsem]) AND (gn.next > 0) do
  begin
    if gn.typ = CRTsem then
      HasSemantic := TRUE;
    GetNode(gn.next, gn);
  end;
  Result := gn.typ = CRTif;
end; {NextPointerLeadsToIfStatement}

{ Alternatives         Count alternatives of gp
----------------------------------------------------------------------}
function TTableHandler.Alternatives(gp : integer) : integer;
var
  gn : CRTGraphNode;
  n : integer;
begin
  n := 0;
  while gp > 0 do
  begin
    GetNode(gp, gn);
    gp := gn.p2;
    inc(n);
  end;
  Result := n;
end;

function TTableHandler.IfStatementCheckProduction(
    gp : integer; checked : CRTSet) : integer;
var
  gn, gn2, gn3, gn4 : CRTGraphNode;
  gp2, gp3, gp4 : integer;
  s1, s2 : CRTSet;
  alts : integer; // indent1, addInd : integer;
  equal : boolean;
  NextIsIfStatement : boolean;
  Symbol : TSymbolPosition;
  HasSemantic : boolean;
begin
  Result := 0;
  while gp > 0 do
  begin
    GetNode(gp, gn);
    case gn.typ of
      CRTnt : begin end;
      CRTt : begin end;
      CRTwt :
        begin
          CompExpected(ABS(gn.next), curSy, s1);
          GetSet(0, s2);
          Sets.Unite(s1, s2);
        end;
      CRTany : begin end;
      CRTeps : begin end;
      CRTsem : begin end;
      CRTsync : begin
          GetSet(gn.p1, s1);
        end;
      CRTalt :
        begin
          CompFirstSet(gp, s1, false);
          equal := Sets.Equal(s1, checked);
          alts := Alternatives(gp);
          gp2 := gp;
          while gp2 <> 0 do
          begin
            GetNode(gp2, gn2);
            CompExpected(gn2.p1, curSy, s1);
            if gp2 = gp then
            begin
              NextIsIfStatement := NextPointerLeadsToIfStatement(gn2.p1,
                  HasSemantic);
              if NextIsIfStatement then
              begin
                if alts > MaxAlternates then
                  MaxAlternates := alts;
                gp3 := gn2.p1;
                GetNode(gp3, gn3);
                if gn3.p2 > 0 then
                begin
                  Symbol := TSymbolPosition.Create;
                  try
                    Symbol.Line := gn2.line;
                    fOnScannerError(158, Symbol, '', etGrammar);
                    Result := Result + 1;
                  finally
                    Symbol.Free;
                  end;
                end
                else if HasSemantic then
                begin
                  Symbol := TSymbolPosition.Create;
                  try
                    Symbol.Line := gn2.line;
                    fOnScannerError(160, Symbol, '', etGrammar);
                    Result := Result + 1;
                  finally
                    Symbol.Free;
                  end
                end
                else
                begin
                  gp4 := gn3.p1;
                  GetNode(gp4, gn4);
                  gn4.typ := CRTunknown;
                  PutNode(gp4, gn4);
                end;
              end;
            end
            else if (gn2.p2 = 0) and equal then
            begin
            end
            else
            begin
              NextIsIfStatement := NextPointerLeadsToIfStatement(gn2.p1, HasSemantic);
              if NextIsIfStatement then
              begin
                if alts > MaxAlternates then
                  MaxAlternates := alts;
                gp3 := gn2.p1;
                GetNode(gp3, gn3);
                if gn3.p2 > 0 then
                begin
                  Symbol := TSymbolPosition.Create;
                  try
                    Symbol.Line := gn2.line;
                    fOnScannerError(158, Symbol, '', etGrammar);
                    Result := Result + 1;
                  finally
                    Symbol.Free;
                  end;
                end
                else if HasSemantic then
                begin
                  Symbol := TSymbolPosition.Create;
                  try
                    Symbol.Line := gn2.line;
                    fOnScannerError(160, Symbol, '', etGrammar);
                    Result := Result + 1;
                  finally
                    Symbol.Free;
                  end
                end
                else
                begin
                  gp4 := gn3.p1;
                  GetNode(gp4, gn4);
                  gn4.typ := CRTunknown;
                  PutNode(gp4, gn4);
                end;
              end;
            end;
            Sets.Unite(s1, checked);
            Result := Result + IfStatementCheckProduction(gn2.p1, s1);
            gp2 := gn2.p2;
          end; { while }
        end;
      CRTiter :
        begin
          GetNode(gn.p1, gn2);
          NextIsIfStatement := NextPointerLeadsToIfStatement(gn.p1, HasSemantic);
          if NextIsIfStatement then
          begin
            Symbol := TSymbolPosition.Create;
            try
              Symbol.Line := gn.line;
              fOnScannerError(159, Symbol, '', etGrammar);
              Result := Result + 1;
            finally
              Symbol.Free;
            end;
          end;
          if gn2.typ = CRTwt then
          begin
            CompExpected(ABS(gn2.next), curSy, s1);
            CompExpected(ABS(gn.next), curSy, s2);
            Sets.Clear(s1);
            {for inner structure}
            if gn2.next > 0 then
              gp2 := gn2.next
            else
              gp2 := 0;
          end
          else
          begin
            gp2 := gn.p1;
            CompFirstSet(gp2, s1, false);
          end;
          Result := Result + IfStatementCheckProduction(gp2, s1);
        end;
      CRTopt :
        begin
          CompFirstSet(gn.p1, s1, false);
          if Sets.Equal(checked, s1) then
            Result := Result + IfStatementCheckProduction(gn.p1, checked)
          else
          begin
            Result := Result + IfStatementCheckProduction(gn.p1, s1);
          end
        end;
      CRTif : begin end;
      CRTendsc : begin end;
      CRTend : begin end;
      CRTelse : begin end;
    end;
    if (gn.typ <> CRTeps) and (gn.typ <> CRTsem) and (gn.typ <> CRTsync) then
      Sets.Clear(checked);
    gp := gn.next;
  end; { WHILE gp > 0 }
end;

(*function TTableHandler.TestIfStatements: integer;
var
  sn : CRTSymbolNode;
  checked : CRTSet;
begin
  Result := 0;
  curSy := FirstNt;
  while curSy <= LastNt do
  begin { for all nonterminals }
    GetSym(curSy, sn);
    Sets.Clear(checked);
    Result := Result + IfStatementCheckProduction(sn.struct, checked);
    inc(curSy);
  end;
end; *)

{ BuildName            Build new Name to represent old AnsiString
----------------------------------------------------------------------}

procedure TTableHandler.BuildName(var oldName, newName : CRTName);
var
  ForLoop, I : integer;
  TargetIndex : integer;
  ascName : CRTName;
begin
  TargetIndex := 1;
  for ForLoop := 2 to Length(oldName) - 1 do
  begin
    case oldName[ForLoop] of
      'A'..'Z' , 'a'..'z':
        begin
          if TargetIndex <= 255 then
          begin
            newName[TargetIndex] := oldName[ForLoop];
            inc(TargetIndex);
          end;
        end;
    else
      begin
        ascName := ASCIIName(oldName[ForLoop]);
        for I := 1 to Length(ascName) do
          if TargetIndex <= CRTMaxNameLength - 3 then
          begin
            newName[TargetIndex] := ascName[I];
            inc(TargetIndex)
          end;
      end;
    end;
  end;
  newName[0] := AnsiChar(TargetIndex - 1);
end;

{ SymName              Generates a new name for a symbol constant
----------------------------------------------------------------------}

procedure TTableHandler.SymName(symn : CRTName; var conn : CRTName);
begin
  if (symn[1] = '''') or (symn[1] = '"') then
    if Length(symn) = 3 then
      conn := ASCIIName(symn[2])
    else
      BuildName(symn, conn)
  else
    conn := symn;
  conn := Concat(conn, 'Sym');
end;

{ AssignSymNames     Assigns the user defined or generated token names
----------------------------------------------------------------------}

procedure TTableHandler.AssignSymNames;

  procedure AssignDef(var n {is not modified}, constant : CRTName);
  var
    ForLoop : integer;
  begin
    for ForLoop := 1 to lastName do
      if n = tt[ForLoop].definition then
      begin
        constant := tt[ForLoop].name;
        fSymNames := true;
        EXIT;
      end;
    SymName(n, constant)
  end;

var
  ForLoop : integer;
begin
  st^[0].constant := 'EOFSYMB';
  for ForLoop := 1 to MaxP do
    AssignDef(st^[ForLoop].name, st^[ForLoop].constant);
  st^[fMaxT].constant := 'NOSYMB';
end;

procedure TTableHandler.Clear;
begin
  if Assigned(gn) then
  begin
    dispose(gn);
    gn := nil;
  end;
  if Assigned(st) then
  begin
    dispose(st);
    st := nil;
  end;
  fInterfaceUses.Clear;
  fImplementationUses.Clear;
end; {Clear}

procedure TTableHandler.Initialize;
begin
  Clear;
  fCocoAborted := FALSE;
  maxSet := 0;
  Sets.Clear(cset[0]);
  Sets.Incl(cset[0], CRTeofSy);
  fFirstNT := MaxSymbols;
  fMaxP := MaxSymbols;
  fMaxT := -1;
  fMaxC := -1;
  fLastNT := fMaxP - 1;
  dummyName := 0;
  lastName := 0;

  fSymNames := false;

  fConstDeclPos.Clear;
  fTypeDeclPos.Clear;
  fPrivateDeclPos.Clear;
  fProtectedDeclPos.Clear;
  fPublicDeclPos.Clear;
  fPublishedDeclPos.Clear;
  fErrorsDeclPos.Clear;
  fDestroyDeclPos.Clear;
  fCreateDeclPos.Clear;
  fSemDeclPos.Clear;
  fHasWeak := false;

  { The dummy node gn^[0] ensures that none of the procedures
     above have to check for 0 indices. }
  NEW(gn);
  NEW(st);
  fnNodes := 0;
  gn^[0].typ := -1;
  gn^[0].p1 := 0;
  gn^[0].p2 := 0;
  gn^[0].next := 0;
  gn^[0].line := 0;
  gn^[0].pos.beg := -1;
  gn^[0].pos.len := 0;
  gn^[0].pos.col := 0;
end;

constructor TTableHandler.Create;
begin
  fInterfaceUses := TStringList.Create;
  fImplementationUses := TStringList.Create;
  fErrorList := TStringList.Create;
  fShowWarnings := TRUE;
  fShowLL1StartSucc := TRUE;

  fConstDeclPos := TCRTPosition.Create;
  fTypeDeclPos := TCRTPosition.Create;
  fPrivateDeclPos := TCRTPosition.Create;
  fProtectedDeclPos := TCRTPosition.Create;
  fPublicDeclPos := TCRTPosition.Create;
  fPublishedDeclPos := TCRTPosition.Create;
  fErrorsDeclPos := TCRTPosition.Create;
  fCreateDeclPos := TCRTPosition.Create;
  fDestroyDeclPos := TCRTPosition.Create;
  fSemDeclPos := TCRTPosition.Create;
  fMaxAlternates := 5;
  fSwitchSet := [];
end; {Create}

destructor TTableHandler.Destroy;
begin
  FreeAndNil(fConstDeclPos);
  FreeAndNil(fTypeDeclPos);
  FreeAndNil(fprivateDeclPos);
  FreeAndNil(fprotectedDeclPos);
  FreeAndNil(fpublicDeclPos);
  FreeAndNil(fpublishedDeclPos);
  FreeAndNil(fErrorsDeclPos);
  FreeAndNil(fCreateDeclPos);
  FreeAndNil(fDestroyDeclPos);
  FreeAndNil(fsemDeclPos);

  FreeAndNil(finterfaceUses);
  FreeAndNil(fimplementationUses);
  FreeAndNil(fErrorList);
  inherited;
end; {Destroy}

procedure TTableHandler.SetErrorList(const Value: TStringList);
begin
  FErrorList.Assign(Value);
end;

procedure TTableHandler.SetImplementationUses(const Value: TStringList);
begin
  FImplementationUses.Assign(Value);
end;

procedure TTableHandler.SetInterfaceUses(const Value: TStringList);
begin
  FInterfaceUses.Assign(Value);
end;

function TTableHandler.GetHasLiterals: boolean;
var
  i : integer;
  sn : CRTSymbolNode;
  Num : integer;
begin
  {-- sort literal list}
  i := 0;
  Num := 0;
  while i <= MaxT do
  begin
    GetSym(i, sn);
    if sn.struct = CRTlitToken then
      inc(Num);
    inc(i);
  end;
  Result := Num > 0;
end; {GetHasLiterals}

function TTableHandler.GetHasLiteralTokens: boolean;
var
  i : integer;
  sn : CRTSymbolNode;
  Num : integer;
begin
  {-- sort literal list}
  i := 0;
  Num := 0;
  while i <= MaxT do
  begin
    GetSym(i, sn);
    if sn.struct = CRTclassLitToken then
      inc(Num);
    inc(i);
  end;
  Result := Num > 0;
end; {GetHasLiteralTokens}

{ WriteStatistics      Write statistics about compilation to list file
----------------------------------------------------------------------}

procedure TTableHandler.WriteStatistics(const MaxSS : integer);

  procedure WriteNumbers(used, available : integer);
  begin
    fOnStreamLn(PadL(IntToStr(Used + 1), ' ', 6) + ' (limit '
      + PadL(IntToStr(available + 1), ' ', 5) + ')', TRUE);
  end;

begin
  fOnStreamLn('Time Compiled: ' + FormatDateTime('dddddd @ tt', Now), TRUE);
  fOnStreamLn('', TRUE);
  fOnStreamLn('Statistics:', TRUE);
  fOnStreamLn('  number of terminals:    ', FALSE);
  WriteNumbers(MaxT, maxTerminals);
  fOnStreamLn('  number of non-terminals:', FALSE);
// in 1.51 was  WriteNumbers(LastNt - FirstNt, maxSymbols - MaxT - 1);
  WriteNumbers(LastNt - FirstNt, maxNt);
  fOnStreamLn('  number of pragmas:      ', FALSE);
  WriteNumbers(maxSymbols - LastNt - 2, maxSymbols - MaxT - 1);
  fOnStreamLn('  number of symbolnodes:  ', FALSE);
  WriteNumbers(maxSymbols - FirstNt + MaxT, maxSymbols);
  fOnStreamLn('  number of graphnodes:   ', FALSE);
  WriteNumbers(nNodes, maxNodes);
  fOnStreamLn('  number of conditionsets:', FALSE);
  WriteNumbers(MaxSS, symSetSize);
  fOnStreamLn('  number of charactersets:', FALSE);
  WriteNumbers(MaxC, maxClasses);
end;

end.

