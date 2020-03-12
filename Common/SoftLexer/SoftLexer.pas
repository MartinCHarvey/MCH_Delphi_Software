unit SoftLexer;
{

Copyright © 2020 Martin Harvey <martin_c_harvey@hotmail.com>

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

{
  Implements a lexer that can be loaded with search strings,
  builds a DFA, which can be used for lexing a source text.

  Lexer is represented as a state machine in memory, not hard coded.
}

interface

{
  TODO - Currently string literals only, not regexps or character ranges
         despite fact NFA and DFA code will handle those nasty cases,
         just needs some changes to the input code.

  TODO - When not just matching literals, will need to give some thought
         to epsilon transitions and the like. At the moment no NFA
         or DFA construct or regexp needs to match "empty string"
}

uses
  Classes, DLList, OrdinalSets
{$IFDEF USE_TRACKABLES}
  , Trackables
{$ENDIF}
  ;

const
  TOK_INVALID = -1;

type
  TTrans = class;

{$IFDEF USE_TRACKABLES}
  TState = class(TTrackable)
{$ELSE}
  TState = class
{$ENDIF}
  private
    FAllStatesLink: TDLEntry; //All states in state machine
    FPredTransHead: TDLEntry; //Transitions leading to this state.
    FSuccTransHead: TDLEntry; //Transitions leading from this state.

    FInitial: boolean;
    FAccepting: boolean;
    FAcceptTokenVal: integer;
    FStateNumber: integer;
  protected
  public
    function FirstSuccTrans: TTrans;
    function NextSuccTrans(ThisTrans: TTrans): TTrans;
    function FirstPredTrans: TTrans;
    function NextPredTrans(ThisTrans: TTrans): TTrans;
    constructor Create;
    destructor Destroy; override;
  end;

  TDFAState = class(TState)
  private
    //Extra working information for NFA to DFA conversion.
    FNFAStateSet: TOrdinalSet; //Set of numbered NFA Nodes.
    FNFANodes: TList; //And a corresponding (unordered) list.
    FNFAAcceptState: TState;
    FMark: boolean;
  protected
  public
    constructor Create;
    destructor Destroy; override;
    procedure AttachNFAState(NFAState: TState);
  end;

{$IFDEF USE_TRACKABLES}
  TTrans = class(TTrackable)
{$ELSE}
  TTrans = class
{$ENDIF}
  private
    FTransCharSet: TOrdinalSet; //Set of characters
    FTransRangeSet: TOrdinalSet; //Set of ranges
    FAllTransLink: TDLEntry; //All transitions in state machine.
    FPredStateLink: TDLEntry; //Link in predecessor state list.
    FPredState: TState;
    FSuccStateLink: TDLEntry; //Link in successor state list.
    FSuccState: TState;
    FTransNumber: integer;
  protected
    procedure SetPredState(NewState: TState);
    procedure SetSuccState(NewState: TState);
  public
    constructor Create;
    destructor Destroy; override;
    property PredState: TState read FPredState write SetPredState;
    property SuccState: TState read FSuccState write SetSuccState;
    property TransCharSet: TOrdinalSet read FTransCharSet;
    property TransRangeSet: TOrdinalSet read FTransRangeSet;
  end;

{$IFDEF USE_TRACKABLES}
  TToken = class(TTrackable)
{$ELSE}
  TToken = class
{$ENDIF}
  private
    FTag: integer;
  public
    property Tag:integer read FTag;
  end;

  TAnsiToken = class(TToken)
  private
    FTokenString: AnsiString;
  public
    property TokenString: AnsiString read FTokenString;
  end;

  TWideToken = class(TToken)
  private
    FTokenString: WideString;
  public
    property TokenString: WideString read FTokenString;
  end;

{$IFDEF USE_TRACKABLES}
  TTokenResult = class(TTrackable)
{$ELSE}
  TTokenResult = class
{$ENDIF}
  private
    FTokenTag: integer;
  public
    property TokenTag: integer read FTokenTag;
  end;

  TOrdinalTokenResult = class(TTokenResult)
  private
    FTokenOrdinals: TList;
  public
    constructor Create;
    destructor Destroy; override;
  end;

  TAnsiTokenResult = class(TTokenResult)
  private
    FTokenString: AnsiString;
  public
    property TokenString: AnsiString read FTokenString;
  end;

  TWideTokenResult = class(TTokenResult)
  private
    FTokenString: string;
  public
    property TokenString: string read FTokenString;
  end;

{$IFDEF USE_TRACKABLES}
  TSoftLexer = class(TTrackable)
{$ELSE}
  TSoftLexer = class
{$ENDIF}
  private
     //List of input strings to be recognised.
    FTokenList: TList;
    //List of char ranges - automatons work on ranges of chars, not chars
    //themselves. Input chars are converted to range numbers before
    //being fed to DFA.
    FRangeList: TOrdinalRangeList;
    //Should be equal to Pred(FRangeList).Count
    FAlphabetSize: integer;
    //NFA Created in first half of lexer setup.
    FNFAStates: TDLEntry;
    FNFATrans: TDLEntry;
    FNFAStateCounter: integer;
    FNFATransCounter: integer;
    FNFAInitialState: TState;
    //DFA created in second half of lexer setup.
    FDFAStates: TDLEntry;
    FDFATrans: TDLEntry;
    FDFAStateCounter: integer;
    FDFATransCounter: integer;
    FDFAInitialState: TDFAState;
    //State used for execution of lexer
    FDFACurrentState: TDFAState;
    FDFATokenOrdinals: TList;
  protected
    function NewNFAState: TState;
    function NewNFATrans: TTrans;
    function NewDFAState: TDFAState;
    function NewDFATrans: TTrans;
    procedure DestroyNFA;
    procedure DestroyDFA;

    //Change all NFA transitions from sets of chars to sets of ranges,
    //
    function RangeifyNFATransitions: integer;
    procedure UnRangeifyDFATransitions;
    function SanityCheckDFA(var ErrMsg: string):boolean;
    procedure CreateDownstreamSets(NFATransitionList: TList;
                                   //List of TTrans for this DFA node.
                                   DownstreamSets:TList);
                                   //List of state sets or NIL if no trans.
    procedure LinkDownstreamStates(DFAState: TDFAState; //DFA state in question.
                                  DownstreamSets:TList);
                                  //List of downstream states, in set representation.
    function MakeNFAFromTokenStrings(var ErrMsg: string): boolean;
    function ConvertNFAToDFA(var ErrMsg: string): boolean;
    function OptimiseDFA(var ErrMsg: string): boolean;
  public
    constructor Create;
    destructor Destroy;override;
    //Reset everything and set up the lexer with new token strings.
    function SetupLexer(var ErrMsg:string): boolean;
    //Reset just the execution progress of the state machine, without
    //rebuilding NFA/DFA.
    procedure ResetInput;
    //Feed a character (actually ordinal) into the lexer.
    //Indicates whether:
    // 1) read was ok.
    // 2) New token produced (possibly unknown).
    function InputOrdinal(Ordinal:integer;
                          var TokenResults: TList): boolean;

    function GetTokenCount: integer;
    function GetTokenTag(Idx: integer): Integer;
    procedure ClearTokens;
  end;

  //TODO - Expand this to handle regexps, character ranges
  //and other things (State machine logic should cope OK).
  TAnsiSoftLexer = class (TSoftLexer)
  public
    function GetATokenString(Idx: Integer): AnsiString;
    procedure AddATokenString(TokenString: AnsiString; Tag:integer);

    //Feed a character (actually ordinal) into the lexer.
    //Indicates whether:
    // 1) read was ok.
    // 2) New token produced (possibly unknown).
    function InputAChar(AChar: AnsiChar;
                          var TokenResults: TList): boolean;

  end;

  //TODO - Expand this to handle regexps, character ranges
  //and other things (State machine logic should cope OK).
  TUnicodeSoftLexer = class(TSoftLexer)
  public
    function GetWTokenString(Idx: integer): UnicodeString;
    procedure AddWTokenString(TokenString: WideString; Tag:integer);

    //Feed a character (actually ordinal) into the lexer.
    //Indicates whether:
    // 1) read was ok.
    // 2) New token produced (possibly unknown).
    function InputWChar(WChar: WideChar;
                        var TokenResults: TList): boolean;
  end;

implementation

uses
  SysUtils
  ;

const
  S_NO_TOKEN_STRINGS = 'No token strings to build state machine';
  S_TOKEN_STRING_EMPTY = 'Token string is empty, cannot match on nothing';
  S_TOKEN_TAG_BAD = 'Token tag specified is that used for the invalid token';
  S_INTERNAL_ERROR = 'Internal error';
  S_NFA_STATES = 'Done (NFA States: ';
  S_NFA_TRANS = ', NFA Trans: ';
  S_DONE = ')';

  S_DFA_STATES = '(DFA States: ';
  S_DFA_TRANS = ', DFA Trans: ';
  S_OPT_DFA_STATES = '(Optimised DFA States: ';
  S_OPT_DFA_TRANS = ', Optimised DFA Trans: ';

  S_NOT_ALL_TRANS_CONNECTED = 'Internal error - not all DFA transitions connected.';
  S_NOT_ALL_NODES_REACHABLE = 'Internal error - not all DFA nodes reachable.';
  S_RANGE_SETS_CONFLICT = 'Internal error - DFA transition range sets conflict.';
  S_NFA_AMBIGUOUS_FOR_TOKENS = 'Unable to generate DFA: Ambiguous tokens: ';
  S_DFA_INITIAL_STATE_ACCEPTING = 'Internal error - DFA initial state is accepting.';

{ TState }

constructor TState.Create;
begin
  inherited;
  DLItemInitList(@FPredTransHead);
  DLItemInitList(@FSuccTransHead);
  DLItemInitObj(self, @FAllStatesLink);
end;

destructor TState.Destroy;
begin
  DLListRemoveObj(@FAllStatesLink);
  while Assigned(FPredTransHead.FLink.Owner) do
    ((FPredTransHead.FLink.Owner) as TTrans).SetSuccState(nil);
  while Assigned(FSuccTransHead.FLink.Owner) do
    ((FSuccTransHead.FLink.Owner) as TTRans).SetPredState(nil);
  inherited;
end;

function TState.FirstSuccTrans: TTrans;
begin
  result := FSuccTransHead.FLink.Owner as TTrans;
end;

function TState.NextSuccTrans(ThisTrans: TTrans): TTrans;
begin
  result := ThisTrans.FPredStateLink.FLink.Owner as TTrans;
end;

function TState.FirstPredTrans: TTrans;
begin
  result := FPredTransHead.Flink.Owner as TTrans;
end;

function TState.NextPredTrans(ThisTrans: TTrans): TTrans;
begin
  result := ThisTrans.FSuccStateLink.FLink.Owner as TTrans;
end;

{ TDFAState }

constructor TDFAState.Create;
begin
  inherited;
  FNFAStateSet := TOrdinalSet.Create;
  FNFANodes := TList.Create;
end;

destructor TDFAState.Destroy;
begin
  FNFAStateSet.Free;
  FNFANodes.Free;
  inherited;
end;

procedure TDFAState.AttachNFAState(NFAState: TState);
begin
  FNFAStateSet.Incl(NFAState.FStateNumber);
  FNFANodes.Add(NFAState);
end;

{ TTrans }

constructor TTrans.Create;
begin
  inherited;
  DlItemInitObj(self, @FAllTransLink);
  DLItemInitObj(self, @FPredStateLink);
  DlItemInitObj(self, @FSuccStateLink);
  FTransCharSet := TOrdinalSet.Create;
  FTransRangeSet := TOrdinalSet.Create;
end;

destructor TTrans.Destroy;
begin
  FTransCharSet.Free;
  FTransRangeSet.Free;
  DLListRemoveObj(@FAllTransLink);
  SetPredState(nil);
  SetSuccState(nil);
  inherited;
end;

procedure TTrans.SetPredState(NewState: TState);
begin
  if Assigned(FPredState) then
  begin
    DLListRemoveObj(@FPredStateLink);
    FPredState := nil;
  end;
  if Assigned(NewState) then
  begin
    DLListInsertTail(@NewState.FSuccTransHead, @FPredStateLink);
    FPredState := NewState;
  end;
end;

procedure TTrans.SetSuccState(NewState: TState);
begin
  if Assigned(FSuccState) then
  begin
    DLListRemoveObj(@FSuccStateLink);
    FSuccState := nil;
  end;
  if Assigned(NewState) then
  begin
    DLListInsertTail(@NewState.FPredTransHead, @FSuccStateLink);
    FSuccState := NewState;
  end;
end;

{ TOrdinalTokenResult }

constructor TOrdinalTokenResult.Create;
begin
  inherited;
  FTokenOrdinals := TList.Create;
end;

destructor TOrdinalTokenResult.Destroy;
begin
  FTokenOrdinals.Free;
  inherited;
end;

{ TSoftLexer }

constructor TSoftLexer.Create;
begin
  inherited;
  FTokenList := TList.Create;
  FRangeList := TOrdinalRangeList.Create;
  FDFATokenOrdinals :=  TList.Create;

  DLItemInitList(@FNFAStates);
  DLItemInitList(@FNFATrans);
  DLItemInitList(@FDFAStates);
  DLItemInitList(@FDFATrans);
end;

procedure TSoftLexer.DestroyNFA;
var
  Cur: PDLEntry;
begin
  Cur := FNFAStates.FLink;
  while Assigned(Cur) and Assigned(Cur.Owner) do
  begin
    Cur.Owner.Free;
    Cur := FNFAStates.FLink;
  end;

  Cur := FNFATrans.FLink;
  while Assigned(Cur) and Assigned(Cur.Owner) do
  begin
    Cur.Owner.Free;
    Cur := FNFATrans.FLink;
  end;

  FNFAInitialState := nil;
  FNFAStateCounter := 0;
  FNFATransCounter := 0;
end;

procedure TSoftLexer.DestroyDFA;
var
  Cur: PDLEntry;
begin
  Cur := FDFAStates.FLink;
  while Assigned(Cur) and Assigned(Cur.Owner) do
  begin
    Cur.Owner.Free;
    Cur := FDFAStates.FLink;
  end;

  Cur := FDFATrans.FLink;
  while Assigned(Cur) and Assigned(Cur.Owner) do
  begin
    Cur.Owner.Free;
    Cur := FDFATrans.FLink;
  end;

  FDFAInitialState := nil;
  FDFAStateCounter := 0;
  FDFATransCounter := 0;
  ResetInput;
end;

destructor TSoftLexer.Destroy;
var
  Idx: integer;
begin
  DestroyNFA;
  DestroyDFA;
  for Idx := 0 to Pred(FTokenList.Count) do
    TObject(FTokenList.Items[Idx]).Free;
  FTokenList.Free;
  FRangeList.Free;
  FDFATokenOrdinals.Free;
  inherited;
end;

function TSoftLexer.NewNFAState: TState;
begin
  result := TState.Create;
  DLListInsertTail(@FNFAStates, @result.FAllStatesLink);
  result.FStateNumber := FNFAStateCounter;
  Inc(FNFAStateCounter);
end;

function TSoftLexer.NewNFATrans: TTrans;
begin
  result := TTrans.Create;
  DLListINsertTail(@FNFATrans, @result.FAllTransLink);
  result.FTransNumber := FNFATransCounter;
  Inc(FNFATransCounter);
end;

function TSoftLexer.NewDFAState: TDFAState;
begin
  result := TDFAState.Create;
  DLListINsertTail(@FDFAStates, @result.FAllStatesLink);
  result.FStateNumber := FDFAStateCounter;
  Inc(FDFAStateCounter);
end;

function TSoftLexer.NewDFATrans: TTrans;
begin
  result := TTrans.Create;
  DLListInsertTail(@FDFATrans, @result.FAllTransLink);
  result.FTransNumber := FDFATransCounter;
  Inc(FDFATransCounter);
end;

function TSoftLexer.GetTokenCount: integer;
begin
  result := FTokenList.Count;
end;

function TSoftLexer.GetTokenTag(Idx: integer): Integer;
begin
  result := (TObject(FTokenList.Items[Idx]) as TToken).Tag;
end;

procedure TSoftLexer.ClearTokens;
var
  Idx: integer;
begin
  for Idx := 0 to Pred(FTokenList.Count) do
    TObject(FTokenList.Items[Idx]).Free;
  FTokenList.Clear;
end;

function TSoftLexer.SetupLexer(var ErrMsg:string): boolean;
begin
  DestroyNFA;
  DestroyDFA;
  result := MakeNFAFromTokenStrings(ErrMsg);
  result := result and ConvertNFAToDFA(ErrMsg);
  result := result and OptimiseDFA(ErrMsg);
  if result then
    ResetInput;
end;

procedure TSoftLexer.ResetInput;
begin
  FDFACurrentState := FDFAInitialState;
  FDFATokenOrdinals.Clear;
end;

function TSoftLexer.MakeNFAFromTokenStrings(var ErrMsg: string): boolean;
var
  Idx, Idx2: integer;
  FMostRecentState, NewState: TState;
  NewTrans: TTrans;
  Token: TToken;
  TokenLength: integer;
begin
  result := true;
  if FTokenList.Count <= 0 then
  begin
    result := false;
    ErrMsg := S_NO_TOKEN_STRINGS;
    exit;
  end;
  FNFAInitialState := NewNFAState;
  FNFAInitialState.FInitial := true;
  //No regexps here, just literal strings, hence NFA construction relatively easy.
  for Idx := 0 to Pred(FTokenList.Count) do
  begin
    Token := TObject(FTokenList.Items[Idx]) as TToken;
    FMostRecentState := FNFAInitialState;
    TokenLength := 0;
    if Token is TAnsiToken then
      TokenLength := Length((Token as TAnsiToken).TokenString)
    else if Token is TWideToken then
      TokenLength := Length((Token as TWideToken).TokenString);
    if not TokenLength > 0 then
    begin
      result := false;
      ErrMsg := S_TOKEN_STRING_EMPTY;
      exit;
    end;
    if Token.Tag = TOK_INVALID then
    begin
      result := false;
      ErrMsg := S_TOKEN_TAG_BAD;
      exit;
    end;
    for Idx2 := 1 to TokenLength do
    begin
      NewState := NewNFAState;
      NewTrans := NewNFATrans;
      NewTrans.SetPredState(FMostRecentState);
      NewTrans.SetSuccState(NewState);
      if Token is TAnsiToken then
        NewTrans.TransCharSet.InclAChar((Token as TAnsiToken).TokenString[Idx2])
      else if Token is TWideToken then
        NewTrans.TransCharSet.InclWChar((Token as TWideToken).TokenString[Idx2])
      else
      begin
        result := false;
        ErrMsg := S_INTERNAL_ERROR;
        exit;
      end;
      if Idx2 = TokenLength then
      begin
        NewState.FAccepting := true;
        NewState.FAcceptTokenVal := Token.Tag
      end;
      FMostRecentState := NewState;
    end;
  end;
  ErrMsg := ErrMsg + S_NFA_STATES + IntToStr(FNFAStateCounter)
                   + S_NFA_TRANS + IntToStr(FNFATransCounter)
                   + S_DONE;
end;

//Find out how many different alphabets there are for a bunch of downstream
//transitions. That is,
procedure TSoftLexer.CreateDownstreamSets(NFATransitionList: TList; //List of TTrans
                                          DownstreamSets: TList); //List of TOrdinalSet.
var
  OrdinalInput: integer;
  States: TOrdinalSet;
  Trans: TTrans;
  Idx: integer;
begin
  States := TOrdinalSet.Create;
  try
    //Clear downstream sets.
    for Idx := 0 to Pred(DownstreamSets.Count) do
      TObject(DownstreamSets[idx]).Free;
    DownstreamSets.Clear;

    //For each char in input alphabet,
    //determine set of resulting transitions, and hence dest nodes.
    for OrdinalInput := 0 to Pred(FAlphabetSize) do
    begin
      States.Clear;
      for Idx := 0 to Pred(NFATransitionList.Count) do
      begin
        Trans := TObject(NFATransitionList[idx]) as TTrans;
        if Trans.TransRangeSet.Contains(OrdinalInput) then
          States.Incl(Trans.FSuccState.FStateNumber);
      end;
      if States.HasMembers then
      begin
        DownstreamSets.Add(States);
        States := TOrdinalSet.Create;
      end
      else
        DownstreamSets.Add(nil);
    end;
  finally
    States.Free;
  end;
end;

procedure TSoftLexer.LinkDownstreamStates(DFAState: TDFAState; //DFA state in question.
                                          DownstreamSets:TList);
                                          //List of downstream states, in set representation.
var
  Idx: integer;
  StateSet: TOrdinalSet;
  NextDFAState: TDFAState;
  NFAState: TState;
  Trans: TTrans;
begin
  for Idx := 0 to Pred(DownstreamSets.Count) do
  begin
    StateSet := TObject(DownstreamSets[idx]) as TOrdinalSet;
    if not Assigned(StateSet) then continue;
    Assert(StateSet.HasMembers);

    //Can we find a DFA Node with this exact set of states?
    NextDFAState := FDFAStates.FLink.Owner as TDFAState;
    while Assigned(NextDFAState) do
    begin
      if StateSet.SetEquals(NextDFAState.FNFAStateSet) then
        break; //We have found the DFA node for this downstream set.
      NextDFAState := NextDFAState.FAllStatesLink.FLink.Owner as TDFAState;
    end;
    //If not, then make one.
    if not Assigned(NextDFAState) then
    begin
      NextDFAState := NewDFAState;
      //Add NFA nodes and build NFA state set.
      NFAState := FNFAStates.FLink.Owner as TState;
      while Assigned(NFAState) do
      begin
        if StateSet.Contains(NFAState.FStateNumber) then
        begin
          NextDFAState.AttachNFAState(NFAState);
          if NFAState.FAccepting then
            NextDFAState.FNFAAcceptState := NFAState;
        end;
        NFAState := NFAState.FAllStatesLink.FLink.Owner as TState;
      end;
    end;
    //We have a DFA node with the exact set of states we require.

    //Transitions. Do we already have one from and to the nodes we want?
    Trans := FDFATrans.FLink.Owner as TTrans;
    while Assigned(Trans) do
    begin
      if (Trans.FPredState = DFAState) and (Trans.FSuccState = NextDFAState) then
        break; //We do.
      Trans := Trans.FAllTransLink.FLink.Owner as TTrans;
    end;
    //If not then make one.
    if not Assigned(Trans) then
    begin
      Trans := NewDFATrans;
      Trans.SetPredState(DFAState);
      Trans.SetSuccState(NextDFAState);
    end;
    //And now set up the set of ranges in the transition, according to the
    //Current alphabet number.
    Trans.TransRangeSet.Incl(Idx);
  end;
end;

 // Returns size of rangeified input dictionary.
 //
function TSoftLexer.RangeifyNFATransitions: integer;
var
  Trans: TTrans;
  TokenRange: TOrdinalRange;
  FirstMRange, LastMRange: integer;
  OK: boolean;
  Idx: Integer;
begin
  //Now, rangeify NFA transitions, so that we get an acceptably small
  //number of items in the input alphabet.
  FRangeList.Clear;
  Trans := FNFATrans.FLink.Owner as TTRans;
  while Assigned(Trans) do
  begin
    Trans.TransRangeSet.Clear;
    FRangeList.MingleRanges(Trans.TransCharSet);
    Trans := Trans.FAllTransLink.FLink.Owner as TTrans;
  end;
  result := FRangeList.RangeCount;
  //FRangeList now has list of all ranges.
  //Now, we know for sure that each token range is covered by one or more
  //"mingled ranges".
  Trans := FNFATrans.FLink.Owner as TTRans;
  while Assigned(Trans) do
  begin
    TokenRange := Trans.TransCharSet.GetFirstRange;
    while Assigned(TokenRange) do
    begin
      OK := FRangeList.LookupRangeNumber(TokenRange.Min, FirstMRange);
      Assert(OK);
      OK := FRangeList.LookupRangeNumber(Pred(TokenRange.Max), LastMRange);
      Assert(OK);
      for Idx := FirstMRange to LastMRange do
        Trans.TransRangeSet.Incl(Idx);
      TokenRange := Trans.TransCharSet.GetNextRange(TokenRange);
    end;
    Trans := Trans.FAllTransLink.FLink.Owner as TTrans;
  end;
end;

procedure TSoftLexer.UnRangeifyDFATransitions;
var
  Trans: TTrans;
  MRange: TOrdinalRange;
  MRangeIdx: integer;
  CharRange: TOrdinalRange;
  TempCharSet: TOrdinalSet;
begin
  TempCharSet := TOrdinalSet.Create;
  TempCharSet.Incl(0);
  CharRange := TempCharSet.GetFirstRange;
  Dec(CharRange.Max);
  try
    MRange := FRangeList.GetFirstRange;
    MRangeIdx := 0;
    while Assigned(MRange) do
    begin
      CharRange.Min := MRange.Min;
      CharRange.Max := MRange.Max;
      //For any specified mingled range, see which transitions use that
      //mingled range, and set the appropriate values in the set or characters.
      //For efficiency, construct the set manually.
      Trans := FDFATrans.FLink.Owner as TTrans;
      while Assigned(Trans) do
      begin
        if Trans.FTransRangeSet.Contains(MRangeIdx) then
          Trans.FTransCharSet.AddSet(TempCharSet);
        Trans := Trans.FAllTransLink.FLink.Owner as TTrans;
      end;
      MRange := FRangeList.GetNextRange(MRange);
      Inc(MRangeIdx);
    end;
  finally
    TempCharSet.Free;
  end;
end;

//Okay, worthwhile doing some sanity checking here.
//0. All transitions have source and dest nodes.
//1. All nodes reachable from initial node.
//2. Transitions from any node all disjoint range sets
//   (fatal error, DFA not deterministic!)
//3. Transitions from any node all disjoint char sets
//   (fatal error, rangeification)
//4. DFA node sets should contain at most one accepting state
//   (fatal error, input grammar ambiguous)
function TSoftLexer.SanityCheckDFA(var ErrMsg: string):boolean;

  function AllTransitionsConnectedWithChars: boolean;
  var
    Trans: TTrans;
  begin
    Trans := FDFATrans.FLink.Owner as TTrans;
    while Assigned(Trans) do
    begin
      if not(Assigned(Trans.PredState) and Assigned(Trans.SuccState)) then
      begin
        result := false;
        exit;
      end;
      if not(Trans.TransCharSet.HasMembers and Trans.TransRangeSet.HasMembers)
      then
      begin
        result := false;
        exit;
      end;
      Trans := Trans.FAllTransLink.FLink.Owner as TTrans;
    end;
    result := true;
  end;

  function AllNodesReachable: boolean;

    procedure RecursiveMark(IState: TDFAState);
    var
      Trans: TTrans;
      ChildState: TDFAState;
    begin
      if not IState.FMark then
      begin
        IState.FMark := true;
        Trans := IState.FSuccTransHead.FLink.Owner as TTrans;
        while Assigned(Trans) do
        begin
          ChildState := Trans.SuccState as TDFAState;
          RecursiveMark(ChildState);
          Trans := Trans.FAllTransLink.FLink.Owner as TTrans;
        end;
      end;
    end;

  var
    State: TDFAState;

  begin
    RecursiveMark(FDFAInitialState);
    // Clear marks while checking - might use them later for DFA optimisation.
    State := FDFAStates.FLink.Owner as TDFAState;
    while Assigned(State) do
    begin
      if not State.FMark then
      begin
        result := false;
        exit;
      end;
      State.FMark := false;
      State := State.FAllStatesLink.FLink.Owner as TDFAState;
    end;
    result := true;
  end;

  function DisjointTransitionSets: boolean;
  var
    State: TDFAState;
    Trans1, Trans2: TTrans;
    Intersection: TOrdinalSet;
  begin
    Intersection := TOrdinalSet.Create;
    try
      State := FDFAStates.FLink.Owner as TDFAState;
      while Assigned(State) do
      begin
        Trans1 := State.FSuccTransHead.FLink.Owner as TTrans;
        while Assigned(Trans1) do
        begin
          Trans2 := Trans1.FPredStateLink.FLink.Owner as TTrans;
          while Assigned(Trans2) do
          begin
            Intersection.Clear;
            Intersection.AddSet(Trans1.FTransCharSet);
            Intersection.IntersectSet(Trans2.FTransCharSet);
            if Intersection.HasMembers then
            begin
              result := false;
              exit;
            end;
            Intersection.Clear;
            Intersection.AddSet(Trans1.FTransRangeSet);
            Intersection.IntersectSet(Trans2.FTransRangeSet);
            if Intersection.HasMembers then
            begin
              result := false;
              exit;
            end;
            Trans2 := Trans2.FPredStateLink.FLink.Owner as TTrans;
          end;
          Trans1 := Trans1.FPredStateLink.FLink.Owner as TTrans;
        end;
        State := State.FAllStatesLink.FLink.Owner as TDFASTate;
      end;
    finally
      Intersection.Free;
    end;
    result := true;
  end;

  function OneAcceptingState(var NFASTate1: TState;
                             var NFAState2: TState): boolean;
  var
    DFAState: TDFAState;
    NFAIter: TState;
    idx: integer;
  begin
    DFAState := FDFAStates.FLink.Owner as TDFAState;
    while Assigned(DFAState) do
    begin
      NFAState1 := DFAState.FNFAAcceptState;
      NFAState2 := nil;
      for Idx := 0 to Pred(DFAState.FNFANodes.Count) do
      begin
        NFAIter := TObject(DFAState.FNFANodes[idx]) as TState;
        if NFAIter.FAccepting then
        begin
          if not (DFAState.FNFAAcceptState = NFAIter) then
          begin
            NFAState2 := NFAIter;
            result := false;
            exit;
          end;
        end;
      end;
      DFAState := DFAState.FAllStatesLink.FLink.Owner as TDFAState;
    end;
    result := true;
  end;

var
  NFAState1, NFAState2: TState;
begin
  if not AllTransitionsConnectedWithChars then
  begin
    result := false;
    ErrMsg := S_NOT_ALL_TRANS_CONNECTED;
    exit;
  end;
  if not AllNodesReachable then
  begin
    result := false;
    ErrMsg := S_NOT_ALL_NODES_REACHABLE;
    exit;
  end;
  if not DisjointTransitionSets then
  begin
    result := false;
    ErrMsg := S_RANGE_SETS_CONFLICT;
    exit;
  end;
  if not OneAcceptingState(NFAState1, NFAState2) then
  begin
    //Prints first conflict.
    result := false;
    Assert(Assigned(NFAState1));
    Assert(NFAState1.FAccepting);
    Assert(Assigned(NFAState2));
    Assert(NFAState2.FAccepting);
    ErrMsg := S_NFA_AMBIGUOUS_FOR_TOKENS
      + ' ' + IntToStr(NFAState1.FAcceptTokenVal)
      + ',' + IntToStr(NfaState2.FAcceptTokenVal);
    exit;
  end;
  //Initial state must not be accepting.
  if Assigned(FDFAInitialState.FNFAAcceptState) then
  begin
    result := false;
    ErrMsg := S_DFA_INITIAL_STATE_ACCEPTING;
    exit;
  end;
  result := true;
end;


function TSoftLexer.ConvertNFAToDFA(var ErrMsg: string): boolean;
var
  DownstreamSets: TList; //List of TOrdinalSet.
  DFAState: TDFAState;
  NFATransitionList: TList;
  NFASourceState: TState;
  NFATransition: TTrans;
  Idx: integer;

begin
  DestroyDFA;
  //Determine input alphabet.
  NFATransitionList := TList.Create;
  DownstreamSets := TList.Create;
  try
    //Set up the NFA alphabet.
    FAlphabetSize := RangeifyNFATransitions;
    //Create initial DFA State.
    FDFAInitialState := NewDFAState;
    FDFAInitialState.FInitial := True;
    FDFAInitialState.AttachNFAState(FNFAInitialState);

    //Create successor state sets and transitions for this DFA State.
    DFAState := FDFAInitialState;
    while Assigned(DFAState) do
    begin
      //Build list of all possible downstream transitions.
      NFATransitionList.Clear;
      for Idx := 0 to Pred(DFAState.FNFANodes.Count) do
      begin
        NFASourceState := TObject(DFAState.FNFANodes[Idx]) as TState;
        NFATransition := NFASourceState.FSuccTransHead.FLink.Owner as TTrans;
        while Assigned(NFATransition) do
        begin
          NFATransitionList.Add(NFATransition);
          NFATransition := NFATransition.FPredStateLink.FLink.Owner as TTrans;
        end;
      end;

      //Create downstream sets for the alphabet
      CreateDownstreamSets(NFATransitionList, DownstreamSets);
      Assert(DownstreamSets.Count = FAlphabetSize);
      //Create and/or link downstream nodes for this DFA node
      LinkDownstreamStates(DFAState, DownstreamSets);
      //And since our "all nodes" list is nicely ordered,
      //we don't need to mark, we just move on to the next.
      DFAState := DFAState.FAllStatesLink.FLink.Owner as TDFAState;
    end;
    UnRangeifyDFATransitions;
    result := SanityCheckDFA(ErrMsg);
    if result then
    begin
      ErrMsg := ErrMsg + S_DFA_STATES + IntToStr(FDFAStateCounter)
                       + S_DFA_TRANS + IntToStr(FDFATransCounter)
                       + S_DONE;
    end
    else
      DestroyDFA;
  finally
    NFATransitionList.Free;
  end;
end;

function TSoftLexer.OptimiseDFA(var ErrMsg: string): boolean;
begin
  //TODO - At some point you might want to
  //optimise the DFA.
  //Not much point whilst the input is only string literals.
  result := true;
end;

function TSoftLexer.InputOrdinal(Ordinal:integer;
                      var TokenResults: TList //List of TTokenResult
                      ): boolean;
var
  Trans: TTrans;
  NewTokenResult: TOrdinalTokenResult;
  RecRet, CurrentStateAccepting: boolean;
begin
  if not Assigned(FDFACurrentState) then
  begin
    result := false;
    exit;
  end;
  //Longest match first - any successor transitions
  Trans := FDFACurrentState.FSuccTransHead.FLink.Owner as TTrans;
  while Assigned(Trans) do
  begin
    if Trans.TransCharSet.Contains(Ordinal) then
    begin
      //Take the transition.
      FDFACurrentState := Trans.SuccState as TDFAState;
      FDFATokenOrdinals.Add(Pointer(Ordinal));
      //All done, we have consumed the input character.
      //Don't change token results if any.
      result := true;
      exit;
    end;
    Trans := Trans.FAllTransLink.FLink.Owner as TTrans;
  end;

  //Whatever happens now, we definitely have to output a token.
  if not Assigned(TokenResults) then
    TokenResults := TList.Create;
  NewTokenResult := TOrdinalTokenResult.Create;
  TokenResults.Add(NewTokenResult);

  CurrentStateAccepting := Assigned(FDFACurrentState.FNFAAcceptState);

  if CurrentStateAccepting then
  begin
    Assert(FDFACurrentState.FNFAAcceptState.FAccepting);
    //Create new token result from accumulated ordinals.
    NewTokenResult.FTokenTag := FDFACurrentState.FNFAAcceptState.FAcceptTokenVal;
    NewTokenResult.FTokenOrdinals := FDFATokenOrdinals;
    //Reset the accumulated ordinals, and back to the initial state.
    FDFATokenOrdinals := TList.Create;
    FDFACurrentState := FDFAInitialState;
    //Input the new ordinal recursively.
    RecRet := InputOrdinal(Ordinal, TokenResults);
    Assert(RecRet);
  end
  else if FDFACurrentState <> FDFAInitialState then
  begin
    //Create invalid token result from accumulated ordinals
    NewTokenResult.FTokenTag := TOK_INVALID;
    NewTokenResult.FTokenOrdinals := FDFATokenOrdinals;
    //Reset accumulated ordinals, and state.
    FDFATokenOrdinals := TList.Create;
    FDFACurrentState := FDFAInitialState;
    //Input the new ordinal recursively.
    RecRet := InputOrdinal(Ordinal, TokenResults);
    Assert(RecRet);
  end
  else
  begin
    //Initial state, no accumulated ordinals, and invalid input char,
    //so won't accumulate.
    NewTokenResult.FTokenTag := TOK_INVALID;
    NewTokenResult.FTokenOrdinals.Add(Pointer(Ordinal));
    //No need to reset state or accumulated ordinals.
    //No need to feed the input in recursively, it's the error token.
  end;
  result := true;
end;

{ TAnsiSoftLexer }

function TAnsiSoftLexer.GetATokenString(Idx: integer): AnsiString;
begin
  result := (TObject(FTokenList.Items[Idx]) as TAnsiToken).TokenString;
end;

procedure TAnsiSoftLexer.AddATokenString(TokenString: AnsiString; Tag:integer);
var
  Token: TAnsiToken;
begin
  Token := TAnsiToken.Create;
  Token.FTokenString := TokenString;
  Token.FTag := Tag;
  FTokenList.Add(Token);
end;

function TAnsiSoftLexer.InputAChar(AChar: AnsiChar;
                      var TokenResults: TList): boolean;
var
  Idx, Idx2: integer;
  OrdRes: TOrdinalTokenResult;
  AnsiRes: TAnsiTokenResult;
begin
  result := InputOrdinal(Ord(AChar), TokenResults);
  if not Assigned(TokenResults) then exit;
  for Idx := 0 to Pred(TokenResults.Count) do
  begin
    OrdRes := TObject(TokenResults[idx]) as TOrdinalTokenResult;
    AnsiRes := TAnsiTokenResult.Create;
    AnsiRes.FTokenTag := OrdRes.FTokenTag;
    for idx2 := 0 to Pred(OrdRes.FTokenOrdinals.Count) do
    begin
      AnsiRes.FTokenString := AnsiRes.FTokenString
        + AnsiChar(Integer(OrdRes.FTokenOrdinals[idx2]));
    end;
    TokenResults[idx] := AnsiRes;
    OrdRes.Free;
  end;
end;

{ TUnicodeSoftLexer }

function TUnicodeSoftLexer.GetWTokenString(Idx: integer): UnicodeString;
begin
  result := (TObject(FTokenList.Items[Idx]) as TWideToken).TokenString;
end;

procedure TUnicodeSoftLexer.AddWTokenString(TokenString: WideString; Tag: Integer);
var
  Token: TWideToken;
begin
  Token := TWideToken.Create;
  Token.FTokenString := TokenString;
  Token.FTag := Tag;
  FTokenList.Add(Token);
end;

function TUnicodeSoftLexer.InputWChar(WChar: WideChar;
                      var TokenResults: TList): boolean;
var
  Idx, Idx2: integer;
  OrdRes: TOrdinalTokenResult;
  WideRes: TWideTokenResult;
begin
  result := InputOrdinal(Ord(WChar), TokenResults);
  if not Assigned(TokenResults) then exit;
  for Idx := 0 to Pred(TokenResults.Count) do
  begin
    OrdRes := TObject(TokenResults[idx]) as TOrdinalTokenResult;
    WideRes := TWideTokenResult.Create;
    WideRes.FTokenTag := OrdRes.FTokenTag;
    for idx2 := 0 to Pred(OrdRes.FTokenOrdinals.Count) do
    begin
      WideRes.FTokenString := WideRes.FTokenString
        + WideChar(Integer(OrdRes.FTokenOrdinals[idx2]));
    end;
    TokenResults[idx] := WideRes;
    OrdRes.Free;
  end;
end;

end.
