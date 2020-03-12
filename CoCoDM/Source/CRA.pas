unit CRA;
{$INCLUDE CocoCD.inc}

{ CRA     Automaton
  ===     =========

  (1) ConvertToStates translates a top-down graph into a NFA.
      MatchDFA tries to match literal strings against the DFA
  (2) MakeDeterministic converts the NFA into a DFA

NOTE: Scanner generator sections moved to CRS
---------------------------------------------------------------- }

interface

uses
  CRT, Classes, CocoBase, CRTypes;

const
  maxStates = 2000;

type
  {TODO: Change all of these records to objects}
  Action = ^ActionNode;
  Target = ^TargetNode;
  TState =
    record { state of finite automaton }
    firstAction : Action; { to first action of this state }
    endOf : integer; { nr. of recognized token if state is final }
    ctx : boolean; { TRUE: state reached by contextTrans }
  end;
  ActionNode = record { action of finite automaton }
    typ : integer; { type of action symbol: AnsiChar, class }
    sym : integer; { action symbol }
    tc : integer; { transition code: normTrans, contextTrans }
    target : Target; { states after transition with input symbol }
    next : Action;
  end;
  TargetNode = record { state after transition with input symbol }
    theState : integer; { target state }
    next : Target;
  end;
  Melted = ^MeltedNode;
  MeltedNode =
    record { info about melted states }
    sset : CRTSet; { set of old states }
    theState : integer; { new state }
    next : Melted;
  end;

  {TODO: convert this to an object list}
  TStateList = array [0..maxStates] of TState;

  TAutomaton = class(TObject)
  private
    fOnScannerError: TErrorProc;
    fOnPrintDivider: TProcedure;
    fOnStreamLn: TProcedureStreamLine;
    fOnErrorStr: TErrorStr;
    fOnGetCurrentSymbol: TFunctionSymbolPosition;
    fTableHandler: TTableHandler;
    fOnGetErrorData: TFunctionGetErrorData;
    fOnInDistinguishedError: TInDistinguishedErrorEvent;

    NewLine : boolean;
    firstMelted : Melted;             { list of melted states }
    lastSimState : integer;           { last non melted state }
    AlreadyReportedList : TStringList;
    fDirtyDFA : boolean;               { is there any transition based on the left context }
    fLastState : integer;              { last allocated state  }
    fRootState : integer;              { start state of DFA    }
    fStateList : TStateList;

    procedure AddAction(act: Action; var head: Action);
    procedure AddTargetList(var lista, listb: Target);
    procedure ChangeAction(a: Action; sset: CRTSet);
    procedure CombineShifts;
    procedure DeleteActionList(anAction: Action);
    procedure DeleteRedundantStates;
    procedure DeleteTargetList(list: Target);
    procedure DetachAction(a: Action; var L: Action);
    procedure MakeSet(p: Action; var sset: CRTSet);
    procedure MakeUnique(s: integer; var changed: boolean);
    procedure MeltStates(s: integer; var correct: boolean);
    function NewMelted(sset: CRTSet; s: integer): Melted;
    procedure NewTransition(from: integer; gn: CRTGraphNode;
      toState: integer);

    procedure SplitActions(a, b: Action);
    function TheAction(theState: TState; ch: AnsiChar): Action;
    function GetStateList(const Idx: integer): TState;
    procedure SetStateList(const Idx: integer; const Value: TState);
  public
    {TODO This global must go}

    constructor Create;
    destructor Destroy; override;

    procedure Initialize;
    procedure Finalize;

    procedure ConvertToStates(gp0, sp : integer); { Converts top-down graph with root gp into a subautomaton that recognizes token sp }
    { CopyFramePart: "stopStr" must not contain "FileIO.EOL".
       "leftMarg" is in/out-parameter  --  it has to be set once by the
       calling program.    }
    procedure MakeDeterministic(var correct : boolean); { Converts the NFA into a DFA. correct indicates if an error occurred. }
    procedure MatchDFA(str : AnsiString; sp : integer; var matchedSp : integer); { Returns TRUE, if AnsiString str can be recognized by the current DFA. matchedSp is the token as that s can be recognized. }
    procedure PrintStates; { List the automaton for tracing }
    procedure SemErr(nr : integer);
    function NewState: integer;

    property TableHandler : TTableHandler read fTableHandler write fTableHandler;
    property StateList[const Idx : integer] : TState read GetStateList write SetStateList;

    property DirtyDFA : boolean read fDirtyDFA;
    property LastState : integer read fLastState write fLastState;
    property RootState : integer read fRootState write fRootState;

    property OnStreamLn : TProcedureStreamLine read fOnStreamLn write fOnStreamLn;
    property OnPrintDivider : TProcedure read fOnPrintDivider write fOnPrintDivider;
    property OnScannerError : TErrorProc read fOnScannerError write fOnScannerError;
    property OnErrorStr : TErrorStr read fOnErrorStr write fOnErrorStr;
    property OnGetCurrentSymbol : TFunctionSymbolPosition read fOnGetCurrentSymbol
        write fOnGetCurrentSymbol;
    property OnGetErrorData : TFunctionGetErrorData read fOnGetErrorData write fOnGetErrorData;
    property OnInDistinguishedError : TInDistinguishedErrorEvent
        read fOnInDistinguishedError write fOnInDistinguishedError;
  end; {TAutomaton}

implementation

uses
  Sets, SysUtils;

procedure TAutomaton.SemErr(nr : integer);
begin
  fOnScannerError(nr + 100, fOnGetCurrentSymbol, '', etSymantic)
end; {SemErr}

constructor TAutomaton.Create;
begin
  AlreadyReportedList := TStringList.Create;
end;

destructor TAutomaton.Destroy;
begin
  FreeAndNil(AlreadyReportedList);
  inherited;
end;

{ PrintStates          List the automaton for tracing
-------------------------------------------------------------------------}

procedure TAutomaton.PrintStates;

  procedure PrintSymbol(typ, val, width : integer);
  var
    name : CRTName;
    len : integer;
  begin
    if typ = CRTchrclass then
    begin
      fTableHandler.GetClassName(val, name);
      fOnStreamLn(name, FALSE);
      len := Length(name)
    end
    else if (val >= ORD(' ')) and (val < 127) and (val <> 34) then
    begin
      fOnStreamLn('"' + AnsiChar(val) + '"', FALSE);
      len := 3
    end
    else
    begin
      fOnStreamLn('AnsiChar(' + PadL(IntToStr(val), ' ', 2) + ')', FALSE);
      len := 7
    end;
    while len < width do
    begin
      fOnStreamLn(' ', FALSE);
      inc(len)
    end
  end; {PrintSymbol}

var
  anAction : Action;
  first : boolean;
  s, i : integer;
  targ : Target;
  sset : CRTSet;
  name : CRTName;
begin
  fOnPrintDivider;
  fOnStreamLn('Trace Automation:', TRUE);
  fOnStreamLn('---------- states ----------', TRUE);
  s := fRootState;
  while s <= fLastState do
  begin
    anAction := fStateList[s].firstAction;
    first := true;
    if fStateList[s].endOf = CRTnoSym then
      fOnStreamLn('     ', FALSE)
    else
      fOnStreamLn('E(' + PadL(IntToStr(fStateList[s].endOf), ' ', 2) + ')', FALSE);
    fOnStreamLn(PadL(IntToStr(s), ' ', 3) + ':', FALSE);
    if anAction = nil then
      fOnStreamLn('', TRUE);
    while anAction <> nil do
    begin
      if first then
      begin
        fOnStreamLn(' ', FALSE);
        first := false
      end
      else
      begin
        fOnStreamLn('          ', FALSE)
      end;
      PrintSymbol(anAction^.typ, anAction^.sym, 0);
      fOnStreamLn(' ', FALSE);
      targ := anAction^.target;
      while targ <> nil do
      begin
        fOnStreamLn(IntToStr(targ^.theState) + ' ', FALSE);
        targ := targ^.next;
      end;
      if anAction^.tc = contextTrans then
        fOnStreamLn(' context', TRUE)
      else
        fOnStreamLn('', TRUE);
      anAction := anAction^.next
    end;
    inc(s)
  end;
  fOnStreamLn('', TRUE);
  fOnStreamLn('---------- character classes ----------', TRUE);
  i := 0;
  while i <= fTableHandler.MaxC do
  begin
    fTableHandler.GetClass(i, sset);
    fTableHandler.GetClassName(i, name);
    fOnStreamLn(PadR(name, ' ', 10) + ': ', FALSE);
    fOnStreamLn(Sets.SetToCode(sset, 80, 13), FALSE);
    fOnStreamLn('', TRUE);
    inc(i)
  end
end;

{ AddAction            Add a action to the action list of a state
------------------------------------------------------------------------}

procedure TAutomaton.AddAction(act : Action; var head : Action);
var
  a, lasta : Action;
begin
  a := head;
  lasta := nil;
  while true do
  begin
    if (a = nil) or (act^.typ < a^.typ) then
    {collecting classes at the front improves performance}
    begin
      act^.next := a;
      if lasta = nil then
        head := act
      else
        lasta^.next := act;
      EXIT;
    end;
    lasta := a;
    a := a^.next;
  end;
end;

{ DetachAction         Detach action a from list L
------------------------------------------------------------------------}

procedure TAutomaton.DetachAction(a : Action; var L : Action);
begin
  if L = a then
    L := a^.next
  else if L <> nil then
    DetachAction(a, L^.next)
end;

function TAutomaton.TheAction(theState : TState; ch : AnsiChar) : Action;
var
  a : Action;
  sset : CRTSet;
begin
  a := theState.firstAction;
  while a <> nil do
  begin
    if a^.typ = CRTchart then
    begin
      if ORD(ch) = a^.sym then
      begin
        Result := a;
        EXIT
      end
    end
    else if a^.typ = CRTchrclass then
    begin
      fTableHandler.GetClass(a^.sym, sset);
      if Sets.IsIn(sset, ORD(ch)) then
      begin
        Result := a;
        EXIT
      end
    end;
    a := a^.next
  end;
  Result := nil
end;

procedure TAutomaton.AddTargetList(var lista, listb : Target);
var
  p, t : Target;

  procedure AddTarget(t : Target; var list : Target);
  label
    999;
  var
    p, lastp : Target;
  begin
    p := list;
    lastp := nil;
    while true do
    begin
      if (p = nil) or (t^.theState < p^.theState) then
        goto 999;
      if p^.theState = t^.theState then
      begin
        DISPOSE(t);
        EXIT;
      end;
      lastp := p;
      p := p^.next
    end;
    999 :
      t^.next := p;
    if lastp = nil then
      list := t
    else
      lastp^.next := t
  end;

begin
  p := lista;
  while p <> nil do
  begin
    NEW(t);
    t^.theState := p^.theState;
    AddTarget(t, listb);
    p := p^.next
  end
end;

{ NewMelted            Generate new info about a melted state
------------------------------------------------------------------------}

function TAutomaton.NewMelted(sset : CRTSet; s : integer) : Melted;
var
  melt : Melted;
begin
  NEW(melt);
  melt^.sset := sset;
  melt^.theState := s;
  melt^.next := firstMelted;
  firstMelted := melt;
  Result := melt
end;

{ NewState             Return a new state node
------------------------------------------------------------------------}

function TAutomaton.NewState : integer;
begin
  inc(fLastState);
  if fLastState > maxStates then
    fTableHandler.Restriction(7, maxStates);
  fStateList[fLastState].firstAction := nil;
  fStateList[fLastState].endOf := CRTnoSym;
  fStateList[fLastState].ctx := false;
  Result := fLastState
end;

{ NewTransition        Generate transition (gn.theState, gn.p1) --> toState
------------------------------------------------------------------------}

procedure TAutomaton.NewTransition(from : integer; gn : CRTGraphNode; toState : integer);
var
  a : Action;
  t : Target;
begin
  if toState = fRootState then
    SemErr(21);
  NEW(t);
  t^.theState := toState;
  t^.next := nil;
  NEW(a);
  a^.typ := gn.typ;
  a^.sym := gn.p1;
  a^.tc := gn.p2;
  a^.target := t;
  a^.next := nil;
  AddAction(a, fStateList[from].firstAction);
end;

{ DeleteTargetList     Delete a target list
-------------------------------------------------------------------------}

procedure TAutomaton.DeleteTargetList(list : Target);
begin
  if list <> nil then
  begin
    DeleteTargetList(list^.next);
    DISPOSE(list)
  end;
end;

{ DeleteActionList     Delete an action list
-------------------------------------------------------------------------}

procedure TAutomaton.DeleteActionList(anAction : Action);
begin
  if anAction <> nil then
  begin
    DeleteActionList(anAction^.next);
    DeleteTargetList(anAction^.target);
    DISPOSE(anAction)
  end
end;

{ MakeSet              Expand action symbol into symbol set
-------------------------------------------------------------------------}

procedure TAutomaton.MakeSet(p : Action; var sset : CRTSet);
begin
  if p^.typ = CRTchrclass then
    fTableHandler.GetClass(p^.sym, sset)
  else
  begin
    Sets.Clear(sset);
    Sets.Incl(sset, p^.sym)
  end
end;

{ ChangeAction         Change the action symbol to set
-------------------------------------------------------------------------}

procedure TAutomaton.ChangeAction(a : Action; sset : CRTSet);
var
  nr : integer;

begin
  if Sets.Elements(sset, nr) = 1 then
  begin
    a^.typ := CRTchart;
    a^.sym := nr
  end
  else
  begin
    nr := fTableHandler.ClassWithSet(sset);
    if nr < 0 then
      nr := fTableHandler.NewClass('##', sset);
    a^.typ := CRTchrclass;
    a^.sym := nr
  end
end;

{ CombineShifts     Combine shifts with different symbols into same state
-------------------------------------------------------------------------}

procedure TAutomaton.CombineShifts;
var
  s : integer;
  a, b, c : Action;
  seta, setb : CRTSet;
begin
  s := fRootState;
  while s <= fLastState do
  begin
    a := fStateList[s].firstAction;
    while a <> nil do
    begin
      b := a^.next;
      while b <> nil do
      begin
        if (a^.target^.theState = b^.target^.theState) and (a^.tc = b^.tc) then
        begin
          MakeSet(a, seta);
          MakeSet(b, setb);
          Sets.Unite(seta, setb);
          ChangeAction(a, seta);
          c := b;
          b := b^.next;
          DetachAction(c, a)
        end
        else
          b := b^.next
      end;
      a := a^.next
    end;
    inc(s)
  end
end;

{ DeleteRedundantStates   Delete unused and equal states
-------------------------------------------------------------------------}

procedure TAutomaton.DeleteRedundantStates;
var
  anAction : Action;
  s, s2, next : integer;
  used : Sets.BITARRAY;
    {ARRAY [0 .. maxStates DIV Sets.size] OF BITSET }{KJG}
  newStateNr : array[0..maxStates] of integer;

  procedure FindUsedStates(s : integer);
  var
    anAction : Action;
  begin
    if Sets.IsIn(used, s) then
      EXIT;
    Sets.Incl(used, s);
    anAction := fStateList[s].firstAction;
    while anAction <> nil do
    begin
      FindUsedStates(anAction^.target^.theState);
      anAction := anAction^.next
    end
  end;

begin
  Sets.Clear(used);
  FindUsedStates(fRootState);
    {---------- combine equal final states ------------}
  s := fRootState + 1;
    {root state cannot be final}
  while s <= fLastState do
  begin
    if Sets.IsIn(used, s) and (fStateList[s].endOf <> CRTnoSym) then
      if (fStateList[s].firstAction = nil) and not fStateList[s].ctx then
      begin
        s2 := s + 1;
        while s2 <= fLastState do
        begin
          if Sets.IsIn(used, s2) and (fStateList[s].endOf = fStateList[s2].endOf) then
            if (fStateList[s2].firstAction = nil) and not fStateList[s2].ctx then
            begin
              Sets.Excl(used, s2);
              newStateNr[s2] := s
            end;
          inc(s2)
        end
      end;
    inc(s)
  end;
  s := fRootState;
    { + 1 ?  PDT - was rootState, but Oberon had .next ie +1
                    seems to work both ways?? }
  while s <= fLastState do
  begin
    if Sets.IsIn(used, s) then
    begin
      anAction := fStateList[s].firstAction;
      while anAction <> nil do
      begin
        if not Sets.IsIn(used, anAction^.target^.theState) then
          anAction^.target^.theState := newStateNr[anAction^.target^.theState];
        anAction := anAction^.next
      end
    end;
    inc(s)
  end;
    {-------- delete unused states --------}
  s := fRootState + 1;
  next := s;
  while s <= fLastState do
  begin
    if Sets.IsIn(used, s) then
    begin
      if next < s then
        fStateList[next] := fStateList[s];
      newStateNr[s] := next;
      inc(next)
    end
    else
      DeleteActionList(fStateList[s].firstAction);
    inc(s)
  end;
  fLastState := next - 1;
  s := fRootState;
  while s <= fLastState do
  begin
    anAction := fStateList[s].firstAction;
    while anAction <> nil do
    begin
      anAction^.target^.theState := newStateNr[anAction^.target^.theState];
      anAction := anAction^.next
    end;
    inc(s)
  end
end;

{ ConvertToStates    Convert the TDG in gp into a subautomaton of the DFA
------------------------------------------------------------------------}

procedure TAutomaton.ConvertToStates(gp0, sp : integer);
{note: gn.line is abused as a state number!}

var
  stepped, visited : MarkList;

  procedure NumberNodes(gp, snr : integer);
  var
    gn : CRTGraphNode;
  begin
    if gp = 0 then
      EXIT; {end of graph}
    fTableHandler.GetNode(gp, gn);
    if gn.line >= 0 then
      EXIT; {already visited}
    if snr < fRootState then
      snr := NewState;
    gn.line := snr;
    fTableHandler.PutNode(gp, gn);
    if fTableHandler.DelGraph(gp) then
      fStateList[snr].endOf := sp;
      {snr is end state}
    case gn.typ of
      CRTchrclass, CRTchart :
        begin
          NumberNodes(ABS(gn.next), fRootState - 1)
        end;
      CRTopt :
        begin
          NumberNodes(ABS(gn.next), fRootState - 1);
          NumberNodes(gn.p1, snr)
        end;
      CRTiter :
        begin
          NumberNodes(ABS(gn.next), snr);
          NumberNodes(gn.p1, snr)
        end;
      CRTalt :
        begin
          NumberNodes(gn.p1, snr);
          NumberNodes(gn.p2, snr)
        end;
    end;
  end;

  function TheState(gp : integer) : integer;
  var
    s : integer;
    gn : CRTGraphNode;
  begin
    if gp = 0 then
    begin
      s := NewState;
      fStateList[s].endOf := sp;
      Result := s
    end
    else
    begin
      fTableHandler.GetNode(gp, gn);
      Result := gn.line
    end
  end;

  procedure Step(from, gp : integer);
  var
    gn : CRTGraphNode;
    next : integer;
  begin
    if gp = 0 then
      EXIT;
    fTableHandler.InclMarkList(stepped, gp);
    fTableHandler.GetNode(gp, gn);
    case gn.typ of
      CRTchrclass, CRTchart :
        begin
          NewTransition(from, gn, TheState(ABS(gn.next)))
        end;
      CRTalt :
        begin
          Step(from, gn.p1);
          Step(from, gn.p2)
        end;
      CRTopt, CRTiter :
        begin
          next := ABS(gn.next);
          if NOT fTableHandler.IsInMarkList(stepped, next) then
            Step(from, next);
          Step(from, gn.p1);
          {Step(from, ABS(gn.next));
          Step(from, gn.p1)}
        end;
    end
  end;

  procedure FindTrans(gp : integer; start : boolean);
  var
    gn : CRTGraphNode;
  begin
    if (gp = 0) or fTableHandler.IsInMarkList(visited, gp) then
      EXIT;
    fTableHandler.InclMarkList(visited, gp);
    fTableHandler.GetNode(gp, gn);
    if start then
      begin
        fTableHandler.ClearMarkList(stepped);
        Step(gn.line, gp); { start of group of equally numbered nodes }
      end;
    case gn.typ of
      CRTchrclass, CRTchart :
        begin
          FindTrans(ABS(gn.next), true)
        end;
      CRTopt :
        begin
          FindTrans(ABS(gn.next), true);
          FindTrans(gn.p1, false)
        end;
      CRTiter :
        begin
          FindTrans(ABS(gn.next), false);
          FindTrans(gn.p1, false)
        end;
      CRTalt :
        begin
          FindTrans(gn.p1, false);
          FindTrans(gn.p2, false)
        end;
    end;
  end;

var
  gn : CRTGraphNode;
  i : integer;

begin
  if fTableHandler.DelGraph(gp0) then
    SemErr(20);
  for i := 0 to fTableHandler.nNodes do
  begin
    fTableHandler.GetNode(i, gn);
    gn.line := -1;
    fTableHandler.PutNode(i, gn)
  end;
  NumberNodes(gp0, fRootState);
  fTableHandler.ClearMarkList(visited);
  FindTrans(gp0, true)
end;

procedure TAutomaton.MatchDFA(str : AnsiString; sp : integer; var matchedSp : integer);
var
  s, sto : integer {State};
  a : Action;
  gn : CRTGraphNode;
  i, len : integer;
  weakMatch : BOOLEAN;
begin { s with quotes }
  s := fRootState;
  i := 2;
  len := Length(str);
  weakMatch := false;
  while true do
  begin
    { try to match str against existing DFA }
    if i = len then
      Break;
    a := TheAction(fStateList[s], str[i]);
    if a = nil then
      Break;
    if a^.typ = CRTchrclass then
      weakMatch := true;
    s := a^.target^.theState;
    inc(i)
  end;
  if weakMatch and (i < len) then
  begin
    s := fRootState;
    i := 2;
    fDirtyDFA := TRUE
  end;

  while i < len do
  begin
    { make new DFA for str[i..len-1] }
    sto := NewState;
    gn.typ := CRTchart;
    gn.p1 := ORD(str[i]);
    gn.p2 := normTrans;
    NewTransition(s, gn, sto);
    s := sto;
    inc(i)
  end;
  matchedSp := fStateList[s].endOf;
  if fStateList[s].endOf = CRTnoSym then
    fStateList[s].endOf := sp;
end;

{ SplitActions     Generate unique actions from two overlapping actions
-----------------------------------------------------------------------}

procedure TAutomaton.SplitActions(a, b : Action);
var
  c : Action;
  seta, setb, setc : CRTSet;

  procedure CombineTransCodes(t1, t2 : integer; var result : integer);
  begin
    if t1 = contextTrans then
      result := t1
    else
      result := t2
  end;

begin
  MakeSet(a, seta);
  MakeSet(b, setb);
  if Sets.Equal(seta, setb) then
  begin
    AddTargetList(b^.target, a^.target);
    DeleteTargetList(b^.target);
    CombineTransCodes(a^.tc, b^.tc, a^.tc);
    DetachAction(b, a);
    DISPOSE(b);
  end
  else if Sets.Includes(seta, setb) then
  begin
    setc := seta;
    Sets.Differ(setc, setb);
    AddTargetList(a^.target, b^.target);
    CombineTransCodes(a^.tc, b^.tc, b^.tc);
    ChangeAction(a, setc)
  end
  else if Sets.Includes(setb, seta) then
  begin
    setc := setb;
    Sets.Differ(setc, seta);
    AddTargetList(b^.target, a^.target);
    CombineTransCodes(a^.tc, b^.tc, a^.tc);
    ChangeAction(b, setc)
  end
  else
  begin
    Sets.Intersect(seta, setb, setc);
    Sets.Differ(seta, setc);
    Sets.Differ(setb, setc);
    ChangeAction(a, seta);
    ChangeAction(b, setb);
    NEW(c);
    c^.target := nil;
    CombineTransCodes(a^.tc, b^.tc, c^.tc);
    AddTargetList(a^.target, c^.target);
    AddTargetList(b^.target, c^.target);
    ChangeAction(c, setc);
    AddAction(c, a)
  end
end;

{ MakeUnique           Make all actions in this state unique
-------------------------------------------------------------------------}

procedure TAutomaton.MakeUnique(s : integer; var changed : boolean);
var
  a, b : Action;

  function Overlap(a, b : Action) : boolean;
  var
    seta, setb : CRTSet;
  begin
    if a^.typ = CRTchart then
    begin
      if b^.typ = CRTchart then
      begin
        Result := a^.sym = b^.sym
      end
      else
      begin
        fTableHandler.GetClass(b^.sym, setb);
        Result := Sets.IsIn(setb, a^.sym)
      end
    end
    else
    begin
      fTableHandler.GetClass(a^.sym, seta);
      if b^.typ = CRTchart then
      begin
        Result := Sets.IsIn(seta, b^.sym)
      end
      else
      begin
        fTableHandler.GetClass(b^.sym, setb);
        Result := not Sets.Different(seta, setb)
      end
    end
  end;

begin
  a := fStateList[s].firstAction;
  changed := false;
  while a <> nil do
  begin
    b := a^.next;
    while b <> nil do
    begin
      if Overlap(a, b) then
      begin
        SplitActions(a, b);
        changed := true;
        exit;
        { originally no RETURN.  FST blows up if we leave RETURN out.
           Somewhere there is a field that is not properly set, but I
           have not chased this down completely Fri  08-20-1993 }
      end;
      b := b^.next;
    end;
    a := a^.next
  end;
end;

{ MeltStates       Melt states appearing with a shift of the same symbol
-----------------------------------------------------------------------}
procedure TAutomaton.MeltStates(s : integer; var correct : boolean);
var
  anAction : Action;
  ctx : boolean;
  endOf : integer;
  melt : Melted;
  sset : CRTSet;
  s1 : integer;
  changed : boolean;

  procedure AddMeltedSet(nr : integer; var sset : CRTSet);
  var
    m : Melted;
  begin
    m := firstMelted;
    while (m <> nil) and (m^.theState <> nr) do
      m := m^.next;
    if m = nil then
      fTableHandler.Restriction(-1, 0);
    Sets.Unite(sset, m^.sset);
  end;

  function AlreadyReported(const Data : AnsiString) : boolean;
  begin
    Result := AlreadyReportedList.IndexOf(Data) = -1;
  end; {AlreadyReported}

  procedure GetStateSet(t : Target; var sset : CRTSet; var endOf : integer; var ctx : boolean);
  { Modified back to match Oberon version Fri  08-20-1993
     This seemed to cause problems with some larger automata }
     { new bug fix Wed  11-24-1993  from ETHZ incorporated }
  var
    Data : AnsiString;
  begin
    Sets.Clear(sset);
    endOf := CRTnoSym;
    ctx := false;
    while t <> nil do
    begin
      if t^.theState <= lastSimState then
        Sets.Incl(sset, t^.theState)
      else
        AddMeltedSet(t^.theState, sset);
      if fStateList[t^.theState].endOf <> CRTnoSym then
      begin
        if (endOf = CRTnoSym) or (endOf = fStateList[t^.theState].endOf) then
        begin
          endOf := fStateList[t^.theState].endOf;
        end
        else
        begin
          fOnStreamLn('', TRUE);
          Data := fOnGetErrorData(endOf, fStateList[t^.theState].endOf);
          if NOT AlreadyReported(Data) then
          begin
            AlreadyReportedList.Add(Data);
            fOnStreamLn(fOnErrorStr(154, Data), TRUE);
            fOnInDistinguishedError(endOf, Data);
          end;
          correct := false;
        end;
      end;
      if fStateList[t^.theState].ctx then
      begin
        ctx := true;
        if fStateList[t^.theState].endOf <> CRTnoSym then
        begin
          fOnStreamLn('', TRUE);
          fOnStreamLn('Ambiguous CONTEXT clause.', TRUE);
          correct := false
        end
      end;
      t := t^.next
    end;;
  end;

  procedure FillWithActions(s : integer; targ : Target);
  var
    anAction, a : Action;
  begin
    while targ <> nil do
    begin
      anAction := fStateList[targ^.theState].firstAction;
      while anAction <> nil do
      begin
        NEW(a);
        a^ := anAction^;
        a^.target := nil;
        AddTargetList(anAction^.target, a^.target);
        AddAction(a, fStateList[s].firstAction);
        anAction := anAction^.next
      end;
      targ := targ^.next
    end;
  end;

  function KnownMelted(sset : CRTSet; var melt : Melted) : boolean;
  begin
    melt := firstMelted;
    while melt <> nil do
    begin
      if Sets.Equal(sset, melt^.sset) then
      begin
        Result := true;
        EXIT
      end;
      melt := melt^.next
    end;
    Result := false
  end;

begin
  anAction := fStateList[s].firstAction;
  while anAction <> nil do
  begin
    if anAction^.target^.next <> nil then
    begin
      GetStateSet(anAction^.target, sset, endOf, ctx);
      if not KnownMelted(sset, melt) then
      begin
        s1 := NewState;
        fStateList[s1].endOf := endOf;
        fStateList[s1].ctx := ctx;
        FillWithActions(s1, anAction^.target);
        repeat
          MakeUnique(s1, changed)
        until not changed;
        melt := NewMelted(sset, s1);
      end;
      DeleteTargetList(anAction^.target^.next);
      anAction^.target^.next := nil;
      anAction^.target^.theState := melt^.theState
    end;
    anAction := anAction^.next
  end
end;

{ MakeDeterministic     Make NDFA --> DFA
------------------------------------------------------------------------}

procedure TAutomaton.MakeDeterministic(var correct : boolean);
var
  s : integer;
  changed : boolean;

  procedure FindCtxStates;
  { Find states reached by a context transition }
  var
    a : Action;
    s : integer;
  begin
    s := fRootState;
    while s <= fLastState do
    begin
      a := fStateList[s].firstAction;
      while a <> nil do
      begin
        if a^.tc = contextTrans then
          fStateList[a^.target^.theState].ctx := true;
        a := a^.next
      end;
      inc(s)
    end;
  end;

begin
  lastSimState := fLastState;
  FindCtxStates;
  s := fRootState;
  while s <= fLastState do
  begin
    repeat
      MakeUnique(s, changed)
    until not changed;
    inc(s)
  end;
  correct := true;
  s := fRootState;
  while s <= fLastState do
  begin
    MeltStates(s, correct);
    inc(s)
  end;
  DeleteRedundantStates;
  CombineShifts;
end;

procedure TAutomaton.Initialize;
begin
  FillChar(fStateList,sizeof(fStateList),0);
  fLastState := -1;
  fRootState := NewState;
  firstMelted := nil;
  NewLine := true;
  fDirtyDFA := false;
end; {Initialize}

procedure TAutomaton.Finalize;
var
  aMelted : Melted;
begin
  while firstMelted <> nil do
  begin
    aMelted := firstMelted;
    firstMelted := firstMelted.Next;
    dispose(aMelted);
  end;
end;

function TAutomaton.GetStateList(const Idx: integer): TState;
begin
  Result := fStateList[Idx];
end; {GetStateList}

procedure TAutomaton.SetStateList(const Idx: integer;
  const Value: TState);
begin
  fStateList[Idx] := Value;
end; {SetStateList}

end.

