unit CocoSwitch;
{$INCLUDE CocoCD.inc}

interface

type
  TCocoSwitch = (
    crsGenTestProgram,       // p - Generate test program
    crsUseRichEdit,          // m - Use TRichEdit in test project
    crsDfmAsResource,        // r - Save the test program DFM as a resource file
    crsGenRegistration,      // e - Generate a component registration unit
    crsGenConsoleApp,        // z - Generate console app
    crsGenVersionInfo,       // v - Generate version information
    crsAutoIncBuild,         // b - Auto-increment build number
    crsGenCommentEvents,     // c - Generate comment events
    crsUseHashFunctions,     // h - Use Hash functions to lookup literals
    crsUseSameTextCompare,   // y - Use SysUtils.SameText for case insensitive compares
    crsAppendSemiColon,      // d - Append a semi-colon to semantic actions
    crsSpaceAsWhitespace,    // w - Treat the space character as whitespace
    crsDoNotGenSymanticActions, // p - Do not generate symatic actions
    crsForceListing,         // l - Force listing
    crsTraceAutomaton,       // a - Trace automaton
    crsStartFollowSets,      // f - Give Start and Follower sets
    crsPrintGraph,           // g - Print top-down graph
    crsTraceStartSets,       // i - Trace start set computations
    crsPrintSymbolTable,     // s - Print symbol table
    crsGrammarTestsOnly,     // t - Grammar tests only - no code generated
    crsPrintXRef,            // x - Print cross reference list
    crsGenerateXMLResults,   // j - Generate XML results
    crsVerboseMessages       // u - Generate XML results
    ); {TCocoSwitch}

const
  LowSwitch = crsGenTestProgram;
  HighSwitch = crsVerboseMessages;

type
  TCocoSwitchSet = set of TCocoSwitch;

  TCocoSwitches = class(TObject)
  private
    fSwitchSet: TCocoSwitchSet;
    fSecondaryHash: AnsiString;
    fPrimaryHash: AnsiString;
    fRegistrationPalette: AnsiString;

    function DoGetTextOrCode(const ForText : boolean) : AnsiString;
    function GetInsertCode : AnsiString;
    function GetText: AnsiString;
    function GetSwitchString(
        const Switch: TCocoSwitch;
        const SwitchOn : boolean;
        const ForText: boolean): AnsiString;
  public
    procedure Assign(const Source : TCocoSwitchSet);
    procedure Clear;
    function SwitchString(const Switch : TCocoSwitch) : AnsiString; overload;
    function SwitchString(const Ch : AnsiChar) : AnsiString; overload;
    function SwitchChar(const Switch : TCocoSwitch) : AnsiChar;
    function SwitchFromChar(const Ch : AnsiString) : TCocoSwitch;

    property InsertCode : AnsiString read GetInsertCode;
    property SwitchSet : TCocoSwitchSet read fSwitchSet write fSwitchSet;
    property Text : AnsiString read GetText;
    property RegistrationPalette : AnsiString read fRegistrationPalette write fRegistrationPalette;
    property PrimaryHash : AnsiString read fPrimaryHash write fPrimaryHash;
    property SecondaryHash : AnsiString read fSecondaryHash write fSecondaryHash;
  end; {TCocoSwitches}

implementation

uses
  StringTools,
  SysUtils;

ResourceString
  SWITCH_STR_C = 'Generate Delphi test project';
  SWITCH_STR_M = 'Use TRichEdit in test project';
  SWITCH_STR_R = 'Save DFM as resource';
  SWITCH_STR_E = 'Generate a component registration unit';
  SWITCH_STR_Z = 'Generate console app';
  SWITCH_STR_V = 'Generate version information';
  SWITCH_STR_B = 'Auto-increment build number';
  SWITCH_STR_O = 'Generate comment events';
  SWITCH_STR_H = 'Use Hash functions to lookup literals';
  SWITCH_STR_Y = 'Use SysUtils.SameText for case insensitive compares';
  SWITCH_STR_D = 'Append a semi-colon to semantic actions';
  SWITCH_STR_W = 'Treat space character as whitespace';
  SWITCH_STR_P = 'Do not generate symatic actions';

  SWITCH_STR_L = 'Force source listing';
  SWITCH_STR_A = 'Trace automaton';
  SWITCH_STR_F = 'Give Start and Follower sets';
  SWITCH_STR_G = 'Generate top-down graph listing';
  SWITCH_STR_I = 'Trace start set computations';
  SWITCH_STR_S = 'Generate symbol table listing';
  SWITCH_STR_T = 'Grammar tests only - no code generated';
  SWITCH_STR_X = 'Generate cross reference list';
  SWITCH_STR_J = 'Generate XML results';
  SWITCH_STR_U = 'Verbose messages';

const
  // Coco/R Switches
  MaxSwitch = 22;
  {MaxInsertOption: the Switch to insert into the code stream when Ctrl-O
   is pressed -- these need to be at the top of the switch list }
  MaxInsertSwitch = 12;
  TCocoSwitchStrings : array [0..MaxSwitch,0..1] of AnsiString = (
    ('C', SWITCH_STR_C),
    ('M', SWITCH_STR_M),
    ('R', SWITCH_STR_R),
    ('E', SWITCH_STR_E),
    ('Z', SWITCH_STR_Z),
    ('V', SWITCH_STR_V),
    ('B', SWITCH_STR_B),
    ('O', SWITCH_STR_O),
    ('H', SWITCH_STR_H),
    ('Y', SWITCH_STR_Y),
    ('D', SWITCH_STR_D),
    ('W', SWITCH_STR_W),
    ('P', SWITCH_STR_P),

    ('L', SWITCH_STR_L),
    ('A', SWITCH_STR_A),
    ('F', SWITCH_STR_F),
    ('G', SWITCH_STR_G),
    ('I', SWITCH_STR_I),
    ('S', SWITCH_STR_S),
    ('T', SWITCH_STR_T),
    ('X', SWITCH_STR_X),
    ('J', SWITCH_STR_J),
    ('U', SWITCH_STR_U)
    {Available letters: None -- K, Q and N are used in the command line compiler}
    ); {TCocoSwitchStrings}

{ TCocoSwitches }

procedure TCocoSwitches.Assign(const Source: TCocoSwitchSet);
begin
  Clear;
  fSwitchSet := Source;
end; {Assign}

procedure TCocoSwitches.Clear;
begin
  fSwitchSet := []; // clear it out
end; {Clear}

function TCocoSwitches.GetSwitchString(
    const Switch : TCocoSwitch;
    const SwitchOn : boolean;
    const ForText : boolean) : AnsiString;
begin
  Result := '';
  if ForText then
  begin
    if SwitchOn then
      Result := '  ' + SwitchString(Switch) + CRLF;
  end
  else
  begin
    if ord(Switch) <= MaxInsertSwitch then
    begin
      Result := Result + '$' + SwitchChar(Switch);
      if SwitchOn then
      begin
        Result := Result + '+';
        if Switch = crsGenRegistration then
          Result := Result + ' "' + RegistrationPalette + '"'
        else if Switch = crsUseHashFunctions then
          Result := Result + ' ' + PrimaryHash + ', ' + SecondaryHash;
      end
      else
        Result := Result + '-';
      Result := Result + '  //' + SwitchString(Switch) + CRLF;
    end;
  end;
end; {GetSwitchString}

function TCocoSwitches.DoGetTextOrCode(const ForText: boolean): AnsiString;
var
  i : TCocoSwitch;
  Count : integer;
begin
  if ForText then
    Result := 'Switches:' + CRLF
  else
    Result := '';

  Count := 0;
  for i := LowSwitch to HighSwitch do
    if i IN fSwitchSet then
    begin
      Count := Count + 1;
      Result := Result + GetSwitchString(i, TRUE, ForText);
    end
    else
      Result := Result + GetSwitchString(i, FALSE, ForText);

  if ForText AND (Count = 0) then
    Result := Result + '  -- None --' + CRLF;
end; {DoGetTextOrCode}

function TCocoSwitches.GetInsertCode: AnsiString;
begin
  Result := DoGetTextOrCode(FALSE);
end; {GetInsertCode}

function TCocoSwitches.GetText: AnsiString;
begin
  Result := DoGetTextOrCode(TRUE);
end; {GetText}

function TCocoSwitches.SwitchChar(const Switch: TCocoSwitch): AnsiChar;
begin
  Result := TCocoSwitchStrings[ord(Switch),0][1];
end; {SwitchChar}

function TCocoSwitches.SwitchString(const Switch: TCocoSwitch): AnsiString;
begin
  Result := TCocoSwitchStrings[ord(Switch),1];
end; {SwitchString}

function TCocoSwitches.SwitchString(const Ch: AnsiChar): AnsiString;
var
  i : integer;
begin
  Result := 'Unknown Switch';
  for i := Low(TCocoSwitchStrings) to High(TCocoSwitchStrings) do
  begin
    if Uppercase(TCocoSwitchStrings[i, 0]) = Uppercase(Ch) then
      begin
        Result := TCocoSwitchStrings[i, 1];
        Break;
      end;
  end; { for i }
end; {SwitchString}

function TCocoSwitches.SwitchFromChar(const Ch: AnsiString): TCocoSwitch;
var
  i : TCocoSwitch;
begin
  Result := crsGenTestProgram;
  for i := LowSwitch to HighSwitch do
  begin
    if Uppercase(SwitchChar(i)) = Uppercase(Ch) then
      begin
        Result := i;
        Break;
      end;
  end; { for i }
end; {SwitchFromChar}

end.

