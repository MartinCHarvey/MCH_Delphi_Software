unit TZRegistry;

interface

uses
  Registry;

type
  TTZRegistry = class(TRegistry)
  public
    function ReadIntegerDef(const Name : AnsiString; const Default : integer) : integer;
    function ReadStringDef(const Name : AnsiString; const Default : AnsiString) : AnsiString;
    function ReadBoolDef(const Name : AnsiString; const Default : boolean) : boolean;
  end; {TTZRegistry}

implementation

{ TTZRegistry }

function TTZRegistry.ReadBoolDef(const Name: AnsiString;
  const Default: boolean): boolean;
begin
  if ValueExists(Name) then
    Result := ReadBool(Name)
  else
    Result := Default;
end; {ReadBoolDef}

function TTZRegistry.ReadIntegerDef(const Name: AnsiString;
  const Default: integer): integer;
begin
  if ValueExists(Name) then
    Result := ReadInteger(Name)
  else
    Result := Default;
end; {ReadIntegerDef}

function TTZRegistry.ReadStringDef(const Name, Default: AnsiString): AnsiString;
begin
  if ValueExists(Name) then
    Result := ReadString(Name)
  else
    Result := Default;
end; {ReadStringDef}

end.


