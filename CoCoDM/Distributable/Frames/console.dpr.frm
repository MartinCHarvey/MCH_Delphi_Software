program c-->Grammar<--;
{$APPTYPE CONSOLE}

uses
  SysUtils,
  -->Grammar<--;

const
  ResultStr = 'Results can be found in ';

type
  TDisplayObj = class(TObject)
  private
    function CustomErrorEvent(Sender : TObject; const ErrorCode : integer; 
      const Data : string) : string;
    procedure OnSuccess(Sender : TObject);
    procedure OnFailure(Sender : TObject; NumErrors : integer);
  end; // DisplayObj

var
  -->Grammar<--1 : T-->Grammar<--;
  DisplayObj : TDisplayObj;

{ TDisplayObj }

function TDisplayObj.CustomErrorEvent(Sender: TObject;
  const ErrorCode: integer; const Data : string): string;
begin
  Result := 'Error: ' + AnsiString(IntToStr(ErrorCode));
end;

procedure TDisplayObj.OnSuccess(Sender : TObject);
begin
  Writeln('Compile sucessful');
  Writeln(ResultStr + ChangeFileExt(ParamStr(1),'.lst'));
end;

procedure TDisplayObj.OnFailure(Sender : TObject; NumErrors : integer);
begin
  Write('Compile completed with ' + IntToStr(NumErrors) + ' error');
  if NumErrors <> 1 then
    Writeln('s')
  else
    Writeln;
  Writeln(ResultStr + ChangeFileExt(ParamStr(1),'.lst'));
end;

procedure ShowVersion;
begin
  Write('-->Grammar<--');
  -->Console_Version<--
  Writeln;
end;

procedure ShowHelp;
begin
  Writeln('Usage: c-->Grammar<-- [filename]');
  Writeln('Example: c-->Grammar<-- Test.txt');
end;

begin
  ShowVersion;
  if ParamCount = 0 then
  begin
    ShowHelp;
    Exit;
  end;
  -->Grammar<--1 := T-->Grammar<--.Create(nil);
  try
    DisplayObj := TDisplayObj.Create;
    try
      if NOT FileExists(ParamStr(1)) then
      begin
        Writeln('File: ' + ParamStr(1) + ' not found.');
        Exit;
      end;
      -->Grammar<--1.OnCustomError := DisplayObj.CustomErrorEvent;
      -->Grammar<--1.OnSuccess := DisplayObj.OnSuccess;
      -->Grammar<--1.OnFailure := DisplayObj.OnFailure;

      -->Grammar<--1.SourceFileName := ParamStr(1);
      -->Grammar<--1.Execute;
      -->Grammar<--1.ListStream.SaveToFile(ChangeFileExt(ParamStr(1),'.lst'));
    finally
      DisplayObj.Free;
    end;
  finally
    -->Grammar<--1.Free;
  end;

end.

