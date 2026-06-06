program TestHomographTest;

uses
  Forms,
  fTestHomographTest in 'fTestHomographTest.pas' {fmTestHomographTest},
  CocoBase in '..\Frames\CocoBase.pas',
  mwStringHashList in '..\Frames\mwStringHashList.pas';

{$R *.RES}

begin
  Application.Initialize;
  Application.CreateForm(TfmTestHomographTest, fmTestHomographTest);
  Application.Run;
end.    
