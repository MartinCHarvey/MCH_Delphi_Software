program ExactCoverTest;

uses
  FMX.Forms,
  ExactCoverTestFrm in 'ExactCoverTestFrm.pas' {Form1},
  ExactCover in '..\ExactCover.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
