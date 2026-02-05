program RWWLockTest;

uses
  Vcl.Forms,
  RWWLockTestFrm in 'RWWLockTestFrm.pas' {RWWLockTestForm},
  RWWLock in '..\RWWLock.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TRWWLockTestForm, RWWLockTestForm);
  Application.Run;
end.
