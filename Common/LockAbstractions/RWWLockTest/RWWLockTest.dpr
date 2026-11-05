program RWWLockTest;

uses
  Vcl.Forms,
  RWWLockTestFrm in 'RWWLockTestFrm.pas' {RWWLockTestForm},
  RWWLock in '..\RWWLock.pas',
  LockAbstractions in '..\LockAbstractions.pas',
  SRWLockWrapper in '..\..\SRWLockWrapper\SRWLockWrapper.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TRWWLockTestForm, RWWLockTestForm);
  Application.Run;
end.
