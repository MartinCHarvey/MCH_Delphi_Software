program WorkItemTest;

uses
  Forms,
  TestForm in 'TestForm.pas' {WorkItemTestForm},
  WorkItems in '..\WorkItems.pas',
  DLList in '..\..\DLList\DLList.pas',
  LockAbstractions in '..\..\LockAbstractions\LockAbstractions.pas',
  SRWLockWrapper in '..\..\SRWLockWrapper\SRWLockWrapper.pas';

{$R *.RES}

begin
  Application.Initialize;
  Application.CreateForm(TWorkItemTestForm, WorkItemTestForm);
  Application.Run;
end.
