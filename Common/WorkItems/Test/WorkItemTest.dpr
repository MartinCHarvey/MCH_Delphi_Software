program WorkItemTest;

uses
  Forms,
  TestForm in 'TestForm.pas' {WorkItemTestForm},
  WorkItems in '..\WorkItems.pas',
  DLList in '..\..\DLList\DLList.pas';

{$R *.RES}

begin
  Application.Initialize;
  Application.CreateForm(TWorkItemTestForm, WorkItemTestForm);
  Application.Run;
end.
