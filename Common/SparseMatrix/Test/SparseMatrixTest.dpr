program SparseMatrixTest;

uses
  FMX.Forms,
  SparseMatrixTestFrm in 'SparseMatrixTestFrm.pas' {Form1},
  SparseMatrix in '..\SparseMatrix.pas',
  DLList in '..\..\DLList\DLList.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
