program LexerTest;

uses
  Vcl.Forms,
  LexerTestForm in 'LexerTestForm.pas' {Form1},
  SoftLexer in '..\SoftLexer.pas',
  DLList in '..\..\DLList\DLList.pas',
  Trackables in '..\..\Tracking\Trackables.pas',
  BinaryTree in '..\..\Balanced Tree\BinaryTree.pas',
  OrdinalSets in '..\..\OrdinalSets\OrdinalSets.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
