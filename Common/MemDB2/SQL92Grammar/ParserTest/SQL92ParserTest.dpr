program SQL92ParserTest;

uses
  FMX.Forms,
  ParserTestFrm in 'ParserTestFrm.pas' {Form1},
  SQL92Nodes in '..\SQL92Nodes.pas',
  Trackables in '..\..\..\Tracking\Trackables.pas',
  CommonNodes in '..\..\..\HTMLParser\CommonNodes.pas',
  SQL92Grammar_lexer in '..\SQL92Grammar_lexer.pas',
  DLList in '..\..\..\DLList\DLList.pas',
  BinaryTree in '..\..\..\Balanced Tree\BinaryTree.pas',
  SQL92Grammar_parser in '..\SQL92Grammar_parser.pas',
  lexlib_oo in '..\..\..\..\..\github\tply41a\tply41a\lib_oo\lexlib_oo.pas',
  yacclib_oo in '..\..\..\..\..\github\tply41a\tply41a\lib_oo\yacclib_oo.pas',
  lexdstr in '..\..\..\..\..\github\tply41a\tply41a\lexdstr.pas',
  SQL92Grammar_parser_debug in '..\SQL92Grammar_parser_debug.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
