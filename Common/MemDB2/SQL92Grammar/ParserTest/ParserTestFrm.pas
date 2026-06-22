unit ParserTestFrm;

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, FMX.Layouts, FMX.Memo;

type
  TForm1 = class(TForm)
    TestLexer: TButton;
    Memo1: TMemo;
    procedure TestLexerClick(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
  end;

var
  Form1: TForm1;

implementation

{$R *.fmx}

uses SQL92Grammar_lexer, lexlib;

procedure TForm1.TestLexerClick(Sender: TObject);
const
  TEST_STRING = ' "dblstring" ''single string'' N''NString'' B''0101001'' ' +
                ' X''FE44FE'' "DELIMITED" <> >= <= < > = || .. [ ] ( ) ' +
                'AND CONNECTION timestamp ViEW zone' ;
  TEST_LOCATION = 'C:\temp\silly_input.txt';

var
  ret: integer;
  SillyInput: System.Text;
  TestString: string;
  Dummy: integer;
begin
  TestString := TEST_STRING;
  Dummy := IoResult;
  System.assign(SillyInput, TEST_LOCATION);
  SetTextCodePage(SillyInput, 1252);
  rewrite(SillyInput);
  Write(SillyInput, TestString);
  System.close(SillyInput);

  System.assign(yyinput, TEST_LOCATION);
  SetTextCodePage(yyinput, 1252);
  reset(yyinput);
  repeat
    ret := yylex;
    Memo1.Lines.Add('Got a token: ' + TokenName(ret) + '(' + yytoken_text + ')');
  until ret = 0;
  //yyinput closed by yywrap.
end;

end.
