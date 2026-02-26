program NQueens;

{$APPTYPE CONSOLE}

{$R *.res}

{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the “Software”), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

uses
  System.SysUtils,
  ExactCover in '..\Common\ExactCover\ExactCover.pas',
  SparseMatrix in '..\Common\SparseMatrix\SparseMatrix.pas',
  DLList in '..\Common\DLList\DLList.pas',
  Trackables in '..\Common\Tracking\Trackables.pas',
  BinaryTree in '..\Common\Balanced Tree\BinaryTree.pas',
  QueensCover in 'QueensCover.pas';

var
 Queens: TQueensCover;
 N: integer;

begin
  try
    Queens := TQueensCover.Create;
    try
      for N := 1 to 12 do
      begin
        Queens.Solve(N);
        WriteLn(IntToStr(N) + ' queens: '+ IntToStr(Queens.Solutions));
      end;
      WriteLn('Press return to continue.');
      ReadLn;
    finally
      Queens.Free;
    end;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
