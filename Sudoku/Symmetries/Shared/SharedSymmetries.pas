unit SharedSymmetries;

{

Copyright © 2021 Martin Harvey <martin_c_harvey@hotmail.com>

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


interface

{$IFDEF USE_TRACKABLES}
uses Trackables;
{$ENDIF}

const
{$IFDEF ORDER2}
  ORDER = 2;
{$ELSE}
{$IFDEF ORDER3}
  ORDER = 3;
{$ELSE}
ERROR Need to define board order.
{$ENDIF}
{$ENDIF}
   //Order 2 sudoku boards have 16 squares,
   //and 128 symmetries
  // Order 3 sudoku boards have 81 squares
  // and 3,359,232 symmetries

  //All this is for the purpose of finding the number of uniquely different
  //(taking into account isomorphisms)

  //Not that we are (for the moment) only considering the symmetry of givens
  //not any other symmetry involving the 9! renumberings

  //As such, each filled entry on the board contains a unique integer,
  //which is "given number x" - ie which given it is, not the sudoku number
  //that is actually used for that given.

  //All this is quite trivial for low orders, but will get rather more difficult
  //for high orders.

  //From a simple recursive algorithm, we know the final result for a 17 clue
  //sudoku will be less than

  //Conservative (pessimistic) estimates:
  //7 givens: 11440
  //8 givens: 12870
  //9 givens: 11440

  //Board order 3:
  //17 givens: 128,447,994,798,305,325

type
  TRowColIdx = 0 .. Pred((ORDER * ORDER));
  TRowColSet = set of TRowColIdx;
  TOrderIdx = 0 .. Pred(ORDER);
  TOrderSet = set of TOrderIdx;

  TSymRow = array [Low(TRowColIdx) .. High(TRowColIdx)] of integer;
  TSymBoardState = array [Low(TRowColIdx) .. High(TRowColIdx)] of TSymRow;

  //TODO - Might need loads and saves here at some point one day.
{$IFDEF USE_TRACKABLES}
  TSymBoard = class (TTrackable)
{$ELSE}
  TSymBoard = class
{$ENDIF}
  protected
    FSymBoardState: TSymBoardState;
    function GetBoardEntry(Row, Col: TRowColIdx): integer; virtual;
    procedure SetBoardEntry(Row, Col: TRowColIdx; Entry: integer); virtual;
  public
    function AmEqualTo(Other:TSymBoard): boolean; virtual;
    procedure Assign(Src: TSymBoard); virtual;
    //TODO - individual isomorphic comparisons might or might not be the best
    //way to search our way thru the space.
    function AmIsomorphicTo(Other: TSymBoard): boolean; virtual; abstract;
    property Entries[Row, Col: TRowColIdx]: integer read GetBoardEntry write SetBoardEntry;
  end;

implementation

procedure TSymBoard.Assign(Src: TSymBoard);
var
  i,j : TRowColIdx;
begin
  if Assigned(Src) then
    for i := Low(i) to High(i) do
      for j := Low(j) to High(j) do
        Entries[i,j] := Src.Entries[i,j];
end;

function TSymBoard.GetBoardEntry(Row, Col: TRowColIdx): integer;
begin
  result := FSymBoardState[Row, Col];
end;

procedure TSymBoard.SetBoardEntry(Row, Col: TRowColIdx; Entry: integer);
begin
  FSymBoardState[Row, Col] := Entry;
end;

function TSymBoard.AmEqualTo(Other:TSymBoard): boolean;
var
  Row, Col: TRowColIdx;
begin
  result := true;
  for Row := Low(Row) to High(Row) do
    for Col := Low(Col) to High(Col) do
      begin
        if FSymBoardState[Row, Col] <> Other.FSymBoardState[Row, Col] then
        begin
          result := false;
          exit;
        end;
      end;
end;

end.
