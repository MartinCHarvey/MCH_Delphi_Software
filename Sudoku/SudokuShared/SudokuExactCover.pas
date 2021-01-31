unit SudokuExactCover;

interface

{

Application of Exact Cover problem to Sudoku.

This is then simply an application of labelling. For possibilities, instead of
1,2,3,4,5 we have a possible placement of a number in a row and column:

Row Labels, "possibilities" in our matrix:

Row X, Col Y, constains # Z.

R1C1N1, R1C1N2 ... RX, CY, NZ ... R9C9N9.

Column labels.

Each column is a constraint.

There are four types contraints, each of which should be satisfied exactly once:

Row-Column: Each row column intersection contains one number.

81 constraints, one for each Cell.
R1C1 = Entry set in RnCn constraint if that row and column contains a number.

Row-Number: Each row contains a particular number.

81 constraints, 9 rows by 9 possible numbers.
R1#1 = Each entry set in Rn#, constraint if that row contains a particular number.

Column-number: Same as for rows, 81 constraints for that row contains a particular number.

C1#1 = Each entry set in Rn#, constraint if that column contains a particular number.

Block-Number: Like rows and columns, label the boxes (groups of 3x3 cells),
Constraint satisfied if each boxcontains a particular number.

}

uses
  ExactCover;

implementation

end.
