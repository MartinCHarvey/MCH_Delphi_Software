unit SudokuStreaming;

interface

uses
  StreamingSystem, SSStreamables;

var
  SBoardHeirarchy: THeirarchyInfo;

implementation

uses
  SudokuBoard;

initialization
  SBoardHeirarchy := TObjDefaultHeirarchy;
  SetLength(SBoardHeirarchy.MemberClasses, 1);
  SBoardHeirarchy.MemberClasses[0] := TSBoardState;
end.
