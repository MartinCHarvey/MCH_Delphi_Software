unit JSNodes;
{

Copyright © 2020 Martin Harvey <martin_c_harvey@hotmail.com>

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

uses
  Trackables, Classes, DLList, CommonNodes, CocoBase;

const
  NodeTypeJS = 3;

type

  TJSNode = class(TCommonNode)
  public
    constructor Create;
    constructor CreateWithTracker(Tracker: TTracker);
  end;

  TJSScript = class(TJSNode)
  public
    function AsText: string; override;
  end;

  // Categorised by how many sets of sub-statements / exprs they have
  TJSStatementType = (stInvalid, stEmpty,

    stAtom, // No children expected.
    stAtomMax,

    stSingleExpr, // One child max, is expression
    stExpressionStatement, // child is expression node.
    stVarDeclStatement, // child is expr list.
    stReturnStatement, // Child is returned expression.
    stThrowStatement, // Child is thrown expression.
    stContinueStatement, // Optional child is label identifier
    stBreakStatement, // Optional child is label identifier
    stSingleExprMax,

    stPair, // Two children, first is expression / var decl list, second is statement
    stLabelStatement, // Two children, first is label, second is expression.
    stDoStatement, // Two children, *first* is cond expr, second is statement.
    stWhileStatement, // Two children, first is cond expr, second is statement
    stForStatement, // Two children, first is expr list, second is statement
    stWithStatement,
    // Two children, first is scoping expr, second is statement.
    stFunctionDefinition, // First is parameter list, 2nd is body.
    stPairMax,

    stTriple, // Three children,
    stIfStatement, // first is expr, second and third statements
    stTripleMax,

    stList, // One list of children, stmts, use existing list.
    stBlockStatement, // Children are statements.
    stTryStatement,
    // Block, opt followed by list of catch clauses, opt followed by finally clause.
    stSwitchStatement, // Expr followed by list of case groups.
    stListMax); // Have this so always remember to set it up

  TJSStatement = class(TJSNode)
  private
    FStatType: TJSStatementType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property StatType: TJSStatementType read FStatType write FStatType;
  end;

  TJSSwitchStatement = class(TJSStatement)
  public
    function CheckNoDupDefaults: boolean;
  end;

  TJSFunction = class(TJSStatement)
    // children are parameter list followed by block.
  private
    FFuncName: string;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property FuncName: string read FFuncName write FFuncName;
  end;

  TJSExprType = (etInvalid,

    etAtom, // No children.
    etSimpleExpression, // true, false, null, identifiers etc.
    etDefaultCase, // default cause label in a switch statement.
    etAtomMax,

    etSingle, etVarDecl, // Single child is variable declaration list.
    etNewOperator, // Single child is obj or typeref, args not included.
    etFunctionExpression, // Single child is function statement
    etUnaryOperator, // Single child is expression, new, delete, inc, dec etc.
    etSingleMax,

    etPair, // Two children
    etVarInDecl, // expression is "for var x in y"
    etInOperator, // expression is x in y.
    etCommaOperator, // statements separated by comma.
    etAssignOperator, // two children, left = right.
    etMultiAssignOperator, // compound assignments of various types.
    etLogicalOperator, // logical or, and
    etBitwiseOperator, // bitwise or, and, xor
    etEqualityTestOperator, // equals not equals and variants.
    etRelationalOperator, // greater, lesser, and variants.
    etShiftOperator, // shl, shr, unsshr
    etArithOperator, // plus minus times and divide
    etCallOperator, // first child called object or new, second child arg list.
    etArrayAccessOperator, etNamedMemberOperator, etObjectFieldDecl,
    // first child field id, second child val expr.
    etPairMax,

    etTriple, etConditionalOperator, // ternary: x ? y : z
    etTripleMax);

  TJSExpr = class(TJSNode)
  private
    FExprType: TJSExprType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    function RecSubset(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property ExprType: TJSExprType read FExprType write FExprType;
  end;

  TJSLogicalOpType = (otOr, otAnd);

  TJSLogicalOperator = class(TJSExpr)
  private
    FLogicalType: TJSLogicalOpType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property LogicalType: TJSLogicalOpType read FLogicalType write FLogicalType;
  end;

  TJSBitwiseOpType = (btOr, btAnd, btXor);

  TJSBitwiseOperator = class(TJSExpr)
  private
    FBitwiseType: TJSBitwiseOpType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property BitwiseType: TJSBitwiseOpType read FBitwiseType write FBitwiseType;
  end;

  TJSAssignmentType = (atInvalid, atMulAssign, // *=
    atDivAssign, // /=
    atModAssign, // %=
    atAddAssign, // +=
    atSubAssign, // -=
    atShlAssign, // <<=
    atShrAssign, // >>=
    atUnsShrAssign, // >>>=
    atBitAndAssign, // &=
    atBitOrAssign, // |=
    atBitXorAssign);

  TJSMultiAssign = class(TJSExpr)
  private
    FAssignmentType: TJSAssignmentType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property AssignmentType: TJSAssignmentType read FAssignmentType
      write FAssignmentType;
  end;

  TJSEqualityTestType = (eqtInvalid, etEqual, etNotEqual, etStrictlyEqual,
    etNotStrictlyEqual);

  TJSEqualityTest = class(TJSExpr)
  private
    FEqualityTestType: TJSEqualityTestType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property EqualityTestType: TJSEqualityTestType read FEqualityTestType
      write FEqualityTestType;
  end;

  TJSRelationalTestType = (rtInvalid, rtLessThan, rtGreaterThan,
    rtLessThanOrEqual, rtGreaterThanOrEqual, rtInstanceOf, rtIn);

  TJSRelationalTest = class(TJSExpr)
  private
    FRelationalTestType: TJSRelationalTestType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property RelationalTestType: TJSRelationalTestType read FRelationalTestType
      write FRelationalTestType;
  end;

  TJSShiftType = (sstInvalid, stLeft, stRight, stUnsRight);

  TJSShiftOperator = class(TJSExpr)
  private
    FShiftType: TJSShiftType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property ShiftType: TJSShiftType read FShiftType write FShiftType;
  end;

  TJSArithType = (aatInvalid, atAdd, atSub, atMul, atDiv, atMod);

  TJSArithOperator = class(TJSExpr)
  private
    FArithType: TJSArithType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property ArithType: TJSArithType read FArithType write FArithType;
  end;

  TJSUnaryType = (utInvalid, utDelete, utVoid, utTypeof, utPreInc, utPreDec,
    utPlus, utMinus, utBitNot, utLogNot, utPostInc, utPostDec);

  TJSUnaryOperator = class(TJSExpr)
  private
    FUnaryType: TJSUnaryType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property UnaryType: TJSUnaryType read FUnaryType write FUnaryType;
  end;

  TJSSimpleExprType = (setInvalid, setThis, setNull, setFalse, setTrue,
    setNumber, setStringLiteral, // is TJSStringSimpleExpr
    setRegExp, // is TJSStringSimpleExpr
    setIdentifier,
    // is TJSIdentifier //TODO - check set of all identifier construct instances.
    setArrayLit, // child is TExprList.
    setObjLit // child is TExprList.
    );

  TJSSimpleExpr = class(TJSExpr)
  private
    FSimpleExprType: TJSSimpleExprType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property SimpleExprType: TJSSimpleExprType read FSimpleExprType
      write FSimpleExprType;
  end;

  TJSNumber = class(TJSSimpleExpr)
  private
    FStringRep: string;
    FIntValue: integer;
    FInt64Value: Int64;
    FFloatValue: double;
    FIntValid, FInt64Valid, FFloatValid: boolean;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property StringRep: string read FStringRep write FStringRep;
    property IntValue: integer read FIntValue write FIntValue;
    property Int64Value: Int64 read FInt64Value write FInt64Value;
    property FloatValue: double read FFloatValue write FFloatValue;
    property IntValid: boolean read FIntValid write FIntValid;
    property Int64Valid: boolean read FInt64Valid write FInt64Valid;
    property FloatValid: boolean read FFloatValid write FFloatValid;
  end;

  TJSString = class(TJSSimpleExpr)
  private
    FStrData: string;
  protected
    procedure FixStrings; override;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property StrData: string read FStrData write FStrData;
  end;

  TJSIdentifier = class(TJSSimpleExpr)
  private
    FName: string;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property Name: string read FName write FName;
  end;

  TJSExprListType = (eltInvalid, eltVarDecls, eltFormalParameters,
    eltForInitializers, eltArguments, eltArrayElems, eltObjectFields);

  TJSExprList = class(TJSNode)
  private
    FListType: TJSExprListType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property ListType: TJSExprListType read FListType write FListType;
  end;

  // When used in a case statement,
  // Has a list of exprs representing the guards, followed by a block.
  // when used in a catch block, has the catch expr, followed by the block.
  TJSTagGroupType = (ttgInvalid, ttgCaseGroup, ttgCatchBlock);

  TJSTagGroup = class(TJSNode)
  private
    FTagGroupType: TJSTagGroupType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property TagGroupType: TJSTagGroupType read FTagGroupType
      write FTagGroupType;
  end;

  TJSBlockType = (btInvalid, btTopOrFunction, btExplicit, btTry, btCatch,
    btFinally, btCaseGroup);

  TJSBlock = class(TJSStatement) // Blocks and statement lists.
  private
    FBlockType: TJSBlockType;
  public
    function AsText: string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts)
      : boolean; override;
    property BlockType: TJSBlockType read FBlockType write FBlockType;
  end;

  TJSSearchType = (jttStatement, jttExpression, jttObjectField);

  TTrimSearchBailout = class(TNodeSearchBailout)
  private
    FSearchTo: TJSSearchType;
  public
    property SearchTo: TJSSearchType read FSearchTo write FSearchTo;
  end;

  TJSSearchOpts = class(TNodeSearchOpts)
  private
    FSearchType: TJSSearchType;
    FFieldType: TJSSimpleExprType;
  public
    property SearchType: TJSSearchType read FSearchType write FSearchType;
    property FieldType: TJSSimpleExprType read FFieldType write FFieldType;
  end;

  TJSNavHelper = class(TStringNavHelper)
  private
  protected
    function CreateParser: TCoCoRGrammar; override;
    function CreateOpts: TNodeSearchOpts; override;
    procedure PostParse(var ParseResult: TCommonNode); override;
    function GetTreeFragmentForString(Fragment: string): TCommonNode; override;
  public
    function FindChild(var Node: TJSNode; Fragment: string;
      FragType: TJSSearchType = jttExpression;
      FieldType: TJSSimpleExprType = setInvalid): boolean;
    function FindNext(var Node: TJSNode; Fragment: string;
      FragType: TJSSearchType = jttExpression;
      FieldType: TJSSimpleExprType = setInvalid): boolean;
    function FindChildRec(var Node: TJSNode; Fragment: string;
      FragType: TJSSearchType = jttExpression;
      FieldType: TJSSimpleExprType = setInvalid): boolean;
    function PushChild(var Node: TJSNode; Fragment: string;
      FragType: TJSSearchType = jttExpression;
      FieldType: TJSSimpleExprType = setInvalid): boolean;
    function PushChildRec(var Node: TJSNode; Fragment: string;
      FragType: TJSSearchType = jttExpression;
      FieldType: TJSSimpleExprType = setInvalid): boolean;
    procedure Pop(var Node: TJSNode);

    function NavToFieldValue(var Node: TJSNode; DoPush: boolean = true)
      : boolean;
    function NavToFieldList(var Node: TJSNode; DoPush: boolean = true): boolean;
    function NavToArrayElems(var Node: TJSNode; DoPush: boolean = true)
      : boolean;

    function NavChildToFieldValue(var Node: TJSNode; Fragment: string;
      FieldType: TJSSimpleExprType = setInvalid;
      DoPush: boolean = true): boolean;
    function NavChildToChildFieldList(var Node: TJSNode; Fragment: string;
      DoPush: boolean = true): boolean;
    function NavAssignmentToRHS(var Node: TJSNode;
      DoPush: boolean = true): boolean;
  end;

function TrimSearchTree(Node: TJSNode; TrimTo: TJSSearchType): TJSNode;

implementation

uses
  HTMLEscapeHelper, JScriptGrammar
{$IFDEF DEBUG_SEARCH}
    , GlobalLog, SysUtils
{$ENDIF}
    ;

const
  // Categorised by how many sets of sub-statements / exprs they have
  TJSSTatementStrings: array [TJSStatementType] of string = ('', // stInvalid,
    '', // stEmpty,

    '', // stAtom, //No children expected.
    '', // stAtomMax,

    '', // stSingleExpr, //One child max, is expression
    '<stmt>', // stExpressionStatement, //child is expression node.
    'var', // stVarDeclStatement, //child is expr list.
    'return', // stReturnStatement, //Child is returned expression.
    'throw', // stThrowStatement, //Child is thrown expression.
    'continue', // stContinueStatement, //Optional child is label identifier
    'break', // stBreakStatement, //Optional child is label identifier
    '', // stSingleExprMax,

    '', // stPair, //Two children, first is expression / var decl list, second is statement
    'label', // stLabelStatement, //Two children, first is label, second is expression.
    'do', // stDoStatement,   //Two children, *first* is cond expr, second is statement.
    'while', // stWhileStatement, //Two children, first is cond expr, second is statement
    'for', // stForStatement,   //Two children, first is expr list, second is statement
    'with', // stWithStatement,  //Two children, first is scoping expr, second is statement.
    'function', // stFunctionDefinition, //First is parameter list, 2nd is body.
    '', // stPairMax,

    '', // stTriple, //Three children,
    'if', // stIfStatement, //first is expr, second and third statements
    '', // stTripleMax,

    '', // stList, //One list of children, stmts, use existing list.
    '{}', // stBlockStatement, //Children are statements.
    'try', // stTryStatement, //Block, opt followed by list of catch clauses, opt followed by finally clause.
    'switch', // stSwitchStatement, //Expr followed by list of case groups.
    ''); // stListMax); //Have this so always remember to set it up

  TJSExprStrings: array [TJSExprType] of string = ('', // etInvalid,

    '', // etAtom, //No children.
    '<simple expr>',
    // etSimpleExpression, //true, false, null, identifiers etc.
    'default', // etDefaultCase, //default cause label in a switch statement.
    '', // etAtomMax,

    '', // etSingle,
    'var', // etVarDecl, //Single child is variable declaration list.
    'new', // etNewOperator, //Single child is obj or typeref, args not included.
    'function', // etFunctionExpression, //Single child is function statement
    '<unary operator>',
    // etUnaryOperator, //Single child is expression, new, delete, inc, dec etc.
    '', // etSingleMax,

    '', // etPair,  //Two children
    'var .. in ..', // etVarInDecl, //expression is "for var x in y"
    'in', // etInOperator, //expression is x in y.
    ',', // etCommaOperator, //statements separated by comma.
    '=', // etAssignOperator, //two children, left = right.
    '<multi assign op>',
    // etMultiAssignOperator, //compound assignments of various types.
    '<logical op>', // etLogicalOperator,   //logical or, and
    '<bitwise op>', // etBitwiseOperator,   //bitwise or, and, xor
    '<eq test op>', // etEqualityTestOperator, //equals not equals and variants.
    '<relational op>', // etRelationalOperator, //greater, lesser, and variants.
    '<shift op>', // etShiftOperator, //shl, shr, unsshr
    '<arith op>', // etArithOperator, //plus minus times and divide
    '<call>()',
    // etCallOperator, //first child called object or new, second child arg list.
    '<access>[]', // etArrayAccessOperator,
    '<member>.', // etNamedMemberOperator,
    '<field>',
    // etObjectFieldDecl, //first child field id, second child val expr.
    '', // etPairMax,

    '', // etTriple,
    'x?y:z', // etConditionalOperator, //ternary: x ? y : z
    '' // etTripleMax
    );

  TJSLogicalOpStrings: array [TJSLogicalOpType] of string = ('||', '&&');

  TJSBitwiseOpStrings: array [TJSBitwiseOpType] of string = ('|', '&', '^');

  TJSAssignmentStrings: array [TJSAssignmentType] of string = ('', // atInvalid,
    '*=', // atMulAssign, // *=
    '/=', // atDivAssign, // /=
    '%=', // atModAssign, // %=
    '+=', // atAddAssign, // +=
    '-=', // atSubAssign, // -=
    '<<=', // atShlAssign, // <<=
    '>>=', // atShrAssign, // >>=
    '>>>=', // atUnsShrAssign, // >>>=
    '&=', // atBitAndAssign, // &=
    '|=', // atBitOrAssign, // |=
    '^=' // atBitXorAssign
    );

  TJSEqualityTestStrings: array [TJSEqualityTestType] of string = ('',
    // eqtInvalid,
    '==', // etEqual,
    '!=', // etNotEqual,
    '===', // etStrictlyEqual,
    '!==' // etNotStrictlyEqual
    );

  TJSRelationalTestStrings: array [TJSRelationalTestType] of string = ('',
    // rtInvalid,
    '<', // rtLessThan,
    '>', // rtGreaterThan,
    '<=', // rtLessThanOrEqual,
    '>=', // rtGreaterThanOrEqual,
    'instanceof', // rtInstanceOf,
    'in' // rtIn);
    );

  TJSShiftStrings: array [TJSShiftType] of string = ('', // sstInvalid,
    '<<', // stLeft,
    '>>', // stRight,
    '>>>' // stUnsRight);
    );

  TJSArithStrings: array [TJSArithType] of string = ('', // aatInvalid,
    '+', // atAdd,
    '-', // atSub,
    '*', // atMul,
    '/', // atDiv,
    '%' // atMod);
    );

  TJSUnaryStrings: array [TJSUnaryType] of string = ('', // utInvalid,
    'delete', // utDelete,
    'void', // utVoid,
    'typeof', // utTypeof,
    '++(pre)', // utPreInc,
    '--(pre)', // utPreDec,
    '+', // utPlus,
    '-', // utMinus,
    '~', // utBitNot,
    '!', // utLogNot,
    '(post)++', // utPostInc,
    '(post)--' // utPostDec
    );

  TJSSimpleExprStrings: array [TJSSimpleExprType] of string = ('',
    // setInvalid,
    'this', // setThis,
    'null', // setNull,
    'false', // setFalse,
    'true', // setTrue,
    '<number>', // setNumber,
    '<string>', // setStringLiteral, //is TJSStringSimpleExpr
    '<regexp>', // setRegExp, //is TJSStringSimpleExpr
    '<identifier>',
    // setIdentifier, //is TJSIdentifier //TODO - check set of all identifier construct instances.
    '[]<literal>', // setArrayLit,  //child is TExprList.
    '{}<literal>' // setObjLit    //child is TExprList.
    );

  TJSExprListStrings: array [TJSExprListType] of string = ('', // eltInvalid,
    '<var decls>', // eltVarDecls,
    '<formal params>', // eltFormalParameters,
    '<for initializers>', // eltForInitializers,
    '<arguments>', // eltArguments,
    '<array elems>', // eltArrayElems,
    '<obj fields>' // eltObjectFields
    );

  TJSTagGroupStrings: array [TJSTagGroupType] of string = ('', // ttgInvalid,
    '<case grp>', // ttgCaseGroup,
    '<catch block>' // ttgCatchBlock);
    );

  TJSBlockStrings: array [TJSBlockType] of string = ('', // btInvalid
    '{}<top>', // btTopOrFunction,
    '{}<block>', // btExplicit,
    '{}<try>', // btTry,
    '{}<catch>', // btCatch,
    '{}<finally>', // btFinally,
    '{}<case>' // btCaseGroup);
    );

  { General procedures and functions }

procedure TrimMatchExecute(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
var
  Bail: TTrimSearchBailout;
begin
  Bail := Ref1 as TTrimSearchBailout;
  case Bail.SearchTo of
    jttStatement:
      begin
        // For the moment, allow single expression statements, not blocks,
        // or other compound statements.
        if (Node is TJSStatement) and
          ((Node as TJSStatement).StatType > stSingleExpr) and
          ((Node as TJSStatement).StatType < stSingleExprMax) then
          Bail.FoundNode := Node;
      end;
    jttExpression:
      begin
        if (Node is TJSExpr) or (Node is TJSExprList) then
          Bail.FoundNode := Node;
      end;
    jttObjectField:
      begin
        if (Node is TJSExpr) and ((Node as TJSExpr).ExprType = etObjectFieldDecl)
        then
          Bail.FoundNode := Node;
      end
  else
    Assert(false);
  end;
end;

function TrimSearchTree(Node: TJSNode; TrimTo: TJSSearchType): TJSNode;
var
  Bail: TTrimSearchBailout;
begin
  Bail := TTrimSearchBailout.Create;
  try
    Bail.SearchTo := TrimTo;
    Node.IterateNodes(TreeMatchDecision, TrimMatchExecute, TreeMatchRecurse,
      Bail, nil, nil);
    result := Bail.FoundNode as TJSNode;
  finally
    Bail.Free;
  end;
  if result = Node then
    exit // no trim.
  else if not Assigned(result) then
    result := Node
  else
  begin
    DLListRemoveObj(@result.SiblingListEntry);
    result.FContainerNode := nil;
    Node.Free; // Will not percolate down to result.
  end;
end;

{ TJSScript }

function TJSScript.AsText;
begin
  result := '<script>';
end;

{ TJSNode }

constructor TJSNode.Create;
begin
  inherited;
  FNodeType := NodeTypeJS;
end;

constructor TJSNode.CreateWithTracker(Tracker: TTracker);
begin
  inherited;
  FNodeType := NodeTypeJS;
end;

{ TJSSTatement }

function TJSStatement.AsText;
begin
  result := TJSSTatementStrings[FStatType];
end;

function TJSStatement.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and ((Other as TJSStatement).FStatType = Self.FStatType);
end;

{ TJSSwitchStatement }

function TJSSwitchStatement.CheckNoDupDefaults: boolean;
begin
  result := true;
end;

{ TJSFunction }

function TJSFunction.AsText;
begin
  result := '<function>';
end;

function TJSFunction.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and ((Other as TJSFunction).FFuncName = Self.FFuncName);
end;

{ TJSString }

procedure TJSString.FixStrings;
begin
  UnescapeJavascriptString(FStrData);
  inherited;
end;

{ TJSIdentifier }

{ TJSExpr }

function TJSExpr.AsText;
begin
  result := TJSExprStrings[FExprType];
end;

function TJSExpr.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and ((Other as TJSExpr).FExprType = Self.FExprType);
end;

function TJSExpr.RecSubset(Other: TCommonNode; Opts: TNodeSearchOpts): boolean;
var
  JOpts: TJSSearchOpts;
  Name, OName: TJSExpr;
  Val, OVal: TJSExpr;
  SVal, OSVal: TJSSimpleExpr;
begin
{$IFDEF DEBUG_SEARCH}
  GLogLog(SV_TRACE, 'JSExpr rec subset: ' + Self.AsText + ' ' + Other.AsText);
{$ENDIF}
  if LclEqual(Other, Opts) and (ExprType = etObjectFieldDecl) and Assigned(Opts)
    and (Opts is TJSSearchOpts) and
    ((Opts as TJSSearchOpts).SearchType = jttObjectField) then
  begin
{$IFDEF DEBUG_SEARCH}
    GLogLog(SV_TRACE, 'JSExpr rec subset, comparing object fields.');
{$ENDIF}
    // Don't recursively compare down, only compare field names, and (some) basic
    // field types, and ignore values.
    Name := FContainedListHead.FLink.Owner as TJSExpr;
    OName := Other.ContainedListHead.FLink.Owner as TJSExpr;
{$IFDEF DEBUG_SEARCH}
    GLogLog(SV_TRACE, 'JSExpr rec subset, compare field names subset');
{$ENDIF}
    result := Name.RecSubset(OName, Opts);
{$IFDEF DEBUG_SEARCH}
    GLogLog(SV_TRACE, 'JSExpr field names subset done');
{$ENDIF}
    if result then
    begin
{$IFDEF DEBUG_SEARCH}
      GLogLog(SV_TRACE, 'JSExpr rec subset, field names match.');
{$ENDIF}
      Val := FContainedListHead.BLink.Owner as TJSExpr;
      OVal := Other.ContainedListHead.BLink.Owner as TJSExpr;
{$IFDEF DEBUG_SEARCH}
      GLogLog(SV_TRACE, 'JSExpr rec subset, field values both simple expr?.');
{$ENDIF}
      result := result and (Val is TJSSimpleExpr) and (OVal is TJSSimpleExpr);
      if result then
      begin
{$IFDEF DEBUG_SEARCH}
        GLogLog(SV_TRACE, 'JSExpr rec subset, compare value types.');
{$ENDIF}
        SVal := Val as TJSSimpleExpr;
        OSVal := OVal as TJSSimpleExpr;
        JOpts := (Opts as TJSSearchOpts);
        case JOpts.FieldType of
          setInvalid:
            ; // Don't care about field type.
          // Don't care about field values, but field types matter.
          setThis, setNull, setIdentifier:
            begin
              Assert((SVal.SimpleExprType = setThis) or
                (SVal.SimpleExprType = setNull) or
                (SVal.SimpleExprType = setIdentifier));
              result := (OSVal.SimpleExprType = setThis) or
                (OSVal.SimpleExprType = setNull) or
                (OSVal.SimpleExprType = setIdentifier);
            end;
          setFalse, setTrue:
            begin
              Assert((SVal.SimpleExprType = setFalse) or
                (SVal.SimpleExprType = setTrue));
              result := (OSVal.SimpleExprType = setFalse) or
                (OSVal.SimpleExprType = setTrue);
            end;
          setNumber, setStringLiteral, setRegExp, setArrayLit, setObjLit:
            begin
              Assert(SVal.SimpleExprType = JOpts.FieldType);
              result := OSVal.SimpleExprType = JOpts.FieldType;
            end;
        else
          Assert(false);
        end;
      end;
    end;
  end
  else
    result := inherited;
{$IFDEF DEBUG_SEARCH}
  GLogLog(SV_TRACE, 'JSExpr rec subset end.');
{$ENDIF}
end;

{ TJSExprList }

function TJSExprList.AsText;
begin
  result := TJSExprListStrings[FListType];
end;

function TJSExprList.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and ((Other as TJSExprList).FListType = Self.FListType);
end;

{ TJSLogicalOperator }

function TJSLogicalOperator.AsText;
begin
  result := TJSLogicalOpStrings[FLogicalType];
end;

function TJSLogicalOperator.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and
    ((Other as TJSLogicalOperator).FLogicalType = Self.FLogicalType);
end;

{ TJSBitwiseOperator }

function TJSBitwiseOperator.AsText;
begin
  result := TJSBitwiseOpStrings[FBitwiseType];
end;

function TJSBitwiseOperator.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and
    ((Other as TJSBitwiseOperator).FBitwiseType = Self.FBitwiseType);
end;

{ TJSMultiAssign }

function TJSMultiAssign.AsText;
begin
  result := TJSAssignmentStrings[FAssignmentType];
end;

function TJSMultiAssign.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and
    ((Other as TJSMultiAssign).FAssignmentType = Self.FAssignmentType);
end;

{ TJSEqualityTest }

function TJSEqualityTest.AsText;
begin
  result := TJSEqualityTestStrings[FEqualityTestType];
end;

function TJSEqualityTest.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and
    ((Other as TJSEqualityTest).FEqualityTestType = Self.FEqualityTestType);
end;

{ TJSRelationalTest }

function TJSRelationalTest.AsText;
begin
  result := TJSRelationalTestStrings[FRelationalTestType];
end;

function TJSRelationalTest.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and
    ((Other as TJSRelationalTest).FRelationalTestType = Self.
    FRelationalTestType);
end;

{ TJSShiftOperator }

function TJSShiftOperator.AsText;
begin
  result := TJSShiftStrings[FShiftType];
end;

function TJSShiftOperator.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and
    ((Other as TJSShiftOperator).FShiftType = Self.FShiftType);
end;

{ TJSArithOperator }

function TJSArithOperator.AsText;
begin
  result := TJSArithStrings[FArithType];
end;

function TJSArithOperator.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and
    ((Other as TJSArithOperator).FArithType = Self.FArithType);
end;

{ TJSUnaryOperator }

function TJSUnaryOperator.AsText;
begin
  result := TJSUnaryStrings[FUnaryType];
end;

function TJSUnaryOperator.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and
    ((Other as TJSUnaryOperator).FUnaryType = Self.FUnaryType);
end;

{ TJSSimpleExpr }

function TJSSimpleExpr.AsText;
begin
  result := TJSSimpleExprStrings[FSimpleExprType];
end;

function TJSSimpleExpr.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and
    ((Other as TJSSimpleExpr).FSimpleExprType = Self.FSimpleExprType);
end;

{ TJSNumber }

function TJSNumber.AsText;
begin
  result := FStringRep;
end;

function TJSNumber.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and ((Other as TJSNumber).FStringRep = Self.FStringRep);
end;

{ TJSString }

function TJSString.AsText;
begin
  result := FStrData;
end;

function TJSString.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and ((Other as TJSString).FStrData = Self.FStrData);
end;

{ TJSIdentifier }

function TJSIdentifier.AsText;
begin
  result := FName;
end;

function TJSIdentifier.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and ((Other as TJSIdentifier).FName = Self.FName);
end;

{ TJSTagGroup }

function TJSTagGroup.AsText;
begin
  result := TJSTagGroupStrings[FTagGroupType];
end;

function TJSTagGroup.LclEqual(Other: TCommonNode;
  Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and
    ((Other as TJSTagGroup).FTagGroupType = Self.FTagGroupType);
end;

{ TJSBlock }

function TJSBlock.AsText;
begin
  result := TJSBlockStrings[FBlockType];
end;

function TJSBlock.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts): boolean;
begin
  result := inherited and ((Other as TJSBlock).FBlockType = Self.FBlockType);
end;

{ TJSNavHelper }

function TJSNavHelper.CreateParser: TCoCoRGrammar;
begin
  result := TJScriptGrammar.Create(nil);
  with result as TJScriptGrammar do
  begin
    ClearSourceStream := false;
    GenListWhen := glNever;
    ParentObject := Self;
  end;
end;

function TJSNavHelper.CreateOpts: TNodeSearchOpts;
begin
  result := TJSSearchOpts.Create;
end;

procedure TJSNavHelper.PostParse(var ParseResult: TCommonNode);
begin
  with ParseResult as TJSScript do
  begin
    FixupChildContainedPtrsRec;
    IterateNodes(ExecuteAlways, FixNodeStrings, RecurseAlways, Self, nil, nil);
  end;
  ParseResult := TrimSearchTree(ParseResult as TJSNode, (Opts as TJSSearchOpts)
    .SearchType);
end;

function TJSNavHelper.GetTreeFragmentForString(Fragment: string): TCommonNode;
var
  SubFragment: string;
begin
  case ((Opts as TJSSearchOpts).SearchType) of
    jttStatement:
      ;
    jttExpression:
      Fragment := 'throw ' + Fragment;
    jttObjectField:
      begin
        SubFragment := 'null';
        case ((Opts as TJSSearchOpts).FFieldType) of
          setInvalid, setNull:
            ;
          setThis:
            SubFragment := 'this';
          setFalse:
            SubFragment := 'false';
          setTrue:
            SubFragment := 'true';
          setNumber:
            SubFragment := '0';
          setStringLiteral:
            SubFragment := '""';
          setRegExp:
            SubFragment := '//';
          setIdentifier:
            SubFragment := 'identifier';
          setArrayLit:
            SubFragment := '[]';
          setObjLit:
            SubFragment := '{}';
        end;
        Fragment := 'throw { "' + Fragment + '" : ' + SubFragment + '}'
      end;
  end;
  result := inherited GetTreeFragmentForString(Fragment);
end;

function TJSNavHelper.FindChild(var Node: TJSNode; Fragment: string;
  FragType: TJSSearchType; FieldType: TJSSimpleExprType): boolean;
begin
  (Opts as TJSSearchOpts).SearchType := FragType;
  (Opts as TJSSearchOpts).FieldType := FieldType;
  result := FindChildS(TCommonNode(Node), Fragment);
end;

function TJSNavHelper.FindNext(var Node: TJSNode; Fragment: string;
  FragType: TJSSearchType; FieldType: TJSSimpleExprType): boolean;
begin
  (Opts as TJSSearchOpts).SearchType := FragType;
  (Opts as TJSSearchOpts).FieldType := FieldType;
  result := FindNextS(TCommonNode(Node), Fragment);
end;

function TJSNavHelper.FindChildRec(var Node: TJSNode; Fragment: string;
  FragType: TJSSearchType; FieldType: TJSSimpleExprType): boolean;
begin
  (Opts as TJSSearchOpts).SearchType := FragType;
  (Opts as TJSSearchOpts).FieldType := FieldType;
  result := FindChildRecS(TCommonNode(Node), Fragment);
end;

function TJSNavHelper.PushChild(var Node: TJSNode; Fragment: string;
  FragType: TJSSearchType; FieldType: TJSSimpleExprType): boolean;
begin
  (Opts as TJSSearchOpts).SearchType := FragType;
  (Opts as TJSSearchOpts).FieldType := FieldType;
  result := PushChildS(TCommonNode(Node), Fragment);
end;

function TJSNavHelper.PushChildRec(var Node: TJSNode; Fragment: string;
  FragType: TJSSearchType; FieldType: TJSSimpleExprType): boolean;
begin
  (Opts as TJSSearchOpts).SearchType := FragType;
  (Opts as TJSSearchOpts).FieldType := FieldType;
  result := PushChildRecS(TCommonNode(Node), Fragment);
end;

procedure TJSNavHelper.Pop(var Node: TJSNode);
begin
  PopN(TCommonNode(Node));
end;

function TJSNavHelper.NavToFieldValue(var Node: TJSNode;
  DoPush: boolean): boolean;
begin
  result := false;
  if ((Node is TJSExpr) and ((Node as TJSExpr).ExprType = etObjectFieldDecl))
  then
  begin
    if DoPush then
      Push(Node);
    Node := Node.ContainedListHead.BLink.Owner as TJSNode;
    result := true;
  end;
end;

function TJSNavHelper.NavToFieldList(var Node: TJSNode;
  DoPush: boolean): boolean;
begin
  result := false;
  if (Node is TJSExpr) and ((Node as TJSExpr).ExprType = etSimpleExpression) and
    ((Node as TJSSimpleExpr).SimpleExprType = setObjLit) then
  begin
    if DoPush then
      Push(Node);
    Node := Node.ContainedListHead.FLink.Owner as TJSNode;
    result := true;
  end;
end;

function TJSNavHelper.NavToArrayElems(var Node: TJSNode;
  DoPush: boolean): boolean;
begin
  result := false;
  if (Node is TJSExpr) and ((Node as TJSExpr).ExprType = etSimpleExpression) and
    ((Node as TJSSimpleExpr).SimpleExprType = setArrayLit) then
  begin
    if DoPush then
      Push(Node);
    Node := Node.ContainedListHead.FLink.Owner as TJSNode;
    result := true;
  end;
end;

function TJSNavHelper.NavChildToFieldValue(var Node: TJSNode; Fragment: string;
  FieldType: TJSSimpleExprType; DoPush: boolean): boolean;
var
  NNode: TJSNode;
begin
  result := false;
  NNode := Node;
  if FindChild(NNode, Fragment, jttObjectField, FieldType) then
  begin
    if NavToFieldValue(NNode, false) then
    begin
      if DoPush then
        Push(Node);
      Node := NNode;
      result := true
    end;
  end;
end;

function TJSNavHelper.NavChildToChildFieldList(var Node: TJSNode;
  Fragment: string; DoPush: boolean): boolean;
var
  NNode: TJSNode;
begin
  result := false;
  NNode := Node;
  if NavChildToFieldValue(NNode, Fragment, setObjLit, false) then
  begin
    if NavToFieldList(NNode, false) then
    begin
      if DoPush then
        Push(Node);
      Node := NNode;
      result := true
    end;
  end;
end;

function TJSNavHelper.NavAssignmentToRHS(var Node: TJSNode;
  DoPush: boolean): boolean;
var
  NNode: TJSNode;
begin
  result := false;
  NNode := Node;
  if (NNode is TJSStatement) and
    ((NNode as TJSStatement).StatType = stExpressionStatement) then
  begin
    NNode := NNode.ContainedListHead.FLink.Owner as TJSNode;
    if (NNode is TJSExpr) and (((NNode as TJSExpr).ExprType = etAssignOperator)
      or ((NNode as TJSExpr).ExprType = etMultiAssignOperator)) then
    begin
      if DoPush then
        Push(Node);
      Node := NNode.ContainedListHead.BLink.Owner as TJSNode;
      result := true;
    end;
  end;
end;

end.
