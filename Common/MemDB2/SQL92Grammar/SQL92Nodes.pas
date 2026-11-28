unit SQL92Nodes;

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

interface

uses CommonNodes, SysUtils, Trackables;

{ N.B All this written with "bare minimum necessary" at the moment.
  I don't yet know how the various subsystems / structures will pan out,
  so, I'm just trying to keep things with a minimum of compexity to start
  with. i.e. we have enumerated types for "what sort of", when we
  expect class overrides / specialisation may handle that later on. }

type

{
  TODO - Undefined in type enumerations.
  ValidateAST checks these initially.

  TODO - Set type fields in parent classes in constructors?

  TODO - Query the AI about uses of lists:
   - Where flattened, where not.
   - Where specific list class, where not.

   Need general points:
   - Arbitrary list lengths not flattened.
   - Discrete number of args or clauses flattened.
   - Review where specific list classes are used,
     whether necessary, perhaps we could use enumerators in the containing
     classes.

   TODO - For things which are not List, Ident, Expr
     - Put them under TSqlSynStructural, and have fields init.

For the semantic phase. The parser currently accepts all of these, and they
should be rejected:

(7) NULL and DEFAULT anywhere in an expression.
(8) * in any set function, e.g. SUM(*). COUNT() also parses, because
    set_quantifier_args still begins with |, which gives it an empty first
    alternative. Deleting that | is a one-line fix if you want it now.
(9) COALESCE with only one argument.
(10) Four-part table names.
(11) A row value such as (a, b) on the left of LIKE.
(12) Row subqueries. These parse as scalar subqueries, so a subquery in a
     row position needs to be treated as a row subquery.

  New: attributes written after an ignored CHECK constraint attach to the
  constraint before it. That goes away once CHECK builds a node, as part
  of your nil TODO.
}

  //TODO - We have expr types,
  // structural types etc - also need a node base type.

  //Which direct descendant of TSQLSynNode a node is: set by the constructor,
  //from the class. Each of those classes has its own type field telling its
  //descendants apart (ExprType, StructuralType, then LitType, PredicateType
  //etc. further down), so a node's class can be found by following the
  //type fields down from here.
  TSQLSynNodeType = (
    sntNode,              //TSQLSynNode itself. Should not occur.
    sntList,              //TSqlSynList
    sntExpr,              //TSQLSynExpr
    sntIdent,             //TSQLSynIdent
    sntType,              //TSQLSynType
    sntStructural         //TSQLSynStructural
  );

  TSQLSynNode = class(TCommonNode)
  private
    FNodeType: TSQLSynNodeType;
    procedure InitNodeType;
  public
    constructor Create; virtual;
    constructor CreateWithTracker(Tracker: TTracker); virtual;
    destructor Destroy; override;

    function FirstChild: TSQLSynNode; inline;
    function LastChild: TSQLSynNode; inline;
    function NextSibling: TSQLSynNode; inline;
    function PrevSibling: TSQLSynNode; inline;

    procedure InsertHeadChild(Dest: TSQLSynNode);
    procedure InsertTailChild(Dest: TSQLSynNode);

    procedure InsertAfterSibling(Dest: TSQLSynNode);
    procedure InsertBeforeSibling(Dest: TSQLSynNode);

    procedure RemoveFromTree;

    procedure ValidateAST;
    procedure ValidateSelf; virtual;

    function Clone: TSQLSynNode; virtual; //TODO - Write and override this.

    property NodeType: TSQLSynNodeType read FNodeType;
  end;

  TSQLSynNodeClass = class of TSQLSynNode;

  //An ordered list of child nodes.
  //Rely on tracking and assertions to make sure this is not misused.
  TSqlSynList = class(TSqlSynNode)
  public
    //TODO.
  end;

  ESQLSynException = class(Exception)
  private
    FN1, FN2: TSQLSynNode;
  public
    constructor CreateCtxt(const S:string; N1, N2: TSQLSynNode);
    property N1: TSQLSynNode read FN1;
    property N2: TSQLSynNode read FN2;
  end;

  //These not strictly done by type, i.e.
  //and, or, etc ops might be applicable to more than one datatype,
  //and these are based around parser classifications, not
  //semantic classifications.
  TSqlSynExprType = (
    setLiteral,
    setIdentWrapper,
    setUnaryNot,
    setUnaryPostfixIntervalQualifier,
    //todo Unary postfix timezones, and Unary postfix collations.
    setUnaryPlus,
    setUnaryMinus,
    setBinaryOr,
    setBinaryAnd,
    setBinaryTest,
    setBinaryMul,
    setBinaryDiv,
    setBinaryAdd,
    setBinarySub,
    setBinaryCat,
    setPredicate,
    setRValConstructor,
    setTValConstructor,
    setSetFunction,
    setParameter,
    setQueryUnion,
    setQueryExcept,
    setQueryIntersect,
    setQuerySelect,
    setDirectQueryWrapper,
    setCorrelation,
    setJoin,
    setExplicitTable,
    setCaseAbbr,
    setSimpleSearchedCase,
    setCast,
    setNumericFunction,
    setCharacterFunction,
    setSpecification
  );

  TSQLSynExpr = class(TSqlSynNode)
  private
    FExprType: TSqlSynExprType;
  public
    property ExprType: TSqlSynExprType read FExprType write FExprType;
  end;

  TSqlSynIdent = class;

  TSqlSynIdentWrapper = class(TSqlSynExpr)
  private
    FIdent: TSqlSynIdent;
  public
    property Ident: TSqlSynIdent read FIdent write FIdent;
  end;

  TSQLSynLiteralType = (
    //Basic, simple string represented literals.
    sltUnsInt,
    sltInt,
    sltExactNumeric,
    sltApproxNumeric,
    sltSignedExactNumeric,
    sltSignedApproxNumeric,
    sltNatString,
    sltString,
    sltBitString,
    sltHexString,
    sltIntervalString,
    sltDate,
    sltTime,
    sltTimestamp,
    //Compound literals
    sltInterval,
    sltBuiltIn, //Need to separate "as evaluated" type from "parser type".
    sltTruthValue
  );

  TSQLSynLiteral = class(TSQlSynExpr)
  private
    FText: string;
    FLitType: TSQLSynLiteralType;
  public
    property Text: string read FText write FText;
    property LitType: TSQLSynLiteralType read FLitType write FLitType;
  end;

  TSQLSynIntervalStringType = (
    istPlainInt,
    istYearMonth,
    istDayTime1,
    istDayTime2,
    istDayTime3,
    istTime1,
    istTime2,
    istTime3);

  TSQLSynIntervalStringLiteral = class(TSQLSynLiteral)
  private
    FIntervalStringType: TSQLSynIntervalStringType;
  public
    property IntervalStringType: TSQLSynIntervalStringType
      read FIntervalStringType write FIntervalStringType;
  end;

  TSQLSynIntervalQualifier = class;

  TSQLSynIntervalLiteral = class(TSQLSynLiteral)
  private
    FInterval: TSQLSynIntervalStringLiteral;
    FQualifier: TSQLSynIntervalQualifier;
    FNegated: boolean;
  public
    property Negated: boolean read FNegated write FNegated;
    property Interval: TSQLSynIntervalStringLiteral read FInterval write FInterval;
    property Qualifier: TSQLSynIntervalQualifier read FQualifier write FQualifier;
  end;

  TSQLSynIdent = class(TSqlSynNode)
  private
    FIdentName: string;
    FWildcard: boolean;
    FLocalName: boolean;
  public
    property IdentName: string read FIdentName write FIdentName;
    property Wildcard: boolean read FWildcard write FWildCard;
    property LocalName: boolean read FLocalName write FLocalName;
  end;

  //Expect some level of coercion allowed between literal types, and
  //general types, but we'll get there in a bit.
  //This is what you'd expect a basic language type to be.

  TSQLGeneralType = (
    sgtExactNumeric,
    sgtApproxNumeric,
    sgtNatString,
    sgtString,
    sgtBitString,
    sgtHexString,
    sgtDate,
    sgtTime,
    sgtTimestamp,
    sgtInterval,
    sgtRelation, { Returns something relation-like.
                   Perhaps return for both rVal constructors as tVal constructors? }
    sgtTruthValue,
    sgtDomain    { With further type checking either by name or structure... }
  );

  TSQLSynType = class(TSqlSynNode)
  private
    FGeneralType: TSQLGeneralType;
  public
    property GeneralType: TSQLGeneralType read FGeneralType write FGeneralType;
  end;

  TSQLSynIntervalType = class(TSQLSynType)
  private
    FQualifier: TSQLSynIntervalQualifier;
  public
    property Qualifier: TSQLSynIntervalQualifier read FQualifier write FQualifier;
  end;

  TSQLSynQualField = (
    sqfSecond,
    sqfMinute,
    sqfHour,
    sqfDay,
    sqfMonth,
    sqfYear
  );

  { Built in functions / constants which are effectively exprs. }
  TSQLSynBuiltInType = (
    sftCurrentDate,
    sftCurrentTime,
    sftCurrentTimestamp,
    sftUser,
    sftCurrentUser,
    sftSessionUser,
    sftSystemUser,
    sftNull,
    sftValue
  );

  TSqlSynBuiltin = class(TSqlSynLiteral)
  private
    FBuiltInType: TSqlSynBuiltinType;
  public
    property BuiltInType: TSQLSynBuiltinType read FBuiltInType write FBuiltInType;
  end;

  //TODO - I'm going to worry about identifier resolution
  //and namespaces a bit later, because there are many and varied ways of introducing them.
  TSQLSynStructuralType = (
    sstModule,
    sstModuleAuth,
    sstTopLevel,
    sstCreateOrDecl,
    sstAlter,
    sstDrop,
      //Whereyou're actually creating or declaring underlying objects in the DB,
      //not just SQL syntactical / semantic constructs.
    sstColDef,
    sstConstraint,
    sstConstraintDetail,
    sstRefAction,
    sstConstraintAttributes,
    sstDerivedColumn,
    sstJoinSpec,
    sstSortSpecification,
    sstUpdatability,
    sstProcedure,
    sstParameterDecl,
    sstStatusParamDecl,
    sstMiscStatement,
    sstPrivilege,
    //TODO - Prob make drop its own sst not a misc statement.
    sstAlterTableAction,
    sstAlterDomainAction,
    sstDmlWithCursor,
    sstDmlNoCursor,
    sstFetchOrientation,
    sstSetClause,
    sstTransactionMode,
    sstSimpleCaseWhen,
    sstTrimSpecification,
    sstIntervalQualifier,
    sstSelectList
  );

  TSQLSynStructural = class(TSQLSynNode)
  private
    FStructuralType: TSQLSynStructuralType;
  public
    property StructuralType:TSQLSynStructuralType read FStructuralType write FStructuralType;
  end;

  TSQLSynIntervalQualifier = class(TSQLSynStructural)
  private
    FStart, FEnd: TSQLSynQualField;
  public
    property Start: TSQLSynQualField read FStart write FStart;
    property _End: TSQLSynQualField read FEnd write FEnd;
  end;

  TSQLSynNamedStructural = class(TSQLSynStructural)
  private
    FName: TSqlSynIdent;
    //TODO - First and last pointers for contained list of stuff?
  public
    property Name:TSqlSynIdent read FName write FName;
  end;


  TSqlSynCreateOrDeclTable = class;

  TSqlSynTopLevel = class(TSQLSynStructural)
  end;

  TSqlSynModuleAuth = class;

  TSQLSynModule = class(TSQLSynNamedStructural)
  private
    FLanguage: TSQLSynIdent;
    FAuthClause: TSqlSynModuleAuth;
    FTempTables: TSqlSynList;
    FContents: TSqlSynList;
  public
    property Language: TSqlSynIdent read FLanguage write FLanguage;
    property AuthClause: TSqlSynModuleAuth read FAuthClause write FAuthClause;
    property TempTables: TSqlSynList read FTempTables write FTempTables;
    property Contents: TSqlSynList read FContents write FContents;
  end;

  TSqlSynModuleAuth = class(TSqlSynNamedStructural)
  private
    FSchema: TSQLSynIdent;
    FAuth: TSQLSynIdent;
  public
    property Schema: TSQLSynIdent read FSchema write FSchema;
    property Auth: TSQLSynIdent read FAuth write FAuth;
  end;

  TSQLSynCreateOrDeclType = (
    codTable,
    codCursor,
    codSchema,
    codDomain,
    codView,
    codAssertion
  ) ;

  TSQLSynCreateOrDecl = class(TSQLSynNamedStructural)
  private
    FType: TSQlSynCreateOrDeclType;
  public
    property CreateOrDeclType: TSQlSynCreateOrDeclType read FType write FType;
  end;

  TSqlSynCreateOrDeclTableOpts = (
    dtoNone,
    dtoGlobalTemporary,
    dtoLocalTemporary
  );

  TSqlSynRowCommitAction = (
    rcaUnspecified,
    rcaCommitRows,
    rcaDeleteRows
  );

  TSqlSynColDef = class;
  TSqlSynConstraint = class;

  TSqlSynCreateOrDeclTable = class(TSQLSynCreateOrDecl)
  private
    FTempOpts: TSqlSynCreateOrDeclTableOpts;
    FElementList: TSqlSynList;
    FRowCommitAction: TSqlSynRowCommitAction;
  public
    property TempOpts: TSqlSynCreateOrDeclTableOpts read FTempOpts write FTempOpts;
    property ElementList: TSqlSynList read FElementList write FElementList;
    property RowCommitAction: TSqlSynRowCommitAction
      read FRowCommitAction write FRowCommitAction;
  end;

  TSqlSynColDef = class(TSQLSynNamedStructural)
  private
    FDataType: TSqlSynNode; //Might be datatype or domain ...
    FDefault: TSqlSynLiteral;
    FConstraints: TSqlSynList;
  public
    property Datatype: TSqlSynNode read FDataType write FDataType;
    property _Default: TSqlSynLiteral read FDefault write FDefault;
    property Constraints: TSqlSynList read FConstraints write FConstraints;
  end;

  { The idea here is we'll post process column constraints into table
    constraints after parse ...}
  TSqlSynConstraintType = (
    sctColumn,
    sctTable,
    sctDomain);

  TSqlSynConstraintDetail = class;
  TSqlSynConstraintAttributes = class;

  TSqlSynConstraint = class(TSQLSynNamedStructural)
  private
    FConstraintType: TSqlSynConstraintType;
    FDetail: TSqlSynConstraintDetail;
    FAttributes: TSqlSynConstraintAttributes;
  public
    property ConstraintType: TSqlSynConstraintType read FConstraintType write FConstraintType;
    property Detail: TSqlSynConstraintDetail read FDetail write FDetail;
    property Attributes: TSqlSynConstraintAttributes read FAttributes write FAttributes;
  end;

  TSqlSynTableConstraint = class(TSqlSynConstraint)
  private
    FColumns: TSqlSynList;
  public
    property Columns: TSqlSynList read FColumns write FColumns;
  end;

  TSqlSynConstraintDetailType = (
    cdtNotNull,
    cdtUnique,
    cdtPrimaryKey,
    cdtReferences
    //cdtCheck TODO - omitted for the moment.
    ) ;

  TSqlSynConstraintDetail = class(TSQLSynStructural)
  private
    FDetailType: TSqlSynConstraintDetailType;
  public
    property DetailType: TSqlSynConstraintDetailType read FDetailType write FDetailType;
  end;

  TSqlSynMatchType = (
    mtUnspec,
    mtFull,
    mtPartial
  );

  TSqlSynRefAction = class;

  //This detail used for foreign keys.
  TSqlSynReferencesConstraintDetail = class(TSqlSynConstraintDetail)
  private
    FReferencesTable: TSqlSynIdent;
    FReferencesColumns: TSqlSynList;
    FMatchType: TSqlSynMatchType;
    FRefAction: TSqlSynRefAction;
  public
    property MatchType: TSqlSynMatchType read FMatchType write FMatchType;
    property ReferencesTable: TSqlSynIdent read FReferencesTable write FReferencesTable;
    property ReferencesColumns: TSqlSynList read FReferencesColumns write FReferencesColumns;
    property RefAction: TSqlSynRefAction read FRefAction write FRefAction;
  end;

  TSqlSynDeferrable = (
    ssdUnspec,
    ssdNotDeferrable,
    ssdDeferrable
  );

  TSqlSynInitDeferred = (
    sidUnspec,
    sidNotInitDeferred,
    sidInitDeferred
  );

  TSqlSynConstraintAttributes = class(TSqlSynStructural)
  private
    FDeferrable: TSqlSynDeferrable;
    FInitDeferred: TSqlSynInitDeferred;
  public
    property Deferrable: TSqlSynDeferrable read FDeferrable write FDeferrable;
    property InitDeferred: TSqlSynInitDeferred read FInitDeferred write FInitDeferred;
  end;

  TSqlSynRefDoAction = (
    rdaNone,
    rdaCascade,
    rdaSetNull,
    rdaSetDefault
  );

  TSqlSynRefAction = class(TSqlSynStructural)
  private
    FUpdateAction: TSqlSynRefDoAction;
    FDeleteAction: TSqlSynRefDoAction;
  public
    procedure MergeWith(Other: TSqlSynRefAction);
    property UpdateAction: TSqlSynRefDoAction read FUpdateAction write FUpdateAction;
    property DeleteAction: TSqlSynRefDoAction read FDeleteAction write FDeleteAction;
  end;

  TSqlSynUnaryOperator = class(TSqlSynExpr)
  private
    FExpr: TSqlSynExpr;
  public
    property Expr: TSqlSynExpr read FExpr write FExpr;
  end;

  //Postfix interval qualifier, e.g. (a - b) DAY TO SECOND.
  //Expr is the operand.
  TSqlSynIntervalQualifierOperator = class(TSqlSynUnaryOperator)
  private
    FQualifier: TSQLSynIntervalQualifier;
  public
    property Qualifier: TSQLSynIntervalQualifier read FQualifier write FQualifier;
  end;

  //Only takes one expr, needs other class for type info
  //either Id (name) or TSqlSynType
  TSqlSynCastOperator = class(TSqlSynUnaryOperator)
  private
    FCastType: TSqlSynNode;
  public
    property CastType: TSqlSynNode read FCastType write FCastType;
  end;

  TSqlSynBinaryOperator = class(TSqlSynExpr)
  private
    FLeft: TSqlSynExpr;
    FRight: TSqlSynExpr;
  public
    property Left: TSqlSynExpr read FLeft write FLeft;
    property Right: TSqlSynExpr read FRight write FRight;
  end;

  TSqlSynCompOp = (
    coEquals,
    coNotEquals,
    coLessThan,
    coGreaterThan,
    coLessThanOrEq,
    coGreaterThanOrEq
  );


  TSqlRowValConstructor = class(TSqlSynExpr)
    //TODO.
  end;

  TSqlTableValConstructor = class(TSqlSynExpr)
    //TODO.
  end;

  TSqlSynParameterSpec = class(TSqlSynExpr)
  private
    FParamName: TSqlSynIdent;
    FIndicator: TSqlSynIdent;
  public
    property ParamName: TSqlSynIdent read FParamName write FParamName;
    property Indicator: TSqlSynIdent read FIndicator write FIndicator;
  end;

  TSqlSynSetFunctionType = (
    ssfAvg,
    ssfMin,
    ssfMax,
    ssfSum,
    ssfCount);

  TSqlSynSetFunctionQuantifier = (
    ssqNone,
    ssqWildCard,
    ssqDistinct,
    ssqAll
  );

  TSqlSynSetFunction = class(TSqlSynExpr)
  private
    FSetFunctionType: TSqlSynSetFunctionType;
    FQuantifier: TSqlSynSetFunctionQuantifier;
    FExpr: TSqlSynExpr;
  public
    property SetFunctionType: TSqlSynSetFunctionType read FSetFunctionType write FSetFunctionType;
    property Quantifier: TSqlSynSetFunctionQuantifier read FQuantifier write FQuantifier;
    property Expr: TSqlSynExpr read FExpr write FExpr;
  end;

  TSqlSynQueryBinary = class(TSqlSynExpr)
  private
    FAll: boolean;
    FLeft: TSqlSynExpr; //TODO - Diff class type for query exprs?
    FRight: TSqlSynExpr;
    FCorresponding: TSqlSynList;
  public
    property Left: TSqlSynExpr read FLeft write FLeft;
    property Right: TSqlSynExpr read FRight write FRight;
    property All: boolean read FAll write FAll;
    property Corresponding: TSqlSynList read FCorresponding write FCorresponding;
  end;

  TSqlSynSelectList = class(TSqlSynStructural)
  private
    FWildcard: boolean;
  public
      property Wildcard: boolean read FWildcard write FWildcard;
  end;

  TSqlSynQuerySelect = class(TSqlSynExpr)
  private
    FQuantifier: TSqlSynSetFunctionQuantifier;
    FSelectList: TSqlSynSelectList;
    //TODO - Class types for these??
    FFrom: TSqlSynList;
    FWhere: TSqlSynExpr;
    FGroupBy: TSqlSynList;
    FHaving: TSqlSynExpr;
  public
    property Quantifier: TSqlSynSetFunctionQuantifier read FQuantifier write FQuantifier;
    property SelectList: TSqlSynSelectList read FSelectList write FSelectList;
    property From: TSqlSynList read FFrom write FFrom;
    property Where: TSqlSynExpr read FWhere write FWhere;
    property GroupBy: TSqlSynList read FGroupBy write FGroupBy;
    property Having: TSqlSynExpr read FHaving write FHaving;
  end;

  TSqlSynDirectQueryWrapper = class(TSqlSynExpr)
  private
    FQuery: TSqlSynExpr;
    FOrderBy: TSqlSynList;
  public
    property Query: TSqlSynExpr read FQuery write FQuery;
    property OrderBy: TSqlSynList read FOrderBy write FOrderBy;
  end;

  TSqlSynDerivedColumn = class(TSqlSynStructural)
  private
    FExpr: TSqlSynExpr;
    FAsClause: TSqlSynIdent;
  public
    property Expr: TSqlSynExpr read FExpr write FExpr;
    property AsClause: TSqlSynIdent read FAsClause write FAsClause;
  end;

  TSqlSynCorrelation = class(TSQLSynExpr)
  private
    FTableLike: TSqlSynExpr;
    FCorrelationName: TSqlSynIdent;
    FColumns: TSqlSynList;
  public
    property TableLike: TSqlSynExpr read FTableLike write FTableLike;
    property CorrelationName: TSqlSynIdent read FCorrelationName write FCorrelationName;
    property Columns: TSqlSynList read FColumns write FColumns;
  end;

  TSqlSynJoinType = (
    jtInner,
    jtLeft,
    jtRight,
    jtFull,
    jtCross,
    jtUnion );

  TSqlSynJoinSpecType = (
    stJoinCond,
    stNamedColsJoin
  );

  TSqlSynJoinSpec = class(TSqlSynStructural)
  private
    FSpecType: TSqlSynJoinSpecType;
    FJoinCondition: TSqlSynExpr;
    FColList: TSqlSynList;
  public
    property SpecType: TSqlSynJoinSpecType read FSpecType write FSpecType;
    property JoinCondition: TSqlSynExpr read FJoinCondition write FJoinCondition;
    property ColList: TSqlSynList read FColList write FColList;
  end;

  TSqlSynJoinBinary = class(TSqlSynExpr)
  private
    FJoinType: TSqlSynJoinType;
    FOuter: boolean;
    FNatural: boolean;
    FLeft: TSqlSynExpr;
    FRight: TSqlSynExpr;
    FSpec: TSqlSynJoinSpec;
  public
    property Left: TSqlSynExpr read FLeft write FLeft;
    property Right: TSqlSynExpr read FRight write FRight;
    property JoinType: TSqlSynJoinType read FJoinType write FJoinType;
    property Outer: boolean read FOuter write FOuter;
    property Natural: boolean read FNatural write FNatural;
    property Spec: TSqlSynJoinSpec read FSpec write FSpec;
  end;

  TSqlSynExplicitTable = class(TSqlSynExpr)
  private
    FTableName: TSqlSynIdent;
  public
    property TableName: TSqlSynIdent read FTableName write FTableName;
  end;

  TSqlSynCaseAbbrType = (
    scetNullif,
    scetCoalesce);

  TSqlSynCaseAbbr = class(TSQlSynExpr)
  private
    FCaseType: TSqlSynCaseAbbrType;
    FFirstExpr, FLastExpr: TSqlSynExpr;
    FList: TSqlSynList;
  public
    property CaseType: TSqlSynCaseAbbrType read FCaseType write FCaseType;
    property FirstExpr: TSqlSynExpr read FFirstExpr write FFirstExpr;
    property LastExpr: TSqlSynExpr read FLastExpr write FLastExpr;
    property List: TSqlSynList read Flist write FList;
  end;

  TSqlSynSimpleSearchedCaseType = (
    sscetSimple,
    sscetSearched
  );

  TSqlSynSimpleSearchedCase = class(TSqlSynExpr)
  private
    FSSCaseType: TSqlSynSimpleSearchedCaseType;
    FOperand: TSqlSynExpr;
    FWhenClauses: TSqlSynList;
    F_Else: TSqlSynExpr;
  public
    property SSCaseType: TSqlSynSimpleSearchedCaseType read FSSCaseType write FSSCaseType;
    property Operand: TSqlSynExpr read FOperand write FOperand;
    property WhenClauses: TSqlSynList read FWhenClauses write FWhenClauses;
    property _Else: TSqlSynExpr read F_Else write F_Else;
  end;

  TSqlSynSimpleCaseWhen = class(TSqlSynStructural)
  private
    FComparatorOrSearched: TSqlSynExpr;
    F_When: TSqlSynExpr;
  public
    property ComparatorOrSearched: TSqlSynExpr
      read FComparatorOrSearched write FComparatorOrSearched;
    property _When: TSqlSynExpr read F_When write F_When;
  end;


  TSqlSynNumericFunctionType = (
    nfPosition,
    nfExtract,
    nfLength
  );

  TSqlSynLengthFuncSubtype = (
    lfsChar,
    lfsOctet,
    lfsBit
  );

  TSQLSynUnaryNumericFunction = class(TSQLSynExpr)
  private
    FFuncType: TSqlSynNumericFunctionType;
    FLengthSubtype: TSqlSynLengthFuncSubtype;
    FExpr: TSqlSynExpr;
  public
    property Expr: TSqlSynExpr read FExpr write FExpr;
    property FuncType: TSqlSynNumericFunctionType read FFuncType write FFuncType;
    property LengthSubtype: TSqlSynLengthFuncSubtype read FLengthSubtype write FLengthSubtype;
  end;

  TSqlSynExtractFunction = class(TSQLSynUnaryNumericFunction)
  private
    FExtractField: TSQLSynQualField;
  public
    property ExtractField: TSQLSynQualField read FExtractField write FExtractField;
  end;

  TSqlSynBinaryNumericFunction = class(TSQLSynUnaryNumericFunction)
  private
    FSecondExpr: TSqlSynExpr;
  public
    property SecondExpr: TSqlSynExpr read FSecondExpr write FSecondExpr;
  end;

  TSqlSynCharFuncType = (
    cfsSubString,
    cfsFold,
    //Form of use conversions not supported, UTF-8 should be good enough.
    //Translations not supported.
    cfsTrim,
    cfsTrimChar,
    cfsTrimSpec
  );

  TSqlSynFoldType = (
    sftUpper, sftLower);

  TSqlSynCharacterFunction = class(TSqlsynExpr)
  private
    FFuncType: TSqlSynCharFuncType;
    FFoldType : TSqlSynFoldType;
    FFirst: TSqlSynNode;
    FSecond, FThird: TSqlSynExpr;
  public
    property FuncType: TSqlSynCharFuncType read FFuncType write FFuncType;
    property FoldType: TSqlSynFoldType read FFoldType write FFoldType;
    property First: TSqlSynNode read FFirst write FFirst;
    property Second: TSqlSynExpr read FSecond write FSecond;
    property Third: TSqlSynExpr read FThird write FThird;
  end;

  TSqlSynSpecificationType = (
    sstNull,
    sstDefault
  );

  TSqlSynSpecification = class(TSqlSynExpr)
  private
    FSpecType: TSqlSynSpecificationType;
  public
    property SpecType: TSqlSynSpecificationType read FSpecType write FSpecType;
  end;

  TSqlSynTrimType = (
    sttLeading,
    sttTrailing,
    sttBoth
  );

  TSqlSynTrimSpecification = class(TSqlSynStructural)
  private
    FSpecType: TSqlSynTrimType;
  public
    property SpecType: TSqlSynTrimType read FSpecType write FSpecType;
  end;

  //TODO - Note down which are 2V, 3V, 4V predicates.
  TSqlSynPredicateType = (
    sptComparisonPredicate,
    sptQuantifiedComparisonPredicate,
    sptBetweenPredicate,
    sptNotBetweenPredicate,
    sptInPredicate,
    sptNotInPredicate,
    sptLikePredicate,
    sptNotLikePredicate,
    sptUniquePredicate,
    sptExistsPredicate,
    sptMatchPredicate,
    sptOverlapspredicate,
    sptNullPredicate,
    sptNotNullPredicate
  );

  TSqlSynPredicate = class(TSqlSynExpr)
  private
    FPredicateType: TSqlSynPredicateType;
  public
    property PredicateType: TSqlSynPredicateType read FPredicateType write FPredicateType;
  end;

  TSqlSyn1VPredicate = class(TSqlSynPredicate)
  private
    FExpr: TSqlSynExpr;
  public
    property Expr: TSqlSynExpr read FExpr write FExpr;
  end;

  TSqlSynInPredicate = class(TSqlSyn1vPredicate)
  private
    FInList: TSqlSynNode; //Can be value list or query expr.
  public
    property InList: TSqlSynNode read FInList write FInList;
  end;

  TSqlSyn2VPredicate = class(TSqlSynPredicate)
  private
    FLeft: TSqlSynExpr;
    FRight: TSqlSynExpr;
  public
    property Left: TSqlSynExpr read FLeft write FLeft;
    property Right: TSqlsynExpr read FRight write FRight;
  end;

  TSqlSynComparisonPredicate = class(TSqlSyn2VPredicate)
  private
    FCompOp: TSqlSynCompOp;
  public
    property CompOp: TSqlSynCompOp read FCompOp write FCompOp;
  end;

  TSqlQuantifierType = (
    sqtAll,
    sqtSome,
    sqtAny);

  TSqlSynQuantComparisonPredicate = class(TSQLSynComparisonPredicate)
  private
    FQuantifier: TSqlQuantifierType;
  public
    property Quantifier: TSqlQuantifierType read FQuantifier write FQuantifier;
  end;

  TSqlSynUniqueOpt =
    (suoNone, suoUnique) ;

  TSqlSynPartialOpt =
    (spoNone, spoPartial, spoFull) ;

  TSqlSynMatchPredicate = class(TSqlSyn2VPredicate)
  private
    FUniqueOpt: TSqlSynUniqueOpt;
    FPartialOpt: TSqlSynPartialOpt;
  public
    property UniqueOpt: TSqlSynUniqueOpt read FUniqueOpt write FUniqueOpt;
    property PartialOpt: TSqlSynpartialOpt read FPartialOpt write FPartialOpt;
  end;

  TSqlSyn3VPredicate = class(TSqlSynPredicate)
  private
    FLeft: TSqlSynExpr;
    FMiddle: TSqlSynExpr;
    FRight: TSqlSynExpr;
  public
    property Left: TSqlSynExpr read FLeft write FLeft;
    property Middle: TSqlSynExpr read FMiddle write FMiddle;
    property Right: TSqlsynExpr read FRight write FRight;
  end;

  TSqlSyn4VPredicate = class(TSqlSynPredicate)
  private
    FLeft: TSqlSynExpr;
    FM1: TSqlSynExpr;
    FM2: TSqlSynExpr;
    FRight: TSqlSynExpr;
  public
    property Left: TSqlSynExpr read FLeft write FLeft;
    property M1: TSqlSynExpr read FM1 write FM1;
    property M2: TSqlSynExpr read FM2 write FM2;
    property Right: TSqlsynExpr read FRight write FRight;
  end;

  TSqlSynTruthValueType = (
    tvtFalse,
    tvtTrue,
    tvtUnknown );

  TSqlSynTruthValue = class(TSqlSynLiteral)
  private
    FTruthValue: TSqlSynTruthValueType;
  public
    property TruthValue: TSqlSynTruthValueType read FTruthValue write FTruthValue;
  end;


  TSqlSynUpdatability = class;

  TSqlSynDeclCursor = class(TSQLSynCreateOrDecl)
  private
    FInsensitive: boolean;
    FScroll: boolean;
    FQuery: TSqlSynExpr;
    FOrderBy: TSqlSynList;
    FUpdatability: TSqlSynUpdatability;
  public
    property Insensitive: boolean read FInsensitive write FInsensitive;
    property Scroll: boolean read FScroll write FScroll;
    property Query: TSqlSynExpr read FQuery write FQuery;
    property OrderBy: TSqlSynList read FOrderBy write FOrderBy;
    property Updatability: TSqlSynUpdatability read FUpdatability write FUpdatability;
  end;

  TSqlSynOrderingSpecType = (
    ostNone,
    ostAscending,
    ostDescending);

  TSqlSynSortSpecification = class(TSqlSynNamedStructural)
  private
    FSortKey: TSqlSynNode;
    FOrderingSpecType: TSqlSynOrderingSpecType;
  public
    property SortKey: TSqlSynNode read FSortKey write FSortKey;
    property OrderingSpecType: TSqlSynOrderingSpecType
      read FOrderingSpecType write FOrderingSpecType;
  end;

  TSqlSynUpdatability = class(TSqlSynStructural)
  private
    FColumns: TSqlSynList;
    FForUpdate: boolean;
  public
    property Columns: TSqlSynList read FColumns write FColumns;
    property ForUpdate: boolean read FForUpdate write FForUpdate;
  end;

  TSqlSynProcedure = class(TSqlSynNamedStructural)
  private
    FParamDecls: TSqlSynList;
    FStatement: TSQLSynStructural;
  public
    property ParamDecls: TSqlSynList read FParamDecls write FParamDecls;
    property Statement: TSQLSynStructural read FStatement write FStatement;
  end;

  //Separate from parameter specification or any other sort ...
  TSqlSynParameterDecl = class(TSqlSynNamedStructural)
  private
    FDataType: TSqlSynType;
  public
    property DataType: TSqlSynType read FDataType write FDataType;
  end;

  TSqlSynStatusParamDecl = class(TSqlSynStructural)
  private
    FSqlCode: boolean;
    FSqlState: boolean;
  public
    property SqlCode: boolean read FSQlCode write FSqlCode;
    property SqlState: boolean read FSQLState write FSqlState;
  end;

  TSqlSynSchema = class(TSqlSynCreateOrDecl)
  private
    FAuthorization: TSQLSynIdent;
    FElements: TSqlSynList;
  public
    property Authorization: TSQLSynIdent read FAuthorization write FAuthorization;
    property Elements: TSqlSynList read FElements write FElements;
  end;

  TSqlSynDomain = class(TSqlSynCreateOrDecl)
  private
    FDataType: TSQLSynType;
    F_Default: TSQLSynLiteral;
    FConstraint: TSqlSynConstraint;
  public
    property DataType: TSQLSynType read FDataType write FDataType;
    property _Default: TSQLSynLiteral read F_Default write F_Default;
    property Constraint: TSqlSynConstraint read FConstraint write FConstraint; //TODO - Allowable constraints?
  end;

  TSqlSynViewCheckOption = (
    vcoNone,
    vcoCheck,
    vcoCascadedCheck,
    vcoLocalCheck
  );

  TSqlSynView = class(TSqlSynCreateOrDecl)
  private
    FColumnList: TSqlSynList;
    FQueryExpr: TSqlSynExpr;
    FViewCheck: TSqlSynViewCheckOption;
  public
    property ColumnList: TSqlSynList read FColumnList write FColumnList;
    property QueryExpr: TSqlSynExpr read FQueryExpr write FQueryExpr;
    property ViewCheck: TSqlSynViewCheckOption read FViewcheck write FViewCheck;
  end;

  TSqlSynMiscStatementType = (
    smstGrant,
    smstRevoke,
    smstSetTransaction,
    smstSetConstraintsMode,
    smstCommit,
    smstRollback,
    smstConnect,
    smstSetConnection,
    smstDisconnect,
    smstSessionSet
  );

  TSqlSynMiscStatement = class(TSqlSynStructural)
  private
    FMiscStatementType: TSqlSynMiscStatementType;
  public
    property MiscStatementType: TSqlSynMiscStatementType read FMiscStatementType write FMiscStatementType;
  end;

  TSqlSynGrantStatement = class(TSqlSynMiscStatement)
  private
    FPrivs: TSqlSynNode;
    FObjectName: TSqlSynIdent;
    FGranteeList: TSqlSynList;
    FGrantOption: boolean;
  public
    property Privs: TSqlSynNode read FPrivs write FPrivs;
    property ObjectName: TSqlSynIdent read FObjectName write FObjectName;
    property GranteeList: TSqlSynList read FGranteeList write FGranteeList;
    property GrantOption: boolean read FGrantOption write FGrantOption;
  end;


  TSqlSynPrivType = (
    ssptAll,
    ssptSelect,
    ssptDelete,
    ssptInsert,
    ssptUpdate,
    ssptReferences,
    ssptUsage
  );

  TSqlSynPriv = class(TSqlSynStructural)
  private
    FPriv: TSqlSynPrivType;
    FColList: TSqlSynList;
  public
    property Priv: TSqlSynPrivType read FPriv write FPriv;
    property ColumnList: TSqlSynList read FColList write FColList;
  end;

  TSqlSynAssertion = class(TSQLSynCreateOrDecl)
  private
    FSearchCondition: TSqlSynExpr;
    FAttributes: TSqlSynConstraintAttributes;
  public
    property SearchCondition: TSqlSynExpr read FSearchCondition write FSearchCondition;
    property Attributes: TSqlSynConstraintAttributes read FAttributes write FAttributes;
  end;

  TSQLSynAlter = class(TSQLSynNamedStructural)
  private
    FType: TSQlSynCreateOrDeclType;
  public
    property AlterType: TSQlSynCreateOrDeclType read FType write FType;
  end;

  TSqlSynDropPropagation = (
    sdpRestrict,
    sdpCascade );

  TSQLSynDrop = class(TSQLSynNamedStructural)
  private
    FType: TSQlSynCreateOrDeclType;
  public
    property DropType: TSQlSynCreateOrDeclType read FType write FType;
  end;

  TSqlSynDropEntity = class(TSqlSynDrop)
  private
    FPropagation: TSqlSynDropPropagation;
  public
    property Propagation: TSqlSynDropPropagation read FPropagation write FPropagation;
  end;

  TSqlSynAlterTableAction = class;

  TSqlSynAlterTable = class(TSqlSynAlter)
  private
    FAction: TSqlSynAlterTableAction;
  public
    property Action: TSqlSynAlterTableAction read FAction write FAction;
  end;

  TSqlSynAlterTableActionType = (
    atatAddCol,
    atatAlterColSetDefault,
    atatAlterColDropDefault,
    atatDropCol,
    atatAddCons,
    atatDropCons );

  TSqlSynAlterTableAction = class(TSqlSynStructural)
  private
    FActionType: TSqlSynAlterTableActionType;
  public
    property ActionType: TSqlSynAlterTableActionType
      read FActionType write FActionType;
  end;

  TSqlSynAlterColDef = class(TSqlSynAlterTableAction)
  private
    FColName: TSqlSynIdent;
    FColDef: TSqlSynColDef;
    FDefaultClause: TSqlSynExpr;
  public
    procedure PropagateNameUp;
    property ColName: TSqlSynIdent read FColname write FColName;
    property ColDef: TSqlSynColDef read FColDef write FColDef;
    property DefaultClause: TSqlSynExpr read FDefaultClause write FDefaultClause;
  end;

  TSqlSynDropTableComponent = class(TSqlSynAlterTableAction)
  private
    FName: TSqlSynIdent;
    FPropagation: TSqlSynDropPropagation;
  public
    property Name: TSqlSynIdent read Fname write FName;
    property Propagation: TSqlSynDropPropagation read FPropagation write FPropagation;
  end;

  TSqlSynAddTableConstraint = class(TSqlSynAlterTableAction)
  private
    FName: TSqlSynIdent;
    FConstraint: TSqlSynConstraint;
  public
    procedure PropagateNameUp;
    property Name: TSqlSynIdent read FName write FName;
    property Constraint: TSqlSynConstraint read FConstraint write FConstraint;
  end;

  TSqlSynRevokeStatement = class(TSqlSynGrantStatement)
  private
    FPropagation: TSqlSynDropPropagation;
  public
    property Propagation: TSqlSynDropPropagation read FPropagation write FPropagation;
  end;

  TSqlSynAlterDomainAction = class;

  TSqlSynAlterDomain = class(TSqlSynAlter)
  private
    FAction: TSqlSynAlterDomainAction;
  public
    property Action: TSqlSynAlterDomainAction read FAction write FAction;
  end;

  TSqlSynAlterDomainActionType = (
    adatAlterSetDefault,
    adatAlterDropDefault,
    adatAddCons,
    adatDropCons );

  TSqlSynAlterDomainAction = class(TSqlSynStructural)
  private
    FActionType: TSqlSynAlterDomainActionType;
  public
    property ActionType: TSqlSynAlterDomainActionType
      read FActionType write FActionType;
  end;

  TSqlSynSetDomainDefaultAction = class(TSqlSynAlterDomainAction)
  private
    FDefaultClause: TSqlSynExpr;
  public
    property DefaultClause: TSQLSynExpr read FDefaultClause write FDefaultClause;
  end;

  TSqlSynAddDomainConstraintAction = class(TSqlSynAlterDomainAction)
  private
    FName: TSqlSynIdent;
    FConstraint: TSqlSynConstraint;
  public
    procedure PropagateNameUp;
    property Name: TSqlSynIdent read FName write FName;
    property Constraint: TSqlSynConstraint read FConstraint write FConstraint;
  end;

  TSqlSynDropDomainConstraint = class(TSqlSynAlterDomainAction)
  private
    FName: TSqlSynIdent;
  public
    property Name: TSqlSynIdent read Fname write FName;
  end;

  TSqlSynDmlCursorType = (
    sdcOpen,
    sdcFetch,
    sdcClose,
    sdcDeletePositioned,
    sdcUpdatePositioned
  );

  TSqlSynDmlCursor = class(TSqlSynStructural)
  private
    FDmlCursorType: TSqlSynDmlCursorType;
    FCursorName: TSqlSynIdent;
  public
    property DmlCursorType: TSqlSynDmlCursorType read FDmlCursorType write FDmlCursorType;
    property CursorName: TSqlSynIdent read FCursorName write FCursorName;
  end;

  TSqlSynFetchOrientation = class;

  TSqlSynDmlCursorFetch = class(TSqlSynDmlCursor)
  private
    FOrientation: TSqlSynFetchOrientation;
    FFetchTarget: TSqlSynList;
  public
    property Orientation: TSqlSynFetchOrientation read FOrientation write FOrientation;
    property FetchTarget: TSqlSynList read FFetchTarget write FFetchTarget;
  end;

  TSqlSynFetchOrientDir = (
    sfdNext,
    sfdPrior,
    sfdFirst,
    sfdLast,
    sfdAbsolute,
    sfdRelative
  );

  TSqlSynFetchOrientation = class(TSqlSynStructural)
  private
    FFetchOrientDir: TSqlSynFetchOrientDir;
    FValSpec: TSqlSynNode;
  public
    property FetchOrientDir: TSqlSynFetchOrientDir read FFetchOrientDir write FFetchOrientDir;
    property ValSpec: TSqlSynNode read FValSpec write FValSpec;
  end;

  TSqlSynDmlNoCursorType = (
    sdnSelectToTarget,
    sdnDeleteSearched,
    sdnInsert,
    sdnUpdateSearched
  );

  TSqlSynDmlNoCursor = class(TSqlSynStructural)
  private
    FDmlNoCursorType: TSqlSynDmlNoCursorType;
  public
    property DmlNoCursorType: TSqlSynDmlNoCursorType read FDmlNoCursorType write FDmlNoCursorType;
  end;

  TSqlSynDmlSelectToTarget = class(TSqlSynDmlNoCursor)
  private
    FSelectExpr: TSqlSynExpr;
    FTargetList: TSqlSynList;
  public
    property SelectExpr: TSQLSynExpr read FSelectExpr write FSelectExpr;
    property TargetList: TSqlSynList read FTargetList write FTargetList;
  end;

  TSqlSynDmlDeletePositioned = class(TSqlSynDmlCursor)
  private
    FTable: TSqlSynIdent;
  public
    property Table: TSqlSynIdent read FTable write FTable;
  end;

  TSqlSynDmlDeleteSearched = class(TSqlSynDmlNoCursor)
  private
    FTable: TSqlSynIdent;
    FWhere: TSqlSynExpr;
  public
    property Table: TSqlSynIdent read FTable write FTable;
    property Where: TSqlSynExpr read FWhere write FWhere;
  end;

  TSqlSynDmlInsert = class(TSqlSynDmlNoCursor)
  private
    FTable: TSqlSynIdent;
    FColList: TSqlSynList;
    FQuery: TSqlSynExpr;
  public
    property Table: TSqlSynIdent read FTable write FTable;
    property ColList: TSqlSynList read FColList write FColList;
    property Query: TSqlSynExpr read FQuery write FQuery;
  end;

  TSqlSynDmlCursorUpdate = class(TSqlSynDmlCursor)
  private
    FTable: TSqlSynIdent;
    FSetClauseList: TSqlSynList;
  public
    property Table: TSqlSynIdent read FTable write FTable;
    property SetClauseList: TSqlSynList read FSetClauseList write FSetClauseList;
  end;

  TSqlSynDmlSetClause = class(TSqlSynStructural)
  private
    FColName: TSqlSynIdent;
    FUpdateSource: TSqlSynExpr;
  public
    property ColName: TSqlSynIdent read FColName write FColName;
    property UpdateSource: TSqlSynExpr read FUpdateSource write FUpdateSource;
  end;

  TSqlSymDmlUpdateSearched = class(TSqlSynDmlNoCursor)
  private
    FTable: TSqlSynIdent;
    FSetClauseList: TSqlSynList;
    FWhereClause: TSqlSynExpr;
  public
    property Table: TSqlSynIdent read FTable write FTable;
    property SetClauseList: TSqlSynList read FSetClauseList write FSetClauseList;
    property WhereClause: TSqlSynExpr read FWhereClause write FWhereClause;
  end;

  TSqlSynSetTransactionStatement = class(TSqlSynMiscStatement)
  private
    FModeList: TSqlSynList;
  public
    property ModeList: TSqlSynList read FModeList write FModeList;
  end;

  TTransactionModeType = (
    tmtIsolation,
    tmtAccessType,
    tmtDiagnosticSize
  );

  TTransactionIsoVal = (
    tivReadUncommited,
    tivReadComitted,
    tivReadRepeatable,
    tivSnapshot,
    tivSerializable);

  TTransactionRWVal = (
    trwReadOnly,
    trwReadWrite
  );

  TTransactionMode = class(TSqlSynStructural)
  private
    FModeType: TTransactionModeType;
    FIsoVal: TTransactionIsoVal;
    FRWVal: TTransactionRWVal;
    FDiagSize: TSqlSynNode;
  public
    property ModeType: TTransactionModeType read FModeType write FModeType;
    property IsoVal: TTransactionIsoVal read FIsoVal write FIsoVal;
    property RWVal: TTransactionRWVal read FRWVal write FRWVal;
    property DiagSize: TSqlSynNode read FDiagSize write FDiagSize;
  end;

  //No constraint name list means ALL.
  TSqlSynSetConstraintsModeStatement = class(TSqlSynMiscStatement)
  private
    FImmediate: boolean;
    FConstraintNameList: TSqlSynList;
  public
    property Immediate: boolean read FImmediate write FImmediate;
    property ConstraintNameList: TSqlSynList read FConstraintNameList write FConstraintNameList;
  end;

  TSqlSynCommitRollbackStatement = class(TSqlSynMiscStatement)
  private
    FCommit: boolean;
  public
    property Commit: boolean read FCommit write FCommit;
  end;

  TSqlSynConnectionStatement = class(TSqlSynMiscStatement)
  private
    FConnectionName: TSqlSynNode;
  public
    property ConnectionName: TSqlSynNode read FConnectionName write FConnectionName;
  end;

  TSqlSynConnectStatement = class(TSqlSynConnectionStatement)
  private
    FServerName: TSqlSynNode;
    FUserName: TSqlSynNode;
  public
    property ServerName: TSqlSynNode read FServerName write FServerName;
    property UserName: TSqlSynNode read FUserName write FUserName;
  end;

  TSqlSynDisconnectContext = (
    ssdAll,
    ssdCurrent,
    ssdSpecified
  );

  TSqlSynDisconnectStatement = class(TSqlSynConnectionStatement)
  private
    FConns: TSqlSynDisconnectContext;
  public
    property Conns: TSqlSynDisconnectContext read FConns write FConns;
  end;

  TSqlSynSessionSetType = (
    ssstCatalog,
    ssstSchema,
    ssstNames,
    ssstAuth
  );

  TSqlSynSessionSetStatement = class(TSqlSynMiscStatement)
  private
    FSessionSetType: TSqlSynSessionSetType;
    FValueSpec: TSQLSynExpr;
  public
    property SessionSetType: TSqlSynSessionSetType read FSessionSetType write FSessionSetType;
    property ValueSpec: TSQLSynExpr read FValueSpec write FValueSpec;
  end;

implementation

uses
  DLList;

const
  S_INSERTING_NIL_CHILD = 'Trying to insert a NIL node into tree structure.';
  S_INTERNAL_NODE = 'AST contains compiler internal nodes. Should have been removed by now.';

{ ESmarterNodeException }


constructor ESQLSynException.CreateCtxt(const S:string; N1, N2: TSQLSynNode);
begin
  inherited Create(S);
  FN1 := N1;
  FN2 := N2;
end;

{ TSQLSynNode }

constructor TSQLSynNode.Create;
begin
  inherited;
  InitNodeType;
end;

constructor TSQLSynNode.CreateWithTracker(Tracker: TTracker);
begin
  inherited;
  InitNodeType;
end;

procedure TSQLSynNode.InitNodeType;
begin
  if Self is TSqlSynList then
    FNodeType := sntList
  else if Self is TSQLSynExpr then
    FNodeType := sntExpr
  else if Self is TSQLSynIdent then
    FNodeType := sntIdent
  else if Self is TSQLSynType then
    FNodeType := sntType
  else if Self is TSQLSynStructural then
    FNodeType := sntStructural
  else
    FNodeType := sntNode;
end;

destructor TSQLSynNode.Destroy;
begin
  if not DLItemIsEmpty(@FSiblingListEntry) then
    DLListRemoveObj(@FSiblingListEntry);
  inherited;
end;

function TSQLSynNode.FirstChild: TSQLSynNode;
begin
  result := FContainedListHead.Flink.Owner as TSQLSynNode;
end;

function TSQLSynNode.LastChild: TSQLSynNode;
begin
  result := FContainedListHead.Blink.Owner as TSQLSynNode;
end;

function TSQLSynNode.NextSibling: TSQLSynNode;
begin
  result := FSiblingListEntry.Flink.Owner as TSQLSynNode;
end;

function TSQLSynNode.PrevSibling: TSQLSynNode;
begin
  result := FSiblingListEntry.Blink.Owner as TSQLSynNode;
end;

procedure TSQLSynNode.InsertHeadChild(Dest: TSQLSynNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ESQLSynException.Create(S_INSERTING_NIL_CHILD);
  Assert(DLItemIsEmpty(@Dest.FSiblingListEntry));
  Assert(not Assigned(Dest.FContainerNode));
  DLListInsertHead(@self.FContainedListHead, @Dest.FSiblingListEntry);
  Dest.FContainerNode := self;
end;

procedure TSQLSynNode.InsertTailChild(Dest: TSQLSynNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ESQLSynException.Create(S_INSERTING_NIL_CHILD);
  Assert(DLItemIsEmpty(@Dest.FSiblingListEntry));
  Assert(not Assigned(Dest.FContainerNode));
  DLListInsertTail(@self.FContainedListHead, @Dest.FSiblingListEntry);
  Dest.FContainerNode := self;
end;

procedure TSQLSynNode.InsertAfterSibling(Dest: TSQLSynNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ESQLSynException.Create(S_INSERTING_NIL_CHILD);
  Assert(DLItemIsEmpty(@Dest.FSiblingListEntry));
  Assert(not Assigned(Dest.FContainerNode));
  DLItemInsertAfter(@self.FSiblingListEntry, @dest.FSiblingListEntry);
  Dest.FContainerNode := self.FContainerNode;
end;

procedure TSQLSynNode.InsertBeforeSibling(Dest: TSQLSynNode);
begin
  if not (Assigned(Self) and Assigned(Dest)) then
    raise ESQLSynException.Create(S_INSERTING_NIL_CHILD);
  Assert(DLItemIsEmpty(@Dest.FSiblingListEntry));
  Assert(not Assigned(Dest.FContainerNode));
  DLItemInsertBefore(@self.FSiblingListEntry, @dest.FSiblingListEntry);
  Dest.FContainerNode := self.FContainerNode;
end;

procedure TSQLSynNode.RemoveFromTree;
begin
  Assert(DLItemIsEmpty(@FSiblingListEntry) = not Assigned(FContainerNode));
  if Assigned(FContainerNode) then
  begin
    FContainerNode := nil;
    DLListRemoveObj(@FSiblingListEntry);
  end;
end;

procedure TSQLSynNode.ValidateAST;
var
  N: TSQLSynNode;
begin
  ValidateSelf;
  N := FirstChild;
  while Assigned(N) do
  begin
    N.ValidateAST;
    N := N.NextSibling;
  end;
end;

procedure TSQLSynNode.ValidateSelf;
begin
end;

function TSQlSynNode.Clone: TSQLSynNode;
begin
  Assert(false);
  result := nil;
  //TODO - write this.
end;

{ TSqlSynRefAction }

procedure TSqlSynRefAction.MergeWith(Other: TSqlSynRefAction);
begin
  Assert(false);
  //TODO - write this.
end;

{ TSqlSynAddTableConstraint }

procedure TSqlSynAddTableConstraint.PropagateNameUp;
begin
  Assert(false);
  //TODO - write this.
end;

{ TSqlSynAlterColDef }

procedure TSqlSynAlterColDef.PropagateNameUp;
begin
  Assert(false);
  //TODO - write this.
end;

{ TSqlSynAddDomainConstraintAction }

procedure TSqlSynAddDomainConstraintAction.PropagateNameUp;
begin
  Assert(false);
  //TODO - write this.
end;

end.
