unit CSSNodes;
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
  Trackables, Classes, DLList, CommonNodes;

const
  NodeTypeCSS = 1;

type
  TCSSNode = class(TCommonNode)
  public
    constructor Create;
    constructor CreateWithTracker(Tracker: TTracker);
  end;

  TCSSScript = class(TCSSNode)
  public
    function AsText:string; override;
  end;

  TCSSListType = (cltInvalid,
                  cltRulesetDeclList,
                  cltMediaList,
                  cltSelectorList
                 );

  TCSSList = class(TCSSNode)
  private
    FListType: TCSSListType;
  public
    function AsText:string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean; override;
    property ListType: TCSSListType read FListType write FListType;
  end;

  TCSSAtDeclType = (catInvalid,
                    catRuleset,
                    catMedia,
                    catPage,
                    catFont,
                    catImport,
                    catKeyframes,
                    catWebkitKeyframes);

  TCSSAtDecl = class (TCSSNode)
  private
    FAtDeclType: TCSSAtDeclType;
  public
    function AsText:string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean; override;
    property AtDeclType: TCSSAtDeclType read FAtDeclType write FAtDeclType;
  end;

  TCSSSelectorType = (cstInvalid,
                      cstSimple,
                      cstTrailing,
                      cstCombinatorPlus,
                      cstCombinatorGreater,
                      cstCombinatorTilde,
                      cstStringSelector,
                      cstAttribSelector,
                      cstCombinatorEquals,
                      cstCombinatorIncludes,
                      cstCombinatorDashmatch,
                      cstPseudo
                      );

  TCSSSelector = class(TCSSNode)
  private
    FSelectorType:TCSSSelectorType;
  public
    function AsText:string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean; override;
    property SelectorType: TCSSSelectorType read FSelectorType write FSelectorType;
  end;

  TCSSStringSelectorType = (sstInvalid,
                           sstIdent,
                           sstHash,
                           sstClass,
                           sstFunc,
                           sstString);

  TCSSStringSelector = class(TCSSSelector)
  private
    FStringSelectorType: TCSSStringSelectorType;
    FIdentStarred: boolean;
    FStrData: string;
  protected
    procedure FixStrings; override;
  public
    function AsText:string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean; override;
    property StringSelectorType: TCSSStringSelectorType read FStringSelectorType write FStringSelectorType;
    property IdentStarred: boolean read FIdentStarred write FIdentStarred;
    property StrData: string read FStrData write FStrData;
  end;

  TCSSDeclarationType = (cdtInvalid,
                         cdtDecl,
                         cdtNestedMedia
                        );

  TCSSDeclaration = class(TCSSNode)
  private
    FDeclType: TCSSDeclarationType;
    FHasPriority: boolean;
  public
    function AsText:string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean; override;
    property DeclType:TCSSDeclarationType read FDeclType write FDeclType;
    property HasPriority: boolean read FHasPriority write FHasPriority;
  end;

  TCSSExprType = (cetInvalid,
                  cetTrailing,
                  cetSlashOperator,
                  cetCommaOperator,
                  cetPlusOperator,
                  cetMinusOperator,
                  cetAtom,
                  cetDotOperator
                  );

  TCSSExpr = class(TCSSNode)
  private
    FExprType: TCSSExprType;
  public
    function AsText:string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean; override;
    property ExprType:TCSSExprType read FExprType write FExprType;
  end;

  TCSSAtomType = (atInvalid,
                  atIdent,
                  atString,
                  atUrl,
                  atFunc,
                  atHexColor,
                  atEms,
                  atExs,
                  atLength,
                  atTime,
                  atAngle,
                  atFreq,
                  atResolution,
                  atPercentage,
                  atNumber,
                  atPseudoPage
                  );

  TCSSAtom = class(TCSSExpr)
  private
    FAtomType: TCSSAtomType;
    FStrData: string;
  protected
    procedure FixStrings; override;
  public
    function AsText:string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean; override;
    property AtomType: TCSSAtomType read FAtomType write FAtomType;
    property StrData: string read FStrData write FStrData;
  end;

  TCSFuncBodyType = (fbtFuncTop, fbtFuncText, fbtSubFunc);

  TCSSFuncBody = class(TCSSNode)
  private
    FFuncType: TCSFuncBodyType;
    FBodyData: string;
    FSubFuncName: string;
  protected
    procedure FixStrings; override;
  public
    function AsText:string; override;
    function LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean; override;
    property FuncType:TCSFuncBodyType read FFuncType write FFuncType;
    property BodyData: string read FBodyData write FBodyData;
    property SubFuncName: string read FSubFuncName write FSubFuncName;
  end;

implementation

uses
  HTMLEscapeHelper;

{ Constant strings }

const
  TCSSListStrings:array[TCSSListType] of string
              = ('cltInvalid',
                 'cltRulesetDeclList',
                 'cltMediaList',
                 'cltSelectorList');

  TCSSAtDeclStrings:array[TCSSAtDeclType] of string
                = ('catInvalid',
                   'catRuleset',
                   'catMedia',
                   'catPage',
                   'catFont',
                   'catImport',
                   'catKeyframes',
                   'catWebkitKeyframes');

  TCSSSelectorStrings:array[TCSSSelectorType] of string
                  = ('cstInvalid',
                      'cstSimple',
                      'cstTrailing',
                      'cstCombinatorPlus',
                      'cstCombinatorGreater',
                      'cstCombinatorTilde',
                      'cstStringSelector',
                      'cstAttribSelector',
                      'cstCombinatorEquals',
                      'cstCombinatorIncludes',
                      'cstCombinatorDashmatch',
                      'cstPseudo'
                      );

  TCSSStringSelectorStrings:array[TCSSStringSelectorType] of string
                        = ('sstInvalid',
                           'sstIdent',
                           'sstHash',
                           'sstClass',
                           'sstFunc',
                           'sstString');

  TCSSDeclarationStrings:array[TCSSDeclarationType] of string
                        = ('cdtInvalid',
                           'cdtDecl',
                           'cdtNestedMedia');

  TCSSExprStrings:array[TCSSExprType] of string
                       = ('cetInvalid',
                          'cetTrailing',
                          'cetSlashOperator',
                          'cetCommaOperator',
                          'cetPlusOperator',
                          'cetMinusOperator',
                          'cetAtom',
                          'cetDotOperator');

  TCSSAtomStrings:array[TCSSAtomType] of string
               = ('atInvalid',
                  'atIdent',
                  'atString',
                  'atUrl',
                  'atFunc',
                  'atHexColor',
                  'atEms',
                  'atExs',
                  'atLength',
                  'atTime',
                  'atAngle',
                  'atFreq',
                  'atResolution',
                  'atPercentage',
                  'atNumber',
                  'atPseudoPage');

  TCSSFuncBodyStrings:array[TCSFuncBodyType] of string
    = ('fbtFuncTop', 'fbtFuncText', 'fbtSubFunc');

{ TCSSNode }

constructor TCSSNode.Create;
begin
  inherited;
  FNodeType := NodeTypeCSS;
end;

constructor TCSSNode.CreateWithTracker(Tracker: TTracker);
begin
  inherited;
  FNodeType := NodeTypeCSS;
end;

{ TCSSScript }

function TCSSScript.AsText:string;
begin
  result := '<script>';
end;

{ TCSSStringSelector }

procedure TCSSStringSelector.FixStrings;
begin
  FixAnsifiedBackToUnicode(FStrData);
  inherited;
end;

{ TCSSAtom }

procedure TCSSAtom.FixStrings;
begin
  FixAnsifiedBackToUnicode(FStrData);
  inherited;
end;

{ TCSSFuncBody }

procedure TCSSFuncBody.FixStrings;
begin
  FixAnsifiedBackToUnicode(FBodyData);
  FixAnsifiedBackToUnicode(FSubFuncName);
  inherited;
end;

{ TCSSList }

function TCSSList.AsText:string;
begin
  result := '<list> ' + TCSSListStrings[FListType];
end;

function TCSSList.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
begin
  result := inherited and
    ((Other as TCSSList).FListType = self.FListType);
end;

{ TCSSAtDecl }

function TCSSAtDecl.AsText:string;
begin
  result := '<@decl> ' + TCSSAtDeclStrings[FAtDeclType];
end;

function TCSSAtDecl.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
begin
  result := inherited and
    ((Other as TCSSAtDecl).FAtDeclType = self.FAtDeclType);
end;

{ TCSSSelector }

function TCSSSelector.AsText:string;
begin
  result := '<selector> ' + TCSSSelectorStrings[FSelectorType];
end;

function TCSSSelector.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
begin
  result := inherited and
    ((Other as TCSSSelector).FSelectorType = self.FSelectorType);
end;

{ TCSSStringSelector }

function TCSSStringSelector.AsText:string;
begin
  result := '<string selector> '
    + TCSSStringSelectorStrings[FStringSelectorType]
    + ' ' + FStrData;
end;

function TCSSStringSelector.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
begin
  result := inherited and
    ((Other as TCSSStringSelector).FStringSelectorType = self.FStringSelectorType)
    and ((Other as TCSSStringSelector).FIdentStarred = self.FIdentStarred)
    and ((Other as TCSSStringSelector).FStrData = self.FStrData);
end;

{ TCSSDeclaration }

function TCSSDeclaration.AsText:string;
begin
  result := '<decl> ' + TCSSDeclarationStrings[FDeclType];
end;

function TCSSDeclaration.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
begin
  result := inherited and
    ((Other as TCSSDeclaration).FDeclType = self.FDeclType)
    and ((Other as TCSSDeclaration).FHasPriority = self.FHasPriority);
end;

{ TCSSExpr }

function TCSSExpr.AsText:string;
begin
  result := '<expr> ' + TCSSExprStrings[FExprType];
end;

function TCSSExpr.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
begin
  result := inherited and
    ((Other as TCSSExpr).FExprType = self.FExprType);
end;

{ TCSSAtom }

function TCSSAtom.AsText:string;
begin
  result := '<atom> ' + TCSSAtomStrings[FAtomType] + ' ' + FStrData;
end;

function TCSSAtom.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
begin
  result := inherited and
    ((Other as TCSSAtom).FAtomType = self.FAtomType)
    and ((Other as TCSSAtom).FStrData = self.FStrData);
end;

{ TCSSFuncBody }

function TCSSFuncBody.AsText:string;
begin
  result := '<func body> ' + TCSSFuncBodyStrings[FFuncType];
end;

function TCSSFuncBody.LclEqual(Other: TCommonNode; Opts: TNodeSearchOpts):boolean;
begin
  result := inherited and
    ((Other as TCSSFuncBody).FFuncType = self.FFuncType)
    and ((Other as TCSSFuncBody).FBodyData = self.FBodyData)
    and ((Other as TCSSFuncBody).FSubFuncName = self.FSubFuncName);
end;

end.
