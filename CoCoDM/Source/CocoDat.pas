unit CocoDat;
{$INCLUDE CocoCD.inc}

interface

uses
  CRT, CocoBase, CocoComment;

type
  TCocoData = class(TObject)
  private
    fCommentList: TCommentList;

    procedure SetOnScannerError(const Value: TErrorProc);
    function GetOnScannerError: TErrorProc;
    function GetTableHandler: TTableHandler;
    procedure SetTableHandler(const Value: TTableHandler);
    function GetOnGetCurrentSymbol: TFunctionSymbolPosition;
    procedure SetOnGetCurrentSymbol(const Value: TFunctionSymbolPosition);
  public
    constructor Create;
    destructor Destroy; override;
    procedure Clear;

    property TableHandler : TTableHandler read GetTableHandler write SetTableHandler;

    property CommentList : TCommentList read fCommentList;
    property OnScannerError : TErrorProc read GetOnScannerError write SetOnScannerError;
    property OnGetCurrentSymbol : TFunctionSymbolPosition read GetOnGetCurrentSymbol
        write SetOnGetCurrentSymbol;
  end; {TCocoData}

implementation

uses
  SysUtils;

{ TCocoData }

procedure TCocoData.Clear;
begin
  fCommentList.Clear;
end; {Clear}

constructor TCocoData.Create;
begin
  fCommentList := TCommentList.Create;
end; {Create}

destructor TCocoData.Destroy;
begin
  if Assigned(fCommentList) then
    FreeAndNil(fCommentList);
  inherited;
end; {Destroy}

function TCocoData.GetOnScannerError: TErrorProc;
begin
  Result := fCommentList.OnScannerError;
end; {GetOnScannerError}

procedure TCocoData.SetOnScannerError(const Value: TErrorProc);
begin
  fCommentList.OnScannerError := Value;
end; {SetOnScannerError}

function TCocoData.GetTableHandler: TTableHandler;
begin
  Result := fCommentList.TableHandler;
end; {GetTableHandler}

procedure TCocoData.SetTableHandler(const Value: TTableHandler);
begin
  fCommentList.TableHandler := Value;
end; {SetTableHandler}

function TCocoData.GetOnGetCurrentSymbol: TFunctionSymbolPosition;
begin
  Result := fCommentList.OnGetCurrentSymbol;
end; {GetOnGetCurrentSymbol}

procedure TCocoData.SetOnGetCurrentSymbol(
  const Value: TFunctionSymbolPosition);
begin
  fCommentList.OnGetCurrentSymbol := Value;
end; {SetOnGetCurrentSymbol}

end.
