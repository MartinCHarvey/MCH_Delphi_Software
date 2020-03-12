unit CRC;
{$INCLUDE CocoCD.inc}

{ Emits the source code of a generated driver class using the frame file
  component.frm }

interface

uses
  FrameTools, CRT, CRTypes, CocoBase, CocoOptions, StreamTools;

type
  TComponentGen = class(TObject)
  private
    fOnSetGrammarName: TSetGrammarNameEvent;
    fOnScannerError: TErrorProc;
    fOnAbortErrorMessage: TAbortErrorMessageEvent;
    fTableHandler: TTableHandler;
    fFrameTools: TFrameTools;
    fOnExecuteFrameParser: TExecuteFrameParser;
    fStreamTools: TStreamTools;
    fOptions: TCocoOptions;
    procedure ChangeDfmToResource(const DfmFileName: AnsiString);
  public
    procedure WriteCocoRegistration(OutputDir : AnsiString);
    procedure WriteCocoTestProgram(DPR : AnsiString; PAS : AnsiString; DFM : AnsiString;
      OutputDir : AnsiString);
    procedure WriteConsoleApp(DPR : AnsiString; OutputDir : AnsiString);

    property Options : TCocoOptions read fOptions write fOptions;
    property TableHandler : TTableHandler read fTableHandler write fTableHandler;
    property FrameTools : TFrameTools read fFrameTools write fFrameTools;
    property StreamTools: TStreamTools read fStreamTools write fStreamTools;

    property OnSetGrammarName : TSetGrammarNameEvent read fOnSetGrammarName
        write fOnSetGrammarName;
    property OnScannerError : TErrorProc read fOnScannerError write fOnScannerError;
    property OnAbortErrorMessage : TAbortErrorMessageEvent read fOnAbortErrorMessage write fOnAbortErrorMessage;
    property OnExecuteFrameParser : TExecuteFrameParser
        read fOnExecuteFrameParser write fOnExecuteFrameParser;
  end; {TComponentGen}

implementation

uses
  SysUtils, Classes, CocoSwitch, CocoDefs;

procedure TComponentGen.WriteCocoRegistration(OutputDir : AnsiString);
var
  FileStream : TFileStream;
  gn : CRTGraphNode;
  sn : CRTSymbolNode;
  gramName : AnsiString;
  fGramName : AnsiString;
  ODir : AnsiString;
begin
  if fTableHandler.CocoAborted then
    Exit;
  fTableHandler.GetNode(fTableHandler.Root, gn);
  fTableHandler.GetSym(gn.p1, sn);
  gramName := sn.name;
  if OutputDir = '' then
    ODir := fTableHandler.SrcDirectory
  else
    ODir := OutputDir;
  fGramName := ODir + gramName + 'Reg.pas';

  FileStream := TFileStream.Create(fGramName, fmCreate);
  try
    fStreamTools.Stream := FileStream;
    fStreamTools.StreamLine('unit ' + gramName + 'Reg;');
    fStreamTools.StreamLine('');
    fStreamTools.StreamLine('interface');
    fStreamTools.StreamLine('');
    if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
    begin
      fStreamTools.StreamLine('uses');
      fStreamTools.StreamLine('  {$IFDEF VER140 Delphi 6}');
      fStreamTools.StreamLine('  DesignIntf, DesignEditors;');
      fStreamTools.StreamLine('  {$ELSE NOT Delphi 6}');
      fStreamTools.StreamLine('  DsgnIntf;');
      fStreamTools.StreamLine('  {$ENDIF Delphi 6}');
      fStreamTools.StreamLine('');
      fStreamTools.StreamLine('type');
      fStreamTools.StreamLine('  T' + gramName + 'VersionProperty = class(TStringProperty)');
      fStreamTools.StreamLine('  public');
      fStreamTools.StreamLine('    function GetAttributes: TPropertyAttributes; override;');
      fStreamTools.StreamLine('    procedure Edit; override;');
      fStreamTools.StreamLine('  end;');
      fStreamTools.StreamLine('');
    end;
    fStreamTools.StreamLine('procedure Register;');
    fStreamTools.StreamLine('');
    fStreamTools.StreamLine('implementation');
    fStreamTools.StreamLine('');
    fStreamTools.StreamLine('uses');
    if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
      fStreamTools.StreamLine('  SysUtils,');
    fStreamTools.StreamLine('  Classes,');
    fStreamTools.StreamLine('  Dialogs,');
    fStreamTools.StreamLine('  ' + GramName + ';');
    fStreamTools.StreamLine('');
    fStreamTools.StreamLine('procedure Register;');
    fStreamTools.StreamLine('begin');
    if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
      fStreamTools.StreamLine('  RegisterPropertyEditor(TypeInfo(AnsiString),T' + gramName
        + ',' + AnsiQuotedStr('Version',#39) + ',T' + gramName + 'VersionProperty);');
    fStreamTools.StreamLine('  RegisterComponents('
        + AnsiQuotedStr(fOptions.AGI.RegistrationPalette,#39) + ',[T' + gramName + ']);');
    fStreamTools.StreamLine('end;');
    fStreamTools.StreamLine('');
    if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
    begin
      fStreamTools.StreamLine('function T' + gramName
        + 'VersionProperty.GetAttributes: TPropertyAttributes;');
      fStreamTools.StreamLine('begin');
      fStreamTools.StreamLine('  Result := [paDialog, paReadOnly];');
      fStreamTools.StreamLine('end;');
      fStreamTools.StreamLine('');
      fStreamTools.StreamLine('procedure T' + gramName + 'VersionProperty.Edit;');
      fStreamTools.StreamLine('var');
      fStreamTools.StreamLine('  s : AnsiString;');
      fStreamTools.StreamLine('begin');
      fStreamTools.StreamLine('  s := ' + AnsiQuotedStr(gramName,#39) + ' + #13#10#13#10;');
      fStreamTools.StreamLine('  s := s + T' + gramName + '(GetComponent(0)).VersionStr + #13#10;');
      fStreamTools.StreamLine('  s := s + FormatDateTime(' + AnsiQuotedStr('dddddd  tt',#39) + ',T'
        + gramName + '(GetComponent(0)).BuildDate);');
      if (fOptions.AGI.VersionInfo.Count > 0) then
        fStreamTools.StreamLine('  s := s + #13#10 + T' + gramName
          + '(GetComponent(0)).VersionInfo;');
      fStreamTools.StreamLine('  MessageDlg(s,mtInformation,[mbOk],0);');
      fStreamTools.StreamLine('end;');
      fStreamTools.StreamLine('');
    end;
    fStreamTools.StreamLine('end.');
  finally
    FileStream.Free;
  end;
end;

procedure TComponentGen.ChangeDfmToResource(const DfmFileName : AnsiString);
var
  FormStream : TFileStream;
  TextStream : TFileStream;
begin
  // Change the dfm to a resource.
  TextStream := TFileStream.Create(DfmFileName, fmOpenRead);
  try
    FormStream := TFileStream.Create(ChangeFileExt(DfmFileName, '.dfm'), fmCreate);
    try
      ObjectTextToResource(TextStream, FormStream);
    finally
      FormStream.Free;
    end;
  finally
    TextStream.Free;
  end;
  DeleteFile(DfmFileName);
end; {ChangeDfmToResource}

procedure TComponentGen.WriteCocoTestProgram(DPR : AnsiString; PAS : AnsiString; DFM : AnsiString;
  OutputDir : AnsiString);
var
  gn : CRTGraphNode;
  sn : CRTSymbolNode;
  gramName : AnsiString;
  fGramName : AnsiString;
  Ext : AnsiString;
  ODir : AnsiString;
  Symbol : TSymbolPosition;
begin
  if fTableHandler.CocoAborted then
    Exit;

  Symbol := TSymbolPosition.Create;
  try
    if not (FileExists(DPR) and FileExists(PAS) and FileExists(DFM)) then
    begin
      fTableHandler.CocoAborted := true;
      fOnAbortErrorMessage('Test frame file not found');
      fOnScannerError(152, Symbol, ' test program (could not find frame file)',
          etGeneration);
      Exit; {@Exit}
    end;

    fTableHandler.GetNode(fTableHandler.Root, gn);
    fTableHandler.GetSym(gn.p1, sn);
    gramName := sn.name;
    if OutputDir = '' then
      ODir := fTableHandler.SrcDirectory
    else
      ODir := OutputDir;

    fGramName := ODir + 'Test' + gramName + '.dpr';

    if Assigned(fOnExecuteFrameParser) then
    begin
      if NOT fOnExecuteFrameParser(fGramName, gramName, DPR) then
      begin
        fTableHandler.CocoAborted := true;
        fOnAbortErrorMessage('test program not generated');
        fOnScannerError(152, Symbol, ' test program (parser error - check '
            + lowercase(LST_FILE_EXT) + ' file)', etGeneration);
      end;

      if not fTableHandler.CocoAborted then
      begin
        fGramName := ODir + 'fTest' + gramName + '.pas';
        fOnExecuteFrameParser(fGramName, gramName, PAS);
      end;
      if not fTableHandler.CocoAborted then
      begin
        if crsDfmAsResource IN fOptions.Switches.SwitchSet then
          Ext := '.txt'
        else
          Ext := '.dfm';
        fGramName := ODir + 'fTest' + gramName + Ext;
        fOnExecuteFrameParser(fGramName, gramName, DFM);
        if crsDfmAsResource IN fOptions.Switches.SwitchSet then
          ChangeDfmToResource(fGramName);
      end;
    end;
  finally
    Symbol.Free;
  end;
end;

procedure TComponentGen.WriteConsoleApp(DPR : AnsiString; OutputDir : AnsiString);
var
  gn : CRTGraphNode;
  sn : CRTSymbolNode;
  gramName : AnsiString;
  fGramName : AnsiString;
  ODir : AnsiString;
  Symbol : TSymbolPosition;
begin
  if fTableHandler.CocoAborted then
    Exit;
  Symbol := TSymbolPosition.Create;
  try
    if not FileExists(DPR) then
    begin
      fTableHandler.CocoAborted := true;
      fOnAbortErrorMessage('console frame file not found');
      fOnScannerError(152, Symbol, ' console program (could not find console frame)', etGeneration);
      Exit;
    end;

    fTableHandler.GetNode(fTableHandler.Root, gn);
    fTableHandler.GetSym(gn.p1, sn);
    gramName := sn.name;
    if OutputDir = '' then
      ODir := fTableHandler.SrcDirectory
    else
      ODir := OutputDir;

    fGramName := ODir + 'c' + gramName + '.dpr';

    if Assigned(fOnExecuteFrameParser) then
    begin
      if NOT fOnExecuteFrameParser(fGramName, gramName, DPR) then
      begin
        fTableHandler.CocoAborted := true;
        fOnAbortErrorMessage('console not generated');
        fOnScannerError(152, Symbol, ' console program (parser error - check '
            + lowercase(LST_FILE_EXT) + ' file)', etGeneration);
      end;
    end;
  finally
    Symbol.Free;
  end; 
end;

end {CRC}.

