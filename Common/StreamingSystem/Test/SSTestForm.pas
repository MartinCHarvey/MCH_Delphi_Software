unit SSTestForm;

interface

uses
  Windows, Messages, SysUtils, Classes, Graphics, Controls, Forms, Dialogs,
  StreamSysXML, StdCtrls, SSAbstracts, StreamSysBinary;

type
  TSSTestFrm = class(TForm)
    SmplWrt: TButton;
    Memo1: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure SmplWrtClick(Sender: TObject);
  private
    { Private declarations }
    FSS: TStreamSysXML;
    FBinSS: TStreamSysBinary;
    procedure HandleLogEvent(Sender: TAbstractSSComponent; Severity:
      TSSSeverity; Msg: string);
  public
    { Public declarations }
  end;

var
  SSTestFrm: TSSTestFrm;

implementation

uses TestStreamables, StreamingSystem;

{$R *.DFM}

procedure TSSTestFrm.HandleLogEvent(Sender: TAbstractSSComponent; Severity:
  TSSSeverity; Msg: string);
var
  ErrStr: string;
begin
  case Severity of
    sssInfo: ErrStr := 'Info: ';
    sssWarning: ErrStr := 'Warning: ';
    sssError: ErrStr := 'Error: ';
  else
    ErrStr := '';
  end;
  ErrStr := ErrStr + Msg;
  Memo1.Lines.Add(ErrStr);
end;

procedure TSSTestFrm.FormCreate(Sender: TObject);
begin
  FSS := TStreamSysXML.Create;
  FSS.RegisterHeirarchy(GetTestHeirarchy);
  FSS.OnLogEvent := HandleLogEvent;
  FBinSS := TStreamSysBinary.Create;
  FBinSS.RegisterHeirarchy(GetTestHeirarchy);
  FBinSS.OnLogEvent := HandleLogEvent;
end;

procedure TSSTestFrm.FormDestroy(Sender: TObject);
begin
  FSS.Free;
  FBinSS.Free;
end;

type
  TSSType = (ssXML, ssBinary);

procedure TSSTestFrm.SmplWrtClick(Sender: TObject);
var
  S1, S2: TTestStreamable2;
  TempStream: TFileStream;
  SS: TStreamSystem;
  SSType: TSSType;
  Filename: string;
begin
  Memo1.Clear;
  S1 := TTestStreamable2.Create;
  S1.SetupTestData;
  S1.TS1 := TTestStreamable.Create;
  S1.TS1.SetupTestData;
  S1.TS2 := TTestStreamable3.Create;
  S2 := nil;
  try
    for SSType := Low(SSType) to High(SSType) do
    begin
      case SSType of
        ssXML:
        begin
          SS := FSS;
          Memo1.Lines.Add('Textual streaming system...');
          Filename := 'test_data.xml';
        end;
        ssBinary:
        begin
          SS := FBinSS;
          Memo1.Lines.Add('Binary streaming system...');
          Filename := 'test_data.bin';
        end
      else
        Assert(false);
        SS := nil;
      end;
      TempStream := TFileStream.Create(Filename, fmCreate or
        fmShareExclusive);
      try
        SS.WriteStructureToStream(S1, TempStream);
      finally
        TempStream.Free;
      end;
      TempStream := TFileStream.Create(Filename, fmOpenReadWrite or
        fmShareExclusive);
      try
        S2 := SS.ReadStructureFromStream(TempStream) as TTestStreamable2;
        if Assigned(S2) then
        begin
          if S1.Compare(S2) then
            Memo1.Lines.Add('Compare OK')
          else
            Memo1.Lines.Add('Compare Failed');
        end;
      finally
        TempStream.Free;
      end;
    end;
  finally
    S1.Free;
    S2.Free;
  end;
end;

end.

