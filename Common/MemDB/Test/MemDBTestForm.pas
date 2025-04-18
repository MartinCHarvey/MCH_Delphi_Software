unit MemDBTestForm;

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, MemDB, FMX.Layouts, FMX.Memo;

type
  TForm1 = class(TForm)
    BasicTestBtn: TButton;
    Reset: TButton;
    IndexTest: TButton;
    ResMemo: TMemo;
    IndexTest2: TButton;
    FKTest: TButton;
    CheckpointBtn: TButton;
    SmallTrans: TButton;
    FindEdgeTest: TButton;
    MFIndexTest: TButton;
    MFFKeyTest: TButton;
    BigTable: TButton;
    BigTblMod: TButton;
    ChkBlobs: TButton;
    MiniCommit: TButton;
    procedure BasicTestBtnClick(Sender: TObject);
    procedure ResetClick(Sender: TObject);
    procedure IndexTestClick(Sender: TObject);
    procedure IndexTest2Click(Sender: TObject);
    procedure CheckpointBtnClick(Sender: TObject);
    procedure FKTestClick(Sender: TObject);
    procedure SmallTransClick(Sender: TObject);
    procedure FindEdgeTestClick(Sender: TObject);
    procedure MFIndexTestClick(Sender: TObject);
    procedure MFFKeyTestClick(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure BigTableClick(Sender: TObject);
    procedure BigTblModClick(Sender: TObject);
    procedure ChkBlobsClick(Sender: TObject);
    procedure MiniCommitClick(Sender: TObject);
  private
    { Private declarations }
    FTimeStamp: TDateTime;
    procedure LogTimeIncr(S: string);
    procedure BigTblModFast;
    procedure BigTblModSlow;
  public
    { Public declarations }
  end;

const
  DB_LOCATION = 'c:\temp\MemDB';

var
  Form1: TForm1;
  DBStartTime: TDateTime;

implementation

{$R *.fmx}

uses
  IOUtils, MemDBMisc, MemDBAPI, Math, MemDbBuffered;

const
  LIMIT = 1000;
  TRANS_LIMIT = 65535;
  BIG_ROWS = 128 * 1024;
  BIG_NTABLES = 5;
  BIG_NINDEXES = 5;
  BLOB_SIZE = 1024;

type
  EMemDBTestException = class(EMemDBException);

var
  FDB: TMemDB;
  FSession: TMemDBSession;
  LIMIT_CUBEROOT: integer;

function TestGuidFromInt(i: integer): TGUID;
var
  j: integer;
begin
  result.D1 := Cardinal(i);
  result.D2 := Word(i);
  result.D3 := result.D2;
  for j := 0 to 7 do
    result.D4[j] := Byte(i);
end;

function ElapsedToTimeStr(D: double): string;
var
  MSecsElapsed: integer;
  SecsElapsed: integer;
begin
  SecsElapsed := Trunc(D * (24.0 * 3600.0));
  D := D - (SecsElapsed / (24.0 * 3600.0));
  MSecsElapsed := Trunc(D * (24.0 * 3600.0 * 1000.0));
  if SecsElapsed < 1 then
    result := '('+ IntToStr(MSecsElapsed) + ' msecs)'
  else
    result := '('+ InttoStr(SecsElapsed) + '.' + IntToStr(MSecsElapsed) + ' secs)';
end;

procedure TForm1.LogTimeIncr(S: string);
var
  N: TDateTime;
  MSecsElapsed: integer;
  SecsElapsed: integer;
  ElapsedD: double;
  TimeS: string;
begin
  N := Now;
  ElapsedD := N - FTimeStamp;
  TimeS := ElapsedToTimeStr(ElapsedD);
  ResMemo.Lines.Add(TimeS + ' ' + S);
  FTimeStamp := N;
end;

procedure TForm1.BasicTestBtnClick(Sender: TObject);
var
  Trans: TMemDBTransaction;
  DBAPI: TMemAPIDatabase;
  TableMeta: TMemAPITableMetadata;
  TableData: TMemAPITableData;
  Table: TMemDBHandle;
  Data: TMemDbFieldDataRec;
  DirectionUp: boolean;
  FieldIncrement: integer;
  DataI64: Int64;
  NavOK: boolean;
  i,j: integer;
begin
  ResetClick(Sender);
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      if not Assigned(Table) then
      begin
        Table := DBAPI.CreateTable('Test table');

        TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
        try
          TableMeta.CreateField('Int', ftInteger);
          TableMeta.CreateField('U64', ftUint64);
          TableMeta.CreateField('String', ftUnicodeString);
          TableMeta.CreateField('Double', ftDouble);
          TableMeta.CreateField('Guid', ftGuid);
        finally
          TableMeta.Free;
        end;
        DBAPI.Free;
        Trans.CommitAndFree;
        LogTimeIncr('Table create OK');
        Trans := FSession.StartTransaction(amReadWrite, amLazyWrite, ilDirtyRead);
        DBAPI := Trans.GetAPI;
        Assert(Assigned(DBAPI));
        TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
        try
          for i := 1 to LIMIT do
          begin
            // Whilst we have no keys or indices.
            TableData.Append;
            // Test Field setting.
            Data.FieldType := ftInteger;
            Data.i32Val := i;
            TableData.WriteField('Int', Data);
            Data.FieldType := ftUint64;
            Data.u64Val := i;
            TableData.WriteField('U64', Data);
            Data.FieldType := ftUnicodeString;
            Data.sVal := IntToStr(i);
            TableData.WriteField('String', Data);
            Data.FieldType := ftDouble;
            Data.dVal := i;
            TableData.WriteField('Double', Data);
            Data.FieldType := ftGuid;
            Data.gVal := TestGuidFromInt(i);
            TableData.WriteField('Guid', Data);
            TableData.Post;
          end;

          // 3) Test row navigation by indernal index and field modification.
          for DirectionUp := High(boolean) downto Low(boolean) do
          begin
            if DirectionUp then
              FieldIncrement := 1
            else
              FieldIncrement := -1;

            i := 0;
            NavOK := TableData.Locate(ptFirst, '');
            while NavOK do
            begin
              Inc(i);
              TableData.ReadField('Int', Data);
              Data.i32Val := Data.i32Val + FieldIncrement;
              TableData.WriteField('Int', Data);

              TableData.ReadField('U64', Data);
              //Do the arithmetic signed, and no don't range check the
              //type conversion or sign extension.
              DataI64 := Int64(Data.u64Val);
              DataI64 := DataI64 + FieldIncrement;
              Data.u64Val := UInt64(DataI64);
              TableData.WriteField('U64', Data);

              TableData.ReadField('String', Data);
              j := StrToInt(Data.sVal);
              Data.sVal := IntToStr(j + FieldIncrement);
              TableData.WriteField('String', Data);

              TableData.ReadField('Double', Data);
              Data.dVal := Data.dVal + FieldIncrement;
              TableData.WriteField('Double', Data);

              TableData.ReadField('Guid', Data);
              j := Integer(Data.gVal.D1);
              Inc(j, FieldIncrement);
              Data.gVal := TestGuidFromInt(j);
              TableData.Post;
              NavOK := TableData.Locate(ptNext, '');
            end;
            Assert(i = LIMIT);
          end;
        finally
          TableData.Free;
        end;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Table add data OK');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Table add data failed: ' + E.Message);
      raise;
    end;
  end;
end;

procedure TForm1.BigTableClick(Sender: TObject);
var
  TabI: integer;
  FieldIndexI: integer;
  RowI: integer;
  Trans: TMemDBTransaction;
  DBAPI: TMemAPIDatabase;
  Table, FK: TMemDBHandle;
  TMetAPI: TMemAPITableMetadata;
  TDatAPI: TMemAPITableData;
  FKAPI: TMemAPIForeignKey;
  Data: TMemDBFieldDataRec;
  Mini: TMemAPIUserTransactionControl;
  Start, ElTotal: double;
begin
  ResetClick(Sender);
  Start := Now;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    Mini := DBAPI.GetApiObject(APIUserTransactionControl) as TMemAPIUserTransactionControl;
    try
      for TabI := 0 to Pred(BIG_NTABLES) do
      begin
        Table := DBAPI.CreateTable('BIGTABLE_'+InttoStr(TabI));
        TMetAPI := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
        try
          for FieldIndexI := 0 to Pred(BIG_NINDEXES) do
          begin
            TMetAPI.CreateField('FIELD_'+InttoStr(FieldIndexI), ftInteger);
          end;
        finally
          TMetAPI.Free;
        end;
      end;
      Mini.MiniCommit;
      LogTimeIncr('Big table structure setup OK.');
      for TabI := 0 to Pred(BIG_NTABLES) do
      begin
        Table := DBAPI.OpenTableOrKey('BIGTABLE_'+IntToStr(TabI));
        TDatAPI := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
        try
          for RowI := 0 to Pred(BIG_ROWS) do
          begin
            TDatAPI.Append;
            for FieldIndexI := 0 to Pred(BIG_NINDEXES) do
            begin
              Data.FieldType := ftInteger;
              Data.i32Val := RowI;
              TDatAPI.WriteField('FIELD_'+InttoStr(FieldIndexI), Data);
            end;
            TDatAPI.Post;
          end;
        finally
          TDatAPI.Free;
        end;
      end;
      Mini.MiniCommit;
      LogTimeIncr('Big table data filled OK.');
      for TabI := 0 to Pred(BIG_NTABLES) do
      begin
        Table := DBAPI.OpenTableOrKey('BIGTABLE_'+IntToStr(TabI));
        TMetAPI := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
        try
          for FieldIndexI := 0 to Pred(BIG_NINDEXES) do
          begin
            TMetAPI.CreateIndex('INDEX_'+IntToStr(FieldIndexI), 'FIELD_'+InttoStr(FieldIndexI), [iaUnique]);
          end;
        finally
          TMetAPI.Free;
        end;
      end;
      Mini.MiniCommit;
      LogTimeIncr('Big table indexes added OK.');
      for TabI := 0 to Pred(BIG_NTABLES) do
      begin
        if (TabI > 0) then
        begin
          FK := DBAPI.CreateForeignKey('BIGFK_'+InttoStr(TabI));
          FKAPI := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
          try
            for FieldIndexI := 0 to Pred(BIG_NINDEXES) do
            begin
              FKAPI.SetReferencingChild('BIGTABLE_'+IntToStr(TabI), 'INDEX_'+IntToStr(FieldIndexI));
              FKAPI.SetReferencedParent('BIGTABLE_'+IntToStr(TabI-1), 'INDEX_'+IntToStr(FieldIndexI));
            end;
          finally
              FKAPI.Free;
          end;
        end;
      end;
    finally
      DBAPI.Free;
      Mini.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Big table FKeys added OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Big tables setup / fill / foreign keys failed' + E.Message);
      raise;
    end;
  end;
  ElTotal := Now - Start;
  LogTimeIncr('Total time: ' + ElapsedToTimeStr(ElTotal));
end;

procedure TForm1.BigTblModFast;
var
  Trans: TMemDbTransaction;
  DBAPI: TMemAPIDatabase;
  FieldIndexI: integer;
  TabI: integer;
  Table, FK: TMemDbHandle;
  TDatAPI: TMemAPITableData;
  Found: boolean;
  Data: TMemDBFieldDataRec;
  Mini: TMemAPIUserTransactionControl;
  TMetAPI: TMemAPITableMetadata;
  FKAPI: TMemAPIForeignKey;
begin
  FTimeStamp := Now;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    Mini := DBAPI.GetApiObject(APIUserTransactionControl) as TMemAPIUserTransactionControl;
    try
      for TabI := 0 to Pred(BIG_NTABLES) do
      begin
        if (TabI > 0) then
          DBAPI.DeleteTableOrKey('BIGFK_'+InttoStr(TabI));
      end;
      Mini.MiniCommit;
      LogTimeIncr('Big table dropped FKs OK.');
      for TabI := 0 to Pred(BIG_NTABLES) do
      begin
        Table := DBAPI.OpenTableOrKey('BIGTABLE_'+IntToStr(TabI));
        TMetAPI := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
        try
          for FieldIndexI := 0 to Pred(BIG_NINDEXES) do
            TMetAPI.DeleteIndex('INDEX_'+IntToStr(FieldIndexI));
        finally
          TMetAPI.Free;
        end;
      end;
      Mini.MiniCommit;
      LogTimeIncr('Big table dropped indices OK.');
      for TabI := 0 to Pred(BIG_NTABLES) do
      begin
        //TODO - Yeah, could do per table locking based on API objects,
        //if row locking turned out to be too scary.
        Table := DBAPI.OpenTableOrKey('BIGTABLE_'+IntToStr(TabI));
        TDatAPI := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
        try
          Found:= TDatAPI.Locate(ptFirst, '');
          while Found do
          begin
            for FieldIndexI := 0 to Pred(BIG_NINDEXES) do
            begin
              TDatAPI.ReadField('FIELD_'+InttoStr(FieldIndexI), Data);
              Assert(Data.FieldType = ftInteger);
              Data.i32Val := Data.i32Val * 2;
              TDatAPI.WriteField('FIELD_'+InttoStr(FieldIndexI), Data);
            end;
            TDatAPI.Post;
            Found:= TDatAPI.Locate(ptNext, '');
          end;
        finally
          TDatAPI.Free;
        end;
      end;
      Mini.MiniCommit;
      LogTimeIncr('Big table data modded OK.');
      for TabI := 0 to Pred(BIG_NTABLES) do
      begin
        Table := DBAPI.OpenTableOrKey('BIGTABLE_'+IntToStr(TabI));
        TMetAPI := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
        try
          for FieldIndexI := 0 to Pred(BIG_NINDEXES) do
          begin
            TMetAPI.CreateIndex('INDEX_'+IntToStr(FieldIndexI), 'FIELD_'+InttoStr(FieldIndexI), [iaUnique]);
          end;
        finally
          TMetAPI.Free;
        end;
      end;
      Mini.MiniCommit;
      LogTimeIncr('Big table indexes added OK.');
      for TabI := 0 to Pred(BIG_NTABLES) do
      begin
        if (TabI > 0) then
        begin
          FK := DBAPI.CreateForeignKey('BIGFK_'+InttoStr(TabI));
          FKAPI := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
          try
            for FieldIndexI := 0 to Pred(BIG_NINDEXES) do
            begin
              FKAPI.SetReferencingChild('BIGTABLE_'+IntToStr(TabI), 'INDEX_'+IntToStr(FieldIndexI));
              FKAPI.SetReferencedParent('BIGTABLE_'+IntToStr(TabI-1), 'INDEX_'+IntToStr(FieldIndexI));
            end;
          finally
              FKAPI.Free;
          end;
        end;
      end;
    finally
      DBAPI.Free;
      Mini.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Big table FKeys added OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Big table data mod failed' + E.Message);
      raise;
    end;
  end;
end;

procedure TForm1.BigTblModSlow;
var
  Trans: TMemDBTransaction;
  DBAPI: TMemAPIDatabase;
  Table: TMemDBHandle;
  TabI: integer;
  TDatAPI: TMemAPITableData;
  Found: boolean;
  FieldIndexI: integer;
  Data: TMemDBFieldDataRec;
begin
  FTimeStamp := Now;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      for TabI := 0 to Pred(BIG_NTABLES) do
      begin
        //TODO - Yeah, could do per table locking based on API objects,
        //if row locking turned out to be too scary.
        Table := DBAPI.OpenTableOrKey('BIGTABLE_'+IntToStr(TabI));
        TDatAPI := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
        try
          Found:= TDatAPI.Locate(ptFirst, '');
          while Found do
          begin
            for FieldIndexI := 0 to Pred(BIG_NINDEXES) do
            begin
              TDatAPI.ReadField('FIELD_'+InttoStr(FieldIndexI), Data);
              Assert(Data.FieldType = ftInteger);
              Data.i32Val := Data.i32Val * 2;
              TDatAPI.WriteField('FIELD_'+InttoStr(FieldIndexI), Data);
            end;
            TDatAPI.Post;
            Found:= TDatAPI.Locate(ptNext, '');
          end;
        finally
          TDatAPI.Free;
        end;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Big table data modded OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Big table data mod failed' + E.Message);
      raise;
    end;
  end;
end;

procedure TForm1.BigTblModClick(Sender: TObject);
var
  Start, N, ElFast, ElSlow: double;
begin
  FTimeStamp := Now;
  LogTimeIncr('This will take a moment (comparing different methods)...');
  Application.ProcessMessages;
  Start := Now;
  BigTblModFast;
  N := Now;
  LogTimeIncr('...');
  ElFast := N - Start;
  Start := N;
  BigTblModSlow;
  ElSlow := Now - Start;
  LogTimeIncr('Total time (fast): ' + ElapsedToTimeStr(ElFast));
  LogTimeIncr('Total time (slow): ' + ElapsedToTimeStr(ElSlow));
end;

procedure TForm1.CheckpointBtnClick(Sender: TObject);
begin
  FTimeStamp := Now;
  if FDB.Checkpoint then
  begin
    LogTimeIncr('Checkpoint OK')
  end
  else
  begin
    LogTimeIncr('Cannot checkpoint at this time')
  end;
end;

procedure TForm1.ChkBlobsClick(Sender: TObject);
var
  Trans: TMemDBTransaction;
  DBAPI: TMemAPIDatabase;
  TableData: TMemAPITableData;
  TableMeta: TMemAPITableMetadata;
  Table: TMemDBHandle;
  SearchData, Data, IData: TMemDbFieldDataRec;
  i, j: integer;
  OK: boolean;
  PD: PAnsiChar;

begin
  ResetClick(Sender);
  //Create a table with the right structure.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      if Assigned(Table) then
        DBAPI.DeleteTableOrKey('Test table');
      Table := DBAPI.CreateTable('Test table');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.CreateField('Int', ftInteger);
        TableMeta.CreateIndex('IntIdx', 'Int', []);
        TableMeta.CreateField('Blob', ftBlob);
        TableMeta.CreateIndex('BlobIdx', 'Blob', []);
      finally
        TableMeta.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;

    LogTimeIncr('Blob table setup OK');
  except
    Trans.RollbackAndFree;
    LogTimeIncr('Blob table setup failed');
    raise;
  end;

  //Add some data.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        for i := 0 to LIMIT do
        begin
          TableData.Append;
          Data.FieldType := ftInteger;
          Data.i32Val := i;
          TableData.WriteField('Int', Data);
          TableData.Post;
        end;
        try
          FillChar(Data, sizeof(Data), 0);
          OK := TableData.Locate(ptFirst, '');
          while OK do
          begin
            IData.FieldType := ftInteger;
            TableData.ReadField('Int', IData);
            Data.FieldType := ftBlob;
            Data.size := BLOB_SIZE;
            if not Assigned(Data.Data) then
              GetMem(Data.Data, BLOB_SIZE);
            FillChar(Data.Data^, BLOB_SIZE, IData.i32Val);
            TableData.WriteField('Blob', Data);
            TableData.Post;
            OK := TableData.Locate(ptNext, '');
          end;
        finally
          FreeMem(Data.Data);
        end;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Blob test (1) OK');
  except
    Trans.RollbackAndFree;
    LogTimeIncr('Blob test (1) failed');
    raise;
  end;

  //Modify a byte in the blobs to check replay from journal.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        FillChar(Data, sizeof(Data), 0);
        OK := TableData.Locate(ptFirst, '');
        while OK do
        begin
          IData.FieldType := ftInteger;
          TableData.ReadField('Int', IData);
          i := IData.i32Val;
          Data.FieldType := ftBlob;
          TableData.ReadField('Blob', Data);
          //Find out how big our buffer needs to be.
          if Data.size > 0 then
          begin
            GetMem(Data.Data, Data.size);
            try
              //Get the actual data.
              TableData.ReadField('Blob', Data);
              //Mod one byte.
              PAnsiChar(Data.Data)^ := #42;
              //Write back.
              TableData.WriteField('Blob', Data);
            finally
              FreeMem(Data.Data);
              FillChar(Data, sizeof(Data), 0);
            end;
          end;

          //Re-read back see if OK after write to local copy.

          Data.FieldType := ftBlob;
          TableData.ReadField('Blob', Data);
          //Find out how big our buffer needs to be.
          if Data.size > 0 then
          begin
            GetMem(Data.Data, Data.size);
            try
              //Get the actual data.
              TableData.ReadField('Blob', Data);
              PD := Data.Data;
              for j := 0 to Pred(Data.size) do
              begin
                if j = 0  then
                begin
                  if PD^ <> #42 then
                    raise Exception.Create('Blob test (2) failed.');
                end
                else
                begin
                  if PD^ <> AnsiChar(i) then
                    raise Exception.Create('Blob test (2) failed.');
                end;
                Inc(PD);
              end;
            finally
              FreeMem(Data.Data);
              FillChar(Data, sizeof(Data), 0);
            end;
          end;
          TableData.Post;
          OK := TableData.Locate(ptNext, '');
        end;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Blob test (2) OK');
  except
    Trans.RollbackAndFree;
    LogTimeIncr('Blob test (2) failed');
    raise;
  end;

  //Now check that the blob data is as we expect, after commited
  //all the way back AB buffers, and re-read.
  Trans := FSession.StartTransaction(amRead);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        OK := TableData.Locate(ptFirst, '');
        while OK do
        begin
          Data.FieldType := ftInteger;
          TableData.ReadField('Int', Data);
          i := Data.i32Val;
          FillChar(Data, sizeof(Data), 0);
          Data.FieldType := ftBlob;
          TableData.ReadField('Blob', Data);
          //Find out how big our buffer needs to be.
          if Data.size > 0 then
          begin
            GetMem(Data.Data, Data.size);
            try
              //Get the actual data.
              TableData.ReadField('Blob', Data);
              PD := Data.Data;
              for j := 0 to Pred(Data.size) do
              begin
                if j = 0  then
                begin
                  if PD^ <> #42 then
                    raise Exception.Create('Blob test (3) failed.');
                end
                else
                begin
                  if PD^ <> AnsiChar(i) then
                    raise Exception.Create('Blob test (3) failed.');
                end;
                Inc(PD);
              end;
            finally
              FreeMem(Data.Data);
              FillChar(Data, sizeof(Data), 0);
            end;
          end;
          OK := TableData.Locate(ptNext, '');
        end;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Blob test (3) OK');
  except
    Trans.RollbackAndFree;
    LogTimeIncr('Blob test (3) failed');
    raise;
  end;

  //Search for an existent, and nonexistent blob.
  Trans := FSession.StartTransaction(amRead);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        FillChar(SearchData, sizeof(SearchData), 0);
        SearchData.FieldType := ftBlob;
        SearchData.size := BLOB_SIZE;
        GetMem(SearchData.Data, SearchData.size);
        try
          //Search for a blob we know exists.
          FillChar(SearchData.Data^, SearchData.size, #50);
          PAnsiChar(SearchData.Data)^ := #42;
          OK := TableData.FindByIndex('BlobIdx', SearchData);
          if not OK then
            raise Exception.Create('Blob test (4) - find blob failed');
          //And one that we know doesn't.
          PD := PAnsiChar(SearchData.Data);
          for i := 0 to Pred(SearchData.size) do
          begin
            PD^ := AnsiChar(i);
            Inc(PD);
          end;
          OK := TableData.FindByIndex('BlobIdx', SearchData);
          if OK then
            raise Exception.Create('Blob test (4) - find blob succeeded erroneously.');
        finally
          FreeMem(SearchData.Data);
          FillChar(SearchData, sizeof(SearchData), 0);
        end;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.RollbackAndFree;
    LogTimeIncr('Blob test (4) OK');
  except
    Trans.RollbackAndFree;
    LogTimeIncr('Blob test (4) Failed.');
    raise;
  end;
end;

procedure TForm1.FindEdgeTestClick(Sender: TObject);

const
  DUP_FACTOR = 3;

var
  Trans: TMemDBTransaction;
  DBAPI: TMemAPIDatabase;
  TableData: TMemAPITableData;
  TableMeta: TMemAPITableMetadata;
  Table: TMemDBHandle;
  SearchData, Data: TMemDbFieldDataRec;
  i, j: Integer;
  OK: boolean;
begin
  ResetClick(Sender);
  //Create a table with the right structure.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      if Assigned(Table) then
        DBAPI.DeleteTableOrKey('Test table');
      Table := DBAPI.CreateTable('Test table');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.CreateField('Int', ftInteger);
        TableMeta.CreateIndex('IntIdx', 'Int', []);
      finally
        TableMeta.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Edge test OK (1)');
  except
    Trans.RollbackAndFree;
    LogTimeIncr('Edge test failed (1)');
    raise;
  end;

  //Add some data.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        for i := 0 to Pred(DUP_FACTOR) do
        begin
          for j := 0 to LIMIT do
          begin
            TableData.Append;
            Data.FieldType := ftInteger;
            Data.i32Val := j;
            TableData.WriteField('Int', Data);
            TableData.Post;
          end;
        end;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Edge test OK (2)');
  except
    Trans.RollbackAndFree;
    LogTimeIncr('Edge test failed (2)');
    raise;
  end;

  Trans := FSession.StartTransaction(amRead);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        SearchData.FieldType := ftInteger;
        SearchData.i32Val := LIMIT div 2;

        i := 0;
        //Use top level API's to check location of records with same field.
        OK := TableData.FindByIndex('IntIdx', SearchData);
        if OK then
          Inc(i);
        while OK do
        begin
          //Go through previous recs.
          OK := TableData.Locate(ptPrevious, 'IntIdx');
          if OK then
          begin
            TableData.ReadField('Int', Data);
            OK := OK and DataRecsSame(Data, SearchData);
            if OK then
              Inc(i);
          end;
        end;
        OK := TableData.FindByIndex('IntIdx', SearchData);
        while OK do
        begin
          //Go through next recs.
          OK := TableData.Locate(ptNext, 'IntIdx');
          if OK then
          begin
            TableData.ReadField('Int', Data);
            OK := OK and DataRecsSame(Data, SearchData);
            if OK then
              Inc(i);
          end;
        end;
        if i <> DUP_FACTOR then
          LogTimeIncr('Edge test failed (3)');

        //Use somewhat internal API's to do the same searching.
        i := 0;
        j := 0;
        OK := TableData.FindEdgeByIndex(ptFirst, 'IntIdx', SearchData);
        while OK do
        begin
          Inc(i);
          OK := TableData.Locate(ptNext, 'IntIdx');
          if OK then
          begin
            TableData.ReadField('Int', Data);
            OK := OK and DataRecsSame(Data, SearchData);
          end;
        end;

        OK := TableData.FindEdgeByIndex(ptLast, 'IntIdx', SearchData);
        while OK do
        begin
          Inc(j);
          OK := TableData.Locate(ptPrevious, 'IntIdx');
          if OK then
          begin
            TableData.ReadField('Int', Data);
            OK := OK and DataRecsSame(Data, SearchData);
          end;
        end;
        if i <> DUP_FACTOR then
          LogTimeIncr('Edge test failed (4)');
        if j <> DUP_FACTOR then
          LogTimeIncr('Edge test failed (5)');

      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    LogTimeIncr('Edge test OK (3)');
  finally
    Trans.CommitAndFree;
  end;
end;

procedure TForm1.FKTestClick(Sender: TObject);
var
  Trans: TMemDBTRansaction;
  DBAPI: TMemAPIDatabase;
  TableMeta: TMemAPITableMetadata;
  TableData: TMemAPITableData;
  Tbl: TMemDBHandle;
  FK: TMemDBHandle;
  FKey: TMemAPIForeignKey;
  Data: TMemDbFieldDataRec;
  Pass: boolean;

  procedure DelRows;
  begin
    //Delete all existing rows, but keep table intact and present.
    Trans := FSession.StartTransaction(amReadWrite, amLazyWrite, ilDirtyRead);
    try
      DBAPI := Trans.GetAPI;
      try
        Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
        TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
        try
          TableData.Locate(TMemAPIPosition.ptFirst);
          while TableData.RowSelected do
          begin
            TableData.Delete;
          end;
        finally
          TableData.Free;
        end;
        Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
        TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
        try
          TableData.Locate(TMemAPIPosition.ptFirst);
          while TableData.RowSelected do
          begin
            TableData.Delete;
          end;
        finally
          TableData.Free;
        end;
      finally
        DBAPI.Free;
      end;
      Trans.CommitAndFree;
      LogTimeIncr('FK test delete rows OK.');
    except
      on E:Exception do
      begin
        Trans.RollbackAndFree;
        LogTimeIncr('FK test delete rows failed.');
        raise;
      end;
    end;
  end;

  procedure SetBaseRowSet;
  var
    i: integer;
    Data: TMemDbFieldDataRec;
  begin
    //Delete all existing rows, but keep table intact and present.
    Trans := FSession.StartTransaction(amReadWrite, amLazyWrite, ilDirtyRead);
    try
      DBAPI := Trans.GetAPI;
      try
        Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
        TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
        try
          Data.FieldType := ftInteger;
          for i := 1 to LIMIT do
          begin
            TableData.Append;
            Data.i32Val := i;
            TableData.WriteField('MasterKey', Data);
            TableData.Post;
          end;
        finally
          TableData.Free;
        end;
        Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
        TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
        try
          Data.FieldType := ftInteger;
          for i := 1 to LIMIT do
          begin
            TableData.Append;
            Data.i32Val := i;
            TableData.WriteField('ReferringField', Data);
            TableData.Post;
          end;
        finally
          TableData.Free;
        end;
      finally
        DBAPI.Free;
      end;
      Trans.CommitAndFree;
      LogTimeIncr('FK test set base row set OK.');
    except
      on E:Exception do
      begin
        Trans.RollbackAndFree;
        LogTimeIncr('FK test set base row set Failed.');
        raise;
      end;
    end;
  end;

begin
  ResetClick(Sender);

  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
      if not Assigned(Tbl) then
      begin
        Tbl := DBAPI.CreateTable('FKTestTableMaster');
        TableMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
        try
          TableMeta.CreateField('MasterKey', ftInteger);
          TableMeta.CreateIndex('MasterKeyIdx', 'MasterKey', [iaUnique, iaNotEmpty]);
        finally
          TableMeta.Free;
        end;
      end;
      Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
      if not Assigned(Tbl) then
      begin
        Tbl := DBAPI.CreateTable('FKTestTableSub');
        TableMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
        try
          TableMeta.CreateField('ReferringField', ftInteger);
          TableMeta.CreateIndex('ReferringFieldIdx', 'ReferringField', [iaNotEmpty]);
        finally
          TableMeta.Free;
        end;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Foreign key test setup failed.');
      raise;
    end;
  end;

  //Now delete all the rows.
  DelRows;

  //Test 1.
  //Add rows satisfying FK relationship to both tables (Base row set).
  SetBaseRowSet;

  //Add FK, check all OK.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      FK := DBAPI.CreateForeignKey('ForeignKey1');
      FKey := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
      try
        FKey.SetReferencingChild('FKTestTableSub', 'ReferringFieldIdx');
        FKey.SetReferencedParent('FKTestTableMaster','MasterKeyIdx');
      finally
        FKey.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 1, add foreign key relationship OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('FK Test 1, add foreign key relationship failed.');
      raise;
    end;
  end;

  //Test 1a.
  //Remove FK.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      DBAPI.DeleteTableOrKey('ForeignKey1');
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('FK Test 1a, delete foreign key relationship failed.');
      raise;
    end;
  end;

  //Add extra row in Referring, re-add FK (all in same transaction).
  Pass := false;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
      TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        TableData.Append;
        Data.FieldType := ftInteger;
        Data.i32Val := LIMIT + 50;
        TableData.WriteField('ReferringField', Data);
        TableData.Post;
      finally
        TableData.Free;
      end;
      FK := DBAPI.CreateForeignKey('ForeignKey1');
      FKey := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
      try
        FKey.SetReferencingChild('FKTestTableSub', 'ReferringFieldIdx');
        FKey.SetReferencedParent('FKTestTableMaster','MasterKeyIdx');
      finally
        FKey.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 1a, foreign key violation (same transaction) failed.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('FK Test 1a, foreign key violation (same transaction) OK.')
      end
      else
      begin
        LogTimeIncr('FK Test 1a, foreign key violation (same transaction) failed.');
        raise;
      end;
    end;
  end;

  //Test 1b.
  //Remove row in master, re-add FK (all in same transaction)
  DelRows;
  SetBaseRowSet;

  Pass := false;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
      TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        TableData.Locate(TMemAPIPosition.ptFirst);
        TableData.Delete;
      finally
        TableData.Free;
      end;
      FK := DBAPI.CreateForeignKey('ForeignKey1');
      FKey := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
      try
        FKey.SetReferencingChild('FKTestTableSub', 'ReferringFieldIdx');
        FKey.SetReferencedParent('FKTestTableMaster','MasterKeyIdx');
      finally
        FKey.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 1b, foreign key violation (same transaction) failed.');
  //Check fails.
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('FK Test 1b, foreign key violation (same transaction) OK.')
      end
      else
      begin
        LogTimeIncr('FK Test 1b, foreign key violation (same transaction) failed.');
        raise;
      end;
    end;
  end;


  //Test2.
  //Set base Row set.
  DelRows;
  SetBaseRowSet;
  //Add FK, check all OK.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      FK := DBAPI.CreateForeignKey('ForeignKey1');
      FKey := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
      try
        FKey.SetReferencingChild('FKTestTableSub', 'ReferringFieldIdx');
        FKey.SetReferencedParent('FKTestTableMaster','MasterKeyIdx');
      finally
        FKey.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
  //Check fails.
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('FK Test 2, add foreign key failed.');
      raise;
    end;
  end;

  //Test 2a.
  //Add a row to referring (different key).
  //Try to commit, check fails.
  Pass := false;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
      TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        TableData.Append;
        Data.FieldType := ftInteger;
        Data.i32Val := LIMIT + 50;
        TableData.WriteField('ReferringField', Data);
        TableData.Post;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 2a, foreign key violation (separate transaction) failed.');
  //Check fails.
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('FK Test 2a, foreign key violation (separate transaction) OK.')
      end
      else
      begin
        LogTimeIncr('FK Test 2a, foreign key violation (separate transaction) failed.');
        raise;
      end;
    end;
  end;

  //Test 2b.
  //Remove a row from master,
  //Try to commit, check fails.
  Pass := false;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
      TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        TableData.Locate(TMemAPIPosition.ptFirst);
        TableData.Delete;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 2b, foreign key violation (separate transaction) failed.');
  //Check fails.
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('FK Test 2b, foreign key violation (separate transaction) OK.')
      end
      else
      begin
        LogTimeIncr('FK Test 2b, foreign key violation (separate transaction) failed.');
        raise;
      end;
    end;
  end;

  //Test 2c.
  //Change referring row to something else (different key)
  //Try to commit, check fails.
  Pass := false;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
      TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        TableData.Locate(ptFirst, '');
        TableData.ReadField('ReferringField', Data);
        Assert(Data.FieldType = ftInteger);
        Inc(Data.i32Val, LIMIT + 50);
        TableData.WriteField('ReferringField', Data);
        TableData.Post;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 2c, foreign key violation (separate transaction) failed.');
  //Check fails.
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('FK Test 2c, foreign key violation (separate transaction) OK.')
      end
      else
      begin
        LogTimeIncr('FK Test 2c, foreign key violation (separate transaction) failed.');
        raise;
      end;
    end;
  end;

  //Test 2d. Change a master row to something else (key not in referring)
  //Try to commit, check fails.
  Pass := false;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
      TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        TableData.Locate(ptFirst, '');
        TableData.ReadField('MasterKey', Data);
        Assert(Data.FieldType = ftInteger);
        Inc(Data.i32Val, LIMIT + 50);
        TableData.WriteField('MasterKey', Data);
        TableData.Post;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 2d, foreign key violation (separate transaction) failed.');
  //Check fails.
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('FK Test 2d, foreign key violation (separate transaction) OK.')
      end
      else
      begin
        LogTimeIncr('FK Test 2d, foreign key violation (separate transaction) failed.');
        raise;
      end;
    end;
  end;

  //Test 2e.
  //Add duplicate row in referring table,
  //check addition set pruned (debugger),
  //FK relationship still satisfied.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
      TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        TableData.Locate(ptFirst, '');
        TableData.ReadField('ReferringField', Data);
        TableData.Append;
        TableData.WriteField('ReferringField', Data);
        TableData.Post;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 2e, dup rows OK.');
  //Check fails.
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('FK Test 2e, dup rows failed.');
      raise;
    end;
  end;

  //Test 2f.
  //Remove existing row in master table,
  //but add new row with same key.
  //check deletion set pruned (debugger).
  //FK relation still holds.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
      TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        TableData.Locate(ptFirst, '');
        TableData.ReadField('MasterKey', Data);
        TableData.Delete;
        TableData.Append;
        TableData.WriteField('MasterKey', Data);
        TableData.Post
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 2d, master key moved OK.');
  //Check fails.
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('FK Test 2d, master key moved failed.');
      raise;
    end;
  end;

  //Test 3.
  //Field / index rearrangement and simul FK add/check.

  //Test 3a.
  //Add extra fields to both tables, in preparation for...
  //Dup row contents, add indexes, and add dup foreign key.
  //Check FK checking OK, even when index being added.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
      TableMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.CreateField('MasterKey2', ftInteger);
      finally
        TableMeta.Free;
      end;
      Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
      TableMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.CreateField('ReferringField2', ftInteger);
      finally
        TableMeta.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 3a, master key add fields OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('FK Test 3a, master key add fields failed.');
      raise;
    end;
  end;

  //Test 3b.
  //Extra fields now added, so ...
  //Dup row contents, add indexes, and add dup foreign key.
  //Check FK checking OK, even when index being added.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
      TableMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        TableMeta.CreateIndex('MasterKey2Idx', 'MasterKey2', [iaUnique, iaNotEmpty]);
        TableData.Locate(ptFirst, '');
        while TableData.RowSelected do
        begin
          TableData.ReadField('MasterKey', Data);
          TableData.WriteField('MasterKey2', Data);
          TableData.Post;
          TableData.Locate(ptNext, '');
        end;
      finally
        TableMeta.Free;
        TableData.Free;
      end;
      Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
      TableMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      TableData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        TableMeta.CreateIndex('ReferringField2Idx', 'ReferringField2', []);
        TableData.Locate(ptFirst, '');
        while TableData.RowSelected do
        begin
          TableData.ReadField('ReferringField', Data);
          TableData.WriteField('ReferringField2', Data);
          TableData.Post;
          TableData.Locate(ptNext, '');
        end;
      finally
        TableMeta.Free;
        TableData.Free;
      end;
      FK := DBAPI.CreateForeignKey('ForeignKey2');
      FKey := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
      try
        FKey.SetReferencingChild('FKTestTableSub', 'ReferringField2Idx');
        FKey.SetReferencedParent('FKTestTableMaster','MasterKey2Idx');
      finally
        FKey.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
      LogTimeIncr('FK Test 3b, master key populate fields OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('FK Test 3b, master key populate fields failed.');
      raise;
    end;
  end;

  //Test 3c.
  //Add data to dependent table whilst deleting fields from master.
  Pass := false;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      FK := DBAPI.OpenTableOrKey('ForeignKey1');
      if Assigned(FK) then
        DBAPI.DeleteTableOrKey('ForeignKey1');

      Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
      TableMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.DeleteIndex('MasterKeyIdx');
        TableMeta.DeleteField('MasterKey');
      finally
        TableMeta.Free;
      end;
      Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
      TableData := DBAPI.GetApiObjectFromHandle(TBL, APITableData) as TMemAPITableData;
      try
        TableData.Append;
        Data.FieldType := ftInteger;
        Data.u32Val := LIMIT + 100;
        TableData.WriteField('ReferringField', Data);
        TableData.WriteField('ReferringField2', Data);
        TableData.Post;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 3c, delete fields whilst FK reverify failed.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('FK Test 3c, delete fields whilst FK reverify OK.')
      end
      else
      begin
        LogTimeIncr('FK Test 3c, delete fields whilst FK reverify failed.');
        raise;
      end;
    end;
  end;

  //Test 3d.
  //Delete data from master table whilst deleting fields from child.
  Pass := false;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      FK := DBAPI.OpenTableOrKey('ForeignKey1');
      if Assigned(FK) then
        DBAPI.DeleteTableOrKey('ForeignKey1');

      Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
      TableMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.DeleteIndex('ReferringFieldIdx');
        TableMeta.DeleteField('ReferringField');
      finally
        TableMeta.Free;
      end;
      Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
      TableData := DBAPI.GetApiObjectFromHandle(TBL, APITableData) as TMemAPITableData;
      try
        TableData.Locate(ptFirst, '');
        TableData.Delete;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 3d, delete fields whilst FK reverify failed.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('FK Test 3d, delete fields whilst FK reverify OK.')
      end
      else
      begin
        LogTimeIncr('FK Test 3d, delete fields whilst FK reverify failed.');
        raise;
      end;
    end;
  end;

  //3e.
  //Do same set of deletes without changing data rows
  //check OK.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      FK := DBAPI.OpenTableOrKey('ForeignKey1');
      if Assigned(FK) then
        DBAPI.DeleteTableOrKey('ForeignKey1');

      Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
      TableMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.DeleteIndex('ReferringFieldIdx');
        TableMeta.DeleteField('ReferringField');
      finally
        TableMeta.Free;
      end;
      Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
      TableMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.DeleteIndex('MasterKeyIdx');
        TableMeta.DeleteField('MasterKey');
      finally
        TableMeta.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 3e, delete multiple indices and fields OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('FK Test 3e, delete multiple indices and fields failed.');
      raise;
    end;
  end;

  //Test4.
  //Check table / index renames - rename *everything*.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('FKTestTableMaster');
      TableMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.RenameField('MasterKey2', 'MasterKey');
        TableMeta.RenameIndex('MasterKey2Idx', 'MasterKeyIdx');
      finally
        TableMeta.Free;
      end;
      Tbl := DBAPI.OpenTableOrKey('FKTestTableSub');
      TableMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.RenameField('ReferringField2', 'ReferringField');
        TableMeta.RenameIndex('ReferringField2Idx', 'ReferringFieldIdx');
      finally
        TableMeta.Free;
      end;
      DBAPI.RenameTableOrKey('FKTestTableMaster', 'FKTestTableMaster_');
      DBAPI.RenameTableOrKey('FKTestTableSub', 'FKTestTableSub_');
      DBAPI.RenameTableOrKey('ForeignKey2', 'ForeignKey1');
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('FK Test 3f, rename everything OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('FK Test 3f, rename everything failed.');
      raise;
    end;
  end;
end;

procedure TForm1.FormCreate(Sender: TObject);
begin
  FTimeStamp := DBStartTime;
  LogTimeIncr('DB load time.');
end;

procedure TForm1.IndexTest2Click(Sender: TObject);
var
  Pass: boolean;
  Trans: TMemDBTransaction;
  DBAPI: TMemAPIDatabase;
  Table: TMemDBHandle;
  TableMeta: TMemAPITableMetadata;
  TableData: TMemAPITableData;
  i: integer;
  Data: TMemDbFieldDataRec;

  procedure DelTable;
  begin
    Trans := FSession.StartTransaction(amReadWrite);
    try
      DBAPI := Trans.GetAPI;
      try
        Table := DBAPI.OpenTableOrKey('IndexTest');
        if Assigned(Table) then
          DBAPI.DeleteTableOrKey('IndexTest');
      finally
        DBAPI.Free;
      end;
      Trans.CommitAndFree;
      LogTimeIncr('IndexTest DelTable OK.');
    except
      on E:Exception do
      begin
        Trans.RollbackAndFree;
        LogTimeIncr('IndexTest DelTable failed.');
        raise;
      end;
    end;
  end;

  procedure DelRows;
  begin
    //Delete all existing rows, but keep table intact and present.
    Trans := FSession.StartTransaction(amReadWrite, amLazyWrite, ilDirtyRead);
    try
      DBAPI := Trans.GetAPI;
      try
        Table := DBAPI.OpenTableOrKey('IndexTest');
        TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
        try
          Pass := TableData.Locate(TMemAPIPosition.ptFirst);
          while TableData.RowSelected do
          begin
            TableData.Delete;
          end;
        finally
          TableData.Free;
        end;
      finally
        DBAPI.Free;
      end;
      Trans.CommitAndFree;
      LogTimeIncr('Index test delete rows OK.');
    except
      on E:Exception do
      begin
        Trans.RollbackAndFree;
        LogTimeIncr('Index test delete rows failed.');
        raise;
      end;
    end;
  end;

begin
  FTimeStamp := Now;
  DelTable;

  //1. Test Index constraint checking on newly created table with added index.
  //1a) Unique values nonzero.
  //1ai) Pass case.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.CreateTable('IndexTest');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.CreateField('Field1', ftInteger);
      finally
        TableMeta.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Index test 1ai failed.');
      raise;
    end;
  end;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('IndexTest');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        TableMeta.CreateIndex('Index1', 'Field1', [iaUnique, iaNotEmpty]);
        for i := 1 to LIMIT do
        begin
          TableData.Append;
          Data.FieldType := ftInteger;
          Data.i32Val := i;
          TableData.WriteField('Field1',Data);
          TableData.Post;
        end;
      finally
        TableMeta.Free;
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Index test 1ai OK.');
  except
    on E:Exception do
    begin
      LogTimeIncr('Index test 1ai failed.');
      Trans.RollbackAndFree;
      raise;
    end;
  end;

  DelTable;

  //1b) Unique values, one zero.
  //1bii) Fail case.
  Pass := false;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.CreateTable('IndexTest');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.CreateField('Field1', ftInteger);
      finally
        TableMeta.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Index test 1bi failed.');
      raise;
    end;
  end;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('IndexTest');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        TableMeta.CreateIndex('Index1', 'Field1', [iaUnique, iaNotEmpty]);
        for i := 0 to LIMIT do
        begin
          TableData.Append;
          Data.FieldType := ftInteger;
          Data.i32Val := i;
          TableData.WriteField('Field1',Data);
          TableData.Post;
        end;
      finally
        TableMeta.Free;
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('Index test 1bi failed.');
  except
    on E:Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('Index test 1bi OK.')
      end
      else
      begin
        LogTimeIncr('Index test 1bi failed.');
        raise;
      end;
    end;
  end;

  DelTable;

  //1c) A duplicate value.
  //1cii) Fail case.
  Pass := false;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.CreateTable('IndexTest');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.CreateField('Field1', ftInteger);
      finally
        TableMeta.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Index test 1ci failed.');
      raise;
    end;
  end;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('IndexTest');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        TableMeta.CreateIndex('Index1', 'Field1', [iaUnique, iaNotEmpty]);
        for i := 1 to LIMIT do
        begin
          TableData.Append;
          Data.FieldType := ftInteger;
          Data.i32Val := i;
          TableData.WriteField('Field1',Data);
          TableData.Post;
        end;
        TableData.Append;
        Data.FieldType := ftInteger;
        Data.i32Val := 7;
        TableData.WriteField('Field1',Data);
        TableData.Post;
      finally
        TableMeta.Free;
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('Index test 1ci failed.');
  except
    on E:Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('Index test 1ci OK.')
      end
      else
      begin
        LogTimeIncr('Index test 1ci failed.');
        raise;
      end;
    end;
  end;


  DelRows;

  //Now, create the index (we expected previous test to fail).
  //... and leave it in place.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('IndexTest');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
      try
        TableMeta.CreateIndex('Index1', 'Field1', [iaUnique, iaNotEmpty]);
      finally
        TableMeta.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Index test, add permanent index OK');
  except
    on E:Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Index test, add permanent index failed');
      raise;
    end;
  end;

  //2. Test Index constraint checking on updated table with previously added index.
  //2a) Unique values nonzero.
  //2ai) Pass case.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('IndexTest');
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        for i := 1 to LIMIT do
        begin
          TableData.Append;
          Data.FieldType := ftInteger;
          Data.i32Val := i;
          TableData.WriteField('Field1',Data);
          TableData.Post;
        end;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Index test 2ai OK.');
  except
    on E:Exception do
    begin
      LogTimeIncr('Index test 2ai failed.');
      Trans.RollbackAndFree;
      raise;
    end;
  end;

  DelRows;

  //2b) Unique values, one zero.
  //2bii) Fail case.
  Pass := false;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('IndexTest');
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        for i := 0 to LIMIT do
        begin
          TableData.Append;
          Data.FieldType := ftInteger;
          Data.i32Val := i;
          TableData.WriteField('Field1',Data);
          TableData.Post;
        end;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('Index test 2bi failed.');
  except
    on E:Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('Index test 2bi OK.')
      end
      else
      begin
        LogTimeIncr('Index test 2bi failed.');
        raise;
      end;
    end;
  end;

  DelRows;

  //2c) A duplicate value.
  //2cii) Fail case.
  Pass := false;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('IndexTest');
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        for i := 1 to LIMIT do
        begin
          TableData.Append;
          Data.FieldType := ftInteger;
          Data.i32Val := i;
          TableData.WriteField('Field1',Data);
          TableData.Post;
        end;
        TableData.Append;
        Data.FieldType := ftInteger;
        Data.i32Val := 7;
        TableData.WriteField('Field1',Data);
        TableData.Post;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('Index test 2ci failed.');
  except
    on E:Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('Index test 2ci OK.')
      end
      else
      begin
        LogTimeIncr('Index test 2ci failed.');
        raise;
      end;
    end;
  end;
end;

//Basic test of indexing.
procedure TForm1.IndexTestClick(Sender: TObject);
var
  Trans: TMemDBTransaction;
  DBAPI: TMemAPIDatabase;
  Table: TMemDBHandle;
  TableMeta: TMemAPITableMetadata;
  TableData: TMemAPITableData;
  Data: TMemDbFieldDataRec;
begin
  BasicTestBtnClick(Sender);
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata)
        as TMemAPITableMetadata;
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData)
        as TMemAPITableData;
      try
        TableMeta.CreateIndex('Index1', 'Int', [iaUnique, iaNotEmpty]);
        TableData.Locate(ptFirst, '');
        TableData.Delete;
        TableData.Append;
        Data.FieldType := ftInteger;
        Data.i32Val := LIMIT + 345;
        TableData.WriteField('Int', Data);
        Data.FieldType := ftUint64;
        Data.u64Val := LIMIT + 345;
        TableData.WriteField('U64', Data);
        Data.FieldType := ftUnicodeString;
        Data.sVal := IntToStr(LIMIT + 345);
        TableData.WriteField('String', Data);
        Data.FieldType := ftDouble;
        Data.dVal := LIMIT + 345;
        TableData.WriteField('Double', Data);
        Data.FieldType := ftGuid;
        Data.gVal := TestGuidFromInt(LIMIT + 345);
        TableData.WriteField('Guid', Data);
        TableData.Post;

        TableData.Locate(ptLast, '');
        TableData.ReadField('Int', Data);
        Inc(Data.i32Val, LIMIT + 37);
        TableData.WriteField('Int', Data);
        TableData.ReadField('U64', Data);
        Inc(Data.u64Val, LIMIT + 37);
        TableData.WriteField('U64', Data);
        TableData.ReadField('String', Data);
        Data.sVal := Data.sVal + IntToStr(LIMIT + 37);
        TableData.WriteField('String', Data);
        TableData.ReadField('Double', Data);
        Data.dVal := Data.dVal + LIMIT + 37;
        TableData.WriteField('Double', Data);
        Data.FieldType := ftGuid;
        Data.gVal := TestGuidFromInt(LIMIT + 37);
        TableData.WriteField('Guid', Data);
        TableData.Post;

      finally
        TableMeta.Free;
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Table add data with index OK');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Table add data with index failed: ' + E.Message);
      raise;
    end;
  end;

  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata)
        as TMemAPITableMetadata;
      try
        TableMeta.CreateIndex('Index2', 'U64', [iaUnique, iaNotEmpty]);
        TableMeta.CreateIndex('Index3', 'String', [iaUnique, iaNotEmpty]);
        TableMeta.CreateIndex('Index4', 'Double', [iaUnique, iaNotEmpty]);
        TableMeta.CreateIndex('Index5', 'Guid', [iaUnique, iaNotEmpty]);
        TableMeta.CreateIndex('Index2Fourth', 'U64', []);
        TableMeta.CreateIndex('Index2Fifth', 'U64', []);
      finally
        TableMeta.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Table add multiple indices OK');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Table add multiple indices failed: ' + E.Message);
      raise;
    end;
  end;

    //Delete index and associated field.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata)
        as TMemAPITableMetadata;
      try
        TableMeta.DeleteIndex('Index1');
        TableMeta.DeleteField('Int');
      finally
        TableMeta.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Table delete index and field OK');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Table delete index and field failed: ' + E.Message);
      raise;
    end;
  end;

    //Delete field whilst adding indices.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('Test table');
      TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata)
        as TMemAPITableMetadata;
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData)
        as TMemAPITableData;
      try
        //Add /delete indices at same time as add/deleting deleting field.
        TableMeta.DeleteIndex('Index2');
        TableMeta.DeleteIndex('Index2Fourth');
        TableMeta.DeleteIndex('Index2Fifth');
        TableMeta.DeleteField('U64');
        TableMeta.CreateField('U64Second', ftUint64);
        TableMeta.CreateIndex('Index2Second', 'U64Second', []);
        TableMeta.CreateIndex('Index2Third', 'U64Second', []);
      finally
        TableMeta.Free;
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Table add/delete multiple indexes simultaneously OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Table add/delete multiple indexes simultaneously failed: ' + E.Message);
      raise;
    end;
  end;
end;

procedure TForm1.MFFKeyTestClick(Sender: TObject);

var
  Trans:TMemDBTransaction;
  Pass: boolean;

  procedure PopulateRowsInTransaction;
  var
    i,j,k: integer;
    TableData1, TableData2: TMemAPITableData;
    Table1, Table2: TMemDBHandle;
    Data: TMemDbFieldDataRec;
    DBAPI: TMemAPIDatabase;

  begin
    DBAPI := Trans.GetAPI;
    try
      Table1 := DBAPI.OpenTableOrKey('FKTestTableMaster');
      Table2 := DBAPI.OpenTableOrKey('FKTestTableSub');
      TableData1 := DBAPI.GetApiObjectFromHandle(Table1, APITableData) as TMemAPITableData;
      TableData2 := DBAPI.GetApiObjectFromHandle(Table2, APITableData) as TMemAPiTableData;
      try
        for i := 1 to LIMIT_CUBEROOT do
          for j := 1 to LIMIT_CUBEROOT do
            for k := 1 to LIMIT_CUBEROOT do
            begin
              TableData1.Append;
              TableData2.Append;
              Data.FieldType := ftInteger;
              Data.i32Val := i;
              TableData1.WriteField('MasterKey1', Data);
              TableData2.WriteField('ReferringField1', Data);
              Data.FieldType := ftUnicodeString;
              Data.sVal := IntToStr(j);
              TableData1.WriteField('MasterKey2',Data);
              TableData2.WriteField('ReferringField2', Data);
              Data.FieldType := ftGuid;
              FillChar(Data.gVal, sizeof(Data.gVal),0);
              Data.gVal.D3 := k;
              TableData1.WriteField('MasterKey3', Data);
              TableData2.WriteField('ReferringField3', Data);
              TableData1.Post;
              TableData2.Post;
            end;
      finally
        TableData1.Free;
        TableData2.Free;
      end;
    finally
      DBAPI.Free;
    end;
  end;

  procedure CreateTablesInTransaction;
  var
    FNames: TMDBFieldNames;
    DBAPI: TMemAPIDatabase;
    TableMeta: TMemAPITableMetadata;
    Table: TMemDBHandle;
  begin
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('FKTestTableMaster');
      if not Assigned(Table) then
      begin
        Table := DBAPI.CreateTable('FKTestTableMaster');
        TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
        try
          //Fields in different order in tables, for checking.
          TableMeta.CreateField('MasterKey2', ftUnicodeString);
          TableMeta.CreateField('MasterKey1', ftInteger);
          TableMeta.CreateField('MasterKey3', ftGuid);
          SetLength(FNames,2);
          FNames[0] := 'MasterKey1';
          FNames[1] := 'MasterKey2';
          TableMeta.CreateMultiFieldIndex('MasterKeyIdx2', FNames, [iaNotEmpty]);
          FNames[1] := 'MasterKey3';
          TableMeta.CreateMultiFieldIndex('MasterKeyIdx3', FNames, [iaNotEmpty]);
          SetLength(FNames,3);
          FNames[0] := 'MasterKey1';
          FNames[1] := 'MasterKey2';
          FNames[2] := 'MasterKey3';
          TableMeta.CreateMultiFieldIndex('MasterKeyIdx1', FNames, [iaUnique, iaNotEmpty]);
        finally
          TableMeta.Free;
        end;
      end;
      Table := DBAPI.OpenTableOrKey('FKTestTableSub');
      if not Assigned(Table) then
      begin
        Table := DBAPI.CreateTable('FKTestTableSub');
        TableMeta := DBAPI.GetApiObjectFromHandle(Table, APITableMetadata) as TMemAPITableMetadata;
        try
          //Fields in different order in tables, for checking.
          TableMeta.CreateField('ReferringField1', ftInteger);
          TableMeta.CreateField('ReferringField3', ftGuid);
          TableMeta.CreateField('ReferringField2', ftUnicodeString);
          SetLength(FNames, 2);
          FNames[0] := 'ReferringField1';
          FNames[1] := 'ReferringField2';
          TableMeta.CreateMultiFieldIndex('ReferringIdx2', FNames, [iaNotEmpty]);
          FNames[0] := 'ReferringField3';
          TableMeta.CreateMultiFieldIndex('ReferringIdx3', FNames, [iaNotEmpty]);
          TableMeta.CreateIndex('ExtraIndex', 'ReferringField1', []);
          SetLength(FNames,3);
          FNames[0] := 'ReferringField1';
          FNames[1] := 'ReferringField2';
          FNames[2] := 'ReferringField3';
          TableMeta.CreateMultiFieldIndex('ReferringIdx1', FNames, [iaNotEmpty]);
        finally
          TableMeta.Free;
        end;
      end;
    finally
      DBAPI.Free;
    end;
  end;

  procedure CreateFKeysInTransaction;
  var
    DBAPI: TMemAPIDatabase;
    FKHandle: TMemDBHandle;
    FKMeta: TMemAPIForeignKey;

  begin
    DBAPI := Trans.GetAPI;
    try
      FKHandle := DBAPI.OpenTableOrKey('FKTest1');
      if not Assigned(FKHandle) then
      begin
        FKHandle := DBAPI.CreateForeignKey('FKTest1');
        FKMeta := DBAPI.GetApiObjectFromHandle(FKHandle, APIForeignKey) as TMemAPIForeignKey;
        try
          FKMeta.SetReferencedParent('FKTestTableMaster','MasterKeyIdx1');
          FKMeta.SetReferencingChild('FKTestTableSub', 'ReferringIdx1');
        finally
          FKMeta.Free;
        end;
      end;
    finally
      DBAPI.Free;
    end;
  end;

  procedure AddMasterVal(i: integer);
  var
    TableData: TMemAPITableData;
    Table: TMemDBHandle;
    Data: TMemDbFieldDataRec;
    DBAPI: TMemAPIDatabase;

  begin
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('FKTestTableMaster');
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        TableData.Append;
        Data.FieldType := ftInteger;
        Data.i32Val := i;
        TableData.WriteField('MasterKey1', Data);
        Data.FieldType := ftUnicodeString;
        Data.sVal := IntToStr(i);
        TableData.WriteField('MasterKey2',Data);
        Data.FieldType := ftGuid;
        FillChar(Data.gVal, sizeof(Data.gVal),0);
        Data.gVal.D3 := i;
        TableData.WriteField('MasterKey3', Data);
        TableData.Post;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
  end;

  procedure AddSubVal(i: integer);
  var
    TableData: TMemAPITableData;
    Table: TMemDBHandle;
    Data: TMemDbFieldDataRec;
    DBAPI: TMemAPIDatabase;

  begin
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('FKTestTableSub');
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        TableData.Append;
        Data.FieldType := ftInteger;
        Data.i32Val := i;
        TableData.WriteField('ReferringField1', Data);
        Data.FieldType := ftUnicodeString;
        Data.sVal := IntToStr(i);
        TableData.WriteField('ReferringField2',Data);
        Data.FieldType := ftGuid;
        FillChar(Data.gVal, sizeof(Data.gVal),0);
        Data.gVal.D3 := i;
        TableData.WriteField('ReferringField3', Data);
        TableData.Post;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
  end;


  procedure AddMasterUnique;
  begin
    AddMasterVal(LIMIT_CUBEROOT + 1);
  end;

  procedure DelMasterVal(i: integer);
  var
    TableData: TMemAPITableData;
    Table: TMemDBHandle;
    DataRecs: TMemDbFieldDataRecs;
    DBAPI: TMemAPIDatabase;

  begin
    DBAPI := Trans.GetAPI;
    try
      Table := DBAPI.OpenTableOrKey('FKTestTableMaster');
      TableData := DBAPI.GetApiObjectFromHandle(Table, APITableData) as TMemAPITableData;
      try
        SetLength(DataRecs, 3);
        DataRecs[0].FieldType := ftInteger;
        DataRecs[1].FieldType := ftUnicodeString;
        DataRecs[2].FieldType := ftGuid;
        DataRecs[0].i32Val := i;
        DataRecs[1].sVal := IntToStr(i);
        FillChar(DataRecs[2].gVal, sizeof(DataRecs[2].gVal), 0);
        DataRecs[2].gVal.D3 := i;
        TableData.FindByMultiFieldIndex('MasterKeyIdx1', DataRecs);
        TableData.Delete;
      finally
        TableData.Free;
      end;
    finally
      DBAPI.Free;
    end;
  end;

  procedure RemoveMasterUnique;
  begin
    DelMasterVal(LIMIT_CUBEROOT + 1);
  end;

  procedure AddMasterDup;
  begin
    AddMasterVal(5);
  end;

  procedure RemoveMasterDup;
  begin
    DelMasterVal(5);
  end;

  procedure AddSubPresent;
  begin
    AddSubVal(5);
  end;

  procedure AddSubNewUnique;
  begin
    AddSubVal(LIMIT_CUBEROOT + 1);
  end;

  procedure AddSubUtterlyUnique;
  begin
    AddSubVal(LIMIT_CUBEROOT + 2);
  end;

begin
  ResetClick(Sender);
  try
    Trans := FSession.StartTransaction(amReadWrite);
    try
      CreateTablesInTransaction;
      CreateFKeysInTransaction;
      Trans.CommitAndFree;
    except
      Trans.RollbackAndFree;
      raise;
    end;
    Trans := FSession.StartTransaction(amReadWrite);
    try
      PopulateRowsInTransaction;
      Trans.CommitAndFree;
    except
      Trans.RollbackAndFree;
      raise;
    end;
    LogTimeIncr('1: Foreign key MF test (Add keys before data) OK.');
  except
    on E: Exception do
    begin
      LogTimeIncr('1: Foreign key test (Add keys before data) failed:' + E.Message);
      raise;
    end;
  end;

  ResetClick(Sender);
  try
    Trans := FSession.StartTransaction(amReadWrite);
    try
      CreateTablesInTransaction;
      Trans.CommitAndFree;
    except
      Trans.RollbackAndFree;
      raise;
    end;
    Trans := FSession.StartTransaction(amReadWrite);
    try
      PopulateRowsInTransaction;
      CreateFKeysInTransaction;
      Trans.CommitAndFree;
    except
      Trans.RollbackAndFree;
      raise;
    end;
    LogTimeIncr('2: Foreign key MF test (Add data before keys) OK.');
  except
    on E: Exception do
    begin
      LogTimeIncr('2: Foreign key test (Add data before keys) failed:' + E.Message);
      raise;
    end;
  end;

  //3. Add item in referred, unique from all others. Expect OK.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    AddMasterUnique;
    Trans.CommitAndFree;
    LogTimeIncr('3. Added new unique row to master table OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('3. Added new unique row to master table failed: ' + E.Message);
      raise;
    end;
  end;

  //4. Remove new unique item. Expect OK.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    RemoveMasterUnique;
    Trans.CommitAndFree;
    LogTimeIncr('4. Removed new unique row from master table OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('4. Removed new unique row from master table failed:' + E.Message);
      raise;
    end;
  end;

  //5. Add item in referred, already present, expect fail.
  Trans := FSession.StartTransaction(amReadWrite);
  Pass := false;
  try
    AddMasterDup;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('5. Add duplicate item to master table foreign key Failed.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('5. Add duplicate item to master table foreign key OK.')
      end
      else
      begin
        LogTimeIncr('5. Add duplicate item to master table foreign key Failed: ' + E.Message);
        raise;
      end;
    end;
  end;

  //6. Re-add new unique item, Expect OK.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    AddMasterUnique;
    Trans.CommitAndFree;
    LogTimeIncr('6. Re add unique item to master table OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('6. Re add unique item to master table failed:' + E.Message);
      raise;
    end;
  end;

  //7. Remove item in referred, already present, expect FK fail.
  Trans := FSession.StartTransaction(amReadWrite);
  Pass := false;
  try
    RemoveMasterDup;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('7. Remove item in master required by FK failed.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('7. Remove item in master required by FK OK.')
      end
      else
      begin
        LogTimeIncr('7. Remove item in master required by FK failed:' + E.Message);
        raise;
      end;
    end;
  end;

  //8. Add item in referring same as already present, expect OK.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    AddSubPresent;
    Trans.CommitAndFree;
    LogTimeIncr('8. Add duplicate item in referring OK');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('8. Add duplicate item in referring failed:' + E.Message);
      raise;
    end;
  end;

  //9. Add item in referring same as new unique, expect OK.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    AddSubNewUnique;
    Trans.CommitAndFree;
    LogTimeIncr('9. Add new item in referring, same as new master val OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('9. Add new item in referring, same as new master val failed:' + E.Message);
      raise;
    end;
  end;

  //10. Add item in referring diff from all in master, expect fail.
  Trans := FSession.StartTransaction(amReadWrite);
  Pass := false;
  try
    AddSubUtterlyUnique;
    Pass := true;
    Trans.CommitAndFree;
    LogTimeIncr('10. Add new item in referring, master key not present Failed.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      if Pass then
      begin
        LogTimeIncr('10. Add new item in referring, master key not present OK.')
      end
      else
      begin
        LogTimeIncr('10. Add new item in referring, master key not present Failed: '+ E.Message);
        raise;
      end;
    end;
  end;
end;

procedure TForm1.MFIndexTestClick(Sender: TObject);
var
  Trans:TMemDBTransaction;
  DBAPI: TMemAPIDatabase;
  Tbl: TMemDBHandle;
  TblMeta: TMemAPITableMetadata;
  TblData: TMemAPITableData;
  i: integer;
  Data: TMemDbFieldDataRec;
  DataRecs: TMemDbFieldDataRecs;
  IndexedFields: TMDBFieldNames;
  Ret, Pass: boolean;

begin
  ResetClick(Sender);
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.CreateTable('Test table');
      TblMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      try
        TblMeta.CreateField('U64', ftUint64);
        TblMeta.CreateField('IntLo', ftInteger);
        TblMeta.CreateField('IntHi', ftInteger);
        TblMeta.CreateField('String', ftUnicodeString);
        TblMeta.CreateIndex('I64', 'U64', [iaUnique, iaNotEmpty]);
        SetLength(IndexedFields, 2);
        IndexedFields[0] := 'IntHi';
        IndexedFields[1] := 'IntLo';
        TblMeta.CreateMultiFieldIndex('IntComp', IndexedFields, [iaUnique, iaNotEmpty]);
      finally
        TblMeta.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('MF Indexes, table setup failed.: ' + E.Message);
      raise;
    end;
  end;
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('Test table');
      TblData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        FillChar(Data, sizeof(Data), 0);
        for i := 1 to LIMIT do
        begin
          TblData.Append;
          Data.FieldType := ftInteger;
          Data.i32Val := i mod 10;
          TblData.WriteField('IntLo', Data);
          Data.i32Val := i div 10;
          TblData.WriteField('IntHi', Data);
          Data.FieldType := ftUInt64;
          Data.u64Val := i;
          TblData.WriteField('U64', Data);
          Data.FieldType := ftUnicodeString;
          Data.sVal := IntToStr(i);
          TblData.WriteField('String', Data);
          TblData.Post;
        end;
      finally
        TblData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('MF Indexes, table populate OK');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('MF Indexes, table populate failed.: ' + E.Message);
      raise;
    end;
  end;
  //Check basic indexing traversal.
  Pass := true;
  Trans := FSession.StartTransaction(amRead);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('Test table');
      TblData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        i := 1;
        Ret := TblData.Locate(ptFirst, 'I64');
        while Ret do
        begin
          TblData.ReadField('U64', Data);
          if Data.u64Val <> i then
            Pass := false;
          TblData.ReadField('IntHi', Data);
          if Data.i32Val <> i div 10 then
            Pass := false;
          TblData.ReadField('IntLo', Data);
          if Data.i32Val <> i mod 10 then
            Pass := false;
          Ret := TblData.Locate(ptNext, 'I64');
          Inc(i);
        end;
        i := 1;
        Ret := TblData.Locate(ptFirst, 'IntComp');
        while Ret do
        begin
          TblData.ReadField('U64', Data);
          if Data.u64Val <> i then
            Pass := false;
          TblData.ReadField('IntHi', Data);
          if Data.i32Val <> i div 10 then
            Pass := false;
          TblData.ReadField('IntLo', Data);
          if Data.i32Val <> i mod 10 then
            Pass := false;
          Ret := TblData.Locate(ptNext, 'IntComp');
          Inc(i);
        end;
      finally
        TblData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    if Pass then
    begin
      LogTimeIncr('MF Indexes, traversal OK.')
    end
    else
    begin
      LogTimeIncr('MF Indexes, traversal Failed.')
    end;
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('MF Indexes, traversal failed.: ' + E.Message);
      raise;
    end;
  end;
  //Check couple of random index find. examples.
  Pass := true;
  Trans := FSession.StartTransaction(amRead);
  try
    DBAPI := Trans.GetAPI;
    try
      Tbl := DBAPI.OpenTableOrKey('Test table');
      TblData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
      try
        i := 1;
        SetLength(DataRecs, 2);
        while i <= LIMIT do
        begin
          DataRecs[0].FieldType := ftInteger;
          DataRecs[0].i32Val := i div 10; //First field is IntHi.
          DataRecs[1].FieldType := ftInteger;
          DataRecs[1].i32Val := i mod 10; //First field is IntLo.
          Ret := TblData.FindByMultiFieldIndex('IntComp', DataRecs);
          if not Ret then
            Pass := false;
          TblData.ReadField('U64', Data);
          if (Data.FieldType <> ftUint64) or (Data.u64Val <> i) then
            Pass := false;
          Inc(i, 3); //Would pick a random number, but I like repeatable tests.
        end;
      finally
        TblData.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    if Pass then
    begin
      LogTimeIncr('MF Indexes, find OK.')
    end
    else
    begin
      LogTimeIncr('MF Indexes, find Failed.')
    end;
  except
    on E:Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('MF Indexes, find failed.: ' + E.Message);
      raise;
    end;
  end;
  //Check index Nonzero constraint, and unique constraints.
  Pass := false;
  for i := 0 to 1 do
  begin
    Trans := FSession.StartTransaction(amReadWrite);
    try
      DBAPI := Trans.GetAPI;
      try
        Tbl := DBAPI.OpenTableOrKey('Test table');
        TblData := DBAPI.GetApiObjectFromHandle(Tbl, APITableData) as TMemAPITableData;
        try
          SetLength(DataRecs, 2);
          DataRecs[0].FieldType := ftInteger;
          DataRecs[0].i32Val := 7;
          DataRecs[1].FieldType := ftInteger;
          DataRecs[1].i32Val := 4;
          Ret := TblData.FindByMultiFieldIndex('IntComp', DataRecs);
          if not Ret then
            raise EMemDBTestException.Create('Expected find op to work.');
          Data := DataRecs[0];
          if i = 0 then
            Data.i32Val := 0
          else
            Data.i32Val := 7;
          TblData.WriteField('IntLo', Data);
          TblData.WriteField('IntHi', Data);
          TblData.Post;
        finally
          TblData.Free;
        end;
      finally
        DBAPI.Free;
      end;
      Pass := true;
      Trans.CommitAndFree;
      LogTimeIncr('MF Index constraints failed.');
    except
      on E:Exception do
      begin
        Trans.RollbackAndFree;
        if Pass then
        begin
          LogTimeIncr('MF Indexes, constraints: (' + E.Message +'): OK')
        end
        else
        begin
          LogTimeIncr('MF Indexes, constraints: (' + E.Message +'): failed.');
          raise;
        end;
      end;
    end;
  end;
end;

procedure TForm1.MiniCommitClick(Sender: TObject);

const
  MAGIC_FIELD_VALUE = $1234ABCD;
  MAGIC_FIELD_VALUE_2 = $12ABCD34;

var
  Trans: TMemDBTransaction;
  Handle: TMemDbHandle;
  DBAPI: TMemAPIDatabase;
  TblMeta: TMemAPITableMetadata;
  DbSync: TMemAPIUserTransactionControl;
  AddLater: boolean;
  Value: integer;
  Bookmark: TMemDbBookMark;

  procedure AddARow;
  var
    TblData: TMemAPITableData;
    Data: TMemDbFieldDataRec;
  begin
    TblData := DBAPI.GetApiObjectFromHandle(Handle, APITableData) as TMemAPITableData;
    try
      TblData.Append;
      Data.FieldType := ftInteger;
      Data.i32Val := MAGIC_FIELD_VALUE;
      TblData.WriteField('IntField', Data);
      TblData.Post;
    finally
      TblData.Free;
    end;
  end;

  function ReadTheRow: integer;
  var
    TblData: TMemAPITableData;
    Data: TMemDbFieldDataRec;
  begin
    TblData := DBAPI.GetApiObjectFromHandle(Handle, APITableData) as TMemAPITableData;
    try
      TblData.Locate(ptFirst, '');
      TblData.ReadField('IntField', Data);
      Assert(Data.FieldType = ftInteger);
      Result := Data.i32Val;
    finally
      TblData.Free;
    end;
  end;

  procedure ChangeRowData;
  var
    TblData: TMemAPITableData;
    Data: TMemDbFieldDataRec;
  begin
    TblData := DBAPI.GetApiObjectFromHandle(Handle, APITableData) as TMemAPITableData;
    try
      TblData.Locate(ptFirst, '');
      TblData.ReadField('IntField', Data);
      Assert(Data.FieldType = ftInteger);
      Data.i32Val := MAGIC_FIELD_VALUE_2;
      TblData.WriteField('IntField', Data);
      TblData.Post;
    finally
      TblData.Free;
    end;
  end;

  procedure CreateTheTable;
  begin
    Handle := DBAPI.CreateTable('Test table');
    TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
    try
      TblMeta.CreateField('IntField', ftInteger);
      //OK, now try to add data simultaneously, expect this to fail.
    finally
      TblMeta.Free;
    end;
  end;

begin
  ResetClick(Sender);
  AddLater := false;
  //Firstly, see if Mini-commit allows you to create table, and pop with data.
  //No "special" exception handling or retryable exceptions yet, just whether
  //mini-commit allows you to create a multi-transaction.
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      CreateTheTable;
      try
        AddARow;
      except
        on EMemDBException do
        begin
          DbSync := DBAPI.GetApiObject(APIUserTransactionControl) as TMemAPIUserTransactionControl;
          try
            DbSync.MiniCommit;
          finally
            DBSync.Free;
          end;
          AddLater := true;
        end;
      end;
    if AddLater then
      AddARow;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    if AddLater then
      LogTimeIncr('Mini-commit / multi changeset (1) OK.')
    else
      LogTimeIncr('Mini-commit / multi changeset (1) Failed.')
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Mini-commit / multi changeset (1) Failed.');
    end;
  end;
  //OK, now check that you can stop and restart the DB, and the journal
  //replays over the multi-transaction OK.

  LogTimeIncr('Journal replay over multi-changeset');
  try
    FSession.Free;
    FDB.StopDB;
    FDB.InitDB(DB_LOCATION, jtV2);
    FSession := FDB.StartSession;
    LogTimeIncr('Journal replay over multi-changeset OK');
  except
    on E: Exception do
    begin
      LogTimeIncr('Journal replay over multi-changeset failed.');
      raise;
    end;
  end;

  //OK, now check we can read back the magic value.
  LogTimeIncr('Check readback.');
  Trans := FSession.StartTransaction(amRead);
  try
    DBAPI := Trans.GetAPI;
    try
      Handle := DBAPI.OpenTableOrKey('Test table');
      Value := ReadTheRow;
      if Value = MAGIC_FIELD_VALUE then
        LogTimeIncr('Readback OK')
      else
        LogTimeIncr('Readback Failed.');
    finally
      DBAPI.Free;
      Trans.RollbackAndFree;
    end;
  except
    on E: Exception do
    begin
      LogTimeIncr('Readback Failed.');
      raise;
    end;
  end;

  //OK, Now try multiple mini-commits, and mini-rollback one at a time.
  ResetClick(Sender);
  LogTimeIncr('Mini-commit and mini rollback...');
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      DbSync := DBAPI.GetApiObject(APIUserTransactionControl) as TMemAPIUserTransactionControl;
      try
        CreateTheTable;
        DbSync.MiniCommit;
        AddARow;
        DbSync.MiniCommit;
        ChangeRowData;
        DbSync.MiniCommit;
        //Expect latest value.
        Value := ReadTheRow;
        if Value = MAGIC_FIELD_VALUE_2 then
          LogTimeIncr('Readback miniCR (1) OK')
        else
          LogTimeIncr('Readback miniCR (1) Failed.');
        DBSync.MiniRollback();
        //Expect older value.
        Value := ReadTheRow;
        if Value = MAGIC_FIELD_VALUE then
          LogTimeIncr('Readback miniCR (2) OK')
        else
          LogTimeIncr('Readback miniCR (2) Failed.');
        DBSync.MiniRollback();
        //Expect no row in table.
        try
          Value := ReadTheRow;
          LogTimeIncr('Readback miniCR (3) Failed')
        except
          on Exception do
            LogTimeIncr('Readback miniCR (3) OK')
        end;
        DBSync.MiniRollback();
        //Expect no table.
        Handle := DBAPI.OpenTableOrKey('Test table');
        if Assigned(Handle) then
          LogTimeIncr('Readback miniCR (4) Failed.')
        else
          LogTimeIncr('Readback miniCR (4) OK.');
      finally
        DbSync.Free;
      end;
    finally
      DBAPI.Free;
    end;
    //Have rolled back so far here that this should not make any changes (check in debugger).
    Trans.CommitAndFree;
    LogTimeIncr('Readback miniCR OK')
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Readback miniCR Failed');
      raise;
    end;
  end;

  //And now try multi-level rollback.
  ResetClick(Sender);
  LogTimeIncr('Multi-level rollback...');
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      DbSync := DBAPI.GetApiObject(APIUserTransactionControl) as TMemAPIUserTransactionControl;
      try
        CreateTheTable;
        DbSync.MiniCommit;
        AddARow;
        DbSync.MiniCommit;
        ChangeRowData;
        DbSync.MiniCommit;
        ChangeRowData; //So buffered, but don't mini-commit.
      finally
        DbSync.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.RollbackAndFree;
    LogTimeIncr('Multi-level rollback OK')
  except
    on E: Exception do
    begin
      LogTimeIncr('Multi-level rollback failed.');
      raise;
    end;
  end;

  //And now bookmark.
  ResetClick(Sender);
  LogTimeIncr('Bookmark and rollback...');
  Trans := FSession.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      DbSync := DBAPI.GetApiObject(APIUserTransactionControl) as TMemAPIUserTransactionControl;
      try
        CreateTheTable;
        DbSync.MiniCommit;

        AddARow;
        Bookmark := DbSync.Bookmark;
        ChangeRowData;

        Value := ReadTheRow;
        if Value = MAGIC_FIELD_VALUE_2 then
          LogTimeIncr('Bookmark (1) OK')
        else
          LogTimeIncr('Bookmark (1) Failed.');

        DbSync.MiniCommit;

        Value := ReadTheRow;
        if Value = MAGIC_FIELD_VALUE_2 then
          LogTimeIncr('Bookmark (2) OK')
        else
          LogTimeIncr('Bookmark (2) Failed.');

        DbSync.MiniRollbackToBookmark(Bookmark);

        Value := ReadTheRow;
        if Value = MAGIC_FIELD_VALUE then
          LogTimeIncr('Bookmark (3) OK')
        else
          LogTimeIncr('Bookmark (3) Failed.');

      finally
        DbSync.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.RollbackAndFree;
    LogTimeIncr('Rollback after bookmark OK');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Rollback after bookmark failed.');
      raise;
    end;
  end;
end;

procedure TForm1.ResetClick(Sender: TObject);
var
  Trans: TMemDBTransaction;
  DBAPI: TMemAPIDatabase;
  Handle: TMemDBHandle;
  TestAPI: TMemDBAPI;
  Strings: TStringList;
  i: integer;
begin
  FTimeStamp := Now;
  Trans := FSession.StartTransaction(amReadWrite, amLazyWrite, ilCommittedRead);
  try
    DBAPI := Trans.GetAPI;
    try
      Strings := DBAPI.GetEntityNames;
      try
        //Delete the foreign keys first.
        for i := 0 to Pred(Strings.Count) do
        begin
          Handle := DBAPI.OpenTableOrKey(Strings[i]);
          TestAPI := DBAPI.GetApiObjectFromHandle(Handle, APIForeignKey, false);
          if Assigned(TestAPI) then
          begin
            TestAPI.Free;
            DBAPI.DeleteTableOrKey(Strings[i]);
          end;
        end;
        //And now the tables.
        for i := 0 to Pred(Strings.Count) do
        begin
          Handle := DBAPI.OpenTableOrKey(Strings[i]);
          TestAPI := DBAPI.GetApiObjectFromHandle(Handle, APITableData, false);
          if Assigned(TestAPI) then
          begin
            TestAPI.Free;
            DBAPI.DeleteTableOrKey(Strings[i]);
          end;
        end;
      finally
        Strings.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
    LogTimeIncr('Reset state OK.');
  except
    on E: Exception do
    begin
      Trans.RollbackAndFree;
      LogTimeIncr('Reset state failed.');
      raise;
    end;
  end;
end;

procedure TForm1.SmallTransClick(Sender: TObject);
var
  idx: integer;
  Trans: TMemDBTransaction;
begin
  ResetClick(Sender);
  try
    for Idx := 0 to Pred(TRANS_LIMIT) do
    begin
      Trans := FSession.StartTransaction(amReadWrite);
      try
        Trans.CommitAndFree;
      except
        on E: Exception do
        begin
          Trans.RollbackAndFree;
          raise;
        end;
      end;
    end;
    LogTimeIncr('Many small transactions OK');
  except
    on E: Exception do
    begin
      LogTimeIncr('Many small transactions failed: ' + E.Message);
      raise;
    end;
  end;
end;

initialization
  LIMIT_CUBEROOT := Trunc(Math.Power(LIMIT, (1/3)));
  if LIMIT_CUBEROOT < 2 then
    LIMIT_CUBEROOT := 2;
  FDB := TMemDB.Create;
  DBStartTime := Now;
  FDB.InitDB(DB_LOCATION, jtV2);
  FSession := FDB.StartSession;
finalization
  FSession.Free;
  FDB.Free;
end.
