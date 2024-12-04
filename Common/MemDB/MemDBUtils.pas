unit MemDBUtils;

{ Martin Harvey 2019. Some top level utility routines }

interface

uses
  MemDB, MemDBAPI, MEMDBMisc, Classes;

procedure CopyDatabase(Src: TMemDB; Dst: TMemDB);

implementation

procedure ClearDestinationDB(Session: TMemDBSession);
var
  Trans: TMemDBTransaction;
  EntityNames: TStringList;
  DBAPI: TMemAPIDatabase;
  Entity: TMemDBHandle;
  FKAPI: TMemAPIForeignKey;
  idx: integer;
begin
  Trans := Session.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      //Get a list of top level entities.
      EntityNames := DBAPI.GetEntityNames;
      try
        //Delete all foreign keys.
        for idx := 0 to Pred(EntityNames.Count) do
        begin
          Entity := DBAPI.OpenTableOrKey(EntityNames[idx]);
          FKAPI := DBAPI.GetApiObjectFromHandle(Entity, APIForeignKey, false) as TMemAPIForeignKey;
          if Assigned(FKAPI) then
          begin
            DBAPI.DeleteTableOrKey(EntityNames[idx]);
            FKAPI.Free;
          end;
        end;
      finally
        EntityNames.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
  except
    Trans.RollbackAndFree;
    raise;
  end;
  Trans := Session.StartTransaction(amReadWrite);
  try
    DBAPI := Trans.GetAPI;
    try
      //Get a list of top level entities.
      EntityNames := DBAPI.GetEntityNames;
      try
        //Delete all other entities
        for idx := 0 to Pred(EntityNames.Count) do
          DBAPI.DeleteTableOrKey(EntityNames[idx]);
      finally
        EntityNames.Free;
      end;
    finally
      DBAPI.Free;
    end;
    Trans.CommitAndFree;
  except
    Trans.RollbackAndFree;
    raise;
  end;
end;

procedure CopyTable(SrcAPI: TMemAPIDatabase;
                    SrcEntity: TMemDBHandle;
                    EntityName: string;
                    DstSess: TMemDBSession);
var
  SrcTableMeta: TMemAPITableMetadata;
  SrcTableData: TMemAPITableData;
  DstTableMeta: TMemAPITableMetadata;
  DstTableData: TMemAPITableData;
  DstTrans: TMemDBTransaction;
  DstEntity: TMemDBHandle;
  DstAPI: TMemAPIDatabase;
  idx: integer;
  SList: TStringList;
  FieldType: TMDBFieldType;
  OK: boolean;
  GotRow: boolean;
  IndexedFields: TMDBFieldNames;
  IndexAttrs: TMDBIndexAttrs;
  Data: TMemDbFieldDataRec;
begin
  SrcTableMeta := SrcAPI.GetApiObjectFromHandle(SrcEntity, APITableMetadata) as TMemAPITableMetadata;
  SrcTableData := SrcAPI.GetApiObjectFromHandle(SrcEntity, APITableData) as TMemAPITableData;
  try
    //First, create destination table, and add same fields and indexes as source.
    DstTrans := DstSess.StartTransaction(amReadWrite);
    try
      DstAPI := DstTrans.GetAPI;
      try
        DstEntity := DstAPI.CreateTable(EntityName);
        DstTableMeta := DstAPI.GetApiObjectFromHandle(DstEntity, APITableMetadata) as TMemAPITableMetadata;
        try
          //First, fields.
          SList := SrcTableMeta.GetFieldNames;
          try
            for idx := 0 to Pred(SList.Count) do
            begin
              OK := SrcTableMeta.FieldInfo(SList[idx], FieldType);
              Assert(OK);
              DstTableMeta.CreateField(SList[idx], FieldType);
            end;
          finally
            SList.Free;
          end;
          //Second, indexes.
          SList := SrcTableMeta.GetIndexNames;
          try
            for idx := 0 to Pred(SList.Count) do
            begin
              OK := SrcTableMeta.IndexInfo(SList[idx], IndexedFields, IndexAttrs);
              Assert(OK);
              DstTableMeta.CreateMultiFieldIndex(SList[idx], IndexedFields, IndexAttrs);
            end;
          finally
            SList.Free;
          end;
        finally
          DstTableMeta.Free;
        end;
      finally
        DstAPI.Free;
      end;
      DstTrans.CommitAndFree
    except
      DstTrans.RollbackAndFree;
      raise;
    end;
    //Now, copy the data from one table to the other.
    DstTrans := DstSess.StartTransaction(amReadWrite);
    try
      DstAPI := DstTrans.GetAPI;
      try
        DstEntity := DstAPI.OpenTableOrKey(EntityName);
        DstTableData := DstAPI.GetApiObjectFromHandle(DstEntity, APITableData) as TMemAPITableData;
        try
          SList := SrcTableMeta.GetFieldNames;
          try
            GotRow := SrcTableData.Locate(ptFirst, '');
            while GotRow do
            begin
              DstTableData.Append;
              for idx := 0 to Pred(SList.Count) do
              begin
                SrcTableData.ReadField(SList[idx], Data);
                //TODO - Memcopies for blobs?
                Assert(Data.FieldType <> ftBlob);
                DstTableData.WriteField(SList[idx], Data);
              end;
              DstTableData.Post;
              GotRow := SrcTableData.Locate(ptNext, '');
            end;
          finally
            SList.Free;
          end;
        finally
          DstTableData.Free;
        end;
      finally
        DstAPI.Free;
      end;
      DstTrans.CommitAndFree;
    except
      DstTrans.RollbackAndFree;
      raise;
    end;
  finally
    SrcTableMeta.Free;
    SrcTableData.Free;
  end;
end;

procedure CopyTables(SrcAPI: TMemAPIDatabase; DstSess: TMemDBSession);
var
  EntityNames: TStringList;
  Entity: TMemDBHandle;
  idx: integer;
  TblMetaAPI: TMemAPITableMetadata;
begin
  EntityNames := SrcAPI.GetEntityNames;
  try
    for idx := 0 to Pred(EntityNames.Count) do
    begin
      Entity := SrcAPI.OpenTableOrKey(EntityNames[idx]);
      TblMetaAPI := SrcAPI.GetApiObjectFromHandle(Entity, APITableMetadata, false) as TMemAPITableMetadata;
      if Assigned(TblMetaAPI) then
      begin
        CopyTable(SrcAPI, Entity, EntityNames[idx], DstSess);
        TblMetaAPI.Free;
      end;
    end;
  finally
    EntityNames.Free;
  end;
end;

procedure CopyForeignKey(SrcFKAPI: TMemAPIForeignKey; EntityName: string; DstSess: TMemDBSession);
var
  DstTrans: TMemDBTransaction;
  DstEntity: TMemDBHandle;
  DstAPI: TMemAPIDatabase;
  DstFKAPI: TMemAPIForeignKey;
  TableName, IndexName: string;
begin
  DstTrans := DstSess.StartTransaction(amReadWrite);
  try
    DstAPI := DstTrans.GetAPI;
    try
      DstEntity := DstAPI.CreateForeignKey(EntityName);
      DstFKAPI := DstAPI.GetApiObjectFromHandle(DstEntity, APIForeignKey) as TMemAPIForeignKey;
      try
        SrcFKAPI.GetReferencingChild(TableName, IndexName);
        DstFKAPI.SetReferencingChild(TableName, IndexName);
        SrcFKAPI.GetReferencedParent(TableName, IndexName);
        DstFKAPI.SetReferencedParent(TableName, IndexName);
      finally
        DstFKAPI.Free;
      end;
    finally
      DstAPI.Free;
    end;
    DstTrans.CommitAndFree;
  except
    DstTrans.RollbackAndFree;
    raise;
  end;
end;

procedure CopyForeignKeys(SrcAPI: TMemAPIDatabase; DstSess: TMemDBSession);
var
  EntityNames: TStringList;
  Entity: TMemDBHandle;
  idx: integer;
  FKAPI: TMemAPIForeignKey;
begin
  EntityNames := SrcAPI.GetEntityNames;
  try
    for idx := 0 to Pred(EntityNames.Count) do
    begin
      Entity := SrcAPI.OpenTableOrKey(EntityNames[idx]);
      FKAPI := SrcAPI.GetApiObjectFromHandle(Entity, APIForeignKey, false) as TMemAPIForeignKey;
      if Assigned(FKAPI) then
      begin
        CopyForeignKey(FKAPI, EntityNames[idx], DstSess);
        FKAPI.Free;
      end;
    end;
  finally
    EntityNames.Free;
  end;
end;

procedure CopyDatabase(Src: TMemDB; Dst: TMemDB);
var
  SrcSess, DstSess: TMemDBSession;
  SrcTrans: TMemDBTransaction;
  SrcAPI: TMemAPIDatabase;
begin
  SrcSess := Src.StartSession;
  DstSess := Dst.StartSession;
  SrcTrans := SrcSess.StartTransaction(amRead);
  SrcAPI := SrcTrans.GetAPI;
  try
    try
      ClearDestinationDB(DstSess);
      CopyTables(SrcAPI, DstSess);
      CopyForeignKeys(SrcAPI, DstSess);
      Dst.Checkpoint;
    finally
      SrcAPI.Free;
      SrcTrans.RollbackAndFree;
    end;
  finally
    SrcSess.Free;
    DstSess.Free;
  end;
end;

end.
