unit MemDB2Utils;

{ Martin Harvey 2019. Some top level utility routines }

{ Copies a database from an old style MemDB instance (MemDB.TMemDB) into a
  MemDB2 instance (MemDB2.TMemDB).

  The two libraries declare many identically named types and enum values
  (TMemDB, TMemDBSession, TMemAPIDatabase, TMDBFieldType, ftGuid, ptFirst ...).
  They are distinct types, so every reference below is unit qualified:

    Source (old):  MemDB, MemDBApi, MemDBMisc        - handle based API.
    Dest (new):    MemDB2, MemDB2Api, MemDB2Misc     - entity name based API.

  Field types, index attributes, field name arrays and field data records are
  converted explicitly between the two. }

interface

uses
  MemDB, MemDB2;

procedure CopyDatabase(Src: MemDB.TMemDB; Dst: MemDB2.TMemDB);

implementation

uses
  SysUtils, Classes,
  MemDBApi, MemDBMisc,
  MemDB2Api, MemDB2Misc;

const
  S_UNKNOWN_FIELD_TYPE = 'CopyDatabase: unknown field type.';
  S_BLOB_NOT_SUPPORTED = 'CopyDatabase: blob fields not supported.';

{ Type conversions, old MemDB -> MemDB2 }

function ConvFieldType(FT: MemDBMisc.TMDBFieldType): MemDB2Misc.TMDBFieldType;
begin
  case FT of
    MemDBMisc.ftInteger: result := MemDB2Misc.ftInteger;
    MemDBMisc.ftCardinal: result := MemDB2Misc.ftCardinal;
    MemDBMisc.ftInt64: result := MemDB2Misc.ftInt64;
    MemDBMisc.ftUint64: result := MemDB2Misc.ftUint64;
    MemDBMisc.ftUnicodeString: result := MemDB2Misc.ftUnicodeString;
    MemDBMisc.ftDouble: result := MemDB2Misc.ftDouble;
    MemDBMisc.ftBlob: result := MemDB2Misc.ftBlob;
    MemDBMisc.ftGuid: result := MemDB2Misc.ftGuid;
  else
    raise SysUtils.Exception.Create(S_UNKNOWN_FIELD_TYPE);
  end;
end;

function ConvIndexAttrs(Attrs: MemDBMisc.TMDBIndexAttrs): MemDB2Misc.TMDBIndexAttrs;
begin
  result := [];
  if MemDBMisc.iaUnique in Attrs then
    Include(result, MemDB2Misc.iaUnique);
  if MemDBMisc.iaNotEmpty in Attrs then
    Include(result, MemDB2Misc.iaNotEmpty);
end;

function ConvFieldNames(const Names: MemDBMisc.TMDBFieldNames): MemDB2Misc.TMDBFieldNames;
var
  idx: integer;
begin
  SetLength(result, Length(Names));
  for idx := 0 to Pred(Length(Names)) do
    result[idx] := Names[idx];
end;

procedure ConvDataRec(const Src: MemDBMisc.TMemDbFieldDataRec;
                      var Dst: MemDB2Misc.TMemDbFieldDataRec);
begin
  Dst.FieldType := ConvFieldType(Src.FieldType);
  Dst.sVal := Src.sVal;
  case Src.FieldType of
    MemDBMisc.ftUnicodeString: ; //sVal only.
    MemDBMisc.ftInteger: Dst.i32Val := Src.i32Val;
    MemDBMisc.ftCardinal: Dst.u32Val := Src.u32Val;
    MemDBMisc.ftInt64: Dst.i64Val := Src.i64Val;
    MemDBMisc.ftUint64: Dst.u64Val := Src.u64Val;
    MemDBMisc.ftDouble: Dst.dVal := Src.dVal;
    MemDBMisc.ftGuid: Dst.gVal := Src.gVal;
    MemDBMisc.ftBlob:
      //TODO - Memcopies for blobs?
      raise SysUtils.Exception.Create(S_BLOB_NOT_SUPPORTED);
  else
    raise SysUtils.Exception.Create(S_UNKNOWN_FIELD_TYPE);
  end;
end;

{ Destination (MemDB2) }

procedure ClearDestinationDB(Session: MemDB2.TMemDBSession);
var
  Trans: MemDB2.TMemDBTransaction;
  EntityNames: Classes.TStringList;
  DBAPI: MemDB2Api.TMemAPIDatabase;
  FKAPI: MemDB2Api.TMemAPIForeignKey;
  idx: integer;
begin
  Trans := Session.StartTransaction(MemDB2Misc.amWriteExclusive);
  try
    DBAPI := Trans.GetAPI;
    try
      //Get a list of top level entities.
      EntityNames := DBAPI.GetEntityNames;
      try
        //Delete all foreign keys.
        for idx := 0 to Pred(EntityNames.Count) do
        begin
          //Nil (no exception) if the entity is a table, not a foreign key.
          FKAPI := DBAPI.GetAPIObjectFromEntity(EntityNames[idx],
            MemDB2Misc.APIForeignKey, false) as MemDB2Api.TMemAPIForeignKey;
          if Assigned(FKAPI) then
          begin
            //Free the API object before deleting the entity it refers to.
            FKAPI.Free;
            DBAPI.DeleteTableOrKey(EntityNames[idx]);
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
  Trans := Session.StartTransaction(MemDB2Misc.amWriteExclusive);
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

{ Source (old MemDB) -> destination (MemDB2) }

procedure CopyTable(SrcAPI: MemDBApi.TMemAPIDatabase;
                    SrcEntity: MemDBMisc.TMemDBHandle;
                    EntityName: string;
                    DstSess: MemDB2.TMemDBSession);
var
  SrcTableMeta: MemDBApi.TMemAPITableMetadata;
  SrcTableData: MemDBApi.TMemAPITableData;
  DstTableMeta: MemDB2Api.TMemAPITableMetadata;
  DstTableData: MemDB2Api.TMemAPITableData;
  DstTrans: MemDB2.TMemDBTransaction;
  DstAPI: MemDB2Api.TMemAPIDatabase;
  idx: integer;
  SList: Classes.TStringList;
  FieldType: MemDBMisc.TMDBFieldType;
  OK: boolean;
  GotRow: boolean;
  IndexedFields: MemDBMisc.TMDBFieldNames;
  IndexAttrs: MemDBMisc.TMDBIndexAttrs;
  SrcData: MemDBMisc.TMemDbFieldDataRec;
  DstData: MemDB2Misc.TMemDbFieldDataRec;
begin
  SrcTableData := nil;
  SrcTableMeta := SrcAPI.GetApiObjectFromHandle(SrcEntity,
    MemDBMisc.APITableMetadata) as MemDBApi.TMemAPITableMetadata;
  try
    SrcTableData := SrcAPI.GetApiObjectFromHandle(SrcEntity,
      MemDBMisc.APITableData) as MemDBApi.TMemAPITableData;
    //First, create destination table, and add same fields and indexes as source.
    DstTrans := DstSess.StartTransaction(MemDB2Misc.amWriteExclusive);
    try
      DstAPI := DstTrans.GetAPI;
      try
        DstAPI.CreateTable(EntityName);
        DstTableMeta := DstAPI.GetAPIObjectFromEntity(EntityName,
          MemDB2Misc.APITableMetadata, true) as MemDB2Api.TMemAPITableMetadata;
        try
          //First, fields.
          SList := SrcTableMeta.GetFieldNames;
          try
            for idx := 0 to Pred(SList.Count) do
            begin
              OK := SrcTableMeta.FieldInfo(SList[idx], FieldType);
              Assert(OK);
              DstTableMeta.CreateField(SList[idx], ConvFieldType(FieldType));
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
              DstTableMeta.CreateMultiFieldIndex(SList[idx],
                ConvFieldNames(IndexedFields), ConvIndexAttrs(IndexAttrs));
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
    DstTrans := DstSess.StartTransaction(MemDB2Misc.amWriteExclusive);
    try
      DstAPI := DstTrans.GetAPI;
      try
        DstTableData := DstAPI.GetAPIObjectFromEntity(EntityName,
          MemDB2Misc.APITableData, true) as MemDB2Api.TMemAPITableData;
        try
          SList := SrcTableMeta.GetFieldNames;
          try
            GotRow := SrcTableData.Locate(MemDBMisc.ptFirst, '');
            while GotRow do
            begin
              DstTableData.Append;
              for idx := 0 to Pred(SList.Count) do
              begin
                SrcTableData.ReadField(SList[idx], SrcData);
                ConvDataRec(SrcData, DstData);
                DstTableData.WriteField(SList[idx], DstData);
              end;
              DstTableData.Post;
              GotRow := SrcTableData.Locate(MemDBMisc.ptNext, '');
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

procedure CopyTables(SrcAPI: MemDBApi.TMemAPIDatabase; DstSess: MemDB2.TMemDBSession);
var
  EntityNames: Classes.TStringList;
  Entity: MemDBMisc.TMemDBHandle;
  idx: integer;
  TblMetaAPI: MemDBApi.TMemAPITableMetadata;
begin
  EntityNames := SrcAPI.GetEntityNames;
  try
    for idx := 0 to Pred(EntityNames.Count) do
    begin
      Entity := SrcAPI.OpenTableOrKey(EntityNames[idx]);
      //Nil (no exception) if the entity is a foreign key, not a table.
      TblMetaAPI := SrcAPI.GetApiObjectFromHandle(Entity,
        MemDBMisc.APITableMetadata, false) as MemDBApi.TMemAPITableMetadata;
      if Assigned(TblMetaAPI) then
      begin
        TblMetaAPI.Free;
        CopyTable(SrcAPI, Entity, EntityNames[idx], DstSess);
      end;
    end;
  finally
    EntityNames.Free;
  end;
end;

procedure CopyForeignKey(SrcFKAPI: MemDBApi.TMemAPIForeignKey; EntityName: string;
                         DstSess: MemDB2.TMemDBSession);
var
  DstTrans: MemDB2.TMemDBTransaction;
  DstAPI: MemDB2Api.TMemAPIDatabase;
  DstFKAPI: MemDB2Api.TMemAPIForeignKey;
  TableName, IndexName: string;
begin
  DstTrans := DstSess.StartTransaction(MemDB2Misc.amWriteExclusive);
  try
    DstAPI := DstTrans.GetAPI;
    try
      DstAPI.CreateForeignKey(EntityName);
      DstFKAPI := DstAPI.GetAPIObjectFromEntity(EntityName,
        MemDB2Misc.APIForeignKey, true) as MemDB2Api.TMemAPIForeignKey;
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

procedure CopyForeignKeys(SrcAPI: MemDBApi.TMemAPIDatabase; DstSess: MemDB2.TMemDBSession);
var
  EntityNames: Classes.TStringList;
  Entity: MemDBMisc.TMemDBHandle;
  idx: integer;
  FKAPI: MemDBApi.TMemAPIForeignKey;
begin
  EntityNames := SrcAPI.GetEntityNames;
  try
    for idx := 0 to Pred(EntityNames.Count) do
    begin
      Entity := SrcAPI.OpenTableOrKey(EntityNames[idx]);
      //Nil (no exception) if the entity is a table, not a foreign key.
      FKAPI := SrcAPI.GetApiObjectFromHandle(Entity,
        MemDBMisc.APIForeignKey, false) as MemDBApi.TMemAPIForeignKey;
      if Assigned(FKAPI) then
      begin
        try
          CopyForeignKey(FKAPI, EntityNames[idx], DstSess);
        finally
          FKAPI.Free;
        end;
      end;
    end;
  finally
    EntityNames.Free;
  end;
end;

procedure CopyDatabase(Src: MemDB.TMemDB; Dst: MemDB2.TMemDB);
var
  SrcSess: MemDB.TMemDBSession;
  DstSess: MemDB2.TMemDBSession;
  SrcTrans: MemDB.TMemDBTransaction;
  SrcAPI: MemDBApi.TMemAPIDatabase;
begin
  SrcSess := Src.StartSession;
  DstSess := Dst.StartSession;
  SrcTrans := SrcSess.StartTransaction(MemDBMisc.amRead);
  SrcAPI := SrcTrans.GetAPI;
  try
    try
      ClearDestinationDB(DstSess);
      //Tables before foreign keys: keys refer to tables and their indexes.
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
