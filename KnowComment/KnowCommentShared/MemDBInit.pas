unit MemDBInit;
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

uses MemDB, DBGeneric, DataObjects, SyncObjs;

type
  TMemKDB = class(TGenericDB)
  private
    FDB: TMemDB;
    FDBPath: string;
    FTablesSetup: boolean;
  protected
    function GetIsInit: boolean; override;

    procedure OpenOrCreateUserTable(T:TMemDBTransaction; out Msg: string);
    procedure OpenOrCreateMediaTable(T:TMemDBTransaction; out Msg: string);
    procedure OpenOrCreateCommentTable(T:TMemDBTransaction; out Msg: string);
    function GetSession: TMemDBSession;
    function SetupTables(out Msg:String): boolean;
    function UpgradeTables(out Msg:string): boolean;
  public
    constructor Create;
    destructor Destroy; override;
    function InitAsync: boolean;
    function InitAsyncCompletion: boolean;
    function InitDBAndTables(out Msg: string): boolean; override;
    function FiniDBAndTables: boolean; override;
    function DestroyTables: boolean; override;
    function ClearTablesData: boolean; override;

    function PersisterClass(Level: TKListLevel): TDBPersistClass; override;

    property DBPath: string read FDBPath write FDBPath;
    property MemDB: TMemDB read FDB;
  end;

implementation

uses
  MemDBMisc, SysUtils, MemDBApi, DBSchemaStrs, MemDBPersist, Classes;

type
  TUpgradeState = (ugsPre, ugsPost, ugsInconsistent);
  EDatabaseUpgradeException = class(EMemDBException);

function TMemKDB.PersisterClass(Level: TKListLevel): TDBPersistClass;
begin
  case Level of
    klUserList:
      result := TMDBUserProfilePersister;
    klMediaList:
      result := TMDBMediaItemPersister;
    klCommentList:
      result := TMDBCommentItemPersister;
  else
    Assert(false);
    result := nil;
  end;
end;

function TMemKDB.GetSession: TMemDBSession;
begin
  result := FDB.StartSession;
end;

procedure TMemKDB.OpenOrCreateUserTable(T:TMemDBTransaction; out Msg: string);
var
  DBAPI: TMemAPIDatabase;
  Tbl: TMemDBHandle;
  TblMeta: TMemAPITableMetadata;
  ST: TKSiteType;
  UBX: TUserBlockIndexes;
begin
  DBAPI := T.GetAPI;
  try
    Tbl := DBAPI.OpenTableOrKey(S_USER_TABLE);
    if not Assigned(Tbl) then
    begin
      //Table.
      Tbl := DBAPI.CreateTable(S_USER_TABLE);
      TblMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      try
        //Fields.
        TblMeta.CreateField(S_USER_KEY, ftGuid);
        TblMeta.CreateField(S_USER_INSTA_UNAME, ftUnicodeString);
        TblMeta.CreateField(S_USER_INSTA_UID, ftUnicodeString);
        TblMeta.CreateField(S_USER_INSTA_VERIFIED, ftInteger);
        TblMeta.CreateField(S_USER_INSTA_PROF_URL, ftUnicodeString);
        TblMeta.CreateField(S_USER_INSTA_FULL_NAME, ftUnicodeString);
        TblMeta.CreateField(S_USER_INSTA_BIO, ftUnicodeString);
        TblMeta.CreateField(S_USER_INSTA_FOLLOWS_COUNT, ftInteger);
        TblMeta.CreateField(S_USER_INSTA_FOLLOWER_COUNT, ftInteger);
        TblMeta.CreateField(S_USER_TWITTER_UNAME, ftUnicodeString);
        TblMeta.CreateField(S_USER_TWITTER_UID, ftUnicodeString);
        TblMeta.CreateField(S_USER_TWITTER_VERIFIED, ftInteger);
        TblMeta.CreateField(S_USER_TWITTER_PROF_URL, ftUnicodeString);
        TblMeta.CreateField(S_USER_TWITTER_FULL_NAME, ftUnicodeString);
        TblMeta.CreateField(S_USER_TWITTER_BIO, ftUnicodeString);
        TblMeta.CreateField(S_USER_TWITTER_FOLLOWS_COUNT, ftInteger);
        TblMeta.CreateField(S_USER_TWITTER_FOLLOWER_COUNT, ftInteger);
        TblMeta.CreateField(S_LAST_UPDATED, ftDouble);
        TblMeta.CreateField(S_USER_INTEREST, ftInteger);
        //Indexes - key based, ref integrity.
        TblMeta.CreateIndex(S_INDEX_USER_TABLE_PRIMARY, S_USER_KEY, [iaUnique, iaNotEmpty]);
        //Indexes, UID etc lookup.
        for ST := Low(ST) to High(ST) do
        begin
          for UBX := Low(UBX) to High(UBX) do
          begin
            TblMeta.CreateIndex(
              UserBlockIndexNames[ST][UBX],
              UserBlockIndexFieldNames[ST][UBX], []);
          end;
        end;
      finally
        TblMeta.Free;
      end;
    end;
  finally
    DBAPI.Free;
  end;
end;

procedure TMemKDB.OpenOrCreateMediaTable(T:TMemDBTransaction; out Msg: string);
var
  DBAPI: TMemAPIDatabase;
  Tbl, FK: TMemDBHandle;
  TblMeta: TMemAPITableMetadata;
  FKMeta: TMemAPIForeignKey;
  ST: TKSiteType;
  MBX: TMediaBlockIndexes;
begin
  DBAPI := T.GetAPI;
  try
    //Table.
    Tbl := DBAPI.OpenTableOrKey(S_MEDIA_TABLE);
    if not Assigned(Tbl) then
    begin
      Tbl := DBAPI.CreateTable(S_MEDIA_TABLE);
      TblMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      try
        TblMeta.CreateField(S_MEDIA_KEY, ftGuid);
        TblMeta.CreateField(S_MEDIA_OWNER_KEY, ftGuid);
        TblMeta.CreateField(S_MEDIA_DATE, ftDouble);
        TblMeta.CreateField(S_MEDIA_DATA_STR, ftUnicodeString);
        TblMeta.CreateField(S_MEDIA_TYPE, ftInteger);
        TblMeta.CreateField(S_MEDIA_RESOURCE_URL, ftUnicodeString);
        TblMeta.CreateField(S_MEDIA_INSTA_MEDIA_ID, ftUnicodeString);
        TblMeta.CreateField(S_MEDIA_INSTA_OWNER_ID, ftUnicodeString);
        TblMeta.CreateField(S_MEDIA_INSTA_MEDIA_CODE, ftUnicodeString);
        TblMeta.CreateField(S_MEDIA_TWITTER_MEDIA_ID, ftUnicodeString);
        TblMeta.CreateField(S_MEDIA_TWITTER_OWNER_ID, ftUnicodeString);
        TblMeta.CreateField(S_MEDIA_TWITTER_MEDIA_CODE, ftUnicodeString);
        TblMeta.CreateField(S_LAST_UPDATED, ftDouble);
        //Indexes - key based, ref integrity.
        TblMeta.CreateIndex(S_INDEX_MEDIA_TABLE_PRIMARY, S_MEDIA_KEY, [iaUnique, iaNotEmpty]);
        TblMeta.CreateIndex(S_INDEX_MEDIA_OWNER_KEY, S_MEDIA_OWNER_KEY, [iaNotEmpty]);
        //Indexes, site UIDs.
        for ST := Low(ST) to High(ST) do
        begin
          for MBX := Low(MBX) to High(MBX) do
          begin
            TblMeta.CreateIndex(
              MediaBlockIndexNames[ST][MBX],
              MediaBlockIndexFieldNames[ST][MBX], []);
          end;
        end;
      finally
        TblMeta.Free
      end;
    end;
    //Foreign key constraints.
    FK := DBAPI.OpenTableOrKey(S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID);
    if not Assigned(FK) then
    begin
      FK := DBAPI.CreateForeignKey(S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID);
      FKMeta := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
      try
        FKMeta.SetReferencingChild(S_MEDIA_TABLE, S_INDEX_MEDIA_OWNER_KEY);
        FKMeta.SetReferencedParent(S_USER_TABLE, S_INDEX_USER_TABLE_PRIMARY);
      finally
        FKMeta.Free;
      end;
    end;
  finally
    DBAPI.Free;
  end;
end;

procedure TMemKDB.OpenOrCreateCommentTable(T:TMemDBTransaction; out Msg: string);
var
  DBAPI: TMemAPIDatabase;
  Tbl, FK: TMemDBHandle;
  TblMeta: TMemAPITableMetadata;
  FKMeta: TMemAPIForeignKey;
  ST: TKSiteType;
  CBX: TCommentBlockIndexes;
begin
  DBAPI := T.GetAPI;
  try
    //Table.
    Tbl := DBAPI.OpenTableOrKey(S_COMMENT_TABLE);
    if not Assigned(Tbl) then
    begin
      Tbl := DBAPI.CreateTable(S_COMMENT_TABLE);
      TblMeta := DBAPI.GetApiObjectFromHandle(Tbl, APITableMetadata) as TMemAPITableMetadata;
      try
        TblMeta.CreateField(S_COMMENT_KEY, ftGuid);
        TblMeta.CreateField(S_COMMENT_OWNER_KEY, ftGuid);
        TblMeta.CreateField(S_COMMENT_MEDIA_KEY, ftGuid);
        TblMeta.CreateField(S_COMMENT_INSTA_COMMENT_ID, ftUnicodeString);
        TblMeta.CreateField(S_COMMENT_INSTA_OWNER_ID, ftUnicodeString);
        TblMeta.CreateField(S_COMMENT_TWITTER_COMMENT_ID, ftUnicodeString);
        TblMeta.CreateField(S_COMMENT_TWITTER_OWNER_ID, ftUnicodeString);
        TblMeta.CreateField(S_COMMENT_DATE, ftDouble);
        TblMeta.CreateField(S_COMMENT_DATA_STR, ftUnicodeString);
        TblMeta.CreateField(S_COMMENT_TYPE, ftInteger);
        TblMeta.CreateField(S_LAST_UPDATED, ftDouble);

        //Indexes - key based, ref integrity.
        TblMeta.CreateIndex(S_INDEX_COMMENT_TABLE_PRIMARY, S_COMMENT_KEY, [iaUnique, iaNotEmpty]);
        TblMeta.CreateIndex(S_INDEX_COMMENT_OWNER_KEY, S_COMMENT_OWNER_KEY, [iaNotEmpty]);
        TblMeta.CreateIndex(S_INDEX_COMMENT_MEDIA_KEY, S_COMMENT_MEDIA_KEY, [iaNotEmpty]);
        //Indexes, site UIDs.
        for ST := Low(ST) to High(ST) do
        begin
          for CBX := Low(CBX) to High(CBX) do
          begin
            TblMeta.CreateIndex(
              CommentBlockIndexNames[ST][CBX],
              CommentBlockIndexFieldNames[ST][CBX], []);
          end;
        end;
      finally
        TblMeta.Free
      end;
    end;
    //Foreign key constraints.
    FK := DBAPI.OpenTableOrKey(S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID);
    if not Assigned(FK) then
    begin
      FK := DBAPI.CreateForeignKey(S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID);
      FKMeta := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
      try
        FKMeta.SetReferencingChild(S_COMMENT_TABLE, S_INDEX_COMMENT_OWNER_KEY);
        FKMeta.SetReferencedParent(S_USER_TABLE, S_INDEX_USER_TABLE_PRIMARY);
      finally
        FKMeta.Free;
      end;
    end;
    FK := DBAPI.OpenTableOrKey(S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID);
    if not Assigned(FK) then
    begin
      FK := DBAPI.CreateForeignKey(S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID);
      FKMeta := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
      try
        FKMeta.SetReferencingChild(S_COMMENT_TABLE, S_INDEX_COMMENT_MEDIA_KEY);
        FKMeta.SetReferencedParent(S_MEDIA_TABLE, S_INDEX_MEDIA_TABLE_PRIMARY);
      finally
        FKMeta.Free;
      end;
    end;
  finally
    DBAPI.Free;
  end;
end;

function TMemKDB.InitAsync: boolean;
begin
  result := FDB.InitDB(DBPath, jtV2, true);
end;

function TMemKDB.InitAsyncCompletion: boolean;
var
  Msg: string;
begin
  result := SetupTables(Msg);
end;


function TMemKDB.UpgradeTables(out Msg:string): boolean;
var
  UpgradeState: TUpgradeState;
  S: TMemDBSession;
  Phase1Done, Phase2Done, Phase3Done: boolean;

  procedure Phase1Convert;

  var
    T: TMemDBTransaction;
    TblMeta: TMemAPITableMetadata;
    TblData: TMemAPITableData;
    Handle: TMemDBHandle;
    DBAPI: TMemAPIDatabase;
    Iter: boolean;
  begin
    T := S.StartTransaction(amReadWrite);
    try
      DBAPI := T.GetAPI;
      try
        //Rename old keys, fields, indices FK's to "old" values
        Handle := DBAPI.OpenTableOrKey(S_USER_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.RenameField(S_USER_KEY, S_USER_KEY_OLD);
          TblMeta.RenameIndex(S_INDEX_USER_TABLE_PRIMARY, S_INDEX_USER_TABLE_PRIMARY_OLD);
          TblMeta.CreateField(S_USER_KEY, ftGuid);
        finally
          TblMeta.Free;
        end;

        Handle := DBAPI.OpenTableOrKey(S_MEDIA_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.RenameField(S_MEDIA_KEY, S_MEDIA_KEY_OLD);
          TblMeta.RenameIndex(S_INDEX_MEDIA_TABLE_PRIMARY, S_INDEX_MEDIA_TABLE_PRIMARY_OLD);
          TblMeta.RenameField(S_MEDIA_OWNER_KEY, S_MEDIA_OWNER_KEY_OLD);
          TblMeta.RenameIndex(S_INDEX_MEDIA_OWNER_KEY, S_INDEX_MEDIA_OWNER_KEY_OLD);
          TblMeta.CreateField(S_MEDIA_KEY, ftGuid);
          TblMeta.CreateField(S_MEDIA_OWNER_KEY, ftGuid);
        finally
          TblMeta.Free;
        end;

        Handle := DBAPI.OpenTableOrKey(S_COMMENT_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.RenameField(S_COMMENT_KEY, S_COMMENT_KEY_OLD);
          TblMeta.RenameIndex(S_INDEX_COMMENT_TABLE_PRIMARY, S_INDEX_COMMENT_TABLE_PRIMARY_OLD);
          TblMeta.RenameField(S_COMMENT_OWNER_KEY, S_COMMENT_OWNER_KEY_OLD);
          TblMeta.RenameIndex(S_INDEX_COMMENT_OWNER_KEY, S_INDEX_COMMENT_OWNER_KEY_OLD);
          TblMeta.RenameField(S_COMMENT_MEDIA_KEY, S_COMMENT_MEDIA_KEY_OLD);
          TblMeta.RenameIndex(S_INDEX_COMMENT_MEDIA_KEY, S_INDEX_COMMENT_MEDIA_KEY_OLD);
          TblMeta.CreateField(S_COMMENT_KEY, ftGuid);
          TblMeta.CreateField(S_COMMENT_OWNER_KEY, ftGuid);
          TblMeta.CreateField(S_COMMENT_MEDIA_KEY, ftGuid);
        finally
          TblMeta.Free;
        end;

        DBAPI.RenameTableOrKey(S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID, S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID_OLD);
        DBAPI.RenameTableOrKey(S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID, S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID_OLD);
        DBAPI.RenameTableOrKey(S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID, S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID_OLD);
        //Created new fields. Don't change data, or set up index contraints yet.
      finally
        DBAPI.Free;
      end;
      T.CommitAndFree;
      Phase1Done := true;
    except
      on Exception do
      begin
        T.RollbackAndFree;
        Phase1Done := false;
        raise;
      end;
    end;
  end;

  procedure Phase1Rollback;
  var
    T: TMemDBTransaction;
    TblMeta: TMemAPITableMetadata;
    TblData: TMemAPITableData;
    Handle: TMemDBHandle;
    DBAPI: TMemAPIDatabase;
  begin
    T := S.StartTransaction(amReadWrite);
    try
      DBAPI := T.GetAPI;
      try
        //Rename old keys, fields, indices FK's to "old" values
        Handle := DBAPI.OpenTableOrKey(S_USER_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.DeleteField(S_USER_KEY);
          TblMeta.RenameField(S_USER_KEY_OLD, S_USER_KEY);
          TblMeta.RenameIndex(S_INDEX_USER_TABLE_PRIMARY_OLD, S_INDEX_USER_TABLE_PRIMARY);
        finally
          TblMeta.Free;
        end;

        Handle := DBAPI.OpenTableOrKey(S_MEDIA_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.DeleteField(S_MEDIA_KEY);
          TblMeta.DeleteField(S_MEDIA_OWNER_KEY);
          TblMeta.RenameField(S_MEDIA_KEY_OLD, S_MEDIA_KEY);
          TblMeta.RenameIndex(S_INDEX_MEDIA_TABLE_PRIMARY_OLD, S_INDEX_MEDIA_TABLE_PRIMARY);
          TblMeta.RenameField(S_MEDIA_OWNER_KEY_OLD, S_MEDIA_OWNER_KEY);
          TblMeta.RenameIndex(S_INDEX_MEDIA_OWNER_KEY_OLD, S_INDEX_MEDIA_OWNER_KEY);
        finally
          TblMeta.Free;
        end;

        Handle := DBAPI.OpenTableOrKey(S_COMMENT_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.DeleteField(S_COMMENT_KEY);
          TblMeta.DeleteField(S_COMMENT_OWNER_KEY);
          TblMeta.DeleteField(S_COMMENT_MEDIA_KEY);
          TblMeta.RenameField(S_COMMENT_KEY_OLD, S_COMMENT_KEY);
          TblMeta.RenameIndex(S_INDEX_COMMENT_TABLE_PRIMARY_OLD, S_INDEX_COMMENT_TABLE_PRIMARY);
          TblMeta.RenameField(S_COMMENT_OWNER_KEY_OLD, S_COMMENT_OWNER_KEY);
          TblMeta.RenameIndex(S_INDEX_COMMENT_OWNER_KEY_OLD, S_INDEX_COMMENT_OWNER_KEY);
          TblMeta.RenameField(S_COMMENT_MEDIA_KEY_OLD, S_COMMENT_MEDIA_KEY);
          TblMeta.RenameIndex(S_INDEX_COMMENT_MEDIA_KEY_OLD, S_INDEX_COMMENT_MEDIA_KEY);
        finally
          TblMeta.Free;
        end;

        DBAPI.RenameTableOrKey(S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID_OLD, S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID);
        DBAPI.RenameTableOrKey(S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID_OLD, S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID);
        DBAPI.RenameTableOrKey(S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID_OLD, S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID);
      finally
        DBAPI.Free;
      end;
      T.CommitAndFree;
    except
      on Exception do
      begin
        T.RollbackAndFree;
        raise;
      end;
    end;
  end;


  procedure Phase2Convert;

      procedure ConvField(TblData:TMemAPITableData;
                                   OldName, NewName: string);
      var
        Data: TMemDbFieldDataRec;
      begin
        TblData.ReadField(OldName, Data);
        Data.FieldType := ftGuid;
        Data.gVal := StringToGUID(Data.sVal);
        TblData.WriteField(NewName, Data);
      end;

  var
    T: TMemDBTransaction;
    DBAPI: TMemAPIDatabase;
    TblData: TMemAPITableData;
    TblMeta: TMemAPITableMetadata;
    FKMeta: TMemAPIForeignKey;
    Handle, FK: TMemDBHandle;
    Iter: boolean;
  begin
    T := S.StartTransaction(amReadWrite);
    try
      DBAPI := T.GetAPI;
      try
        //Populate new fields with data.
        Handle := DBAPI.OpenTableOrKey(S_USER_TABLE);
        TblData := DBAPI.GetApiObjectFromHandle(Handle, APITableData) as TMemAPITableData;
        try
          Iter := TblData.Locate(ptFirst, '');
          while Iter do
          begin
            ConvField(TblData, S_USER_KEY_OLD, S_USER_KEY);
            TblData.Post;
            Iter := TblData.Locate(ptNext, '');
          end;
        finally
          TblData.Free;
        end;

        Handle := DBAPI.OpenTableOrKey(S_MEDIA_TABLE);
        TblData := DBAPI.GetApiObjectFromHandle(Handle, APITableData) as TMemAPITableData;
        try
          Iter := TblData.Locate(ptFirst, '');
          while Iter do
          begin
            ConvField(TblData, S_MEDIA_KEY_OLD, S_MEDIA_KEY);
            ConvField(TblData, S_MEDIA_OWNER_KEY_OLD, S_MEDIA_OWNER_KEY); //TODO - post
            TblData.Post;
            Iter := TblData.Locate(ptNext, '');
          end;
        finally
          TblData.Free;
        end;

        Handle := DBAPI.OpenTableOrKey(S_COMMENT_TABLE);
        TblData := DBAPI.GetApiObjectFromHandle(Handle, APITableData) as TMemAPITableData;
        try
          Iter := TblData.Locate(ptFirst, '');
          while Iter do
          begin
            ConvField(TblData, S_COMMENT_KEY_OLD, S_COMMENT_KEY);
            ConvField(TblData, S_COMMENT_OWNER_KEY_OLD, S_COMMENT_OWNER_KEY);
            ConvField(TblData, S_COMMENT_MEDIA_KEY_OLD, S_COMMENT_MEDIA_KEY);
            TblData.Post;
            Iter := TblData.Locate(ptNext, '');
          end;
        finally
          TblData.Free;
        end;

        Handle := DBAPI.OpenTableOrKey(S_USER_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.CreateIndex(S_INDEX_USER_TABLE_PRIMARY, S_USER_KEY, [iaUnique, iaNotEmpty]);
        finally
          TblMeta.Free;
        end;

        Handle := DBAPI.OpenTableOrKey(S_MEDIA_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.CreateIndex(S_INDEX_MEDIA_TABLE_PRIMARY, S_MEDIA_KEY, [iaUnique, iaNotEmpty]);
          TblMeta.CreateIndex(S_INDEX_MEDIA_OWNER_KEY, S_MEDIA_OWNER_KEY, [iaNotEmpty]);
        finally
          TblMeta.Free;
        end;

        Handle := DBAPI.OpenTableOrKey(S_COMMENT_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.CreateIndex(S_INDEX_COMMENT_TABLE_PRIMARY, S_COMMENT_KEY, [iaUnique, iaNotEmpty]);
          TblMeta.CreateIndex(S_INDEX_COMMENT_OWNER_KEY, S_COMMENT_OWNER_KEY, [iaNotEmpty]);
          TblMeta.CreateIndex(S_INDEX_COMMENT_MEDIA_KEY, S_COMMENT_MEDIA_KEY, [iaNotEmpty]);
        finally
          TblMeta.Free;
        end;

        FK := DBAPI.CreateForeignKey(S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID);
        FKMeta := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
        try
          FKMeta.SetReferencingChild(S_MEDIA_TABLE, S_INDEX_MEDIA_OWNER_KEY);
          FKMeta.SetReferencedParent(S_USER_TABLE, S_INDEX_USER_TABLE_PRIMARY);
        finally
          FKMeta.Free;
        end;

        FK := DBAPI.CreateForeignKey(S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID);
        FKMeta := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
        try
          FKMeta.SetReferencingChild(S_COMMENT_TABLE, S_INDEX_COMMENT_OWNER_KEY);
          FKMeta.SetReferencedParent(S_USER_TABLE, S_INDEX_USER_TABLE_PRIMARY);
        finally
          FKMeta.Free;
        end;

        FK := DBAPI.CreateForeignKey(S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID);
        FKMeta := DBAPI.GetApiObjectFromHandle(FK, APIForeignKey) as TMemAPIForeignKey;
        try
          FKMeta.SetReferencingChild(S_COMMENT_TABLE, S_INDEX_COMMENT_MEDIA_KEY);
          FKMeta.SetReferencedParent(S_MEDIA_TABLE, S_INDEX_MEDIA_TABLE_PRIMARY);
        finally
          FKMeta.Free;
        end;
      finally
        DBAPI.Free;
      end;
      T.CommitAndFree;
      Phase2Done := true;
    except
      on Exception do
      begin
        T.RollbackAndFree;
        Phase2Done := false;
        raise;
      end;
    end;
  end;

  procedure Phase2Rollback;
  var
    DBAPI: TMemAPIDatabase;
    T: TMemDBTransaction;
    TblMeta: TMemAPITableMetadata;
    FKMeta: TMemAPIForeignKey;
    Handle, FK: TMemDBHandle;
  begin
    T := S.StartTransaction(amReadWrite);
    try
      DBAPI := T.GetAPI;
      try
        DBAPI.DeleteTableOrKey(S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID);
        DBAPI.DeleteTableOrKey(S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID);
        DBAPI.DeleteTableOrKey(S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID);

        Handle := DBAPI.OpenTableOrKey(S_MEDIA_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.DeleteIndex(S_INDEX_MEDIA_TABLE_PRIMARY);
          TblMeta.DeleteIndex(S_INDEX_MEDIA_OWNER_KEY);
        finally
          TblMeta.Free;
        end;

        Handle := DBAPI.OpenTableOrKey(S_COMMENT_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.DeleteIndex(S_INDEX_COMMENT_TABLE_PRIMARY);
          TblMeta.DeleteIndex(S_INDEX_COMMENT_OWNER_KEY);
          TblMeta.DeleteIndex(S_INDEX_COMMENT_MEDIA_KEY);
        finally
          TblMeta.Free;
        end;
      finally
        DBAPI.Free;
      end;
      T.CommitAndFree;
    except
      on Exception do
      begin
        T.RollbackAndFree;
        raise;
      end;
    end;
  end;

  procedure Phase3Convert;
  var
    T: TMemDBTransaction;
    Handle: TMemDBHandle;
    TblMeta: TMemAPITableMetadata;
    DBAPI: TMemAPIDatabase;
  begin
    T := S.StartTransaction(amReadWrite);
    try
      //If first two phases have worked OK, then we can just delete the old stuff.
      DBAPI := T.GetAPI;
      try
        DBAPI.DeleteTableOrKey(S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID_OLD);
        DBAPI.DeleteTableOrKey(S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID_OLD);
        DBAPI.DeleteTableOrKey(S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID_OLD);

        //Rename old keys, fields, indices FK's to "old" values
        Handle := DBAPI.OpenTableOrKey(S_USER_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.DeleteIndex(S_INDEX_USER_TABLE_PRIMARY_OLD);
          TblMeta.DeleteField(S_USER_KEY_OLD);
        finally
          TblMeta.Free;
        end;

        Handle := DBAPI.OpenTableOrKey(S_MEDIA_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.DeleteIndex(S_INDEX_MEDIA_TABLE_PRIMARY_OLD);
          TblMeta.DeleteIndex(S_INDEX_MEDIA_OWNER_KEY_OLD);
          TblMeta.DeleteField(S_MEDIA_KEY_OLD);
          TblMeta.DeleteField(S_MEDIA_OWNER_KEY_OLD);
        finally
          TblMeta.Free;
        end;

        Handle := DBAPI.OpenTableOrKey(S_COMMENT_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Handle, APITableMetadata) as TMemAPITableMetadata;
        try
          TblMeta.DeleteIndex(S_INDEX_COMMENT_MEDIA_KEY_OLD);
          TblMeta.DeleteIndex(S_INDEX_COMMENT_OWNER_KEY_OLD);
          TblMeta.DeleteIndex(S_INDEX_COMMENT_TABLE_PRIMARY_OLD);
          TblMeta.DeleteField(S_COMMENT_KEY_OLD);
          TblMeta.DeleteField(S_COMMENT_OWNER_KEY_OLD);
          TblMeta.DeleteField(S_COMMENT_MEDIA_KEY_OLD);
        finally
          TblMeta.Free;
        end;
        //And we are done.
      finally
        DBAPI.Free;
      end;
      T.CommitAndFree;
      Phase3Done := true;
    except
      on Exception do
      begin
        T.RollbackAndFree;
        Phase3Done := false;
        raise;
      end;
    end;
  end;

  procedure DetermineUpgradeState;
  var
    T: TMemDBTransaction;
    DBAPI: TMemAPIDatabase;
    TblMeta: TMemAPITableMetadata;
    Entity: TMemDBHandle;
    SL: TStringList;
    AllOldFormat, AllNewFormat: boolean;

    procedure UpdateFormatInfo(FieldName: string);
    var
      FieldType: TMDBFieldType;
    begin
      if not TblMeta.FieldInfo(FieldName, FieldType) then
        raise EDatabaseUpgradeException.Create('');
      if FieldType <> ftUnicodeString then
        AllOldFormat := false;
      if FieldType <> ftGuid then
        AllNewFormat := false;
    end;

  begin
    T := S.StartTransaction(amRead);
    try
      DBAPI := T.GetAPI;
      try
        //First, check appropriate entities and fields exist.
        AllOldFormat := true;
        AllNewFormat := true;

        Entity := DBAPI.OpenTableOrKey(S_USER_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Entity, APITableMetadata) as TMemAPITableMetadata;
        try
          SL := TblMeta.GetFieldNames;
          try
            if SL.IndexOf(S_USER_KEY) < 0 then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_USER_KEY_OLD) >=0  then
              raise EDatabaseUpgradeException.Create('');
            //TODO - Check the required indices exist.
          finally
            SL.Free;
          end;
          SL := TblMeta.GetIndexNames;
          try
            if SL.IndexOf(S_INDEX_USER_TABLE_PRIMARY) < 0 then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_INDEX_USER_TABLE_PRIMARY_OLD) >=0  then
              raise EDatabaseUpgradeException.Create('');
          finally
            SL.Free;
          end;
          UpdateFormatInfo(S_USER_KEY);
        finally
          TblMeta.Free;
        end;

        Entity := DBAPI.OpenTableOrKey(S_MEDIA_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Entity, APITableMetadata) as TMemAPITableMetadata;
        try
          SL := TblMeta.GetFieldNames;
          try
            if SL.IndexOf(S_MEDIA_KEY) < 0 then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_MEDIA_KEY_OLD) >=0  then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_MEDIA_OWNER_KEY) < 0 then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_MEDIA_OWNER_KEY_OLD) >=0  then
              raise EDatabaseUpgradeException.Create('');
          finally
            SL.Free;
          end;
          SL := TblMeta.GetIndexNames;
          try
            if SL.IndexOf(S_INDEX_MEDIA_TABLE_PRIMARY) < 0 then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_INDEX_MEDIA_TABLE_PRIMARY_OLD) >=0  then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_INDEX_MEDIA_OWNER_KEY) < 0 then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_INDEX_MEDIA_OWNER_KEY_OLD) >=0  then
              raise EDatabaseUpgradeException.Create('');
          finally
            SL.Free;
          end;
          UpdateFormatInfo(S_MEDIA_KEY);
          UpdateFormatInfo(S_MEDIA_OWNER_KEY);
        finally
          TblMeta.Free;
        end;


        Entity := DBAPI.OpenTableOrKey(S_COMMENT_TABLE);
        TblMeta := DBAPI.GetApiObjectFromHandle(Entity, APITableMetadata) as TMemAPITableMetadata;
        try
          SL := TblMeta.GetFieldNames;
          try
            if SL.IndexOf(S_COMMENT_KEY) < 0 then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_COMMENT_KEY_OLD) >=0  then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_COMMENT_OWNER_KEY) < 0 then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_COMMENT_OWNER_KEY_OLD) >=0  then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_COMMENT_MEDIA_KEY) < 0 then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_COMMENT_MEDIA_KEY_OLD) >=0  then
              raise EDatabaseUpgradeException.Create('');
            //TODO - Check the required indices exist.
          finally
            SL.Free;
          end;
          SL := TblMeta.GetIndexNames;
          try
            if SL.IndexOf(S_INDEX_COMMENT_TABLE_PRIMARY) < 0 then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_INDEX_COMMENT_TABLE_PRIMARY_OLD) >=0  then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_INDEX_COMMENT_OWNER_KEY) < 0 then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_INDEX_COMMENT_OWNER_KEY_OLD) >=0  then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_INDEX_COMMENT_MEDIA_KEY) < 0 then
              raise EDatabaseUpgradeException.Create('');
            if SL.IndexOf(S_INDEX_COMMENT_MEDIA_KEY_OLD) >=0  then
              raise EDatabaseUpgradeException.Create('');
          finally
            SL.Free;
          end;
          UpdateFormatInfo(S_COMMENT_KEY);
          UpdateFormatInfo(S_COMMENT_OWNER_KEY);
          UpdateFormatInfo(S_COMMENT_MEDIA_KEY);
        finally
          TblMeta.Free;
        end;

        //TODO - Check existence of foreign keys, but we won't check
        //exactly which indices they refer to.
        SL := DBAPI.GetEntityNames;
        try
          if SL.IndexOf(S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID) < 0 then
            raise EDatabaseUpgradeException.Create('');
          if SL.IndexOf(S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID_OLD) >=0  then
            raise EDatabaseUpgradeException.Create('');
          if SL.IndexOf(S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID) < 0 then
            raise EDatabaseUpgradeException.Create('');
          if SL.IndexOf(S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID_OLD) >=0  then
            raise EDatabaseUpgradeException.Create('');
          if SL.IndexOf(S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID) < 0 then
            raise EDatabaseUpgradeException.Create('');
          if SL.IndexOf(S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID_OLD) >=0  then
            raise EDatabaseUpgradeException.Create('');
        finally
          SL.Free;
        end;

        Assert(not(AllOldFormat and AllNewFormat));
        //If exception raised - then fields not as expected.
        if AllOldFormat then
          UpgradeState := ugsPre
        else if AllNewFormat then
          UpgradeState := ugsPost
        else
          UpgradeState := ugsInconsistent;
      finally
        DBAPI.Free;
      end;
    finally
      T.RollbackAndFree;
    end;
  end;

begin
  S := FDB.StartSession;
  S.TempStorageMode := tsmDisk;
  try
    try
      DetermineUpgradeState;
    except
      on Exception do UpgradeState := ugsInconsistent;
    end;
    result := UpgradeState = ugsPost;
    if result or (UpgradeState = ugsInconsistent) then
      exit;
    //Cool, we are in the pre-upgraded state.
    //How many transactions?
    //1. Rename old stuff, create new fields (no data change allowed).
    //2. Populate new fields, create new FK relationships, delete old fields.
    Phase1Done := false;
    Phase2Done := false;
    Phase3Done := false;
    try
      Phase1Convert;
      Phase2Convert;
      Phase3Convert;
      FDB.Checkpoint;
    except
      on Exception do
      begin
        try
          Assert(not Phase3Done);
          if Phase2Done then
            Phase2Rollback;
          if Phase1Done then
            Phase1Rollback;
        except
          on Exception do
          begin
            Phase1Done := false;
            Phase2Done := false;
            //FSCKED.
          end;
        end;
      end;
    end;
    result := Phase1Done and Phase2Done and Phase3Done;
  finally
    S.Free;
  end;
end;

function TMemKDB.SetupTables(out Msg:String): boolean;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
begin
  result := false;
  S := FDB.StartSession;
  try
    T := S.StartTransaction(amReadWrite);
    try
      OpenOrCreateUserTable(T, Msg);
      OpenOrCreateMediaTable(T, Msg);
      OpenOrCreateCommentTable(T, Msg);
      T.CommitAndFree;
      result := true;
    except
      on E: Exception do
      begin
        Msg := E.Message;
        T.RollbackAndFree;
      end;
    end;
  finally
    S.Free;
  end;
  if Result then
    Result := UpgradeTables(Msg);
end;

function TMemKDB.InitDBAndTables(out Msg: string): boolean;
begin
  result := FDB.InitDB(DBPath, jtV2, false);
  if result then
    result := SetupTables(Msg);
end;

function TMemKDB.FiniDBAndTables: boolean;
begin
  FDB.StopDB;
  result := true;
end;

function TMemKDB.DestroyTables: boolean;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  E: TMemDBHandle;
  DBAPI: TMemAPIDatabase;
  i: integer;
  EStr: string;
begin
  result := true;
  S := FDB.StartSession;
  try
    T := S.StartTransaction(amReadWrite);
    try
      DBAPI := T.GetAPI;
      try
        for i := 0 to 5 do
        begin
          case i of
            0: EStr:= S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID;
            1: EStr:= S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID;
            2: EStr:= S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID;
            3: EStr:= S_COMMENT_TABLE;
            4: EStr:= S_MEDIA_TABLE;
            5: EStr:= S_USER_TABLE;
          else
            Assert(false);
          end;
          E := DBAPI.OpenTableOrKey(EStr);
          if Assigned(E) then
            DBAPI.DeleteTableOrKey(EStr);
        end;
      finally
        DBAPI.Free;
      end;
      T.CommitAndFree;
    except
      on E: Exception do
      begin
        T.RollbackAndFree;
        result := false;
      end;
    end;
  finally
    S.Free;
  end;
end;

function TMemKDB.ClearTablesData: boolean;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  E: TMemDBHandle;
  DBAPI: TMemAPIDatabase;
  TblData: TMemAPITableData;
  i: integer;
  EStr: string;
begin
  result := true;
  S := FDB.StartSession;
  try
    T := S.StartTransaction(amReadWrite);
    try
      DBAPI := T.GetAPI;
      try
        for i := 0 to 2 do
        begin
          case i of
            0: EStr:= S_COMMENT_TABLE;
            1: EStr:= S_MEDIA_TABLE;
            2: EStr:= S_USER_TABLE;
          else
            Assert(false);
          end;
          E := DBAPI.OpenTableOrKey(EStr);
          if Assigned(E) then
          begin
            TblData := DBAPI.GetApiObjectFromHandle(E, APITableData) as TMemAPITableData;
            try
              TblData.Locate(ptFirst, '');
              while TblData.RowSelected do
                TblData.Delete;
            finally
              TblData.Free;
            end;
          end;
        end;
      finally
        DBAPI.Free;
      end;
      T.CommitAndFree;
    except
      on E: Exception do
      begin
        T.RollbackAndFree;
        result := false;
      end;
    end;
  finally
    S.Free;
  end;
end;

function TMemKDB.GetIsInit: boolean;
begin
  result := FDB.DBState in [mdbInit, mdbRunning];
end;

constructor TMemKDB.Create;
begin
  inherited;
  FDB := TMemDB.Create;
end;

destructor TMemKDB.Destroy;
begin
  FiniDBAndTables;
  FDB.Free;
  inherited;
end;

end.
