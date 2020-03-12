unit DBGeneric;
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

uses Trackables, SysUtils, Classes, DLList, DataObjects;

// , DataObjects, SimpleDS, SyncObjs, SqlExpr,
// DLList, DB, Classes, SysUtils;

type
  TGenericDB = class;

  EDBPersistError = class(Exception); // For other misc errors.

  // Need to do the actions in the right order, otherwise duplicate updates.
  // update first, then add, then delete
  TPersistAction = (patUpdate, patAdd, patDelete);
  TPersistActionStrs = array [TPersistAction] of string;
  TPersistActionSet = set of TPersistAction;

{$IFDEF USE_TRACKABLES}

  TDBPersist = class(TTrackable)
{$ELSE}
  TDBPersist = class
{$ENDIF}
  private
    FParentDB: TGenericDB;
    FOnRequestCompleted: TNotifyEvent;
  protected
    procedure DoRequestCompleted;
  public
    constructor Create; virtual;

    function Stop: boolean; virtual; abstract; // returns whether idle.
    property ParentDB: TGenericDB read FParentDB write FParentDB;
    property OnRequestCompleted: TNotifyEvent read FOnRequestCompleted write FOnRequestCompleted;
  end;

  TDBPersistClass = class of TDBPersist;

{$IFDEF USE_TRACKABLES}

  TGenericDB = class(TTrackable)
{$ELSE}
  TGenericDB = class
{$ENDIF}
  protected
    function GetIsInit: boolean; virtual; abstract;
  public
    function InitDBAndTables(out Msg: string): boolean; virtual; abstract;
    function FiniDBAndTables: boolean; virtual; abstract;
    function DestroyTables: boolean; virtual; abstract;
    function ClearTablesData: boolean; virtual; abstract;

    function CreateItemPersister(Level: TKListLevel): TDBPersist;
    function PersisterClass(Level: TKListLevel): TDBPersistClass; virtual; abstract;

    property IsInit: boolean read GetIsInit;
  end;

const
  //TODO - Can we get rid of any of these strings?

  // Logging.
  S_TRANSACTION_COMMIT_ON_DESTROY = 'Transaction auto-committing on destroy,' +
    'bad idea since under DB global lock.';
  S_GET_DATASET_FROM_CACHE = 'Got dataset from cache.';
  S_CREATE_DATASET = 'Created new dataset.';
  S_PUT_DATASET_TO_CACHE = 'Returned dataset to cache.';
  S_CLEARING_READ_CACHES = 'Clearing read dataset caches...';
  S_CLEARED_READ_CACHES = 'Cleared read caches.';
  S_CLEARING_WRITER_TABLE = 'Clearing writer table...';
  S_CLEARED_WRITER_TABLE = 'Cleared writer table.';
  S_DESTROYED_DATASET_SPARE = 'Destroyed spare dataset';
  S_DESTROYED_DATASET_USED = 'Destroyed used dataset';
  S_USE_WRITER_TABLE = 'Get table - using writer table';
  S_GET_CACHED_TABLE = 'Get table - getting cached table';
  S_TABLE_GET_FAILED = 'Get table failed.';
  S_PUT_CACHED_TABLE = 'Put table - Put cached table.';
  S_FREE_WRITER_TABLE = 'Put table - Put writer table (not freed).';

  PersistActionStrs: TPersistActionStrs = ('patUpdate', 'patAdd', 'patDelete');

implementation

uses
  GlobalLog;

const
  EnableTableCaching = true;

  { TDBPersist }

constructor TDBPersist.Create;
begin
  inherited;
  // Yes, this needs to be here, switch from static to dynamically
  // resolved constructors.
end;

procedure TDBPersist.DoRequestCompleted;
begin
  if Assigned(FOnRequestCompleted) then
    FOnRequestCompleted(self);
end;

{ TGenericDB}

function TGenericDB.CreateItemPersister(Level: TKListLevel): TDBPersist;
begin
  result := nil;
  if Assigned(PersisterClass(Level)) then
  begin
    result := PersisterClass(Level).Create;
    result.ParentDB := self;
  end
  else
    Assert(false);
end;

end.
