unit DBSchemaStrs;
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
  DataObjects;

const
  //Important constants like db field names.
  GUIDStringLength = 38;
  SiteIDLength = 64;

  S_LAST_UPDATED = 'LAST_UPDATED';
  S_USER_TABLE = 'USER_TABLE';
  S_USER_KEY = 'USER_KEY';
  S_USER_KEY_OLD = 'USER_KEY_OLD'; //For upgrades.
  S_USER_INTEREST = 'USER_INTEREST';

  S_USER_INSTA_UNAME = 'USER_INSTA_UNAME';
  S_USER_INSTA_UID = 'USER_INSTA_UID';
  S_USER_INSTA_VERIFIED = 'USER_INSTA_VERIFIED';
  S_USER_INSTA_PROF_URL = 'USER_INSTA_PROF_URL';
  S_USER_INSTA_FULL_NAME = 'USER_INSTA_FULL_NAME;';
  S_USER_INSTA_BIO = 'USER_INSTA_BIO';
  S_USER_INSTA_FOLLOWS_COUNT = 'USER_INSTA_FOLLOWS_COUNT';
  S_USER_INSTA_FOLLOWER_COUNT = 'USER_INSTA_FOLLOWER_COUNT';

  S_USER_TWITTER_UNAME = 'USER_TWITTER_UNAME';
  S_USER_TWITTER_UID = 'USER_TWITTER_UID';
  S_USER_TWITTER_VERIFIED = 'USER_TWITTER_VERIFIED';
  S_USER_TWITTER_PROF_URL = 'USER_TWITTER_PROF_URL';
  S_USER_TWITTER_FULL_NAME = 'USER_TWITTER_FULL_NAME;';
  S_USER_TWITTER_BIO = 'USER_TWITTER_BIO';
  S_USER_TWITTER_FOLLOWS_COUNT = 'USER_TWITTER_FOLLOWS_COUNT';
  S_USER_TWITTER_FOLLOWER_COUNT = 'USER_TWITTER_FOLLOWER_COUNT';

  S_INDEX_USER_INSTA_UNAME = 'IDX_U_INST_UNAME';
  S_INDEX_USER_INSTA_UID = 'IDX_U_INST_UID';
  S_INDEX_USER_TWITTER_UNAME = 'IDX_U_TW_UNAME';
  S_INDEX_USER_TWITTER_UID = 'INDEX_U_TW_UID';

  S_INDEX_USER_TABLE_PRIMARY = 'IDX_U_TAB_PRIMARY';
  S_INDEX_USER_TABLE_PRIMARY_OLD = 'IDX_U_TAB_PRIMARY_OLD';

type
  TUserBlockFields = (tubUserName,
                      tubUserId,
                      tubVerified,
                      tubProfUrl,
                      tubFullName,
                      tubBio,
                      tubFollowsCount,
                      tubFollowerCount);

  TSiteUserBlockFieldNames = array [TUserBlockFields] of string;
  TUserBlockFieldNames = array[TKSiteType] of TSiteUserBlockFieldNames;

  TUserBlockIndexes = (tuxUserName,
                       tuxUserId);

  TSiteUserBlockIndexNames = array [TUserBlockIndexes] of string;
  TUserBlockIndexNames = array[TKSiteType] of TSiteUserBlockIndexNames;

const
  UserBlockFieldNames: TUserBlockFieldNames =
    ( //tstInstagram
      (
        S_USER_INSTA_UNAME, //tubUserName,
        S_USER_INSTA_UID, //tubUserId,
        S_USER_INSTA_VERIFIED, //tubVerified,
        S_USER_INSTA_PROF_URL, //tubProfUrl,
        S_USER_INSTA_FULL_NAME, //tubFullName,
        S_USER_INSTA_BIO, //tubBio,
        S_USER_INSTA_FOLLOWS_COUNT, //tubFollowsCount,
        S_USER_INSTA_FOLLOWER_COUNT //tubFollowerCount
      ),
      //tstTwitter
      (
        S_USER_TWITTER_UNAME, //tubUserName,
        S_USER_TWITTER_UID, //tubUserId,
        S_USER_TWITTER_VERIFIED, //tubVerified,
        S_USER_TWITTER_PROF_URL, //tubProfUrl,
        S_USER_TWITTER_FULL_NAME, //tubFullName,
        S_USER_TWITTER_BIO, //tubBio,
        S_USER_TWITTER_FOLLOWS_COUNT, //tubFollowsCount,
        S_USER_TWITTER_FOLLOWER_COUNT //tubFollowerCount
      )
    );

    UserBlockIndexNames: TUserBlockIndexNames =
    ( //tstInstagram
      (
        S_INDEX_USER_INSTA_UNAME,
        S_INDEX_USER_INSTA_UID
      ),
      //tstTwitter
      (
        S_INDEX_USER_TWITTER_UNAME,
        S_INDEX_USER_TWITTER_UID
      )
    );

    UserBlockIndexFieldNames: TUserBlockIndexNames =
    ( //tstInstagram
      (
        S_USER_INSTA_UNAME,
        S_USER_INSTA_UID
      ),
      //tstTwitter
      (
        S_USER_TWITTER_UNAME,
        S_USER_TWITTER_UID
      )
    );

  S_MEDIA_TABLE = 'MEDIA_TABLE';
  S_MEDIA_KEY = 'MEDIA_KEY';
  S_MEDIA_KEY_OLD = 'MEDIA_KEY_OLD';

  S_MEDIA_OWNER_KEY = 'MEDIA_OWNER_KEY';
  S_MEDIA_OWNER_KEY_OLD = 'MEDIA_OWNER_KEY_OLD';

  S_MEDIA_DATE = 'MEDIA_DATE';
  S_MEDIA_DATA_STR = 'MEDIA_DATA_STR';
  S_MEDIA_TYPE = 'MEDIA_TYPE';
  S_MEDIA_RESOURCE_URL = 'MEDIA_RESOURCE_URL';

  S_MEDIA_INSTA_MEDIA_ID = 'MEDIA_INSTA_MEDIA_ID';
  S_MEDIA_INSTA_OWNER_ID = 'MEDIA_INSTA_OWNER_ID';
  S_MEDIA_INSTA_MEDIA_CODE = 'MEDIA_INSTA_MEDIA_CODE';

  S_MEDIA_TWITTER_MEDIA_ID = 'MEDIA_TWITTER_MEDIA_ID';
  S_MEDIA_TWITTER_OWNER_ID = 'MEDIA_TWITTER_OWNER_ID';
  S_MEDIA_TWITTER_MEDIA_CODE = 'MEDIA_TWITTER_MEDIA_CODE';

  S_INDEX_MEDIA_INSTA_MEDIA_ID = 'IDX_M_INST_MEDIA_ID';
  S_INDEX_MEDIA_INSTA_OWNER_ID = 'IDX_M_INST_OWNER_ID';
  S_INDEX_MEDIA_INSTA_MEDIA_CODE = 'IDX_M_INST_MEDIA_CODE';

  S_INDEX_MEDIA_TWITTER_MEDIA_ID = 'IDX_M_TW_MEDIA_ID';
  S_INDEX_MEDIA_TWITTER_OWNER_ID = 'IDX_M_TW_OWNER_ID';
  S_INDEX_MEDIA_TWITTER_MEDIA_CODE = 'IDX_M_TW_MEDIA_CODE';

type
  TMediaBlockFields = (tmbMediaId,
                       tmbOwnerId,
                       tmbMediaCode);

  TSiteMediaBlockFieldNames = array[TMediaBlockFields] of string;
  TMediaBlockFieldNames = array[TKSiteType] of TSiteMediaBlockFieldNames;

  TMediaBlockIndexes = (tmxMediaId,
                        tmxOwnerId,
                        tmxMediaCode);

  TSiteMediaBlockIndexes = array[TMediaBlockIndexes] of string;
  TMediaBlockIndexNames = array[TKSiteType] of TSiteMediaBlockIndexes;


  //TODO TODO TODO - should definitely be able to trim some indexes,
  //get the thing to load quicker - some may be redundant or unnecessary.

const
  MediaBlockFieldNames:TMediaBlockFieldNames =
    ( //tstInstagram
      (
        S_MEDIA_INSTA_MEDIA_ID, //tmbMediaId
        S_MEDIA_INSTA_OWNER_ID, //tmbOwnerId
        S_MEDIA_INSTA_MEDIA_CODE //tmbCode
      ),
      //tstTwitter
      (
        S_MEDIA_TWITTER_MEDIA_ID, //tmbMediaId
        S_MEDIA_TWITTER_OWNER_ID, //tmbOwnerId
        S_MEDIA_TWITTER_MEDIA_CODE //tmbCode
      )
    );

  MediaBlockIndexNames:TMediaBlockIndexNames =
    ( //tstInstagram
      (
        S_INDEX_MEDIA_INSTA_MEDIA_ID,
        S_INDEX_MEDIA_INSTA_OWNER_ID,
        S_INDEX_MEDIA_INSTA_MEDIA_CODE
      ),
      //tstTwitter
      (
        S_INDEX_MEDIA_TWITTER_MEDIA_ID,
        S_INDEX_MEDIA_TWITTER_OWNER_ID,
        S_INDEX_MEDIA_TWITTER_MEDIA_CODE
      )
    );

  MediaBlockIndexFieldNames:TMediaBlockIndexNames =
    ( //tstInstagram
      (
        S_MEDIA_INSTA_MEDIA_ID,
        S_MEDIA_INSTA_OWNER_ID,
        S_MEDIA_INSTA_MEDIA_CODE
      ),
      //tstTwitter
      (
        S_MEDIA_TWITTER_MEDIA_ID,
        S_MEDIA_TWITTER_OWNER_ID,
        S_MEDIA_TWITTER_MEDIA_CODE
      )
    );

  S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID = 'MEDIA_TABLE_CONSTRAINT_REFS_VALID_USER';
  S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID_OLD = 'MEDIA_TABLE_CONSTRAINT_REFS_VALID_USER_OLD';
  S_INDEX_MEDIA_TABLE_PRIMARY = 'INDEX_MEDIA_TABLE_PRIMARY';
  S_INDEX_MEDIA_OWNER_KEY = 'INDEX_MEDIA_OWNER_KEY';

  S_INDEX_MEDIA_TABLE_PRIMARY_OLD = 'INDEX_MEDIA_TABLE_PRIMARY_OLD';
  S_INDEX_MEDIA_OWNER_KEY_OLD = 'INDEX_MEDIA_OWNER_KEY_OLD';

  S_COMMENT_TABLE = 'COMMENT_TABLE';
  S_COMMENT_KEY = 'COMMENT_KEY';
  S_COMMENT_KEY_OLD = 'COMMENT_KEY_OLD';
  S_COMMENT_OWNER_KEY = 'COMMENT_OWNER_KEY';
  S_COMMENT_OWNER_KEY_OLD = 'COMMENT_OWNER_KEY_OLD';
  S_COMMENT_MEDIA_KEY = 'COMMENT_MEDIA_KEY';
  S_COMMENT_MEDIA_KEY_OLD = 'COMMENT_MEDIA_KEY_OLD';

  S_COMMENT_INSTA_COMMENT_ID = 'COMMENT_INSTA_COMMENT_ID'; //Indexed.
  S_COMMENT_INSTA_OWNER_ID = 'COMMENT_INSTA_OWNER_ID'; //Indexed

  S_COMMENT_TWITTER_COMMENT_ID = 'COMMENT_TWITTER_COMMENT_ID'; //Indexed.
  S_COMMENT_TWITTER_OWNER_ID = 'COMMENT_TWITTER_OWNER_ID'; //Indexed

  S_COMMENT_DATE = 'COMMENT_DATE';
  S_COMMENT_DATA_STR = 'COMMENT_DATA_STR';
  S_COMMENT_TYPE = 'COMMENT_TYPE';

  S_INDEX_COMMENT_INSTA_COMMENT_ID = 'IDX_C_INST_COMMENT_ID';
  S_INDEX_COMMENT_INSTA_OWNER_ID = 'IDX_C_INST_OWNER_ID';

  S_INDEX_COMMENT_OWNER_KEY = 'IDX_C_OWNER_KEY';
  S_INDEX_COMMENT_MEDIA_KEY = 'IDX_C_MEDIA_KEY';

  S_INDEX_COMMENT_OWNER_KEY_OLD = 'IDX_C_OWNER_KEY_OLD';
  S_INDEX_COMMENT_MEDIA_KEY_OLD = 'IDX_C_MEDIA_KEY_OLD';

  S_INDEX_COMMENT_TWITTER_COMMENT_ID = 'IDX_C_TW_COMMENT_ID';
  S_INDEX_COMMENT_TWITTER_OWNER_ID = 'IDX_C_TW_OWNER_ID';

  S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID = 'COMMENT_TABLE_CONSTRAINT_REFS_VALID_USER';
  S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID_OLD = 'COMMENT_TABLE_CONSTRAINT_REFS_VALID_USER_OLD';
  S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID = 'COMMENT_TABLE_CONSTRAINT_REFS_VALID_MEDIA';
  S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID_OLD = 'COMMENT_TABLE_CONSTRAINT_REFS_VALID_MEDIA_OLD';
  S_INDEX_COMMENT_TABLE_PRIMARY = 'IDX_C_TAB_PRIMARY';
  S_INDEX_COMMENT_TABLE_PRIMARY_OLD = 'IDX_C_TAB_PRIMARY_OLD';

type
  TCommentBlockFields = (tcbCommentId, tcbCommentOwnerId );

  TSiteCommentBlockFieldNames = array [TCommentBlockFields] of string;
  TCommentBlockFieldNames = array[TKSiteType] of TSiteCommentBlockFieldNames;

  TCommentBlockIndexes = (tcxCommentId,
                          tcxOwnerId);

  TSiteCommentBlockIndexes = array[TCommentBlockIndexes] of string;
  TCommentBlockIndexNames = array[TKSiteType] of TSiteCommentBlockIndexes;

const
  CommentBlockFieldNames: TCommentBlockFieldNames =
    (
      (//tstInstagram
        S_COMMENT_INSTA_COMMENT_ID,
        S_COMMENT_INSTA_OWNER_ID
      ),
      (//tstTwitter
        S_COMMENT_TWITTER_COMMENT_ID,
        S_COMMENT_TWITTER_OWNER_ID
      )
    );

  CommentBlockIndexNames: TCommentBlockIndexNames =
    (
      (//tstInstagram
        S_INDEX_COMMENT_INSTA_COMMENT_ID,
        S_INDEX_COMMENT_INSTA_OWNER_ID
      ),
      (//tstTwitter
        S_INDEX_COMMENT_TWITTER_COMMENT_ID,
        S_INDEX_COMMENT_TWITTER_OWNER_ID
      )
    );

  CommentBlockIndexFieldNames: TCommentBlockIndexNames =
    (
      (//tstInstagram
        S_COMMENT_INSTA_COMMENT_ID,
        S_COMMENT_INSTA_OWNER_ID
      ),
      (//tstTwitter
        S_COMMENT_TWITTER_COMMENT_ID,
        S_COMMENT_TWITTER_OWNER_ID
      )
    );

implementation

{
  Schema design:

  Initial schema, may later have to mod code to add in columns to extend
  column sets for twitter etc. Will require "alter table" to add new columns and indexes.

  Pending user table:


  User table: (USER_TABLE)
      USER_KEY: ftFixedChar(38) (Primary, Unique)

      USER_INSTA_UNAME: ftString (64) (Indexed) //Keep as unique in s/w
      USER_INSTA_UID: ftString (64) (Indexed) //Keep as unique in s/w
      USER_INSTA_VERIFIED: ftBoolean
      USER_INSTA_PROF_URL: ftWideMemo
      USER_INSTA_FULL_NAME: ftWideMemo
      USER_INSTA_BIO: ftWideMemo
      USER_INSTA_FOLLOWS_COUNT: ftLargeInt
      USER_INSTA_FOLLOWER_COUNT: ftLargeInt

      USER_TWITTER_UNAME: ftString (64) (Indexed) //Keep as unique in s/w
      USER_TWITTER_UID: ftString (64) (Indexed) //Keep as unique in s/w
      USER_TWITTER_VERIFIED: ftBoolean
      USER_TWITTER_PROF_URL: ftWideMemo
      USER_TWITTER_FULL_NAME: ftWideMemo
      USER_TWITTER_BIO: ftWideMemo
      USER_TWITTER_FOLLOWS_COUNT: ftLargeInt
      USER_TWITTER_FOLLOWER_COUNT: ftLargeInt

      LAST_UPDATED: ftDateTime //When last scanned for changes.
      USER_INTEREST: ftSmallInt

  Indexes:
    S_INDEX_USER_TABLE_PRIMARY, S_USER_KEY, [ixPrimary, ixUnique]);

    S_INDEX_USER_INSTA_UID, S_USER_INSTA_UID, []);
    S_INDEX_USER_INSTA_UNAME, S_USER_INSTA_UNAME, []);

    S_INDEX_USER_TWITTER_UID, S_USER_TWITTER_UID, []);
    S_INDEX_USER_TWITTER_UNAME, S_USER_TWITTER_UNAME, []);

  Media Table: (MEDIA_TABLE)
      MEDIA_KEY: ftFixedChar(38) (Primary, Unique)
      MEDIA_OWNER_KEY: ftFixedChar(38). FOREIGN_KEY USER_TABLE(USER_KEY)

      MEDIA_DATE: ftDateTime
      MEDIA_ORIGINATOR_COMMENT: ftWideMemo
      MEDIA_RESOURCE_URL: ftWideMemo

      LAST_UPDATED: ftDateTime //Whan last scanned for changes

      MEDIA_INSTA_MEDIA_ID: ftString(64).
      MEDIA_INSTA_OWNER_ID: ftString(64).
      MEDIA_INSTA_MEDIA_CODE: ftString(16).

      MEDIA_TWITTER_MEDIA_ID: ftString(64).
      MEDIA_TWITTER_OWNER_ID: ftString(64).
      MEDIA_TWITTER_MEDIA_CODE: ftString(16).

  Indexes: in SW, not persistent.
    S_INDEX_MEDIA_TABLE_PRIMARY, S_MEDIA_KEY, [ixPrimary, ixUnique]);

    S_INDEX_MEDIA_INSTA_OWNER_ID, S_MEDIA_INSTA_OWNER_ID, []);
    S_INDEX_MEDIA_INSTA_MEDIA_ID, S_MEDIA_INSTA_MEDIA_ID, []);
    S_INDEX_MEDIA_INSTA_MEDIA_CODE, S_MEDIA_INSTA_MEDIA_CODE, []);

    S_INDEX_MEDIA_TWITTER_OWNER_ID, S_MEDIA_TWITTER_OWNER_ID, []);
    S_INDEX_MEDIA_TWITTER_MEDIA_ID, S_MEDIA_TWITTER_MEDIA_ID, []);
    S_INDEX_MEDIA_TWITTER_MEDIA_CODE, S_MEDIA_TWITTER_MEDIA_CODE, []);

  Comments Table:

      COMMENT_KEY: ftFixedChar(38) (Primary, Unique)
      COMMENT_OWNER_KEY: ftFixedChar(38). FOREIGN_KEY USER_TABLE(USER_KEY).
      COMMENT_MEDIA_KEY: ftFixedChar(38). FOREIGN_KEY MEDIA_TABLE(MEDIA_KEY).

      COMMENT_INSTA_COMMENT_ID: ftString(64). (Indexed) // Keep as unique in sw.
      COMMENT_INSTA_OWNER_ID: ftString(64). (Indexed)

      COMMENT_TWITTER_COMMENT_ID: ftString(64). (Indexed) // Keep as unique in sw.
      COMMENT_TWITTER_OWNER_ID: ftString(64). (Indexed)

      LAST_UPDATED: ftDateTime //When last scanned for changes

      COMMENT_DATE: ftDateTime
      COMMENT_TEXT: ftWideMemo

  Indexes: in SW, not persistent.

    S_INDEX_COMMENT_TABLE_PRIMARY, S_COMMENT_KEY, [ixPrimary, ixUnique]);
    S_INDEX_COMMENT_OWNER_KEY, S_COMMENT_OWNER_KEY []);
    S_INDEX_COMMENT_MEDIA_KEY, S_COMMENT_MEDIA_KEY []);

    S_INDEX_COMMENT_INSTA_COMMENT_ID, S_COMMENT_INSTA_COMMENT_ID, []);
    S_INDEX_COMMENT_INSTA_OWNER_ID, S_COMMENT_INSTA_OWNER_ID, []);

    S_INDEX_COMMENT_TWITTER_COMMENT_ID, S_COMMENT_TWITTER_COMMENT_ID, []);
    S_INDEX_COMMENT_TWITTER_OWNER_ID, S_COMMENT_TWITTER_OWNER_ID, []);

    User feedback table (likes etc etc)

      TODO - feedback table based on item guids. Add feedback type etc.
}

end.
