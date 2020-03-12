unit CocoDefs;
{$INCLUDE CocoCD.inc}

interface

uses
  CocoComment;

type
  TCocoShowMessage = function(const Msg: AnsiString;
      const YesNo : boolean) : boolean of object;
  TInsertCommentAtTop = function (const Start : integer; const  Stop : integer;
      const Nested : boolean) : TCommentItem of object;

const  // file extensions
  ATG_EXT      = 'ATG';
  ATG_FILE_EXT = '.' + ATG_EXT;  // attributed grammar extension
  AGP_EXT      = 'AGP';
  AGP_FILE_EXT = '.' + AGP_EXT;  // attributed grammar project extension
  AGI_EXT      = 'AGI';
  AGI_FILE_EXT = '.' + AGI_EXT;  // attributed grammar initialization extension
  LST_EXT      = 'LST';
  LST_FILE_EXT = '.' + LST_EXT;  // list file extension

  META_ERROR_ABORT_ERROR = 1000;
  META_ERROR_SOURCE_NOT_FOUND = 1001;

var
  ListFileName : AnsiString;
  ComponentFileName : AnsiString;

implementation

end.
 
