program TestComments;

uses
  Forms,
  Comments in 'Comments.PAS',
  fTestComments in 'fTestComments.pas' {fmTestComments}
  , CocoBase in 'C:\Source\Code\CocoR\Frames\CocoBase.pas'  ;

{$R *.RES}

begin
  Application.Initialize;
  Application.CreateForm(TfmTestComments, fmTestComments);
  Application.Run;
end.    
