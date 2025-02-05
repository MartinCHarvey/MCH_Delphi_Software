program MFract;



uses
  Forms,
  MFMainFrm in 'MFMainFrm.pas' {MFMainForm},
  DLList in '..\Common\DLList\DLList.pas',
  WorkItems in '..\Common\WorkItems\WorkItems.pas',
  Trackables in '..\Common\Tracking\Trackables.pas',
  BinaryTree in '..\Common\Balanced Tree\BinaryTree.pas',
  TrivXMLDefs in '..\Common\StreamingSystem\TrivXMLDefs.pas',
  SSAbstracts in '..\Common\StreamingSystem\SSAbstracts.pas',
  SSIntermediates in '..\Common\StreamingSystem\SSIntermediates.pas',
  SSStreamables in '..\Common\StreamingSystem\SSStreamables.pas',
  StreamingSystem in '..\Common\StreamingSystem\StreamingSystem.pas',
  StreamSysXML in '..\Common\StreamingSystem\StreamSysXML.pas',
  TrivXML in '..\Common\StreamingSystem\TrivXML.PAS',
  FractSettingsDialog in 'FractSettingsDialog.pas' {FractSettingsDlg},
  PaletteSettingsDialog in 'PaletteSettingsDialog.pas' {PaletteSettingsDlg},
  FractAboutDialog in 'FractAboutDialog.pas' {FractAboutDlg},
  AppVersionStr in '..\Common\Version\AppVersionStr.pas',
  ExportOptsDialog in 'ExportOptsDialog.pas' {ExportOptsDlg},
  ReRenderForm in 'ReRenderForm.pas' {ReRenderFrm},
  DragHelperFrm in 'DragHelperFrm.pas' {DragHelperForm},
  SizeableImage in '..\Common\SizeableImage\SizeableImage.pas',
  MonRadioButton in '..\Common\MonRadioButton\MonRadioButton.pas',
  DLThreadQueue in '..\Common\WorkItems\DLThreadQueue.pas',
  FractRenderers in '..\MFractCommon\FractRenderers.pas',
  FractSettings in '..\MFractCommon\FractSettings.pas',
  FractLegacy in '..\MFractCommon\FractLegacy.pas',
  FractMisc in '..\MFractCommon\FractMisc.pas',
  FractStreaming in '..\MFractCommon\FractStreaming.pas',
  CocoBase in '..\CoCoDM\Distributable\Frames\CocoBase.pas',
  IndexedStore in '..\Common\Indexed Store\IndexedStore.pas',
  Parallelizer in '..\Common\Parallelizer\Parallelizer.pas',
  LockAbstractions in '..\Common\LockAbstractions\LockAbstractions.pas';

{$R *.RES}

begin
  Application.Initialize;
  Application.CreateForm(TMFMainForm, MFMainForm);
  Application.CreateForm(TFractSettingsDlg, FractSettingsDlg);
  Application.CreateForm(TPaletteSettingsDlg, PaletteSettingsDlg);
  Application.CreateForm(TFractAboutDlg, FractAboutDlg);
  Application.CreateForm(TExportOptsDlg, ExportOptsDlg);
  Application.CreateForm(TDragHelperForm, DragHelperForm);
  Application.Run;
end.

