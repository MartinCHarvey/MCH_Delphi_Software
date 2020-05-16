object ChkInService: TChkInService
  OldCreateOrder = False
  AllowPause = False
  DisplayName = 'CheckIn Service'
  WaitHint = 10000
  OnShutdown = ServiceShutdown
  OnStart = ServiceStart
  OnStop = ServiceStop
  Height = 150
  Width = 215
end
