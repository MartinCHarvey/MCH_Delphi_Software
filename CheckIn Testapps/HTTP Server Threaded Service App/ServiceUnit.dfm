object MCH_HTTPService: TMCH_HTTPService
  OldCreateOrder = False
  DisplayName = 'MCH HTTP Service'
  OnShutdown = ServiceShutdown
  OnStart = ServiceStart
  OnStop = ServiceStop
  Height = 150
  Width = 215
  object HTTPServer: TIdHTTPServer
    Bindings = <>
    Left = 16
    Top = 8
  end
end
