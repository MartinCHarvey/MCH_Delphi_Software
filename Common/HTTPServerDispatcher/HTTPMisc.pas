unit HTTPMisc;

interface

type
  TEndpointRequestEvent = function(Sender: TObject):string of object;

const
  S_LOCALHOST = 'localhost';
  S_HTTPS_PREFIX = 'https://';
  S_HTTP_PREFIX = 'http://';
  S_TRANSPORT_PREFIX = S_HTTPS_PREFIX;

implementation

end.
