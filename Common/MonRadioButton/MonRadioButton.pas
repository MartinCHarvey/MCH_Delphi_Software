unit MonRadioButton;

interface

uses
  Windows, Messages, SysUtils, Classes, Graphics, Controls, Forms, Dialogs,
  StdCtrls;

type
  TMonRadioButton = class(TRadioButton)
  private
    FOnCheckedChange: TNotifyEvent;
  protected
    procedure SetChecked(Value: Boolean); override;
  published
    property OnCheckedChange: TNotifyEvent read FOnCheckedChange write FOnCheckedChange;
  end;

procedure Register;

implementation

procedure TMonRadioButton.SetChecked(Value: Boolean);
begin
  inherited;
  if Assigned(FOnCheckedChange) then
    FOnCheckedChange(self);
end;

procedure Register;
begin
  RegisterComponents('MCH', [TMonRadioButton]);
end;

end.
