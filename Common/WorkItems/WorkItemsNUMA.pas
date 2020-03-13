unit WorkItemsNUMA;

interface

uses WorkItems,WinApi.Windows;

{
  Martin Harvey 02/07/2015

  Extensions to existing threadpool class to allow for NUMA or processor
  group affinitisation of threads.
}

type
  TNumaWorkFarm = class(TWorkFarm)
  private
    FNumaNode: integer;
  protected
    procedure InitThreadRec(var Rec: TWorkerThreadRec); override;
    procedure SetNumaNode(NewNode: integer);
  public
    constructor Create;
    property NumaNode: integer read FNumaNode write SetNumaNode;
  end;

  TGroupWorkFarm = class(TWorkFarm)
  private
    FGroupNumber: integer;
  protected
    procedure InitThreadRec(var Rec: TWorkerThreadRec); override;
    procedure SetGroupNumber(NewGroup: integer);
  public
    constructor Create;
    property GroupNumber: integer read FGroupNumber write SetGroupNumber;
  end;

implementation

function GetNumaNodeProcessorMask(Node: UCHAR; var ProcessorMask: ULONG_PTR): BOOL; stdcall;
  external kernel32 name 'GetNumaNodeProcessorMask';

function VirtualAllocExNuma(hProcess: THandle;
                            lpAddress: Pointer;
                            dwSize: SIZE_T;
                            flAllocationType: DWORD;
                            flProtect: DWORD;
                            nndPreferred: DWORD): Pointer; stdcall;
  external kernel32 name 'VirtualAllocExNuma';

function GetActiveProcessorCount(GroupNumber: WORD):DWORD; stdcall;
  external kernel32 name 'GetActiveProcessorCount';

function GetActiveProcessorGroupCount():WORD; stdcall;
  external kernel32 name 'GetActiveProcessorGroupCount';

function GetMaximumProcessorCount(GroupNumber: WORD):DWORD; stdcall;
  external kernel32 name 'GetMaximumProcessorCount';

function GetMaximumProcessorGroupCount():WORD; stdcall;
  external kernel32 name 'GetMaximumProcessorGroupCount';

function SetThreadGroupAffinity(hThread: THandle;
                                const GroupAffinity: GROUP_AFFINITY;
                                var PreviousGroupAffinity: GROUP_AFFINITY): BOOL;
  stdcall; external kernel32 name 'SetThreadGroupAffinity';

constructor TNumaWorkFarm.Create;
begin
  FNumaNode := -1;
  inherited;
end;

procedure TNumaWorkFarm.InitThreadRec(var Rec: TWorkerThreadRec);
var
  Ret: boolean;
  NumaProcMask, Ret2: ULONG_PTR;
begin
  inherited;
  if FNumaNode >= 0 then
  begin
    Ret := GetNumaNodeProcessorMask(FNumaNode, NumaProcMask);
    if Ret then
    begin
      Ret2 := SetThreadAffinityMask(Rec.Thread.Handle, NumaProcMask);
      Assert(Ret2 <> 0);
    end;
    Assert(Ret);
  end;
end;

procedure TNumaWorkFarm.SetNumaNode(NewNode: Integer);
var
  MaxNode: ULONG;
  ReThread: boolean;
  NewThreadCount: cardinal;
  SysInfo: TSystemInfo;
  NumaProcMask: ULONG_PTR;
  i: cardinal;

begin
  NewThreadCount := ThreadCount;
  ReThread := false;
  if NewNode = FNumaNode then
    exit;

  if NewNode < 0 then
  begin
    GetSystemInfo(SysInfo);
    NewThreadCount := SysInfo.dwNumberOfProcessors;
    ReThread := true;
  end
  else if GetNumaHighestNodeNumber(MaxNode) then
  begin
    //Node number valid?
    if (NewNode >= 0) and (NewNode <= Integer(MaxNode)) then
    begin
      //How many CPU's in node?
      if GetNumaNodeProcessorMask(NewNode, NumaProcMask) then
      begin
        NewThreadCount := 0;
        for i := 0 to Pred(sizeof(NumaProcMask) * 8) do
        begin
          if ((ULONG_PTR(1) shl i) and NumaProcMask) <> 0 then
            Inc(NewThreadCount);
        end;
        ReThread := true;
      end;
    end;
  end;
  if ReThread then
  begin
    FNumaNode := NewNode;
    SetThreadCount(NewThreadCount);
  end;
end;

constructor TGroupWorkFarm.Create;
begin
  inherited;
  FGroupNumber := -1;
end;

procedure TGroupWorkFarm.InitThreadRec(var Rec: TWorkerThreadRec);
var
  ProcCount, i: cardinal;
  GrpAffinity, PrevGrpAffinity: GROUP_AFFINITY;
  Ret: BOOL;
begin
  inherited;
  if FGroupNumber >= 0 then
  begin
    ProcCount := GetActiveProcessorCount(FGroupNumber);
    //Need to create an appropriate GROUP_AFFINITY structure.
    Assert(ProcCount > 0);
    FillMemory(@GrpAffinity, sizeof(GrpAffinity), 0);
    for i := 0 to Pred(ProcCount) do
      GrpAffinity.Mask := GrpAffinity.Mask or (1 shl i);
    GrpAffinity.Group := FGroupNumber;
    Ret := SetThreadGroupAffinity(Rec.Thread.Handle, GrpAffinity, PrevGrpAffinity);
    Assert(Ret);
  end;
end;

procedure TGroupWorkFarm.SetGroupNumber(NewGroup: integer);
var
  ReThread: boolean;
  NewThreadCount: cardinal;
  MaxGroup: cardinal;
begin
  NewThreadCount := ThreadCount;
  ReThread := false;
  if NewGroup = FGroupNumber then
    exit;

  MaxGroup := GetActiveProcessorGroupCount();
  if (NewGroup >= 0) and (NewGroup < MaxGroup) then
  begin
    NewThreadCount := GetActiveProcessorCount(NewGroup);
    ReThread := true;
  end;
  if ReThread then
  begin
    FGroupNumber := NewGroup;
    SetThreadCount(NewThreadCount);
  end;
end;

end.
