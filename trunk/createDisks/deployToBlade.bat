@echo on
REM usage : deployToBlade.bat <computername> [<kernel debug server IP> <kernel debug server port> <kernel debug server key>]

REM Name the computer according to our parameters
wmic computersystem where caption='%COMPUTERNAME%' call rename %1

REM enable the kernel debugger
Set KDHOST=%2
if DEFINED KDHOST ( 
	bcdedit /dbgsettings net hostip:%2 port:%3 key:%4
	bcdedit /debug on
)

REM Instruct WSUS to regen its SID so that it operates correctly
net stop wuauserv
reg delete "HKLM\Software\Microsoft\Windows\CurrentVersion\WindowsUpdate" /v SusClientId /f
reg delete "HKLM\Software\Microsoft\Windows\CurrentVersion\WindowsUpdate" /v SusClientIdValidation /f
reg add "HKLM\Software\Policies\Microsoft\Windows\CurrentVersion\WindowsUpdate" /v TargetGroup /t REG_SZ /d "blades" /f
reg add "HKLM\Software\Policies\Microsoft\Windows\CurrentVersion\WindowsUpdate" /v TargetGroupEnabled /t REG_DWORD /d 1 /f
net start wuauserv
wuauclt.exe /resetauthorization /detectnow

REM Now we must wait for WSUS to do its thing. How do we do that? MSDN says "wait 10 minutes for a detection cycle to finish". :(
REM Let's just wait for one minute.
REM timeout 60

exit 0
