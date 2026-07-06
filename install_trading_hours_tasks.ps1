$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$startTaskName = "AutoBott Paper Dashboard Start"
$stopTaskName = "AutoBott Paper Dashboard Stop"
$wscript = Join-Path $env:SystemRoot "System32\wscript.exe"
$powershell = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
$startScript = Join-Path $repoRoot "start_paper_dashboard.vbs"
$stopScript = Join-Path $repoRoot "stop_paper_dashboard.ps1"

$startCommand = '"' + $wscript + '" "' + $startScript + '"'
$stopCommand = '"' + $powershell + '" -NoProfile -ExecutionPolicy Bypass -File "' + $stopScript + '"'

schtasks /Create /TN $startTaskName /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 08:35 /TR $startCommand /F | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Failed to create task: $startTaskName"
}

schtasks /Create /TN $stopTaskName /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 14:56 /TR $stopCommand /F | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Failed to create task: $stopTaskName"
}

Write-Output "Installed $startTaskName at 08:35 America/Chicago"
Write-Output "Installed $stopTaskName at 14:56 America/Chicago"
