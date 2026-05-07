param(
    [string]$WorkersFile = "$PSScriptRoot\autotrader\replay_workers.json",
    [string]$PythonExe = "$PSScriptRoot\.venv\Scripts\python.exe",
    [string]$OutputRoot = "$PSScriptRoot\autotrader\replay_farm",
    [string]$CacheDir = "$PSScriptRoot\autotrader\historical_cache",
    [int]$StaggerSeconds = 30,
    [switch]$NoOffline
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path $PSScriptRoot

$pythonExePath = Resolve-Path $PythonExe
$workersFilePath = Resolve-Path $WorkersFile

if (-not (Test-Path $pythonExePath)) {
    throw "Python executable not found: $pythonExePath"
}
if (-not (Test-Path $workersFilePath)) {
    throw "Workers file not found: $workersFilePath"
}

New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null
New-Item -ItemType Directory -Path $CacheDir -Force | Out-Null

$logDirectory = Join-Path $OutputRoot "logs"
New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null

$launcherStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$launcherOut = Join-Path $logDirectory "replay_farm_launcher_$launcherStamp.out.log"
$launcherErr = Join-Path $logDirectory "replay_farm_launcher_$launcherStamp.err.log"

$argumentList = @(
    "autotrader\replay_farm.py",
    "start",
    "--workers-file", $workersFilePath,
    "--offline",
    "--restart",
    "--stagger-seconds", $StaggerSeconds,
    "--output-root", $OutputRoot,
    "--cache-dir", $CacheDir,
    "--python", $pythonExePath
)
if ($NoOffline) {
    $argumentList += "--no-offline"
}

$scriptLog = Join-Path $logDirectory "replay_farm_launcher_script_$launcherStamp.log"

$process = Start-Process -FilePath $pythonExePath `
    -ArgumentList $argumentList `
    -WorkingDirectory $repoRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $launcherOut `
    -RedirectStandardError $launcherErr `
    -PassThru

Start-Sleep -Seconds 2

$status = & $pythonExePath "autotrader\replay_farm.py" "status" "--output-root" $OutputRoot | ConvertFrom-Json

$summary = @(
    "Replay farm started.",
    "PID: $($process.Id)",
    "Output root: $OutputRoot",
    "Workers file: $workersFilePath",
    "Launcher logs:",
    "  stdout: $launcherOut",
    "  stderr: $launcherErr",
    "",
    "Current worker status:",
    ($status | ConvertTo-Json -Depth 4)
)

$summary | Out-Host
$summary | Out-File -FilePath $scriptLog -Encoding utf8
