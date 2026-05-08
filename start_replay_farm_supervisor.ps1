param(
    [string]$WorkersFile = "$PSScriptRoot\autotrader\replay_workers.json",
    [string]$PythonExe = "$PSScriptRoot\.venv\Scripts\python.exe",
    [string]$OutputRoot = "$PSScriptRoot\autotrader\replay_farm",
    [string]$CacheDir = "$PSScriptRoot\autotrader\historical_cache",
    [int]$StaggerSeconds = 30,
    [int]$HealthCheckSeconds = 60,
    [switch]$Offline,
    [switch]$NoOffline,
    [switch]$OneShot,
    [switch]$NoStopOnOneShot,
    [switch]$DisableAggregate,
    [int]$AggregateEveryCycles = 5,
    [int]$AggregateMinTotalTrades = 100,
    [int]$AggregateMinWorkers = 2,
    [int]$AggregateMinPassingWorkers = 2,
    [double]$AggregateMinPassingWindowPct = 40.0,
    [double]$AggregateTargetWinRatePct = 55.0,
    [double]$AggregateTargetExpectancyPct = 0.05,
    [double]$AggregateMinWinLossRatio = 1.25,
    [double]$AggregateMinWorkerWinLossRatio = 1.15,
    [switch]$DisableSnapshots,
    [int]$EscalateAfterNoPromotableCycles = 0,
    [double]$EscalateWinLossStep = 0.05,
    [double]$EscalateMaxWinLossRatio = 2.0,
    [int]$PauseAfterErrorCycles = 0,
    [int]$PauseIfWorkersWithNewErrors = 1
)

$ErrorActionPreference = "Stop"

if (-not $PSBoundParameters.ContainsKey("OutputRoot")) {
    $dataDir = [string]$env:DATA_DIR
    if (-not [string]::IsNullOrWhiteSpace($dataDir)) {
        $OutputRoot = Join-Path $dataDir "replay_farm"
    }
}
if (-not $PSBoundParameters.ContainsKey("CacheDir")) {
    $dataDir = [string]$env:DATA_DIR
    if (-not [string]::IsNullOrWhiteSpace($dataDir)) {
        $CacheDir = Join-Path $dataDir "historical_cache"
    }
}

function Write-SupervisorLog {
    param(
        [string]$Message,
        [string]$LogFile
    )
    $line = "{0:u} {1}" -f (Get-Date), $Message
    if ($LogFile) {
        $line | Out-File -FilePath $LogFile -Append -Encoding utf8
    }
    Write-Host $line
}

function Get-LatestRatio {
    param([string]$WorkerOutputDir)
    $path = Join-Path $WorkerOutputDir "optimizer_win_loss_ratio.csv"
    if (-not (Test-Path $path)) {
        return $null
    }
    $lines = Get-Content $path -Tail 2
    if (-not $lines -or $lines.Count -lt 2) {
        return $null
    }
    $header = $lines[0]
    $tail = $lines[-1]
    if ($tail -eq $header) {
        return $null
    }
    $cols = $header -split ","
    $vals = $tail -split ","
    $obj = [ordered]@{}
    for ($i = 0; $i -lt [Math]::Min($cols.Count, $vals.Count); $i++) {
        $obj[$cols[$i]] = $vals[$i]
    }
    return [pscustomobject]$obj
}

$repoRoot = Resolve-Path $PSScriptRoot
$pythonExePath = Resolve-Path $pythonExe
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
$logPath = Join-Path $logDirectory "replay_farm_supervisor.log"

$startArgs = @(
    "autotrader\replay_farm.py",
    "start",
    "--workers-file", $workersFilePath,
    "--no-offline",
    "--restart",
    "--stagger-seconds", $StaggerSeconds,
    "--output-root", $OutputRoot,
    "--cache-dir", $CacheDir,
    "--python", $pythonExePath
)
if ($Offline) {
    $startArgs += "--offline"
}
if ($NoOffline) {
    $startArgs += "--no-offline"
}

function Start-Once {
    Write-SupervisorLog -Message "Starting/restarting farm with args: $($startArgs -join ' ')" -LogFile $logPath
    & $pythonExePath @startArgs | Out-Null
}

function Get-FarmStatus {
    try {
        $jsonText = & $pythonExePath "autotrader\replay_farm.py" "status" "--output-root" $OutputRoot
        if (-not $jsonText) {
            return $null
        }
        return $jsonText | ConvertFrom-Json
    }
    catch {
        Write-SupervisorLog -Message "Status check failed: $($_.Exception.Message)" -LogFile $logPath
        return $null
    }
}

function Get-FarmAggregate {
    try {
        $aggregateArgs = @(
            "autotrader\replay_farm.py",
            "aggregate",
            "--output-root", $OutputRoot,
            "--min-total-trades", [string]$AggregateMinTotalTrades,
            "--min-workers", [string]$AggregateMinWorkers,
            "--min-passing-workers", [string]$AggregateMinPassingWorkers,
            "--min-passing-window-pct", [string]$AggregateMinPassingWindowPct,
            "--target-win-rate-pct", [string]$AggregateTargetWinRatePct,
            "--target-expectancy-pct", [string]$AggregateTargetExpectancyPct,
            "--min-win-loss-ratio", [string]$AggregateMinWinLossRatio,
            "--min-worker-win-loss-ratio", [string]$AggregateMinWorkerWinLossRatio
        )
        $jsonText = & $pythonExePath @aggregateArgs
        if (-not $jsonText) {
            return $null
        }
        return $jsonText | ConvertFrom-Json
    }
    catch {
        Write-SupervisorLog -Message "Aggregate check failed: $($_.Exception.Message)" -LogFile $logPath
        return $null
    }
}

function Write-AggregateSnapshot {
    param(
        [pscustomobject]$Aggregate,
        [string]$SnapshotDirectory,
        [double]$CurrentMinWinLossRatio,
        [double]$CurrentMinWorkerWinLossRatio
    )
    if (-not $Aggregate) {
        return
    }
    New-Item -ItemType Directory -Path $SnapshotDirectory -Force | Out-Null

    $timestamp = (Get-Date).ToUniversalTime().ToString("u")
    $bestCandidate = ""
    $bestPromotable = $false
    $bestWinLoss = ""
    if ($Aggregate.best) {
        $bestCandidate = [string]$Aggregate.best.candidate
        $bestPromotable = [bool]$Aggregate.best.promotable
        if ($Aggregate.best.PSObject.Properties.Name -contains "win_loss_ratio") {
            $bestWinLoss = [string]$Aggregate.best.win_loss_ratio
        }
    }
    $promotableCount = 0
    if ($Aggregate.PSObject.Properties.Name -contains "promotable_count") {
        $promotableCount = [int]$Aggregate.promotable_count
    }

    $payload = [ordered]@{
        timestamp_utc = $timestamp
        generated_at = [string]$Aggregate.generated_at
        worker_count = [int]$Aggregate.worker_count
        row_count = [int]$Aggregate.row_count
        promotable_count = $promotableCount
        best_candidate = $bestCandidate
        best_promotable = $bestPromotable
        best_win_loss_ratio = $bestWinLoss
        min_win_loss_ratio = $CurrentMinWinLossRatio
        min_worker_win_loss_ratio = $CurrentMinWorkerWinLossRatio
    }

    $dayStamp = Get-Date -Format "yyyy-MM-dd"
    $jsonPath = Join-Path $SnapshotDirectory "farm_summary_$dayStamp.json"
    $csvPath = Join-Path $SnapshotDirectory "farm_summary_history.csv"
    ($payload | ConvertTo-Json -Depth 4) | Out-File -FilePath $jsonPath -Encoding utf8

    $csvExists = Test-Path $csvPath
    $payloadObj = [pscustomobject]$payload
    if ($csvExists) {
        $payloadObj | Export-Csv -Path $csvPath -Append -NoTypeInformation
    }
    else {
        $payloadObj | Export-Csv -Path $csvPath -NoTypeInformation
    }
}

Write-SupervisorLog -Message "Replay farm supervisor started. Output root: $OutputRoot" -LogFile $logPath
$aggregateEvery = [Math]::Max(1, $AggregateEveryCycles)
$cycle = 0
$snapshotDirectory = Join-Path $OutputRoot "snapshots"
$noPromotableCycles = 0
$escalateTrigger = [Math]::Max(0, $EscalateAfterNoPromotableCycles)
$escalateStep = [Math]::Max(0.0, $EscalateWinLossStep)
$escalateMax = [Math]::Max(0.0, $EscalateMaxWinLossRatio)
$pauseErrorCycles = [Math]::Max(0, $PauseAfterErrorCycles)
$pauseErrorWorkerThreshold = [Math]::Max(1, $PauseIfWorkersWithNewErrors)
$errorStreak = 0
$lastErrorSignature = @{}

while ($true) {
    $status = Get-FarmStatus
    $needsStart = $false
    if (-not $status -or -not $status.workers -or $status.workers.Count -eq 0) {
        $needsStart = $true
    }
    else {
        $allRunning = $true
        foreach ($worker in $status.workers) {
            if (-not [bool]$worker.running -or [int]$worker.pid -le 0) {
                $allRunning = $false
                Write-SupervisorLog -Message "Worker not running: $($worker.worker) pid=$($worker.pid)" -LogFile $logPath
            }
        }
        if (-not $allRunning) {
            $needsStart = $true
        }
    }

    if ($needsStart) {
        Start-Once
        Start-Sleep -Seconds 2
        $status = Get-FarmStatus
    }

    if ($status -and $status.workers) {
        $parts = @()
        $workersWithNewErrors = 0
        $newErrorWorkers = @()
        foreach ($worker in $status.workers) {
            $workerName = [string]$worker.worker
            $ratio = Get-LatestRatio -WorkerOutputDir $worker.output_dir
            if ($ratio) {
                $parts += "$workerName`:rows=$($worker.rows),winloss=$($ratio.win_loss_ratio),winrate=$($ratio.win_rate_pct),iter=$($ratio.iteration)"
            }
            else {
                $parts += "$workerName`:rows=$($worker.rows),no_ratio_yet"
            }

            $errorLines = @()
            if ($worker.PSObject.Properties.Name -contains "latest_errors" -and $worker.latest_errors) {
                $errorLines = @($worker.latest_errors | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
            }
            $signature = ""
            if ($errorLines.Count -gt 0) {
                $signature = ($errorLines -join "`n").Trim()
            }
            $priorSignature = ""
            if ($lastErrorSignature.ContainsKey($workerName)) {
                $priorSignature = [string]$lastErrorSignature[$workerName]
            }
            if ($signature -and $signature -ne $priorSignature) {
                $workersWithNewErrors += 1
                $newErrorWorkers += $workerName
            }
            $lastErrorSignature[$workerName] = $signature
        }
        Write-SupervisorLog -Message ("Workers -> " + ($parts -join " | ")) -LogFile $logPath

        if ($pauseErrorCycles -gt 0) {
            if ($workersWithNewErrors -ge $pauseErrorWorkerThreshold) {
                $errorStreak += 1
                Write-SupervisorLog -Message ("New error signatures detected on workers: " + ($newErrorWorkers -join ",") + ". Error streak $errorStreak/$pauseErrorCycles.") -LogFile $logPath
            }
            else {
                if ($errorStreak -gt 0) {
                    Write-SupervisorLog -Message "Error streak reset (no new error spike this cycle)." -LogFile $logPath
                }
                $errorStreak = 0
            }
            if ($errorStreak -ge $pauseErrorCycles) {
                Write-SupervisorLog -Message "Auto-pausing farm due to repeated new error spikes. Stopping workers." -LogFile $logPath
                try {
                    & $pythonExePath "autotrader\replay_farm.py" "stop" "--output-root" $OutputRoot "--workers-file" $workersFilePath | Out-Null
                    Start-Sleep -Seconds 1
                }
                catch {
                    Write-SupervisorLog -Message "Auto-pause stop failed: $($_.Exception.Message)" -LogFile $logPath
                }
                break
            }
        }
    }
    else {
        Write-SupervisorLog -Message "No workers reported in status payload." -LogFile $logPath
    }

    $cycle += 1
    if (-not $DisableAggregate -and ($cycle % $aggregateEvery -eq 0)) {
        $aggregate = Get-FarmAggregate
        if ($aggregate) {
            $promotableCount = 0
            if ($aggregate.PSObject.Properties.Name -contains "promotable_count") {
                $promotableCount = [int]$aggregate.promotable_count
            }
            $bestCandidate = "none"
            $bestPromotable = "false"
            $bestRatio = "n/a"
            if ($aggregate.best) {
                $bestCandidate = [string]$aggregate.best.candidate
                $bestPromotable = [string][bool]$aggregate.best.promotable
                if ($aggregate.best.PSObject.Properties.Name -contains "win_loss_ratio") {
                    $bestRatio = [string]$aggregate.best.win_loss_ratio
                }
            }
            Write-SupervisorLog -Message ("Aggregate -> workers=$($aggregate.worker_count),rows=$($aggregate.row_count),promotable=$promotableCount,best=$bestCandidate,best_promotable=$bestPromotable,best_winloss=$bestRatio,min_winloss=$AggregateMinWinLossRatio,min_worker_winloss=$AggregateMinWorkerWinLossRatio") -LogFile $logPath

            if (-not $DisableSnapshots) {
                Write-AggregateSnapshot `
                    -Aggregate $aggregate `
                    -SnapshotDirectory $snapshotDirectory `
                    -CurrentMinWinLossRatio $AggregateMinWinLossRatio `
                    -CurrentMinWorkerWinLossRatio $AggregateMinWorkerWinLossRatio
            }

            if ($escalateTrigger -gt 0 -and $escalateStep -gt 0.0) {
                if ($promotableCount -le 0) {
                    $noPromotableCycles += 1
                }
                else {
                    $noPromotableCycles = 0
                }
                if ($noPromotableCycles -ge $escalateTrigger) {
                    $newGlobal = [Math]::Round([Math]::Min($escalateMax, ($AggregateMinWinLossRatio + $escalateStep)), 4)
                    $newWorker = [Math]::Round([Math]::Min($escalateMax, ($AggregateMinWorkerWinLossRatio + $escalateStep)), 4)
                    if ($newGlobal -gt $AggregateMinWinLossRatio -or $newWorker -gt $AggregateMinWorkerWinLossRatio) {
                        $AggregateMinWinLossRatio = $newGlobal
                        $AggregateMinWorkerWinLossRatio = $newWorker
                        Write-SupervisorLog -Message "Escalated ratio thresholds after no-promotable streak. min_winloss=$AggregateMinWinLossRatio min_worker_winloss=$AggregateMinWorkerWinLossRatio" -LogFile $logPath
                    }
                    else {
                        Write-SupervisorLog -Message "No-promotable streak hit escalation trigger, but thresholds are already at max." -LogFile $logPath
                    }
                    $noPromotableCycles = 0
                }
            }
        }
    }

    if ($OneShot) {
        if (-not $NoStopOnOneShot) {
            Write-SupervisorLog -Message "One-shot completed; stopping farm workers." -LogFile $logPath
            try {
                & $pythonExePath "autotrader\replay_farm.py" "stop" "--output-root" $OutputRoot "--workers-file" $workersFilePath | Out-Null
                Start-Sleep -Seconds 1
            }
            catch {
                Write-SupervisorLog -Message "One-shot stop failed: $($_.Exception.Message)" -LogFile $logPath
            }
        }
        break
    }
    Start-Sleep -Seconds $HealthCheckSeconds
}
