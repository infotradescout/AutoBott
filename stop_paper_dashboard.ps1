$targets = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -in @('python.exe', 'pythonw.exe') -and
    $_.CommandLine -and
    $_.CommandLine.Contains('autobott_v2.launch_dashboard')
}

foreach ($target in $targets) {
    try {
        Stop-Process -Id $target.ProcessId -Force -ErrorAction Stop
        Write-Output "Stopped AutoBott dashboard process $($target.ProcessId)"
    } catch {
        Write-Warning "Failed to stop process $($target.ProcessId): $($_.Exception.Message)"
    }
}
