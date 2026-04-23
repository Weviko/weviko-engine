$pythonCandidates = @(
    ".\.venv313\Scripts\python.exe",
    ".\venv\Scripts\python.exe",
    ".\.venv\Scripts\python.exe"
)

$pythonExe = $null
foreach ($candidate in $pythonCandidates) {
    if (Test-Path $candidate) {
        $pythonExe = $candidate
        break
    }
}

if (-not $pythonExe) {
    $pythonExe = "python"
}

Write-Host "[Weviko] Using Python:" $pythonExe
& $pythonExe "live_capture_server.py"
