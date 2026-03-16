Param(
    [string]$EnvFile = ".env",
    [switch]$SkipInstall,
    [switch]$OpenBrowser,
    [int]$BackendStartupTimeoutSeconds = 20
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Import-DotEnv {
    param(
        [string]$Path
    )

    if (-not (Test-Path -Path $Path)) {
        throw "Environment file not found: $Path"
    }

    foreach ($line in Get-Content -Path $Path) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        $trimmed = $line.Trim()
        if ($trimmed.StartsWith("#")) { continue }
        $parts = $trimmed -split "=", 2
        if ($parts.Length -lt 2) { continue }
        $name = $parts[0].Trim()
        $value = $parts[1].Trim([char]39).Trim([char]34)
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
}

function Resolve-CommandPath {
    param(
        [string[]]$Candidates
    )

    foreach ($candidate in $Candidates) {
        $command = Get-Command $candidate -ErrorAction SilentlyContinue
        if ($command) {
            return $command.Source
        }
        if (Test-Path $candidate) {
            return (Resolve-Path $candidate).Path
        }
    }

    throw "Could not find executable. Tried: $($Candidates -join ', ')"
}

Import-DotEnv -Path $EnvFile

$RepoRoot = $PSScriptRoot
$FrontendRoot = Join-Path $RepoRoot "frontend"
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"

if (Test-Path -Path $VenvPython) {
    $PythonExe = $VenvPython
} else {
    $PythonExe = Resolve-CommandPath @("py.exe", "python.exe")
}

$NpmExe = Resolve-CommandPath @(
    "npm.cmd",
    "C:\Program Files\nodejs\npm.cmd",
    "C:\Program Files (x86)\nodejs\npm.cmd"
)

if (-not $SkipInstall) {
    Write-Host "Installing backend and frontend dependencies..."
    & $NpmExe install
    if ($LASTEXITCODE -ne 0) { throw "npm install failed in repo root." }

    & $NpmExe install --prefix $FrontendRoot
    if ($LASTEXITCODE -ne 0) { throw "npm install failed in frontend." }

    & $PythonExe -m pip install -r (Join-Path $RepoRoot "backend\requirements.txt")
    if ($LASTEXITCODE -ne 0) { throw "pip install failed." }
}

$backendArgs = @(
    "-m",
    "backend.app.serve",
    "--host", "127.0.0.1",
    "--port", "8000",
    "--reload"
)

Get-NetTCPConnection -LocalPort 8000 -ErrorAction SilentlyContinue |
    Where-Object { $_.State -eq "Listen" -and $_.OwningProcess -gt 0 } |
    ForEach-Object {
        try {
            Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue
        }
        catch {
        }
    }

Write-Host "Starting backend with reload on http://127.0.0.1:8000"
$escapedRepoRoot = $RepoRoot.Replace('"', '""')
$escapedPythonExe = $PythonExe.Replace('"', '""')
$backendCommand = "cd /d `"$escapedRepoRoot`" && `"$escapedPythonExe`" -m backend.app.serve --host 127.0.0.1 --port 8000 --reload"
$backendProcess = Start-Process `
    -FilePath "cmd.exe" `
    -ArgumentList @("/k", $backendCommand) `
    -WorkingDirectory $RepoRoot `
    -PassThru

$frontendStarted = $false
try {
    $healthUrl = "http://127.0.0.1:8000/health"
    $deadline = (Get-Date).AddSeconds($BackendStartupTimeoutSeconds)
    $backendReady = $false

    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds 500

        if ($backendProcess.HasExited) {
            throw "Backend process exited before frontend startup. Check the backend terminal for the traceback."
        }

        try {
            $response = Invoke-WebRequest -Uri $healthUrl -UseBasicParsing -TimeoutSec 2
            if ($response.StatusCode -eq 200) {
                $backendReady = $true
                break
            }
        }
        catch {
        }
    }

    if (-not $backendReady) {
        throw "Backend did not become ready on http://127.0.0.1:8000. Check the backend terminal for logs."
    }

    $url = "http://127.0.0.1:3000"
    Write-Host "Starting frontend dev server with HMR on $url"

    if ($OpenBrowser) {
        Start-Process $url | Out-Null
    }

    $frontendStarted = $true
    & $NpmExe run dev --prefix $FrontendRoot
}
finally {
    if ($frontendStarted -and $backendProcess -and -not $backendProcess.HasExited) {
        try {
            Stop-Process -Id $backendProcess.Id -Force
        }
        catch {
        }
    }
}
