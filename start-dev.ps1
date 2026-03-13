Param(
    [string]$EnvFile = ".env",
    [switch]$SkipInstall,
    [switch]$OpenBrowser
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

    $lines = Get-Content -Path $Path
    foreach ($line in $lines) {
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
    "uvicorn",
    "backend.app.main:app",
    "--host", "127.0.0.1",
    "--port", "8000",
    "--reload"
)

Write-Host "Starting backend with reload on http://127.0.0.1:8000"
$backendProcess = Start-Process -FilePath $PythonExe -ArgumentList $backendArgs -WorkingDirectory $RepoRoot -PassThru

try {
    Start-Sleep -Seconds 2

    if ($backendProcess.HasExited) {
        throw "Backend process exited before frontend startup."
    }

    $url = "http://127.0.0.1:3000"
    Write-Host "Starting frontend dev server with HMR on $url"

    if ($OpenBrowser) {
        Start-Process $url | Out-Null
    }

    & $NpmExe run dev --prefix $FrontendRoot
}
finally {
    if ($backendProcess -and -not $backendProcess.HasExited) {
        Stop-Process -Id $backendProcess.Id -Force
    }
}
