Param(
    [string]$EnvFile = ".env",
    [switch]$SkipInstall,
    [switch]$OpenBrowser,
    [switch]$Reload
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

function Resolve-CanonicalPath {
    param(
        [string]$Path
    )

    if ([string]::IsNullOrWhiteSpace($Path)) {
        return ""
    }

    try {
        return (Resolve-Path -Path $Path -ErrorAction Stop).Path
    }
    catch {
        return [System.IO.Path]::GetFullPath($Path)
    }
}

function Test-PythonExecutable {
    param(
        [string]$Path
    )

    if (-not (Test-Path -Path $Path)) {
        return $false
    }

    try {
        & $Path -c "import sys; print(sys.executable)" *> $null
        return $LASTEXITCODE -eq 0
    }
    catch {
        return $false
    }
}

function Test-WindowsVenvConfig {
    param(
        [string]$VenvRoot
    )

    $configPath = Join-Path $VenvRoot "pyvenv.cfg"
    if (-not (Test-Path -Path $configPath)) {
        return $false
    }

    try {
        $config = Get-Content -Path $configPath -Raw -ErrorAction Stop
    }
    catch {
        return $false
    }

    if ($config -match "(?im)^\s*home\s*=\s*/") {
        return $false
    }

    if ($config -match "(?im)^\s*executable\s*=\s*/") {
        return $false
    }

    return $true
}

function Resolve-BasePython {
    param(
        [string]$VenvRoot
    )

    $resolvedVenvRoot = Resolve-CanonicalPath -Path $VenvRoot

    $pyLauncher = Get-Command "py.exe" -ErrorAction SilentlyContinue
    if ($pyLauncher) {
        try {
            & $pyLauncher.Source -3 -c "import sys; print(sys.executable)" *> $null
            if ($LASTEXITCODE -eq 0) {
                return @{
                    Path = $pyLauncher.Source
                    Arguments = @("-3")
                }
            }
        }
        catch {
        }
    }

    $pythonCommands = @(Get-Command "python.exe" -All -ErrorAction SilentlyContinue)
    foreach ($command in $pythonCommands) {
        $candidate = Resolve-CanonicalPath -Path $command.Source
        if (-not $candidate) {
            continue
        }
        if ($resolvedVenvRoot -and $candidate.ToLowerInvariant().StartsWith($resolvedVenvRoot.ToLowerInvariant())) {
            continue
        }
        if (Test-PythonExecutable -Path $candidate) {
            return @{
                Path = $candidate
                Arguments = @()
            }
        }
    }

    throw "Could not find a usable system Python interpreter. Install Python for Windows or the py launcher."
}

function Ensure-WindowsVenv {
    param(
        [string]$VenvRoot,
        [hashtable]$BasePython
    )

    $venvPython = Join-Path $VenvRoot "Scripts\python.exe"
    if ((Test-PythonExecutable -Path $venvPython) -and (Test-WindowsVenvConfig -VenvRoot $VenvRoot)) {
        return $venvPython
    }

    if (Test-Path -Path $VenvRoot) {
        Write-Host "Removing incompatible virtual environment at $VenvRoot"
        Remove-Item -Path $VenvRoot -Recurse -Force
    }

    Write-Host "Creating Windows virtual environment at $VenvRoot"
    & $BasePython.Path @($BasePython.Arguments + @("-m", "venv", $VenvRoot))

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create virtual environment at $VenvRoot"
    }

    if (-not (Test-WindowsVenvConfig -VenvRoot $VenvRoot)) {
        throw "Virtual environment created at $VenvRoot, but pyvenv.cfg is not Windows-compatible."
    }

    if (-not (Test-PythonExecutable -Path $venvPython)) {
        throw "Virtual environment created, but $venvPython is still not executable."
    }

    return $venvPython
}

function Clear-ListeningPort {
    param(
        [int]$Port
    )

    $processIds = @()

    $connections = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue |
        Where-Object { $_.OwningProcess -gt 0 } |
        Select-Object -ExpandProperty OwningProcess -Unique
    if ($connections) {
        $processIds += $connections
    }

    $netstatLines = netstat -ano | Select-String -Pattern (":{0}\s" -f $Port)
    foreach ($line in $netstatLines) {
        $parts = ($line.ToString().Trim() -split "\s+") | Where-Object { $_ }
        if ($parts.Length -ge 5) {
            $pidText = $parts[-1]
            $parsedPid = 0
            if ([int]::TryParse($pidText, [ref]$parsedPid) -and $parsedPid -gt 0) {
                $processIds += $parsedPid
            }
        }
    }

    $processIds = $processIds | Select-Object -Unique

    foreach ($processId in $processIds) {
        try {
            Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
        }
        catch {
        }
    }

    $deadline = (Get-Date).AddSeconds(5)
    while ((Get-Date) -lt $deadline) {
        $stillBound = netstat -ano | Select-String -Pattern ("127.0.0.1:{0}\s" -f $Port)
        if (-not $stillBound) {
            break
        }
        Start-Sleep -Milliseconds 250
    }
}

function Get-WatchSignature {
    param(
        [string]$RepoRoot,
        [string]$EnvFilePath
    )

    $roots = @(
        (Join-Path $RepoRoot "backend"),
        $EnvFilePath,
        (Join-Path $RepoRoot "SOUL.md"),
        (Join-Path $RepoRoot "AGENTS.md"),
        (Join-Path $RepoRoot "build")
    )

    $latestTicks = 0L

    foreach ($root in $roots) {
        if (-not (Test-Path -Path $root)) {
            continue
        }

        $item = Get-Item -LiteralPath $root -ErrorAction SilentlyContinue
        if (-not $item) {
            continue
        }

        if (-not $item.PSIsContainer) {
            $ticks = $item.LastWriteTimeUtc.Ticks
            if ($ticks -gt $latestTicks) {
                $latestTicks = $ticks
            }
            continue
        }

        $files = Get-ChildItem -LiteralPath $root -Recurse -File -ErrorAction SilentlyContinue |
            Where-Object {
                $_.FullName -notmatch "[\\/]\.venv[\\/]" -and
                $_.FullName -notmatch "[\\/]__pycache__[\\/]" -and
                $_.FullName -notmatch "[\\/]runtime[\\/]" -and
                $_.Extension -in @(".py", ".pyi", ".json", ".md", ".txt")
            }

        foreach ($file in $files) {
            $ticks = $file.LastWriteTimeUtc.Ticks
            if ($ticks -gt $latestTicks) {
                $latestTicks = $ticks
            }
        }
    }

    return $latestTicks
}

function Start-BackendProcess {
    param(
        [string]$PythonExe,
        [string]$RepoRoot
    )

    return Start-Process `
        -FilePath $PythonExe `
        -ArgumentList @("-u", "-m", "backend.app.serve", "--host", "127.0.0.1", "--port", "5000") `
        -WorkingDirectory $RepoRoot `
        -NoNewWindow `
        -PassThru
}

function Stop-BackendProcess {
    param(
        [System.Diagnostics.Process]$Process
    )

    if (-not $Process) {
        return
    }

    try {
        if (-not $Process.HasExited) {
            Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
            $Process.WaitForExit(5000) | Out-Null
        }
    }
    catch {
    }
}

function Invoke-BackendReloadLoop {
    param(
        [string]$PythonExe,
        [string]$RepoRoot,
        [string]$EnvFilePath
    )

    $backendProcess = $null
    $lastSignature = Get-WatchSignature -RepoRoot $RepoRoot -EnvFilePath $EnvFilePath

    try {
        Write-Host "Starting backend with auto-reload on http://127.0.0.1:5000 in this terminal"
        $backendProcess = Start-BackendProcess -PythonExe $PythonExe -RepoRoot $RepoRoot

        while ($true) {
            Start-Sleep -Milliseconds 800

            $currentSignature = Get-WatchSignature -RepoRoot $RepoRoot -EnvFilePath $EnvFilePath
            if ($currentSignature -ne $lastSignature) {
                $lastSignature = $currentSignature
                Write-Host "Backend change detected. Restarting..."
                Stop-BackendProcess -Process $backendProcess
                Clear-ListeningPort -Port 5000
                Start-Sleep -Milliseconds 200
                $backendProcess = Start-BackendProcess -PythonExe $PythonExe -RepoRoot $RepoRoot
                continue
            }

            if ($backendProcess -and $backendProcess.HasExited) {
                Start-Sleep -Milliseconds 300
                $currentSignature = Get-WatchSignature -RepoRoot $RepoRoot -EnvFilePath $EnvFilePath
                if ($currentSignature -ne $lastSignature) {
                    $lastSignature = $currentSignature
                    Write-Host "Backend change detected after exit. Restarting..."
                    Clear-ListeningPort -Port 5000
                    $backendProcess = Start-BackendProcess -PythonExe $PythonExe -RepoRoot $RepoRoot
                    continue
                }
            }
        }
    }
    finally {
        Stop-BackendProcess -Process $backendProcess
    }
}

Import-DotEnv -Path $EnvFile

$RepoRoot = $PSScriptRoot
$EnvFilePath = Resolve-CanonicalPath -Path (Join-Path $RepoRoot $EnvFile)
$FrontendRoot = Join-Path $RepoRoot "frontend"
$VenvRoot = Join-Path $RepoRoot ".venv"
$BasePythonExe = Resolve-BasePython -VenvRoot $VenvRoot
$PythonExe = Ensure-WindowsVenv -VenvRoot $VenvRoot -BasePython $BasePythonExe

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

Clear-ListeningPort -Port 5000
Clear-ListeningPort -Port 3000

$backendMode = "without reload"
if ($Reload) {
    $backendMode = "with auto-reload"
}

$url = "http://127.0.0.1:3000"
Write-Host "Starting frontend dev server with HMR on $url in a separate terminal"
$escapedRepoRoot = $RepoRoot.Replace('"', '""')
$escapedFrontendRoot = $FrontendRoot.Replace('"', '""')
$frontendCommand = "cd /d `"$escapedFrontendRoot`" && `"$NpmExe`" run dev"
$frontendProcess = Start-Process `
    -FilePath "cmd.exe" `
    -ArgumentList @("/k", $frontendCommand) `
    -WorkingDirectory $FrontendRoot `
    -PassThru

Start-Sleep -Seconds 5

if ($OpenBrowser) {
    Start-Process $url | Out-Null
}

Write-Host "Starting backend $backendMode on http://127.0.0.1:5000 in this terminal"
[Environment]::SetEnvironmentVariable("PYTHONUNBUFFERED", "1", "Process")

try {
    if ($Reload) {
        Invoke-BackendReloadLoop -PythonExe $PythonExe -RepoRoot $RepoRoot -EnvFilePath $EnvFilePath
    }
    else {
        & $PythonExe -u -m backend.app.serve --host 127.0.0.1 --port 5000
    }
}
finally {
    if ($frontendProcess -and -not $frontendProcess.HasExited) {
        try {
            taskkill /PID $frontendProcess.Id /T /F | Out-Null
        }
        catch {
        }
    }
}
