Param(
    [string]$EnvFile = ".env",
    [switch]$SkipInstall,
    [switch]$OpenBrowser
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "start-demo.ps1 is deprecated. Starting local dev instead."

$startDev = Join-Path $PSScriptRoot "start-dev.ps1"
$args = @("-EnvFile", $EnvFile)

if ($SkipInstall) {
    $args += "-SkipInstall"
}

if ($OpenBrowser) {
    $args += "-OpenBrowser"
}

& $startDev @args
