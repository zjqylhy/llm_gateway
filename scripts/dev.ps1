param([switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $root
if (-not $SkipBuild) {
    Push-Location web
    try {
        if (-not (Test-Path node_modules)) { npm ci; if ($LASTEXITCODE) { throw 'npm ci failed' } }
        npm run build; if ($LASTEXITCODE) { throw 'web build failed' }
    } finally { Pop-Location }
}
# 不加载生产 .env，避免离线模式误使用真实凭据；凭据仅在本次进程生效。
$env:GW_GATEWAY_ADDR = '127.0.0.1:8080'
$env:GW_ADMIN_ADDR = '127.0.0.1:8081'
Write-Host '控制台 http://127.0.0.1:8081；网关 http://127.0.0.1:8080'
go run ./cmd/dev
if ($LASTEXITCODE) { throw 'development server failed' }
