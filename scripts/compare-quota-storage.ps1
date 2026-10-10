$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $root
$run = 'gw_quota_' + [guid]::NewGuid().ToString('N').Substring(0, 12)
if ($run -notmatch '^gw_quota_[a-f0-9]{12}$') { throw 'Invalid comparison ID' }
$runDir = Join-Path $root "data/acceptance/$run"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$privateEnv = Join-Path $runDir 'mysql.env'
$rootPassword = [guid]::NewGuid().ToString('N')
$appPassword = [guid]::NewGuid().ToString('N')
$saved = @{}
$created = @()
function Get-ComparisonPort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    try { $listener.Start(); return $listener.LocalEndpoint.Port } finally { $listener.Stop() }
}
try {
    [IO.File]::WriteAllLines($privateEnv, [string[]]@("MYSQL_ROOT_PASSWORD=$rootPassword", "MYSQL_DATABASE=$run", 'MYSQL_USER=gateway', "MYSQL_PASSWORD=$appPassword"), [Text.UTF8Encoding]::new($false))
    $mysqlPort = Get-ComparisonPort
    & docker run --detach --name "$run-mysql" --label "llm-gateway.comparison=$run" --cpus 1 --memory 1g --env-file $privateEnv -p "127.0.0.1:${mysqlPort}:3306" mysql:8.4 --innodb-buffer-pool-size=134217728 --innodb-flush-log-at-trx-commit=1 --sync-binlog=1 | Out-Null
    if ($LASTEXITCODE) { throw 'Start isolated MySQL failed' }
    $created += "$run-mysql"
    $redisPort = Get-ComparisonPort
    & docker run --detach --name "$run-redis" --label "llm-gateway.comparison=$run" --cpus 1 --memory 512m -p "127.0.0.1:${redisPort}:6379" redis:7.4 redis-server --appendonly yes --appendfsync always --maxmemory 128mb --maxmemory-policy noeviction | Out-Null
    if ($LASTEXITCODE) { throw 'Start isolated Redis failed' }
    $created += "$run-redis"
    $ready = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        & docker exec "$run-mysql" sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql --protocol=TCP -h127.0.0.1 -ugateway -N -e "SELECT 1"' 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Seconds 1
    }
    if (-not $ready) { throw 'Isolated MySQL did not become ready' }
    $testEnv = @{
        GW_RUN_QUOTA_COMPARISON = '1'
        GW_QUOTA_COMPARISON_ID = $run
        GW_QUOTA_COMPARISON_DIRECTORY = $runDir
        GW_MYSQL_DSN = "gateway:${appPassword}@tcp(127.0.0.1:${mysqlPort})/${run}?parseTime=true&charset=utf8mb4"
        GW_REDIS_URL = "redis://127.0.0.1:${redisPort}/0"
    }
    foreach ($name in $testEnv.Keys) {
        $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        [Environment]::SetEnvironmentVariable($name, $testEnv[$name], 'Process')
    }
    Write-Host "配额存储对比：$run；结果：$runDir"
    & go test ./internal/gateway -run '^TestQuotaStorageComparison$' -count=1 -v -timeout 5m 2>&1 | Tee-Object -FilePath (Join-Path $runDir 'test.log')
    if ($LASTEXITCODE) { throw "Comparison failed; see $runDir" }
} finally {
    foreach ($name in $saved.Keys) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
    foreach ($containerName in $created) {
        # 清理仅匹配本次创建的名称和标签，匿名数据卷不涉及既有应用。
        $owned = & docker ps -aq --filter "name=^/${containerName}$" --filter "label=llm-gateway.comparison=$run"
        if ($owned) { & docker rm -f -v $containerName | Out-Null }
        if ($LASTEXITCODE) { Write-Warning "Comparison cleanup failed: $containerName" }
    }
    if (Test-Path -LiteralPath $privateEnv) { Remove-Item -LiteralPath $privateEnv }
}
