param([string]$ProjectName = 'llm-gateway-integration', [switch]$Reliability, [switch]$SkipPressure)
$ErrorActionPreference = 'Stop'
if ($SkipPressure -and -not $Reliability) { throw 'SkipPressure requires Reliability' }
$root = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $root
$values = @{}
foreach ($line in Get-Content -LiteralPath '.env') {
    if ($line -match '^([A-Z][A-Z0-9_]*)=(.*)$') { $values[$Matches[1]] = $Matches[2].Trim().Trim('"').Trim("'") }
}
foreach ($name in @('GW_LOCAL_DB_PASSWORD', 'GW_MASTER_KEY')) {
    if (-not $values[$name]) { throw "Missing $name in local .env" }
}
$composeArgs = @('compose', '--env-file', '.env', '-p', $ProjectName, '-f', 'deploy/compose/compose.yaml', '-f', 'deploy/compose/integration-ports.yaml')
$mysql = & docker @composeArgs ps -q mysql
if ($LASTEXITCODE -or -not $mysql) { throw 'Start the local MySQL dependency first' }
$kafka = & docker @composeArgs ps -q kafka
if ($LASTEXITCODE -or -not $kafka) { throw 'Start the local Kafka dependency first' }
$run = 'gw_acceptance_' + [guid]::NewGuid().ToString('N').Substring(0, 12)
# 名称由本脚本生成；清理只针对本次资源，现有数据库和应用容器不参与故障注入。
if ($run -notmatch '^gw_acceptance_[a-f0-9]{12}$') { throw 'Invalid acceptance resource name' }
$saved = @{}
$testEnv = @{
    GW_RUN_INFRA_TESTS = '1'
    GW_INFRA_RUN_ID = $run
    GW_MYSQL_DSN = "gateway:$($values['GW_LOCAL_DB_PASSWORD'])@tcp(127.0.0.1:13306)/${run}?parseTime=true&charset=utf8mb4"
    GW_MASTER_KEY = $values['GW_MASTER_KEY']
    GW_KAFKA_BROKERS = '127.0.0.1:19092'
    GW_KAFKA_TOPIC = $run
    GW_KAFKA_GROUP = $run
    GW_KAFKA_TLS = ''
    GW_KAFKA_USER = ''
    GW_KAFKA_PASSWORD = ''
    GW_ETCD_ENDPOINTS = 'http://127.0.0.1:12379'
    GW_ETCD_PREFIX = '/' + $run
    GW_ETCD_USER = ''
    GW_ETCD_PASSWORD = ''
    GW_MILVUS_URL = 'http://127.0.0.1:19530'
    GW_MILVUS_TOKEN = ''
    GW_MILVUS_COLLECTION = $run
    GW_ADMIN_ORIGIN = 'https://localhost:8443'
    GW_ADMIN_USER = 'admin'
    GW_ADMIN_PASSWORD = 'acceptance-' + [guid]::NewGuid().ToString('N')
    OTEL_EXPORTER_OTLP_ENDPOINT = ''
    # 历史多实例压力实验使用 128 槽；默认 16 槽的过载行为由独立回归验证。
    GW_MAX_CONCURRENT_REQUESTS = '128'
    GW_MAX_ADMIN_REQUESTS = '8'
}
$redisCreated = $false
$cacheRedisCreated = $false
$cacheRedisName = "$run-cache"
$databaseCreated = $false
$topicCreated = $false
$logDir = Join-Path $root 'data/acceptance'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logPath = Join-Path $logDir "$run.log"
$runDir = Join-Path $logDir $run
$roleEnvPath = Join-Path $runDir 'runtime.env'
try {
    "CREATE DATABASE $run; GRANT ALL PRIVILEGES ON $run.* TO 'gateway'@'%';" | & docker exec -i $mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot'
    if ($LASTEXITCODE) { throw 'Create acceptance database failed' }
    $databaseCreated = $true
    & docker exec $kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic $run --partitions 3 --replication-factor 1 --config retention.ms=3600000
    if ($LASTEXITCODE) { throw 'Create acceptance topic failed' }
    $topicCreated = $true
    $portProbe = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    try { $portProbe.Start(); $redisPort = $portProbe.LocalEndpoint.Port } finally { $portProbe.Stop() }
    $redisArgs = @('run', '--detach', '--name', $run, '--label', "llm-gateway.acceptance=$run", '-p', "127.0.0.1:${redisPort}:6379")
    if ($Reliability) {
        $network = "${ProjectName}_default"
        & docker network inspect $network --format '{{.Name}}' | Out-Null
        if ($LASTEXITCODE) { throw 'Local Compose network unavailable' }
        $redisArgs += @('--network', $network, '--cpus', '1', '--memory', '256m')
    }
    & docker @redisArgs redis:7.4 redis-server --appendonly yes --appendfsync always --maxmemory 8mb --maxmemory-policy noeviction | Out-Null
    if ($LASTEXITCODE) { throw 'Start temporary Redis failed' }
    $redisCreated = $true
    $binding = & docker port $run 6379/tcp
    if ($LASTEXITCODE -or $binding -notmatch '^127\.0\.0\.1:(\d+)$') { throw 'Unexpected temporary Redis binding' }
    $testEnv['GW_REDIS_URL'] = "redis://127.0.0.1:$($Matches[1])/0"
    $cacheProbe = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    try { $cacheProbe.Start(); $cachePort = $cacheProbe.LocalEndpoint.Port } finally { $cacheProbe.Stop() }
    $cacheArgs = @('run', '--detach', '--name', $cacheRedisName, '--label', "llm-gateway.acceptance=$run", '-p', "127.0.0.1:${cachePort}:6379", '--memory', '128m', '--cpus', '1')
    if ($Reliability) { $cacheArgs += @('--network', $network) }
    & docker @cacheArgs redis:7.4 redis-server --save '' --appendonly no --maxmemory 4mb --maxmemory-policy allkeys-lru | Out-Null
    if ($LASTEXITCODE) { throw 'Start temporary cache Redis failed' }
    $cacheRedisCreated = $true
    $testEnv['GW_CACHE_REDIS_URL'] = "redis://127.0.0.1:${cachePort}/0"
    if ($Reliability) {
        New-Item -ItemType Directory -Force -Path $runDir | Out-Null
        $buildEnv = @{}
        foreach ($name in @('GOOS', 'GOARCH', 'CGO_ENABLED')) { $buildEnv[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
        try {
            $env:GOOS = 'linux'; $env:GOARCH = 'amd64'; $env:CGO_ENABLED = '0'
            & go test -c -o (Join-Path $runDir 'gateway.test') ./internal/gateway
            if ($LASTEXITCODE) { throw 'Build Linux acceptance runner failed' }
        } finally {
            foreach ($name in $buildEnv.Keys) { [Environment]::SetEnvironmentVariable($name, $buildEnv[$name], 'Process') }
        }
        $roleEnv = @{}
        foreach ($name in $testEnv.Keys) { $roleEnv[$name] = $testEnv[$name] }
        $roleEnv['GW_MYSQL_DSN'] = "gateway:$($values['GW_LOCAL_DB_PASSWORD'])@tcp(mysql:3306)/${run}?parseTime=true&charset=utf8mb4"
        $roleEnv['GW_REDIS_URL'] = "redis://${run}:6379/0"
        $roleEnv['GW_CACHE_REDIS_URL'] = "redis://${cacheRedisName}:6379/0"
        $roleEnv['GW_KAFKA_BROKERS'] = 'kafka:9092'
        $roleEnv['GW_ETCD_ENDPOINTS'] = 'http://etcd:2379'
        $roleEnv['GW_MILVUS_URL'] = 'http://milvus:19530'
        [IO.File]::WriteAllLines($roleEnvPath, [string[]]@($roleEnv.Keys | Sort-Object | ForEach-Object { "$_=$($roleEnv[$_])" }), [Text.UTF8Encoding]::new($false))
        $image = & docker image inspect gcr.io/distroless/static-debian12:nonroot --format '{{.Id}}'
        if ($LASTEXITCODE) { throw 'Pull the application base image first' }
        $testEnv['GW_RUN_RELIABILITY_TESTS'] = '1'
        $testEnv['GW_RELIABILITY_SKIP_PRESSURE'] = $(if ($SkipPressure) { '1' } else { '0' })
        $testEnv['GW_RELIABILITY_DIRECTORY'] = $runDir
        $testEnv['GW_RELIABILITY_ENV_FILE'] = $roleEnvPath
        $testEnv['GW_RELIABILITY_NETWORK'] = $network
        $testEnv['GW_RELIABILITY_IMAGE'] = $image
        $testEnv['GW_RELIABILITY_REDIS_NAME'] = $run
    }
    foreach ($name in $testEnv.Keys) {
        $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        [Environment]::SetEnvironmentVariable($name, $testEnv[$name], 'Process')
    }
    Write-Host "独立验收资源：$run；输出：$logPath"
    $testName = '^TestInfrastructureIntegration$'
    if ($Reliability) { $testName = '^TestReliabilityIntegration$' }
    & go test ./internal/gateway -run $testName -count=1 -v -timeout 12m 2>&1 | Tee-Object -FilePath $logPath
    if ($LASTEXITCODE) { throw "Infrastructure acceptance failed; see $logPath" }
} finally {
    foreach ($name in $saved.Keys) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
    if ($Reliability) {
        foreach ($role in @('gateway-1', 'gateway-2', 'worker', 'upstream')) {
            $containerName = "$run-$role"
            # 只清理本次随机名称的角色；强制停止中断的测试并同步删除其匿名卷。
            $ownedContainer = & docker ps -aq --filter "name=^/${containerName}$" --filter "label=llm-gateway.acceptance=$run"
            if ($ownedContainer) {
                $roleLog = & docker logs $containerName 2>&1
                [IO.File]::WriteAllLines((Join-Path $runDir "$role.log"), [string[]]@($roleLog | ForEach-Object { "$_" }), [Text.UTF8Encoding]::new($false))
                & docker stop --time 5 $containerName | Out-Null
                & docker rm -f -v $containerName | Out-Null
            }
        }
        if (Test-Path -LiteralPath $roleEnvPath) { Remove-Item -LiteralPath $roleEnvPath }
    }
    if ($redisCreated) {
        & docker rm -f -v $run | Out-Null
        if ($LASTEXITCODE) { Write-Warning "Temporary Redis cleanup failed: $run" }
    }
    if ($cacheRedisCreated) {
        & docker rm -f -v $cacheRedisName | Out-Null
        if ($LASTEXITCODE) { Write-Warning "Temporary cache Redis cleanup failed: $cacheRedisName" }
    }
    if ($topicCreated) {
        # SIGKILL 的成员可能仍在 Kafka 会话有效期内，等待失效后删除专用组。
        $groupDeleted = $false
        for ($attempt = 0; $attempt -lt 8; $attempt++) {
            $groupResult = (& docker exec $kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --delete --group $run 2>&1) -join "`n"
            if ($groupResult -match 'was successful|GroupIdNotFoundException') { $groupDeleted = $true; break }
            if ($attempt -lt 7) { Start-Sleep -Seconds 4 }
        }
        if (-not $groupDeleted) { Write-Warning "Acceptance consumer group cleanup failed: $run" }
        & docker exec $kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic $run
        if ($LASTEXITCODE) { Write-Warning "Acceptance topic cleanup failed: $run" }
    }
    if ($databaseCreated) {
        "DROP DATABASE IF EXISTS $run; REVOKE ALL PRIVILEGES ON $run.* FROM 'gateway'@'%';" | & docker exec -i $mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot'
        if ($LASTEXITCODE) { Write-Warning "Acceptance database cleanup failed: $run" }
    }
}
