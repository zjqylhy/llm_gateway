param([string]$ProjectName = 'llm-gateway-integration')
$ErrorActionPreference = 'Stop'
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
}
$redisCreated = $false
$databaseCreated = $false
$topicCreated = $false
$logDir = Join-Path $root 'data/acceptance'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logPath = Join-Path $logDir "$run.log"
try {
    "CREATE DATABASE $run; GRANT ALL PRIVILEGES ON $run.* TO 'gateway'@'%';" | & docker exec -i $mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot'
    if ($LASTEXITCODE) { throw 'Create acceptance database failed' }
    $databaseCreated = $true
    & docker exec $kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic $run --partitions 3 --replication-factor 1 --config retention.ms=3600000
    if ($LASTEXITCODE) { throw 'Create acceptance topic failed' }
    $topicCreated = $true
    & docker run --detach --name $run --label llm-gateway.acceptance=$run -p '127.0.0.1::6379' redis:7.4 | Out-Null
    if ($LASTEXITCODE) { throw 'Start temporary Redis failed' }
    $redisCreated = $true
    $binding = & docker port $run 6379/tcp
    if ($LASTEXITCODE -or $binding -notmatch '^127\.0\.0\.1:(\d+)$') { throw 'Unexpected temporary Redis binding' }
    $testEnv['GW_REDIS_URL'] = "redis://127.0.0.1:$($Matches[1])/0"
    foreach ($name in $testEnv.Keys) {
        $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        [Environment]::SetEnvironmentVariable($name, $testEnv[$name], 'Process')
    }
    Write-Host "独立验收资源：$run；输出：$logPath"
    & go test ./internal/gateway -run '^TestInfrastructureIntegration$' -count=1 -v -timeout 7m 2>&1 | Tee-Object -FilePath $logPath
    if ($LASTEXITCODE) { throw "Infrastructure acceptance failed; see $logPath" }
} finally {
    foreach ($name in $saved.Keys) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
    if ($redisCreated) {
        & docker rm -f $run | Out-Null
        if ($LASTEXITCODE) { Write-Warning "Temporary Redis cleanup failed: $run" }
    }
    if ($topicCreated) {
        & docker exec $kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --delete --group $run
        if ($LASTEXITCODE) { Write-Warning "Acceptance consumer group cleanup failed: $run" }
        & docker exec $kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic $run
        if ($LASTEXITCODE) { Write-Warning "Acceptance topic cleanup failed: $run" }
    }
    if ($databaseCreated) {
        "DROP DATABASE IF EXISTS $run; REVOKE ALL PRIVILEGES ON $run.* FROM 'gateway'@'%';" | & docker exec -i $mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot'
        if ($LASTEXITCODE) { Write-Warning "Acceptance database cleanup failed: $run" }
    }
}
