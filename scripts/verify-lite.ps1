param(
    [ValidateRange(1, 60)][int]$Minutes = 60,
    [string]$Image = 'llm-gateway:lite-acceptance',
    [string]$EdgeImage = 'caddy:2.10'
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $root
$run = 'gw_lite_' + [guid]::NewGuid().ToString('N').Substring(0, 12)
$runDir = Join-Path $root "data/acceptance/$run"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$envPath = Join-Path $runDir 'runtime.env'
function RandomHex { return [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(16)).ToLowerInvariant() }
function FreePort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    try { $listener.Start(); return $listener.LocalEndpoint.Port } finally { $listener.Stop() }
}
# 不读取用户 .env；全部凭据、数据卷和故障目标只属于本次随机项目。
$values = @{
    GW_IMAGE = $Image; GW_EDGE_IMAGE = $EdgeImage
    GW_LOCAL_DB_PASSWORD = RandomHex; GW_LOCAL_ROOT_PASSWORD = RandomHex; GW_LOCAL_REDIS_PASSWORD = RandomHex
    GW_MASTER_KEY = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
    GW_ADMIN_USER = 'admin'; GW_ADMIN_PASSWORD = 'test-' + (RandomHex)
    GW_ADMIN_ORIGIN = 'https://localhost:8443'
    GW_API_HOST = 'api.example.com'; GW_CONSOLE_HOST = 'console.example.com'
    GW_CACHE_ENABLED = 'false'; GW_CACHE_REDIS_URL = ''; GW_MILVUS_URL = ''
    GW_MYSQL_MAX_OPEN_CONNS = '12'; GW_MYSQL_MAX_IDLE_CONNS = '2'; GW_REDIS_POOL_SIZE = '8'
    GW_MAX_CONCURRENT_REQUESTS = '2'; GW_MAX_ADMIN_REQUESTS = '8'; GW_MAX_METERING_INFLIGHT = '2'; GW_MAX_CONNECTIONS = '64'
    GW_KAFKA_TOPIC = 'gateway-events'; GW_KAFKA_GROUP = 'gateway-metering'; GW_ETCD_PREFIX = '/llm-gateway'
    GW_KAFKA_TLS = ''; GW_KAFKA_USER = ''; GW_KAFKA_PASSWORD = ''; GW_ETCD_USER = ''; GW_ETCD_PASSWORD = ''
    OTEL_EXPORTER_OTLP_ENDPOINT = ''; GOMEMLIMIT = '192MiB'; GOMAXPROCS = '2'
    GW_RUN_LITE_TESTS = '1'; GW_LITE_MINUTES = "$Minutes"; GW_LITE_PROJECT = $run
    GW_LITE_RUN_DIR = $runDir.Replace('\', '/'); GW_LITE_ENV_FILE = $envPath.Replace('\', '/')
}
$ports = @{}
foreach ($name in @('MYSQL', 'REDIS', 'ETCD', 'KAFKA', 'GATEWAY', 'UPSTREAM', 'LOCAL')) {
    do { $port = FreePort } while ($ports.Values -contains $port)
    $ports[$name] = $port
    $values["GW_LITE_${name}_PORT"] = "$port"
}
$values['GW_ADMIN_ORIGIN'] = "https://localhost:$($ports['LOCAL'])"
$values['GW_MYSQL_DSN'] = "gateway:$($values['GW_LOCAL_DB_PASSWORD'])@tcp(127.0.0.1:$($ports['MYSQL']))/gateway?parseTime=true&charset=utf8mb4"
$values['GW_REDIS_URL'] = "redis://:$($values['GW_LOCAL_REDIS_PASSWORD'])@127.0.0.1:$($ports['REDIS'])/0"
$values['GW_KAFKA_BROKERS'] = "127.0.0.1:$($ports['KAFKA'])"
$values['GW_ETCD_ENDPOINTS'] = "http://127.0.0.1:$($ports['ETCD'])"
$saved = @{}
$compose = @('compose', '--env-file', $envPath, '-p', $run, '-f', 'deploy/lite/compose.yaml', '-f', 'deploy/lite/acceptance.yaml')
$created = $false
try {
    [IO.File]::WriteAllLines($envPath, [string[]]@($values.Keys | Sort-Object | ForEach-Object { "$_=$($values[$_])" }), [Text.UTF8Encoding]::new($false))
    foreach ($name in $values.Keys) {
        $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        [Environment]::SetEnvironmentVariable($name, $values[$name], 'Process')
    }
    $buildEnv = @{}
    foreach ($name in @('GOOS', 'GOARCH', 'CGO_ENABLED')) { $buildEnv[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
    try {
        $env:GOOS = 'linux'; $env:GOARCH = 'amd64'; $env:CGO_ENABLED = '0'
        & go test -p 1 -c -o (Join-Path $runDir 'gateway.test') ./internal/gateway
        if ($LASTEXITCODE) { throw 'Build Linux acceptance runner failed' }
    } finally {
        foreach ($name in $buildEnv.Keys) { [Environment]::SetEnvironmentVariable($name, $buildEnv[$name], 'Process') }
    }
    & docker @compose config --quiet
    if ($LASTEXITCODE) { throw 'Invalid lite Compose' }
    $created = $true
    & docker @compose up -d --wait mysql redis kafka etcd upstream
    if ($LASTEXITCODE) { throw 'Lite dependencies failed to start' }
    & docker @compose run --rm kafka-init
    if ($LASTEXITCODE) { throw 'Topic initialization failed' }
    # 使用镜像中的生产命令验收迁移与初始化，再由测试角色验证可控上游。
    & docker @compose run --rm migrate
    if ($LASTEXITCODE) { throw 'Migration failed' }
    $bootstrapped = $false
    for ($attempt = 0; $attempt -lt 6; $attempt++) {
        $output = & docker @compose run --rm bootstrap 2>&1
        $status = $LASTEXITCODE
        $output | ForEach-Object { Write-Host "$_" }
        if (-not $status) { $bootstrapped = $true; break }
        # 首次 KRaft 会异步初始化消费组协调器，只对该明确的暂态错误重试。
        if (($output -join "`n") -notmatch 'Not Coordinator|Coordinator.*(Available|Loading)') { break }
        Start-Sleep -Seconds 5
    }
    if (-not $bootstrapped) { throw 'Bootstrap failed' }
    Write-Host "隔离低配验收：$run；持续 $Minutes 分钟；结果：$runDir"
    & go test -p 1 ./internal/gateway -run '^TestLiteInfrastructure$' -count=1 -v -timeout 90m 2>&1 | Tee-Object -FilePath (Join-Path $runDir 'test.log')
    if ($LASTEXITCODE) { throw "Lite acceptance failed; see $runDir" }
} finally {
    if ($created) {
        # 保留失败现场日志；清理仅针对脚本创建的随机项目，绝不套用用户项目名。
        & docker @compose --profile '*' logs --no-color 2>&1 | Out-File -LiteralPath (Join-Path $runDir 'containers.log') -Encoding utf8
        $ids = @(& docker ps -aq --filter "label=com.docker.compose.project=$run")
        if ($ids.Count -gt 0) {
            & docker inspect --format '{{.Name}} {{json .State}} restarts={{.RestartCount}} memory={{.HostConfig.Memory}}' @ids |
                Out-File -LiteralPath (Join-Path $runDir 'final-state.txt') -Encoding utf8
        }
        if ($run -notmatch '^gw_lite_[a-f0-9]{12}$') { throw 'Unsafe cleanup project' }
        & docker @compose --profile '*' down --volumes --remove-orphans --timeout 10
        if ($LASTEXITCODE) { Write-Warning "验收资源清理未完成：$run" }
    }
    foreach ($name in $saved.Keys) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
    if (Test-Path -LiteralPath $envPath) { Remove-Item -LiteralPath $envPath }
}
