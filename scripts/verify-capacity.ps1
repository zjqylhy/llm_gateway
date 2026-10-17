param(
    [ValidateSet('all','locked-reject','locked-wait','background-wait')][string]$Mode = 'all',
    [ValidateRange(1,60)][int]$Seconds = 20,
    [switch]$Quick
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $root
$run = 'gw_capacity_' + [guid]::NewGuid().ToString('N').Substring(0,12)
$runDir = Join-Path $root "data/acceptance/$run"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
function RandomHex { [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(16)).ToLowerInvariant() }
function FreePort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0)
    try { $listener.Start(); $listener.LocalEndpoint.Port } finally { $listener.Stop() }
}
# 所有凭据和卷都属于随机项目，不读取或改写用户 .env。
$values = @{
    GW_IMAGE='gcr.io/distroless/static-debian12:nonroot'; GW_EDGE_IMAGE='caddy:2.10'
    GW_LOCAL_DB_PASSWORD=(RandomHex); GW_LOCAL_ROOT_PASSWORD=(RandomHex); GW_LOCAL_REDIS_PASSWORD=(RandomHex)
    GW_MASTER_KEY=[Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
    GW_ADMIN_USER='admin'; GW_ADMIN_PASSWORD=('capacity-'+(RandomHex)); GW_ADMIN_ORIGIN='https://localhost:8443'
    GW_API_HOST='api.example.com'; GW_CONSOLE_HOST='console.example.com'
    GW_CACHE_ENABLED='false'; GW_CACHE_REDIS_URL=''; GW_MILVUS_URL=''
    GW_MYSQL_MAX_OPEN_CONNS='30'; GW_MYSQL_MAX_IDLE_CONNS='10'; GW_REDIS_POOL_SIZE='32'
    GW_MAX_CONCURRENT_REQUESTS='128'; GW_MAX_ADMIN_REQUESTS='8'; GW_MAX_METERING_INFLIGHT='8'; GW_MAX_CONNECTIONS='512'
    GW_KAFKA_TLS=''; GW_KAFKA_USER=''; GW_KAFKA_PASSWORD=''; GW_ETCD_USER=''; GW_ETCD_PASSWORD=''
    GW_OUTBOX_APP_BYTES='268435456'; GW_OUTBOX_GLOBAL_BYTES='1073741824'
    GW_METERING_COMMIT_BATCH_SIZE='32'; GW_METERING_COMMIT_INTERVAL_MS='100'
    GW_RUN_CAPACITY_TESTS='1'; GW_CAPACITY_TEST_SECONDS="$Seconds"; GW_CAPACITY_TEST_QUICK=$(if($Quick){'1'}else{'0'})
    OTEL_EXPORTER_OTLP_ENDPOINT=''; GOMEMLIMIT='192MiB'; GOMAXPROCS='2'
    GW_LITE_RUN_DIR=$runDir.Replace('\','/'); GW_LITE_ENV_FILE=(Join-Path $runDir 'infra.env').Replace('\','/')
    GW_KAFKA_TOPIC=$run; GW_KAFKA_GROUP=$run; GW_ETCD_PREFIX="/$run"
}
$ports = @{}
foreach($name in @('MYSQL','REDIS','ETCD','KAFKA','GATEWAY','UPSTREAM','LOCAL')) {
    do { $port = FreePort } while($ports.Values -contains $port)
    $ports[$name] = $port; $values["GW_LITE_${name}_PORT"] = "$port"
}
$infraEnv = Join-Path $runDir 'infra.env'
$compose = @('compose','--env-file',$infraEnv,'-p',$run,'-f','deploy/lite/compose.yaml','-f','deploy/lite/acceptance.yaml')
$saved = @{}
$created = $false
$secretFiles = @($infraEnv)
$caseRuns=@()
try {
    [IO.File]::WriteAllLines($infraEnv,[string[]]@($values.Keys | Sort-Object | ForEach-Object { "$_=$($values[$_])" }),[Text.UTF8Encoding]::new($false))
    $buildEnv = @{}
    foreach($name in @('GOOS','GOARCH','CGO_ENABLED')) { $buildEnv[$name] = [Environment]::GetEnvironmentVariable($name,'Process') }
    try {
        $env:GOOS='linux'; $env:GOARCH='amd64'; $env:CGO_ENABLED='0'
        & go test -p 1 -c -o (Join-Path $runDir 'gateway.test') ./internal/gateway
        if($LASTEXITCODE) { throw 'Build capacity runner failed' }
    } finally { foreach($name in $buildEnv.Keys) { [Environment]::SetEnvironmentVariable($name,$buildEnv[$name],'Process') } }
    $hostRunner=Join-Path $runDir 'gateway-host.test.exe'
    & go test -p 1 -c -o $hostRunner ./internal/gateway
    if($LASTEXITCODE) { throw 'Build host capacity driver failed' }
    & docker @compose config --quiet
    if($LASTEXITCODE) { throw 'Invalid capacity dependency Compose' }
    $created=$true
    & docker @compose up -d --wait mysql redis kafka etcd
    if($LASTEXITCODE) { throw 'Capacity dependencies failed to start' }
    $modes = @('locked-reject','locked-wait','background-wait')
    if($Mode -ne 'all') { $modes=@($Mode) }
    $summary = [ordered]@{run=$run; quick=[bool]$Quick; seconds=$Seconds; started_utc=[DateTime]::UtcNow.ToString('o'); binary_sha256=(Get-FileHash (Join-Path $runDir 'gateway.test') -Algorithm SHA256).Hash; host_binary_sha256=(Get-FileHash $hostRunner -Algorithm SHA256).Hash; modes=$modes}
    $summary | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $runDir 'manifest.json') -Encoding utf8
    $caseIndices=@(0,1,2,3,4,5)
    if($Quick){$caseIndices=@(0)}
    foreach($caseIndex in $caseIndices) {
      for($modeIndex=0;$modeIndex -lt $modes.Count;$modeIndex++) {
        $item=$modes[($modeIndex+$caseIndex)%$modes.Count]
        $suffix=$item.Replace('-','')+[char](97+$caseIndex)
        $caseRun="${run}_$suffix"
        $dir=Join-Path $runDir "case-$caseIndex/$item"
        $caseRuns+=$caseRun
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        Copy-Item -LiteralPath (Join-Path $runDir 'gateway.test') -Destination (Join-Path $dir 'gateway.test')
        if($caseRun -notmatch '^gw_capacity_[a-f0-9]{12}_[a-z]+$') { throw 'Invalid isolated schema' }
        "CREATE DATABASE $caseRun; GRANT ALL PRIVILEGES ON $caseRun.* TO 'gateway'@'%';" | & docker @compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot'
        if($LASTEXITCODE) { throw 'Create capacity schema failed' }
        & docker @compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic $caseRun --partitions 3 --replication-factor 1 --config retention.ms=7200000
        if($LASTEXITCODE) { throw 'Create capacity topic failed' }
        # 每个模式单独 schema/topic/etcd 前缀；应用键沿用 cap-*，模式间重置本次专用 Redis。
        if($caseRuns.Count -gt 1) {
            & docker @compose exec -T redis sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli FLUSHDB' | Out-Null
            if($LASTEXITCODE) { throw 'Reset dedicated capacity Redis failed' }
        }
        $testEnv=@{}
        foreach($name in $values.Keys) { $testEnv[$name]=$values[$name] }
        $testEnv['GW_INFRA_RUN_ID']=$caseRun; $testEnv['GW_KAFKA_TOPIC']=$caseRun; $testEnv['GW_KAFKA_GROUP']=$caseRun; $testEnv['GW_ETCD_PREFIX']="/$caseRun"
        $testEnv['GW_OUTBOX_CAPACITY_MODE']=$item.Split('-')[0]; $testEnv['GW_OUTBOX_CAPACITY_WAIT']=$(if($item.EndsWith('-wait')){'true'}else{'false'})
        $testEnv['GW_CAPACITY_TEST_MODE']=$item
        $testEnv['GW_CAPACITY_CASE_INDEX']="$caseIndex"
        $testEnv['GW_MYSQL_DSN']="gateway:$($values['GW_LOCAL_DB_PASSWORD'])@tcp(127.0.0.1:$($ports['MYSQL']))/$caseRun`?parseTime=true&charset=utf8mb4"
        $testEnv['GW_REDIS_URL']="redis://:$($values['GW_LOCAL_REDIS_PASSWORD'])@127.0.0.1:$($ports['REDIS'])/0"
        $testEnv['GW_KAFKA_BROKERS']="127.0.0.1:$($ports['KAFKA'])"; $testEnv['GW_ETCD_ENDPOINTS']="http://127.0.0.1:$($ports['ETCD'])"
        $testEnv['GW_RELIABILITY_DIRECTORY']=$dir; $testEnv['GW_RELIABILITY_ENV_FILE']=Join-Path $dir 'roles.env'
        $testEnv['GW_RELIABILITY_NETWORK']="${run}_default"; $testEnv['GW_RELIABILITY_IMAGE']=$values['GW_IMAGE']
        $roleEnv=@{}
        foreach($name in $testEnv.Keys) { $roleEnv[$name]=$testEnv[$name] }
        $roleEnv['GW_MYSQL_DSN']="gateway:$($values['GW_LOCAL_DB_PASSWORD'])@tcp(mysql:3306)/$caseRun`?parseTime=true&charset=utf8mb4"
        $roleEnv['GW_REDIS_URL']="redis://:$($values['GW_LOCAL_REDIS_PASSWORD'])@redis:6379/0"; $roleEnv['GW_KAFKA_BROKERS']='kafka:9092'; $roleEnv['GW_ETCD_ENDPOINTS']='http://etcd:2379'
        $rolePath=$testEnv['GW_RELIABILITY_ENV_FILE']; $secretFiles+=$rolePath
        [IO.File]::WriteAllLines($rolePath,[string[]]@($roleEnv.Keys | Sort-Object | ForEach-Object { "$_=$($roleEnv[$_])" }),[Text.UTF8Encoding]::new($false))
        foreach($name in $testEnv.Keys) {
            if(-not $saved.ContainsKey($name)) { $saved[$name]=[Environment]::GetEnvironmentVariable($name,'Process') }
            [Environment]::SetEnvironmentVariable($name,$testEnv[$name],'Process')
        }
        Write-Host "容量对照：$caseRun；结果：$dir"
        $testArgs=@('-test.run=^TestCapacityIntegration$','-test.count=1','-test.v','-test.timeout=95m')
        & $hostRunner @testArgs 2>&1 | Tee-Object -FilePath (Join-Path $dir 'test.log')
        if($LASTEXITCODE) { throw "Capacity acceptance failed: $dir" }
      }
    }
} finally {
    if($created) {
        foreach($caseRun in $caseRuns) {
            $ids=@(& docker ps -aq --filter "label=llm-gateway.capacity=$caseRun")
            foreach($id in $ids) {
                & docker logs $id 2>&1 | Out-File -LiteralPath (Join-Path $runDir "remaining-$id.log") -Encoding utf8
                & docker stop --time 5 $id | Out-Null
                & docker rm -v $id | Out-Null
            }
        }
        & docker @compose logs --no-color 2>&1 | Out-File -LiteralPath (Join-Path $runDir 'dependencies.log') -Encoding utf8
        if($run -notmatch '^gw_capacity_[a-f0-9]{12}$') { throw 'Unsafe capacity cleanup project' }
        & docker @compose down --volumes --remove-orphans --timeout 10
        if($LASTEXITCODE) { Write-Warning "Capacity dependency cleanup incomplete: $run" }
    }
    foreach($name in $saved.Keys) { [Environment]::SetEnvironmentVariable($name,$saved[$name],'Process') }
    foreach($path in $secretFiles) { if(Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path } }
}
