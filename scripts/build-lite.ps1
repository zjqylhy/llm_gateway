param(
    [string]$Image = 'llm-gateway:lite',
    [string]$IcpNumber = '',
    [string]$RuntimeImage = 'gcr.io/distroless/static-debian12:nonroot'
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $root
$buildDir = Join-Path $root ('data/lite-build/' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force -Path (Join-Path $buildDir 'bin') | Out-Null
Push-Location web
$savedFiling = [Environment]::GetEnvironmentVariable('VITE_ICP_NUMBER', 'Process')
try {
    $env:VITE_ICP_NUMBER = $IcpNumber
    & npm ci --prefer-offline
    if ($LASTEXITCODE) { throw 'npm ci failed' }
    & npm run build
    if ($LASTEXITCODE) { throw 'Frontend build failed' }
} finally {
    [Environment]::SetEnvironmentVariable('VITE_ICP_NUMBER', $savedFiling, 'Process')
    Pop-Location
}
Copy-Item -LiteralPath (Join-Path $root 'web/dist') -Destination (Join-Path $buildDir 'web') -Recurse
$saved = @{}
foreach ($name in @('GOOS', 'GOARCH', 'CGO_ENABLED', 'GOMAXPROCS')) { $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
try {
    $env:GOOS = 'linux'; $env:GOARCH = 'amd64'; $env:CGO_ENABLED = '0'; $env:GOMAXPROCS = '4'
    foreach ($target in @('gateway', 'admin', 'worker', 'migrate', 'bootstrap', 'cache-init', 'quota-rebuild', 'metering-audit', 'metering-resume', 'metering-checkpoint-init', 'metering-recover')) {
        & go build -p 1 -trimpath -ldflags='-s -w' -o (Join-Path $buildDir "bin/$target") "./cmd/$target"
        if ($LASTEXITCODE) { throw "Build $target failed" }
    }
} finally {
    foreach ($name in $saved.Keys) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
}
& docker build --platform linux/amd64 --build-arg "RUNTIME_IMAGE=$RuntimeImage" -f deploy/lite/Dockerfile.prebuilt -t $Image $buildDir
if ($LASTEXITCODE) { throw 'Image assembly failed' }
& docker image inspect $Image --format '{{.Id}} {{.Os}}/{{.Architecture}}'
if ($LASTEXITCODE) { throw 'Image inspection failed' }
Write-Host "镜像已构建：$Image；可追踪构建目录：$buildDir"
