$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $root
& node scripts/validate-deploy.mjs
if ($LASTEXITCODE) { throw 'Deployment structure validation failed' }
$dashboard = Get-Content -LiteralPath deploy/observability/dashboard.json -Raw | ConvertFrom-Json
$rules = @()
foreach ($panel in $dashboard.panels) {
    foreach ($target in $panel.targets) {
        if (-not $target.expr) { continue }
        # 用固定范围替换 Grafana 变量，只做语法校验，真实数据口径由验收验证。
        $expression = $target.expr.Replace('$__rate_interval', '1m')
        foreach ($variable in @('app', 'instance', 'api', 'model', 'stream')) {
            $expression = $expression.Replace('$' + $variable, '.*')
        }
        if ($expression.Contains('$')) { throw "Unknown dashboard variable: $expression" }
        $rules += @{ record = "dashboard_panel_$($panel.id)_$($target.refId)"; expr = $expression }
    }
}
$output = Join-Path $root 'data/acceptance/monitoring-validation'
New-Item -ItemType Directory -Force -Path $output | Out-Null
$path = Join-Path $output 'dashboard-rules.yaml'
# JSON 是 YAML 子集，避免手工缩进或添加专用解析依赖。
$json = @{ groups = @(@{ name = 'dashboard_validation'; rules = $rules }) } | ConvertTo-Json -Depth 10
[IO.File]::WriteAllText($path, $json, [Text.UTF8Encoding]::new($false))
& docker run --rm --network none --cpus 0.5 --memory 128m --read-only --cap-drop ALL `
    --mount "type=bind,src=$root/deploy/observability,dst=/rules,readonly" `
    --mount "type=bind,src=$output,dst=/validation,readonly" `
    --entrypoint /bin/promtool prom/prometheus:v3.5.0 check rules /rules/alerts.yaml /validation/dashboard-rules.yaml
if ($LASTEXITCODE) { throw 'Prometheus rule/query validation failed' }

# 覆盖短许可采样误报，以及停止续约、零值和缺失时仍能发现故障。
& docker run --rm --network none --cpus 0.5 --memory 128m --read-only --tmpfs /tmp:rw,size=64m,mode=1777 --cap-drop ALL `
    --mount "type=bind,src=$root/deploy/observability,dst=/rules,readonly" `
    --entrypoint /bin/promtool prom/prometheus:v3.5.0 test rules /rules/alerts.test.yaml
if ($LASTEXITCODE) { throw 'Prometheus permit regression tests failed' }
