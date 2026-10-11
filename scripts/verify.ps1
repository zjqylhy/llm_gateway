$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath (Split-Path $PSScriptRoot -Parent)
$format = gofmt -l internal cmd
if ($format) { throw "Go files require gofmt: $format" }
go test ./...; if ($LASTEXITCODE) { throw 'Go tests failed' }
go vet ./...; if ($LASTEXITCODE) { throw 'go vet failed' }
go build ./...; if ($LASTEXITCODE) { throw 'Go build failed' }
Push-Location web
try {
    npm ci; if ($LASTEXITCODE) { throw 'npm ci failed' }
    npm run build; if ($LASTEXITCODE) { throw 'web build failed' }
    npm test; if ($LASTEXITCODE) { throw 'web refresh tests failed' }
    npx prettier --check src; if ($LASTEXITCODE) { throw 'web formatting failed' }
} finally { Pop-Location }
node scripts/validate-deploy.mjs; if ($LASTEXITCODE) { throw 'deployment validation failed' }
