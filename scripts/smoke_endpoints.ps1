param(
    [string]$BaseUrl = "http://127.0.0.1:8787",
    [string]$Exchange = "hyperliquid",
    [string]$Symbol = "BTC/USDC:USDC",
    [int]$WaitSeconds = 90,
    [int]$TimeoutSeconds = 20,
    [switch]$ShowData
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Assert-True {
    param(
        [bool]$Condition,
        [string]$Message
    )

    if (-not $Condition) {
        throw $Message
    }
}

function Wait-ForReady {
    param(
        [string]$Url,
        [int]$Timeout,
        [int]$Wait
    )

    if ($Wait -le 0) {
        return
    }

    $deadline = (Get-Date).AddSeconds($Wait)
    $lastError = $null

    while ((Get-Date) -lt $deadline) {
        try {
            $health = Invoke-RestMethod -Uri "$Url/healthz" -Method Get -TimeoutSec $Timeout
            if ($null -ne $health -and $health.status -eq "ok") {
                Write-Host ""
                return
            }

            $lastError = "unexpected health response: $($health | ConvertTo-Json -Compress)"
        }
        catch {
            $lastError = $_.Exception.Message
        }

        Write-Host -NoNewline "."
        Start-Sleep -Seconds 1
    }

    Write-Host ""

    throw "server did not become ready in time. Last error: $lastError"
}

Write-Host "testing $BaseUrl exchange=$Exchange symbol=$Symbol"
Write-Host "waiting for server readiness (up to ${WaitSeconds}s)..."
Wait-ForReady -Url $BaseUrl -Timeout $TimeoutSeconds -Wait $WaitSeconds

$health = Invoke-RestMethod -Uri "$BaseUrl/healthz" -Method Get -TimeoutSec $TimeoutSeconds
Assert-True ($health.status -eq "ok") "health check failed"
Write-Host "ok  healthz"

$tradesBody = @{
    exchange = $Exchange
    symbol = $Symbol
    limit = 5
    params = @{}
} | ConvertTo-Json -Compress

$trades = Invoke-RestMethod -Uri "$BaseUrl/v1/fetchTrades" -Method Post -ContentType "application/json" -Body $tradesBody -TimeoutSec $TimeoutSeconds
Assert-True ($trades -is [System.Array]) "fetchTrades did not return array"
Assert-True ($trades.Count -gt 0) "fetchTrades returned no data"
Assert-True ($trades[0].symbol -eq $Symbol) "fetchTrades symbol mismatch"
Write-Host "ok  fetchTrades ($($trades.Count) rows)"
if ($ShowData) {
    Write-Host "sample fetchTrades row:"
    $trades[0] | ConvertTo-Json -Depth 10
}

$ohlcvBody = @{
    exchange = $Exchange
    symbol = $Symbol
    timeframe = "1m"
    limit = 3
    params = @{}
} | ConvertTo-Json -Compress

$ohlcv = Invoke-RestMethod -Uri "$BaseUrl/v1/fetchOHLCV" -Method Post -ContentType "application/json" -Body $ohlcvBody -TimeoutSec $TimeoutSeconds
Assert-True ($ohlcv -is [System.Array]) "fetchOHLCV did not return array"
Assert-True ($ohlcv.Count -gt 0) "fetchOHLCV returned no data"
Assert-True ($ohlcv[0].Count -eq 6) "fetchOHLCV candle tuple should have 6 items"
Write-Host "ok  fetchOHLCV ($($ohlcv.Count) rows)"
if ($ShowData) {
    Write-Host "sample fetchOHLCV row:"
    $ohlcv[0] | ConvertTo-Json -Depth 10
}

$bookBody = @{
    exchange = $Exchange
    symbol = $Symbol
    limit = 2
    params = @{}
} | ConvertTo-Json -Compress

$orderBook = Invoke-RestMethod -Uri "$BaseUrl/v1/fetchOrderBook" -Method Post -ContentType "application/json" -Body $bookBody -TimeoutSec $TimeoutSeconds
Assert-True ($null -ne $orderBook.asks) "fetchOrderBook missing asks"
Assert-True ($null -ne $orderBook.bids) "fetchOrderBook missing bids"
Assert-True ($orderBook.asks.Count -gt 0) "fetchOrderBook asks empty"
Assert-True ($orderBook.bids.Count -gt 0) "fetchOrderBook bids empty"
Assert-True ($orderBook.symbol -eq $Symbol) "fetchOrderBook symbol mismatch"
Write-Host "ok  fetchOrderBook"
if ($ShowData) {
    Write-Host "sample fetchOrderBook:"
    $orderBook | ConvertTo-Json -Depth 10
}

Write-Host "all endpoint checks passed"
