# 🧪 AI Bot API PowerShell Test Script
# Tests AI bot endpoints using PowerShell

Write-Host "🚀 AI BOT API POWERSHELL TEST SUITE" -ForegroundColor Green
Write-Host "⏰ Started at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Cyan
Write-Host "🎯 Testing against: http://127.0.0.1:5000" -ForegroundColor Yellow

$baseUrl = "http://127.0.0.1:5000"
$aiApiBase = "$baseUrl/api/ai"

function Test-Endpoint {
    param(
        [string]$Name,
        [string]$Url,
        [string]$Method = "GET",
        [hashtable]$Body = $null,
        [int]$ExpectedStatus = 200
    )
    
    Write-Host "`n🧪 Testing: $Name" -ForegroundColor Yellow
    
    try {
        if ($Method -eq "GET") {
            $response = Invoke-RestMethod -Uri $Url -Method GET -ErrorAction Stop
        } else {
            $jsonBody = $Body | ConvertTo-Json
            $response = Invoke-RestMethod -Uri $Url -Method POST -Body $jsonBody -ContentType "application/json" -ErrorAction Stop
        }
        
        Write-Host "✅ $Name - SUCCESS" -ForegroundColor Green
        Write-Host "   Response: $($response | ConvertTo-Json -Depth 2 | Out-String -Width 200)" -ForegroundColor White
        
    } catch {
        Write-Host "❌ $Name - ERROR" -ForegroundColor Red
        Write-Host "   Error: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# Test 1: Basic Endpoints
Write-Host "`n" + "="*60 -ForegroundColor Magenta
Write-Host "🧪 BASIC AI BOT ENDPOINTS" -ForegroundColor Magenta
Write-Host "="*60 -ForegroundColor Magenta

Test-Endpoint -Name "Get Suggestions" -Url "$aiApiBase/suggestions"

$chatBody = @{
    message = "What is BVMT?"
    session_id = "test_session_001"
}
Test-Endpoint -Name "Basic Chat - What is BVMT?" -Url "$aiApiBase/chat" -Method "POST" -Body $chatBody

$marketBody = @{
    message = "BVMT market analysis"
    session_id = "test_session_002"
}
Test-Endpoint -Name "Market Analysis Chat" -Url "$aiApiBase/chat" -Method "POST" -Body $marketBody

$investBody = @{
    message = "How to invest in BVMT?"
    session_id = "test_session_003"
}
Test-Endpoint -Name "Investment Advice Chat" -Url "$aiApiBase/chat" -Method "POST" -Body $investBody

# Test 2: Advanced Endpoints
Write-Host "`n" + "="*60 -ForegroundColor Magenta
Write-Host "🧪 ADVANCED AI BOT ENDPOINTS" -ForegroundColor Magenta
Write-Host "="*60 -ForegroundColor Magenta

Test-Endpoint -Name "Intelligence Status" -Url "$aiApiBase/intelligence-status"
Test-Endpoint -Name "Market Intelligence" -Url "$aiApiBase/market-intelligence"
Test-Endpoint -Name "Smart Suggestions" -Url "$aiApiBase/suggestions-smart"
Test-Endpoint -Name "User Profile" -Url "$aiApiBase/user-profile/test_user_001"
Test-Endpoint -Name "Health Check" -Url "$aiApiBase/health-smart"

# Test 3: Chat Scenarios
Write-Host "`n" + "="*60 -ForegroundColor Magenta
Write-Host "🧪 CHAT SCENARIOS TESTING" -ForegroundColor Magenta
Write-Host "="*60 -ForegroundColor Magenta

$testMessages = @(
    "What is BVMT?",
    "Tell me about Tunisian companies",
    "BVMT market analysis",
    "How to invest in BVMT?",
    "Which sectors perform well in Tunisia?",
    "BVMT trading hours",
    "Tunisian stock market trends",
    "Latest BVMT news"
)

for ($i = 0; $i -lt $testMessages.Count; $i++) {
    $message = $testMessages[$i]
    $chatBody = @{
        message = $message
        session_id = "scenario_test_$($i.ToString('000'))"
    }
    Test-Endpoint -Name "Chat Scenario $($i+1): $($message.Substring(0, [Math]::Min(30, $message.Length)))..." -Url "$aiApiBase/chat" -Method "POST" -Body $chatBody
    Start-Sleep -Milliseconds 500
}

# Test 4: Error Handling
Write-Host "`n" + "="*60 -ForegroundColor Magenta
Write-Host "🧪 ERROR HANDLING TESTING" -ForegroundColor Magenta
Write-Host "="*60 -ForegroundColor Magenta

$emptyBody = @{
    message = ""
    session_id = "error_test_001"
}
Test-Endpoint -Name "Empty Message (Should return 400)" -Url "$aiApiBase/chat" -Method "POST" -Body $emptyBody -ExpectedStatus 400

# Test 5: ETL Integration
Write-Host "`n" + "="*60 -ForegroundColor Magenta
Write-Host "🧪 ETL INTEGRATION TESTING" -ForegroundColor Magenta
Write-Host "="*60 -ForegroundColor Magenta

$etlEndpoints = @(
    "/api/scraping/status",
    "/api/bronze/status", 
    "/api/silver/status",
    "/api/golden/status",
    "/api/diamond/status"
)

foreach ($endpoint in $etlEndpoints) {
    $layerName = $endpoint.Split('/')[-2].ToUpper()
    Test-Endpoint -Name "ETL $layerName Status" -Url "$baseUrl$endpoint"
}

Write-Host "`n" + "="*60 -ForegroundColor Green
Write-Host "🎉 TEST SUITE COMPLETED" -ForegroundColor Green
Write-Host "⏰ Finished at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Cyan
Write-Host "="*60 -ForegroundColor Green

