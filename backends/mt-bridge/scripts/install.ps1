# MetaTrader Bridge - Installation Script
# Run as Administrator

Write-Host "=== MetaTrader Bridge Installation ===" -ForegroundColor Cyan
Write-Host ""

# Check if running as Administrator
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "ERROR: This script must be run as Administrator" -ForegroundColor Red
    Write-Host "Right-click PowerShell and select 'Run as Administrator'" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "Step 1: Checking Python installation..." -ForegroundColor Green
try {
    $pythonVersion = python --version 2>&1
    Write-Host "Found: $pythonVersion" -ForegroundColor Gray
    
    # Check if version is 3.8+
    if ($pythonVersion -match "Python 3\.([8-9]|[1-9][0-9])") {
        Write-Host "✓ Python version OK" -ForegroundColor Green
    } else {
        Write-Host "WARNING: Python 3.8+ required. Current: $pythonVersion" -ForegroundColor Yellow
    }
} catch {
    Write-Host "ERROR: Python not found. Please install Python 3.8+ from python.org" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "Step 2: Installing Python dependencies..." -ForegroundColor Green
Set-Location "$PSScriptRoot\..\server"

try {
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
    Write-Host "✓ Dependencies installed" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Failed to install dependencies" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "Step 3: Downloading ZeroMQ library for MQL..." -ForegroundColor Green

# Create temp directory
$tempDir = "$env:TEMP\mt-bridge-install"
New-Item -ItemType Directory -Force -Path $tempDir | Out-Null

# Download mql-zmq
$zmqUrl = "https://github.com/dingmaotu/mql-zmq/archive/refs/heads/master.zip"
$zmqZip = "$tempDir\mql-zmq.zip"

try {
    Write-Host "Downloading from GitHub..." -ForegroundColor Gray
    Invoke-WebRequest -Uri $zmqUrl -OutFile $zmqZip
    
    # Extract
    Expand-Archive -Path $zmqZip -DestinationPath $tempDir -Force
    
    # Copy to MQL directories
    $mt4Include = "$PSScriptRoot\..\ea\mt4\Include"
    $mt5Include = "$PSScriptRoot\..\ea\mt5\Include"
    
    Copy-Item "$tempDir\mql-zmq-master\Include\Zmq" -Destination $mt4Include -Recurse -Force
    Copy-Item "$tempDir\mql-zmq-master\Include\Zmq" -Destination $mt5Include -Recurse -Force
    
    Write-Host "✓ ZeroMQ library installed" -ForegroundColor Green
} catch {
    Write-Host "WARNING: Could not download ZeroMQ automatically" -ForegroundColor Yellow
    Write-Host "Please download manually from: https://github.com/dingmaotu/mql-zmq" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Step 4: Generating API token..." -ForegroundColor Green

try {
    $tokenOutput = python "$PSScriptRoot\generate_token.py"
    Write-Host $tokenOutput -ForegroundColor Gray
    Write-Host "✓ API token generated" -ForegroundColor Green
} catch {
    Write-Host "WARNING: Could not generate token automatically" -ForegroundColor Yellow
    Write-Host "Run: python scripts\generate_token.py" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Step 5: Configuring Windows Firewall..." -ForegroundColor Green

try {
    # Add firewall rule for bridge server
    $ruleName = "MT Bridge Server"
    $existingRule = Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
    
    if ($existingRule) {
        Write-Host "Firewall rule already exists" -ForegroundColor Gray
    } else {
        New-NetFirewallRule -DisplayName $ruleName `
                            -Direction Inbound `
                            -Action Allow `
                            -Protocol TCP `
                            -LocalPort 8443 `
                            -Profile Any | Out-Null
        Write-Host "✓ Firewall rule added for port 8443" -ForegroundColor Green
    }
} catch {
    Write-Host "WARNING: Could not configure firewall automatically" -ForegroundColor Yellow
    Write-Host "Manually allow port 8443 in Windows Firewall" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=== Installation Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "1. Copy MTBridge.mq5 to your MT5 terminal's Experts folder" -ForegroundColor White
Write-Host "   Location: C:\Users\<USER>\AppData\Roaming\MetaQuotes\Terminal\<ID>\MQL5\Experts" -ForegroundColor Gray
Write-Host ""
Write-Host "2. Compile the EA in MetaEditor (F7)" -ForegroundColor White
Write-Host ""
Write-Host "3. Attach the EA to any chart in MT5" -ForegroundColor White
Write-Host "   - Ensure DLL imports are enabled in Tools > Options > Expert Advisors" -ForegroundColor Gray
Write-Host ""
Write-Host "4. Start the bridge server:" -ForegroundColor White
Write-Host "   cd $PSScriptRoot\..\server" -ForegroundColor Gray
Write-Host "   python main.py" -ForegroundColor Gray
Write-Host ""
Write-Host "5. Test the bridge:" -ForegroundColor White
Write-Host "   Visit: http://localhost:8443/docs" -ForegroundColor Gray
Write-Host ""
Write-Host "For full documentation, see: docs\INSTALL.md" -ForegroundColor Green
Write-Host ""

Read-Host "Press Enter to exit"
