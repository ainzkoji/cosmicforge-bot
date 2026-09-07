# MetaTrader Bridge - VPS Deployment Guide

> **⚠️ CRITICAL: Single-User Architecture**
> 
> **One bridge instance = One MT terminal = One MT account = One platform user**
> 
> This bridge is **NOT** multi-tenant. Each bridge serves exactly ONE user's MT account.
> 
> **Multi-User Platform Model**:
> - The **bot backend** is multi-user (manages many users)
> - Each user configures their own `bridge_url` + `bridge_token` in their account
> - The bot backend routes requests to the correct user's bridge
> - **Result**: One bridge deployment per platform user
> 
> **Deployment Options**:
> 1. **One VPS per user** - Simplest, most isolated
> 2. **Multiple bridges on one VPS** - Requires:
>    - Separate MT terminals per user
>    - Unique HTTP ports (8443, 8444, 8445...)
>    - Unique ZMQ ports (5555, 5556, 5557...)
>    - Separate config files and Windows services

## Overview

This guide covers deploying the MT Bridge to a Windows VPS for production use.

## VPS Setup

### 1. Choose a VPS Provider

Recommended providers:
- **Vultr**: Good performance, affordable Windows VPS
- **DigitalOcean**: Reliable, but pricier for Windows
- **AWS EC2**: Flexible, pay-as-you-go (t3.medium recommended)
- **Azure**: Good for enterprise deployments

**Minimum Specs**:
- Windows Server 2019/2022
- 2 CPU cores
- 4GB RAM
- 40GB SSD
- Dedicated IP address

### 2. Initial VPS Configuration

1. **Connect via RDP**:
   - Use Windows Remote Desktop
   - IP: Your VPS IP
   - Username: Administrator (or as provided)
   - Password: Provided by VPS host

2. **Set Strong Admin Password**:
   ```
   Control Panel → User Accounts → Change Password
   ```

3. **Enable Windows Updates**:
   ```
   Settings → Update & Security → Check for updates
   ```

4. **Configure Time Zone**:
   ```
   Settings → Time & Language → Set to your trading timezone
   ```

## Installing Components

### 1. Install Python

1. Download Python 3.11 from [python.org](https://www.python.org/downloads/windows/)
2. Run installer:
   - ☑ Add Python to PATH
   - ☑ Install for all users
3. Verify: `python --version`

### 2. Install MT5 Terminal

1. Download from [metatrader5.com](https://www.metatrader5.com/en/download)
2. Install to default location
3. Log in with your broker credentials
4. Configure chart layout (EA will work on any chart)

### 3. Copy Bridge Files

1. Upload bridge files to VPS:
   - Use RDP file transfer
   - Or use SFTP client (FileZilla, WinSCP)
   - Recommended location: `C:\MTBridge`

2. Run installation:
   ```powershell
   cd C:\MTBridge
   .\scripts\install.ps1
   ```

### 4. Install EA

Follow INSTALL.md to:
1. Copy `MTBridge.mq5` to MT5 Experts folder
2. Copy ZeroMQ includes and DLL
3. Compile in MetaEditor
4. Attach to chart

## SSL/TLS Configuration

### Option 1: Self-Signed Certificate (Testing)

```powershell
# Generate self-signed cert (valid for 1 year)
openssl req -new -x509 -keyout key.pem -out cert.pem -days 365 -nodes
```

Copy `key.pem` and `cert.pem` to `server/` directory.

### Option 2: Let's Encrypt (Production)

> **Note**: Requires a domain name pointing to your VPS IP

1. Install Certbot for Windows:
   ```powershell
   choco install certbot
   ```

2. Generate certificate:
   ```powershell
   certbot certonly --standalone -d yourdomain.com
   ```

3. Update `main.py`:
   ```python
   uvicorn.run(
       "main:app",
       host="0.0.0.0",
       port=8443,
       ssl_keyfile="C:/Certbot/live/yourdomain.com/privkey.pem",
       ssl_certfile="C:/Certbot/live/yourdomain.com/fullchain.pem"
   )
   ```

### Option 3: Cloudflare Tunnel (No SSL needed)

1. Install Cloudflare Tunnel
2. Point tunnel to `localhost:8443`
3. Access via Cloudflare URL (already has SSL)

## Firewall Configuration

### Windows Firewall

Already configured by `install.ps1`, but to verify:

```powershell
# List rules
Get-NetFirewallRule -DisplayName "MT Bridge Server"

# Manually add if needed
New-NetFirewallRule -DisplayName "MT Bridge Server" `
                    -Direction Inbound `
                    -Action Allow `
                    -Protocol TCP `
                    -LocalPort 8443 `
                    -Profile Any
```

### Cloud Provider Firewall

**AWS Security Group**:
- Type: Custom TCP
- Port: 8443
- Source: Your bot backend IP (or 0.0.0.0/0 for any)

**Azure Network Security Group**:
- Priority: 100
- Name: AllowBridge
- Port: 8443
- Protocol: TCP
- Source: Specific IP or Any

**Vultr/DigitalOcean**:
- Add firewall rule for TCP port 8443
- Source: Your IP or Any

## Running as Windows Service

### Method 1: Using NSSM (Recommended)

1. Download NSSM: [nssm.cc](https://nssm.cc/download)
2. Extract to `C:\nssm`
3. Install service:

```powershell
cd C:\nssm\win64
.\nssm.exe install MTBridge "C:\Python311\python.exe" "C:\MTBridge\server\main.py"

# Configure service
.\nssm.exe set MTBridge AppDirectory "C:\MTBridge\server"
.\nssm.exe set MTBridge DisplayName "MetaTrader Bridge"
.\nssm.exe set MTBridge Description "REST API bridge for MetaTrader 4/5"
.\nssm.exe set MTBridge Start SERVICE_AUTO_START

# Start service
.\nssm.exe start MTBridge

# Check status
.\nssm.exe status MTBridge
```

### Method 2: Using Task Scheduler

1. Open Task Scheduler
2. Create Task:
   - General tab:
     - Name: MTBridge
     - Run whether user is logged on or not
     - Run with highest privileges
   - Triggers tab:
     - At startup
   - Actions tab:
     - Program: `C:\Python311\python.exe`
     - Arguments: `C:\MTBridge\server\main.py`
     - Start in: `C:\MTBridge\server`
   - Settings tab:
     - If task fails, restart every 1 minute

### Verify Service

```powershell
# Check if running
Get-Service MTBridge

# View logs (if using NSSM)
notepad C:\MTBridge\server\nssm.log
```

## Auto-Start MT5 Terminal

### Method 1: Startup Folder

1. Press `Win + R`
2. Type: `shell:startup`
3. Create shortcut to MT5:
   - Target: `C:\Program Files\MetaTrader 5\terminal64.exe`
   - Arguments: `/profile:MyProfile.tpl`

### Method 2: Task Scheduler

1. Create Task:
   - Trigger: At login
   - Action: Start program `terminal64.exe`
   - Wait 30 seconds before starting bridge service

## Monitoring

### 1. Check Bridge Health

Create `monitor.ps1`:

```powershell
$url = "https://localhost:8443/v1/health"
$token = "YOUR_API_TOKEN"

$headers = @{
    "Authorization" = "Bearer $token"
}

try {
    $response = Invoke-RestMethod -Uri $url -Headers $headers
    if ($response.status -eq "ok") {
        Write-Host "✓ Bridge is healthy" -ForegroundColor Green
        Write-Host "Account: $($response.account)"
    } else {
        Write-Host "✗ Bridge unhealthy" -ForegroundColor Red
    }
} catch {
    Write-Host "✗ Bridge not responding" -ForegroundColor Red
}
```

Run every 5 minutes via Task Scheduler.

### 2. Set Up Alerting

Use Windows Event Log or external monitoring:

- **UptimeRobot**: Free HTTP monitoring
- **Pingdom**: Professional monitoring
- **CloudWatch**: If using AWS

## Backup & Recovery

### Files to Backup

```
C:\MTBridge\server\config.json     # API tokens
C:\MetaTrader5\MQL5\Experts\MTBridge.mq5  # EA source
```

### Recovery Process

1. Reinstall Python and MT5
2. Restore bridge files
3. Run `install.ps1`
4. Restore `config.json`
5. Compile and attach EA
6. Start service

## Security Hardening

### 1. Restrict RDP Access

```powershell
# Change RDP port (optional)
Set-ItemProperty -Path 'HKLM:\System\CurrentControlSet\Control\Terminal Server\WinStations\RDP-Tcp' `
                 -Name PortNumber -Value 3390

# Enable Network Level Authentication
Set-ItemProperty -Path 'HKLM:\System\CurrentControlSet\Control\Terminal Server\WinStations\RDP-Tcp' `
                 -Name UserAuthentication -Value 1
```

### 2. Enable Windows Defender

```powershell
Set-MpPreference -DisableRealtimeMonitoring $false
```

### 3. IP Whitelisting

In `server/main.py`, add middleware:

```python
ALLOWED_IPS = ["1.2.3.4", "5.6.7.8"]  # Bot backend IPs

@app.middleware("http")
async def check_ip(request: Request, call_next):
    if request.client.host not in ALLOWED_IPS:
        raise HTTPException(status_code=403, detail="IP not allowed")
    return await call_next(request)
```

## Troubleshooting

### Service Won't Start

1. Check logs: `C:\MTBridge\server\nssm.log`
2. Test manually: `python C:\MTBridge\server\main.py`
3. Verify Python path in service config

### MT5 Doesn't Auto-Start

1. Check startup shortcut exists
2. Verify MT5 login credentials saved
3. Check EA Auto Trading is enabled in shortcut

### High Memory Usage

1. MT5 can use 500MB-1GB normally
2. Python server should use <100MB
3. If higher, check for memory leaks
4. Restart services weekly via Task Scheduler

## Performance Optimization

### 1. Disable Unnecessary Services

```powershell
# List services
Get-Service | Where-Object {$_.Status -eq "Running"}

# Disable print spooler (usually not needed on VPS)
Stop-Service -Name Spooler
Set-Service -Name Spooler -StartupType Disabled
```

### 2. Optimize MT5

1. In MT5: `Tools` → `Options` → `Charts`
   - Max bars in chart: 5000
   - Max bars in history: 100000
2. Close unnecessary charts
3. Disable news feed if not needed

## Cost Optimization

### 1. Choose Right VPS Size

- **Testing**: 2GB RAM, $10-15/month
- **Production (1-2 bots)**: 4GB RAM, $20-30/month
- **High Volume (3+ bots)**: 8GB RAM, $40-60/month

### 2. Use Reserved Instances

- AWS: Save 30-40% with reserved instances
- Azure: Use hybrid benefit if you have Windows licenses

## Next Steps

- Test the deployment thoroughly
- Configure bot backend to use bridge
- Monitor logs for 24 hours
- Set up automated backups
- Document your specific VPS credentials

## Support

For deployment issues:
1. Check service logs
2. Verify firewall rules
3. Test with local bot backend first
4. Review security group settings
