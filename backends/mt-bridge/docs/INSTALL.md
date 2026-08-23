# MetaTrader Bridge - Installation Guide

> **⚠️ CRITICAL: Single-User Architecture**
> 
> **One bridge instance = One MT terminal = One MT account = One platform user**
> 
> This bridge is designed to serve a **SINGLE** MetaTrader account. Do NOT share bridge URL/token between multiple users.
> 
> **For multi-user platforms**:
> - Each user must have their own dedicated bridge instance
> - Each bridge needs unique HTTP port + ZMQ port + token
> - The platform achieves multi-user support by having each user provide their own `bridge_url` + `bridge_token`

## Overview

The MetaTrader Bridge enables the CosmicForge trading bot to execute trades through MT4/MT5 platforms. It consists of two components:

1. **Expert Advisor (EA)** - Runs inside MT4/MT5 terminal, handles trading operations
2. **Python Server** - Exposes REST API, communicates with EA via ZeroMQ

## System Requirements

### Minimum Requirements
- **OS**: Windows 10+ or Windows Server 2016+
- **Python**: 3.8 or higher
- **MT Platform**: MetaTrader 4 or MetaTrader 5 terminal
- **RAM**: 4GB minimum
- **Network**: Stable internet connection

### Recommended for VPS
- Windows Server 2019/2022
- 8GB RAM
- SSD storage
- Dedicated IP address

## Installation Steps

### 1. Install Python

1. Download Python 3.8+ from [python.org](https://www.python.org/downloads/)
2. During installation, check "Add Python to PATH"
3. Verify installation:
   ```powershell
   python --version
   ```

### 2. Run Installation Script

1. Open PowerShell as **Administrator**
2. Navigate to the bridge directory:
   ```powershell
   cd C:\path\to\mt-bridge
   ```
3. Run the installation script:
   ```powershell
   .\scripts\install.ps1
   ```

The script will:
- Install Python dependencies (FastAPI, ZeroMQ, etc.)
- Download ZeroMQ library for MQL
- Generate API token
- Configure Windows Firewall

### 3. Install Expert Advisor

#### For MT5:

1. Open MT5 Data Folder:
   - In MT5: `File` → `Open Data Folder`
   - Or navigate to: `C:\Users\<USERNAME>\AppData\Roaming\MetaQuotes\Terminal\<INSTANCE_ID>\MQL5`

2. Copy files:
   ```
   Copy: ea\mt5\MTBridge.mq5
   To:   MQL5\Experts\MTBridge.mq5
   
   Copy: ea\mt5\Include\Zmq\*
   To:   MQL5\Include\Zmq\*
   ```

3. Copy ZeroMQ DLL:
   - Download `libzmq.dll` from [zeromq.org](https://zeromq.org/download/)
   - Copy to: `MQL5\Libraries\libzmq.dll`

4. Compile the EA:
   - Open MetaEditor (F4 from MT5)
   - Open `MTBridge.mq5`
   - Click Compile (F7)
   - Fix any errors (usually missing includes)

#### For MT4:

> **Note**: MT4 version coming soon. The architecture is identical, using MQL4 syntax.

## Configuration

### 1. Enable DLL Imports in MT5

1. In MT5, go to: `Tools` → `Options` → `Expert Advisors`
2. Check ☑ "Allow DLL imports"
3. Check ☑ "Allow WebRequest for listed URL" (if using web features)

### 2. Attach EA to Chart

1. In Navigator panel, expand `Expert Advisors`
2. Drag `MTBridge` onto any chart (symbol doesn't matter)
3. In the settings dialog:
   - **ZMQ_PORT**: `5555` (default, must match server config)
   - **API_SECRET**: Leave empty for now
   - ☑ Allow DLL imports
   - ☑ Allow modify signals
4. Click OK

You should see in the Experts tab:
```
MTBridge EA starting...
MTBridge initialized. Listening on tcp://*:5555
Account: 12345678
Server: Demo-Server
```

### 3. Generate API Token

1. Run the token generator:
   ```powershell
   cd server
   python ..\scripts\generate_token.py
   ```

2. Save the generated token securely. Example output:
   ```
   Generated Token: xK9mP2vL5nQ8rT4wU7yA6bC3dE1fG0hJ
   ```

3. The token is automatically saved to `server\config.json`

> **⚠️ CRITICAL: Token Security Requirements**
> 
> The server enforces strict token validation on startup:
> - **Minimum length**: 24 characters
> - **Cannot contain**: "CHANGE_ME" or "PLACEHOLDER"  
> - **Must exist**: At least one token required
> 
> If validation fails, the server will **refuse to start**. The `generate_token.py` script creates secure 43-character tokens using `secrets.token_urlsafe(32)`.
> 
> **Never** use weak or default tokens in production!

### 4. Configure Server

Edit `server\config.json`:

```json
{
  "zmq_host": "localhost",
  "zmq_port": "5555",
  "api_tokens": [
    "xK9mP2vL5nQ8rT4wU7yA6bC3dE1fG0hJ"
  ],
  "request_timeout": 5000
}
```

- **zmq_host**: `localhost` for same machine, or specific IP if remote
- **zmq_port**: Must match EA setting
- **api_tokens**: List of valid tokens for API access
- **request_timeout**: ZMQ request timeout in milliseconds

## Starting the Bridge

### Method 1: Using Batch Script

Double-click: `scripts\start_bridge.bat`

### Method 2: Manual Start

```powershell
cd server
python main.py
```

###Method 3: Windows Service (Advanced)

See DEPLOYMENT.md for running as a service.

## Verification

### 1. Check EA Status

In MT5 Experts tab, you should see:
```
MTBridge initialized. Listening on tcp://*:5555
```

### 2. Test API Server

1. Open browser: `http://localhost:8443/docs`
2. You should see the FastAPI Swagger UI
3. Click "Authorize", enter your API token
4. Test the `/v1/health` endpoint

Expected response:
```json
{
  "status": "ok",
  "platform": "mt5",
  "account": 12345678,
  "server": "Demo-Server",
  "time": "2026-02-08 18:00:00",
  "connected": true
}
```

### 3. Test from Bot Backend

In the bot backend configuration, add MT5 broker:
- **Bridge URL**: `http://localhost:8443` (or your VPS IP)
- **Bridge Token**: Your generated token

Click "Test Connection" - you should see account details.

## Troubleshooting

### EA Not Starting

**Symptoms**: No messages in Experts tab

**Solutions**:
1. Check that DLL imports are enabled in MT5 options
2. Verify `libzmq.dll` is in `MQL5\Libraries\`
3. Recompile EA in MetaEditor
4. Check MT5 Experts tab for error messages

### ZeroMQ Connection Failed

**Symptoms**: "Failed to bind ZMQ socket" error

**Solutions**:
1. Check if port 5555 is already in use
2. Try a different port in both EA and `config.json`
3. Check Windows Firewall isn't blocking localhost connections

### Python Server Won't Start

**Symptoms**: Import errors or module not found

**Solutions**:
1. Reinstall dependencies: `pip install -r requirements.txt`
2. Check Python version: `python --version` (must be 3.8+)
3. Use a virtual environment:
   ```powershell
   python -m venv venv
   .\venv\Scripts\activate
   pip install -r requirements.txt
   ```

### API Returns 401 Unauthorized

**Symptoms**: "Invalid API token" error

**Solutions**:
1. Verify token in `config.json` matches your input
2. Check for extra spaces or quotes
3. Regenerate token: `python scripts\generate_token.py`

### EA Can't Connect to Server

**Symptoms**: "ZMQ timeout - EA not responding"

**Solutions**:
1. Verify EA is attached to chart and running
2. Check `zmq_port` in `config.json` matches EA setting
3. Restart EA (remove and reattach to chart)
4. Check MT5 Expert Advisors are enabled (green "AutoTrading" button)

## Security Notes

- **API Tokens**: Keep tokens secure, never commit to version control
- **Firewall**: Only open port 8443 if accessing from external network
- **SSL/TLS**: For production, configure SSL certificates (see DEPLOYMENT.md)
- **Network**: Use VPN if accessing bridge over internet

## Next Steps

- For VPS deployment: See [DEPLOYMENT.md](DEPLOYMENT.md)
- For SSL configuration: See [SSL_SETUP.md](SSL_SETUP.md)
- For API reference: See [API_CONTRACT.md](API_CONTRACT.md)

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review EA logs in MT5 Experts tab
3. Check server logs in terminal where Python is running
4. Verify both components are on the same version
