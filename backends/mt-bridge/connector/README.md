# MT Bridge Windows Connector

## What is this?

The Windows Connector automatically sets up and pairs your local MT4/MT5 bridge with the CosmicForge platform using a simple pairing code. It handles everything: starting the bridge server, creating a secure tunnel, and registering your account.

## Features

✅ **Auto Bridge Startup**: Automatically starts the MT Bridge server  
✅ **Cloudflare Tunnel**: Creates a secure public URL without port forwarding  
✅ **Token Generation**: Creates secure API tokens automatically  
✅ **One-Click Pairing**: Simple pairing code from platform  
✅ **TLS Security**: Enforces strict TLS by default (via Cloudflare tunnel)

## Quick Start

### Prerequisites

1. **Python 3.8+** installed on Windows
2. **MT4 or MT5** terminal running
3. **Expert Advisor** attached to a chart (see EA Setup below)
4. **Cloudflared** (bundled or downloaded separately)
5. **Active CosmicForge account**

### Installation

1. **Download the entire mt-bridge folder** to your Windows machine where MT terminal runs

2. **Install dependencies**:
   ```cmd
   cd connector
   pip install -r requirements.txt
   ```

3. **Download cloudflared** (if not bundled):
   - Visit: https://github.com/cloudflare/cloudflared/releases
   - Download `cloudflared-windows-amd64.exe`
   - Rename to `cloudflared.exe`
   - Place in connector folder OR add to PATH

### Usage

1. **Generate pairing code** from CosmicForge platform:
   - Go to "Connect Broker"
   - Select "MetaTrader 4" or "MetaTrader 5"
   - Click "Generate Pairing Code"
   - Copy the code (e.g., `ABCD-EF12`)

2. **Run the connector**:
   ```cmd
   python pair.py
   ```
   
   OR with pairing code as argument:
   ```cmd
   python pair.py --pairing-code ABCD-EF12
   ```

3. **Follow the automated setup**:
   - Script will generate secure token (if needed)
   - Start bridge server on localhost:8443
   - Create Cloudflare tunnel for public access
   - Pair with platform using your code
   - Keep running!

4. **Done!** Leave the window open – your bridge is now connected.

## Expert Advisor Setup

**CRITICAL**: You must have the EA running in MT terminal for the bridge to work.

1. Locate EA files in `mt-bridge/ea/mt4/` or `mt-bridge/ea/mt5/`
2. Copy the `.ex4` or `.ex5` file to your MT terminal's `MQL4/Experts` or `MQL5/Experts` folder
3. Restart MT terminal or refresh Navigator
4. Drag the EA onto any chart
5. In EA settings:
   - Enable "Allow DLL imports"
   - Enable "Allow WebRequests"
   - Verify ZMQ port is 5555 (default)
6. Click OK – EA should connect to bridge

## Command Line Options

```cmd
python pair.py --help

Options:
  --pairing-code CODE   Pairing code from platform (skip manual entry)
  --port PORT           Local bridge server port (default: 8443)
  --skip-tunnel         Skip cloudflared, use local/public IP
```

## File Structure

```
connector/
├── pair.py              # Main connector script
├── requirements.txt     # Python dependencies
├── README.md           # This file
└── [generated files]
    ├── bridge_config.json  # Auto-generated (SENSITIVE!)
    └── cloudflared.exe     # Tunnel binary (if downloaded)
```

## How It Works

1. **Token Creation**: Generates a secure 64-char hex token and saves to `server/config.json`
2. **Bridge Startup**: Launches `server/main.py` as subprocess on localhost:8443
3. **Health Check**: Waits for `/v1/health` to return OK
4. **Tunnel Creation**: Starts `cloudflared tunnel --url http://localhost:8443`
5. **URL Extraction**: Parses tunnel output for `https://xxx.trycloudflare.com`
6. **API Pairing**: Calls `POST /api/v1/mt/pair` with:
   - `pairing_code`: User's code
   - `bridge_url`: Tunnel URL
   - `bridge_token`: Generated token
   - `tls_mode`: "strict" (tunnel provides valid cert)
   - `account`: MT account info from health check
7. **Keep Alive**: Monitors bridge and tunnel processes

## Security

✅ **No User Passwords**: Connector never stores your platform password  
✅ **Short-Lived Codes**: Pairing codes expire in 10 minutes  
✅ **Local Tokens**: Tokens stored only in `server/config.json` (never transmitted to platform)  
✅ **TLS Enforcement**: Cloudflare tunnel provides valid SSL certificates  
✅ **Token Rotation**: Generate new tokens anytime via `scripts/generate_token.py`

## Troubleshooting

### "cloudflared not found"
- Download from https://github.com/cloudflare/cloudflared/releases
- Place `cloudflared.exe` in connector folder
- OR add to system PATH

### "Bridge server failed to start"
- Check Python is installed: `python --version`
- Install requirements: `pip install -r ../server/requirements.txt`
- Check port 8443 is not in use

### "Waiting for bridge to initialize..." timeout
- Check MT terminal is running
- Check EA is attached to chart
- Check EA has DLL imports enabled
- Check ZMQ port 5555 is correct in EA settings

### "Could not find tunnel URL"
- Check cloudflared output manually: `cloudflared tunnel --url http://localhost:8443`
- Try `--skip-tunnel` option and use public IP instead
- Verify no firewall blocking cloudflared

### "Invalid pairing code"
- Codes expire in 10 minutes
- Generate new code from platform

### "Rate limit exceeded"
- Max 5 active pairing sessions
- Wait 10 minutes for old sessions to expire

## Advanced Configuration

### Use Public IP Instead of Tunnel

If your machine has a public IP and port forwarding configured:

```cmd
python pair.py --skip-tunnel
```

Then configure your bridge URL manually (e.g., `https://your-public-ip:8443`)

⚠️ **Note**: You must have valid SSL certificates in `server/` folder for strict TLS.

### Custom Platform URL (Development)

```cmd
set PLATFORM_API_URL=http://localhost:8000
python pair.py
```

### Run as Windows Service

For 24/7 uptime, use Task Scheduler or NSSM to run as service:

```cmd
nssm install MTBridgeConnector "C:\Python39\python.exe" "C:\path\to\pair.py"
nssm start MTBridgeConnector
```

## Support

- **Logs**: Check `../server/` for bridge logs
- **Platform**: Contact support via CosmicForge
- **Documentation**: See main MT Bridge README

## License

Part of CosmicForge Platform – All Rights Reserved
