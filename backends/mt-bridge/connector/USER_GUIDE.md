# MT Bridge Windows Connector - User Guide

## Installation & Setup

### Step 1: Install Prerequisites

1. **Install Python 3.8+**
   - Download from https://www.python.org/downloads/
   - ✅ Check "Add Python to PATH" during installation
   - Verify: Open CMD and type `python --version`

2. **Run Setup Script**
   ```cmd
   cd mt-bridge\connector
   setup.bat
   ```
   This will:
   - Install Python dependencies
   - Check for cloudflared (optional but recommended)

3. **Download Cloudflared** (for secure tunneling)
   - Visit: https://github.com/cloudflare/cloudflared/releases
   - Download: `cloudflared-windows-amd64.exe`
   - Rename to `cloudflared.exe`
   - Place in connector folder

### Step 2: Install Expert Advisor

1. Locate EA files:
   - MT4: `mt-bridge/ea/mt4/CosmicForgeConnector.ex4`
   - MT5: `mt-bridge/ea/mt5/CosmicForgeConnector.ex5`

2. Copy EA to MT terminal:
   - Open MT terminal's data folder: **File → Open Data Folder**
   - Navigate to `MQL4\Experts` (MT4) or `MQL5\Experts` (MT5)
   - Paste the EA file

3. Refresh Navigator:
   - In MT terminal, right-click Navigator → Refresh

4. Attach EA to chart:
   - Drag EA from Navigator to any chart
   - In settings window:
     - ✅ Enable "Allow DLL imports"
     - ✅ Enable "Allow WebRequest for listed URL"
     - Set ZMQ port to `5555` (default)
   - Click OK

5. Verify EA is running:
   - You should see a smiley face icon ☺ in top-right corner
   - Check Experts tab for connection messages

### Step 3: Pair with Platform

1. **Generate Pairing Code**
   - Log in to CosmicForge platform
   - Go to "Connect Broker" → "MetaTrader 4/5"
   - Click "Generate Pairing Code"
   - Copy the code (e.g., `ABCD-EF12`)

2. **Run Connector**
   ```cmd
   python pair.py
   ```
   OR provide code directly:
   ```cmd
   python pair.py --pairing-code ABCD-EF12
   ```

3. **Follow Prompts**
   The connector will:
   - ✅ Generate secure token automatically
   - ✅ Start bridge server on localhost:8443
   - ✅ Wait for bridge health check
   - ✅ Start cloudflared tunnel
   - ✅ Extract public URL (e.g., `https://abc123.trycloudflare.com`)
   - ✅ Pair with platform
   - ✅ Show success message

4. **Keep Running**
   - ⚠️ DO NOT CLOSE the connector window
   - The bridge must stay running for trading to work
   - Press Ctrl+C to stop (only when needed)

### Step 4: Verify Connection

1. Return to CosmicForge platform
2. Check broker connection status
3. You should see your MT account connected
4. Start trading!

## Usage Scenarios

### Scenario A: Quick Pairing (Recommended)

```cmd
python pair.py --pairing-code YOUR-CODE
```

This uses cloudflared for automatic secure tunneling.

### Scenario B: Manual Tunnel (Advanced)

If cloudflared isn't available:

```cmd
python pair.py --skip-tunnel
```

Then you must manually configure:
- Public IP with port forwarding
- Valid SSL certificates in `server/` folder

### Scenario C: Executable Distribution

Build standalone exe (no Python needed on target machine):

```cmd
build.bat
```

Distribute `dist\MTBridgeConnector.exe` + `cloudflared.exe`

## Troubleshooting

### Problem: "Python is not recognized"

**Solution**: Python not in PATH
- Reinstall Python with "Add to PATH" checked
- OR manually add Python to system PATH

### Problem: "cloudflared not found"

**Solution**: Download cloudflared
- Get from: https://github.com/cloudflare/cloudflared/releases
- Place `cloudflared.exe` in connector folder
- OR use `--skip-tunnel` flag

### Problem: "Cannot connect to bridge"

**Causes**:
1. MT terminal not running → Start MT terminal
2. EA not attached → Attach EA to chart
3. EA has DLL imports disabled → Enable in EA settings
4. Wrong ZMQ port → Verify port 5555 in EA settings

**Check**:
- EA shows ☺ icon in chart corner
- Experts tab shows "Connected to bridge"

### Problem: "Invalid pairing code"

**Causes**:
- Code expired (10 min lifetime)
- Wrong platform selected (MT4 vs MT5)

**Solution**:
- Generate new code from platform
- Ensure MT platform matches

### Problem: "Could not find tunnel URL"

**Workaround**:
1. Run cloudflared manually:
   ```cmd
   cloudflared tunnel --url http://localhost:8443
   ```
2. Copy the URL shown
3. Use `--skip-tunnel` and provide URL manually when prompted

### Problem: Bridge crashes after pairing

**Check**:
- EA is still running (☺ icon visible)
- No firewall blocking ZMQ port 5555
- Check bridge logs in `../server/` folder

## Running as Service (24/7)

For production use, run connector as Windows service:

### Option 1: Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Trigger: "At startup"
4. Action: Start program
   - Program: `python.exe`
   - Arguments: `C:\path\to\pair.py --pairing-code YOUR-CODE`
5. Enable "Run whether user is logged on or not"

### Option 2: NSSM (Recommended)

1. Download NSSM: https://nssm.cc/download
2. Install as service:
   ```cmd
   nssm install MTBridge "C:\Python39\python.exe" "C:\path\to\pair.py"
   nssm start MTBridge
   ```

## Security Best Practices

✅ **DO**:
- Keep connector window running during trading hours
- Use cloudflared tunnel for secure access
- Rotate tokens periodically
- Keep bridge server updated

❌ **DON'T**:
- Share `bridge_config.json` (contains token)
- Expose bridge port 8443 directly to internet
- Use weak or default tokens
- Run without SSL in production

## FAQ

**Q: Do I need cloudflared?**  
A: Recommended but not required. Cloudflared provides secure tunneling without port forwarding.

**Q: Can I run multiple MT accounts?**  
A: Yes, but each needs its own bridge instance on a different port.

**Q: Where are tokens stored?**  
A: In `../server/config.json` (never sent to platform)

**Q: How do I change the port?**  
A: Use `--port 9000` flag

**Q: Does this work with MT4 and MT5?**  
A: Yes! Works with both platforms.

## Support

- **Logs**: Check `../server/` for detailed logs
- **Platform**: Use in-app support chat
- **Documentation**: See main README in `mt-bridge/`

---

**CosmicForge Platform** – Automated Trading Made Simple
