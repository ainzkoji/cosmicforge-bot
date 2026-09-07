# MetaTrader Bridge

Windows-hosted bridge that exposes MT4/MT5 trading functionality via REST API.

## ⚠️ CRITICAL: Single-User Architecture

**This bridge serves EXACTLY ONE MT account per instance.**

- One bridge instance = One MT terminal = One user
- Do NOT share the bridge URL/token between multiple users  
- Each user MUST have their own dedicated bridge deployment

### Multi-User Deployment

To support multiple users:
1. Deploy one Windows VPS per user OR
2. Run multiple MT terminals on one VPS with:
   - Unique HTTP ports (8443, 8444, 8445, ...)
   - Unique ZMQ ports (5555, 5556, 5557, ...)
   - Separate `config.json` files
   - Separate Windows Services (NSSM)

## Quick Start

### Prerequisites
- Windows 10+ or Windows Server 2016+
- Python 3.8+
- MetaTrader 4 or MetaTrader 5 terminal

### Installation

1. **Run Installation Script** (as Administrator):
   ```powershell
   cd scripts
   .\install.ps1
   ```

2. **Copy EA to MT Terminal**:
   - MT5: Copy `ea/mt5/MTBridge.mq5` to `MQL5/Experts/`
   - MT4: Copy `ea/mt4/MTBridge.mq4` to `MQL4/Experts/`

3. **Compile EA** in MetaEditor (F7)

4. **Attach EA to Chart** in terminal

5. **Start Server**:
   ```powershell
   cd scripts
   .\start_bridge.bat
   ```

6. **Test**: Visit `http://localhost:8443/docs`

## Architecture

```
Bot Backend (Linux) → HTTPS → Python Server (Windows) → ZeroMQ → EA (MT4/MT5) → Broker
```

**Components**:
- **Expert Advisor**: MQL4/MQL5 EA running in MT terminal
- **Python Server**: FastAPI REST API server
- **ZeroMQ**: Inter-process communication

## API Endpoints

All endpoints require Bearer token authentication.

- `GET /v1/health` - Health check, platform info
- `GET /v1/instruments` - Available symbols
- `POST /v1/prices` - Current prices
- `POST /v1/klines` - Historical candles
- `POST /v1/order` - Place order
- `POST /v1/order/cancel` - Cancel order
- `GET /v1/order/{id}` - Order status
- `GET /v1/positions` - Open positions
- `GET /v1/balance` - Account balance

## Configuration

### Server (`server/config.json`):
```json
{
  "zmq_host": "localhost",
  "zmq_port": "5555",
  "api_tokens": ["your-token-here"],
  "request_timeout": 5000
}
```

### EA Settings:
- **ZMQ_PORT**: `5555` (must match server config)
- **API_SECRET**: Optional additional validation

## Security

- **Token Auth**: Generate secure tokens with `scripts/generate_token.py`
- **SSL/TLS**: For production, configure SSL certificates
- **Firewall**: Only expose port 8443 to trusted IPs

## Documentation

- [Installation Guide](docs/INSTALL.md) - Detailed setup instructions
- [Deployment Guide](docs/DEPLOYMENT.md) - VPS deployment and production setup
- [API Reference](docs/API_CONTRACT.md) - Complete API specification

## File Structure

```
mt-bridge/
├── ea/
│   ├── mt4/
│   │   ├── MTBridge.mq4          # MT4 Expert Advisor
│   │   └── Include/              # ZeroMQ libraries
│   └── mt5/
│       ├── MTBridge.mq5          # MT5 Expert Advisor
│       └── Include/              # ZeroMQ libraries
├── server/
│   ├── main.py                   # FastAPI server
│   ├── config.json               # Configuration
│   └── requirements.txt          # Python dependencies
├── scripts/
│   ├── install.ps1               # Installation script
│   ├── start_bridge.bat          # Start server
│   └── generate_token.py         # Token generator
└── docs/
    ├── INSTALL.md                # Installation guide
    └── DEPLOYMENT.md             # Deployment guide
```

## Troubleshooting

### EA Won't Start
- Enable DLL imports in MT terminal options
- Verify `libzmq.dll` is in `Libraries/` folder
- Check EA logs in Journal tab

### Server Won't Connect
- Check `zmq_port` matches in both EA and config
- Verify EA is attached and running
- Check firewall isn't blocking localhost

### API Returns 401
- Verify token in request matches `config.json`
- Check Authorization header format: `Bearer <token>`

## Support

For issues:
1. Check [INSTALL.md](docs/INSTALL.md) troubleshooting section
2. Review EA logs in MT terminal
3. Check Python server logs in console

## License

Part of the CosmicForge trading bot platform.
