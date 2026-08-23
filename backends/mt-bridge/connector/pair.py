"""
CosmicForge MT Bridge Connector
Simple Windows connector for MetaTrader 4/5 integration.
"""
import sys
import os
import json
import secrets
import subprocess
import time
import re
import argparse
import requests
from pathlib import Path

# Constants
PLATFORM_API_URL = os.getenv("PLATFORM_API_URL", "https://api.cosmicforge.app")
BRIDGE_SERVER_DIR = Path(__file__).parent.parent / "server"
BRIDGE_CONFIG_FILE = BRIDGE_SERVER_DIR / "config.json"
BRIDGE_MAIN_PY = BRIDGE_SERVER_DIR / "main.py"

def print_banner():
    print("=" * 60)
    print("  🚀 COSMICFORGE MT CONNECTOR")
    print("  Professional Trading Platform Integration")
    print("=" * 60)
    print()

def get_or_create_token():
    """Ensure a secure token exists in bridge config"""
    config = {}
    if BRIDGE_CONFIG_FILE.exists():
        try:
            with open(BRIDGE_CONFIG_FILE, 'r') as f:
                config = json.load(f)
        except json.JSONDecodeError:
            pass
    
    tokens = config.get("api_tokens", [])
    if not tokens:
        print("[1/4] Generating secure access token...")
        token = secrets.token_hex(32)
        config["api_tokens"] = [token]
        if "zmq_host" not in config: config["zmq_host"] = "localhost"
        if "zmq_port" not in config: config["zmq_port"] = "5555"
        
        BRIDGE_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(BRIDGE_CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        print("    ✓ Token created")
        return token
    return tokens[0]

def start_bridge_server(port):
    """Start the MT bridge server"""
    print(f"[2/4] Starting MT Bridge Server...")
    
    env = os.environ.copy()
    env["HTTP_PORT"] = str(port)
    env["REQUIRE_SSL"] = "false"  # Tunnel handles SSL
    
    cmd = [sys.executable, str(BRIDGE_MAIN_PY)]
    
    try:
        proc = subprocess.Popen(
            cmd, 
            cwd=str(BRIDGE_SERVER_DIR),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print("    ✓ Bridge server started")
        return proc
    except Exception as e:
        print(f"    ❌ Failed: {e}")
        return None

def wait_for_bridge(url, token, timeout=30):
    """Wait for bridge to be ready"""
    print("    Waiting for bridge to initialize...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            r = requests.get(f"{url}/v1/health", headers={"Authorization": f"Bearer {token}"}, timeout=2, verify=False)
            if r.status_code == 200:
                print("    ✓ Bridge ready")
                return r.json()
        except Exception:
            pass
        time.sleep(1)
    print("    ❌ Timeout")
    return None

def start_cloudflared_tunnel(port):
    """Start cloudflared tunnel"""
    print("[3/4] Creating secure tunnel...")
    
    # Check cloudflared availability
    try:
        subprocess.run(["cloudflared", "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("    ❌ cloudflared not found")
        print("    Download from: https://github.com/cloudflare/cloudflared/releases")
        return None, None

    cmd = ["cloudflared", "tunnel", "--url", f"http://localhost:{port}"]
    
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Extract URL from stderr
    url_pattern = re.compile(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com")
    
    for _ in range(50):  # Try 50 lines
        line = proc.stderr.readline()
        if not line:
            break
        match = url_pattern.search(line)
        if match:
            url = match.group(0)
            print(f"    ✓ Tunnel URL: {url}")
            return proc, url
    
    print("    ❌ Could not extract tunnel URL")
    proc.terminate()
    return None, None

def claim_pairing_code(session_id):
    """Exchange session ID for pairing code via API"""
    print("[4/4] Connecting to platform...")
    
    # Generate a random device secret for this session
    device_secret = secrets.token_hex(16)
    
    try:
        r = requests.post(
            f"{PLATFORM_API_URL}/api/v1/mt/connector/claim",
            json={"session_id": session_id, "device_secret": device_secret},
            timeout=10
        )
        
        if r.status_code == 200:
            data = r.json()
            print(f"    ✓ Authentication successful")
            return data["pairing_code"], data["broker_id"], data["environment"]
        else:
            error = r.json().get("detail", r.text)
            print(f"    ❌ Failed: {error}")
            return None, None, None
    except Exception as e:
        print(f"   ❌ Connection error: {e}")
        return None, None, None

def complete_pairing(pairing_code, bridge_url, bridge_token, tls_mode, platform):
    """Complete the pairing with platform"""
    # This would normally get health info, but we'll do it inline
    try:
        health = requests.get(
            f"http://localhost:8443/v1/health",
            headers={"Authorization": f"Bearer {bridge_token}"},
            timeout=5,
            verify=False
        ).json()
        
        account_info = health.get("account", {})
        
        # New API expects AccountInfo model in "account" field
        # We need to map health info to AccountInfo
        # AccountInfo: login, server, currency, type, platform
        
        account_payload = {
            "login": str(account_info.get("login", "")),
            "server": account_info.get("server", ""),
            "currency": account_info.get("currency", "USD"),
            "type": account_info.get("type", "Demo"),
            "platform": health.get("platform", platform)
        }
        
        payload = {
            "pairing_code": pairing_code,
            "bridge_url": bridge_url,
            "bridge_token": bridge_token,
            "tls_mode": tls_mode,
            "account": account_payload
        }
        
        # The complete_pairing endpoint returns {ok, user_visible_message} (maybe?)
        # Let's check mt_pairing.py again.
        # It returns CompletePairingResponse(ok, user_visible_message, account_id)
        # But wait, we modified the backend logic in service, but NOT the endpoint response model.
        # The endpoint calls service.complete_pairing -> returns session_id (string).
        # The endpoint wraps it in CompletePairingResponse.
        # So we should be good.
        
        r = requests.post(f"{PLATFORM_API_URL}/api/v1/mt/pair", json=payload, timeout=15)
        
        if r.status_code == 200:
            result = r.json()
            if result.get("ok"):
                return True, result.get("user_visible_message", "Connected successfully!")
            else:
                return False, result.get("user_visible_message", "Pairing failed")
        else:
            return False, r.json().get("detail", r.text)
    except Exception as e:
        return False, str(e)

def main():
    print_banner()
    
    parser = argparse.ArgumentParser(description="CosmicForge MT Connector")
    parser.add_argument("--session-id", help="Session ID from web platform")
    parser.add_argument("--port", type=int, default=8443, help="Local bridge port")
    parser.add_argument("--skip-tunnel", action="store_true", help="Skip cloudflared tunnel")
    args = parser.parse_args()
    
    # Get session ID
    session_id = args.session_id
    if not session_id:
        print("Paste your Session ID:")
        print("(From CosmicForge platform → Connect MetaTrader)")
        session_id = input("\nSession ID: ").strip()
    
    if not session_id:
        print("\n❌ Session ID required.")
        return 1
    
    # Generate bridge token
    bridge_token = get_or_create_token()
    
    # Start bridge server
    bridge_proc = start_bridge_server(args.port)
    if not bridge_proc:
        return 1
    
    try:
        # Wait for bridge
        bridge_url_local = f"http://localhost:{args.port}"
        health_info = wait_for_bridge(bridge_url_local, bridge_token)
        
        if not health_info:
            print("\n❌ Bridge failed to start. Check MetaTrader is running with EA attached.")
            return 1
        
        # Start tunnel (or skip)
        tunnel_proc = None
        bridge_final_url = bridge_url_local
        tls_mode = "insecure"
        
        if not args.skip_tunnel:
            tunnel_proc, tunnel_url = start_cloudflared_tunnel(args.port)
            if tunnel_url:
                bridge_final_url = tunnel_url
                tls_mode = "strict"
        
        # Claim connector token → get pairing code
        pairing_code, platform, environment = claim_pairing_code(session_id)
        if not pairing_code:
            print("\n❌ Failed to authenticate with platform.")
            return 1
        
        # Complete pairing
        success, message = complete_pairing(pairing_code, bridge_final_url, bridge_token, tls_mode, platform)

        
        if success:
            print(f"\n✅ {message}")
            print("=" * 60)
            print("  CONNECTION ESTABLISHED")
            print("  Keep this window open for trading")
            print("  Press Ctrl+C to stop")
            print("=" * 60)
            
            # Keep alive
            while True:
                if bridge_proc.poll() is not None:
                    print("\n❌ Bridge stopped unexpectedly")
                    break
                if tunnel_proc and tunnel_proc.poll() is not None:
                    print("\n❌ Tunnel stopped unexpectedly")
                    break
                time.sleep(5)
        else:
            print(f"\n❌ Pairing failed: {message}")
            return 1
            
    except KeyboardInterrupt:
        print("\n\nStopping connector...")
    finally:
        if bridge_proc: bridge_proc.terminate()
        if tunnel_proc: tunnel_proc.terminate()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
