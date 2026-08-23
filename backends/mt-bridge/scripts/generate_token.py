"""
Generate secure API token for MT Bridge
"""
import secrets
import json
import os

def generate_token(length=32):
    """Generate a secure random token"""
    return secrets.token_urlsafe(length)

def save_token_to_config(token):
    """Save token to config.json"""
    config_path = os.path.join(os.path.dirname(__file__), "..", "server", "config.json")
    
    # Load existing config
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
    else:
        config = {
            "zmq_host": "localhost",
            "zmq_port": "5555",
            "api_tokens": [],
            "request_timeout": 5000
        }
    
    # Add token
    if token not in config["api_tokens"]:
        config["api_tokens"].append(token)
    
    # Save config
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"Token saved to: {config_path}")

if __name__ == "__main__":
    print("=== MT Bridge API Token Generator ===\n")
    
    token = generate_token()
    
    print(f"Generated Token: {token}\n")
    print("IMPORTANT: Save this token securely!")
    print("You will need it to configure the bot backend.\n")
    
    save_token_to_config(token)
    
    print("\nToken has been added to config.json")
    print("You can generate multiple tokens by running this script again.")
