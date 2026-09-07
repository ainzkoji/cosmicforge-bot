"""
Scan frontend API client files for endpoint calls.
This script extracts all API endpoint paths from the frontend client files.
"""
import re
from pathlib import Path
import json

def extract_api_calls(file_path):
    """Extract API calls from a TypeScript file."""
    calls = []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Pattern to match API calls like: apiClient.get('/api/...'), apiClient.post('/api/...'), etc.
    patterns = [
        r"(get|post|put|delete|patch)\s*\(\s*['\"]([^'\"]+)['\"]",  # apiClient.get('/path')
        r"await\s+\w+\.(get|post|put|delete|patch)\s*\(\s*['\"]([^'\"]+)['\"]",  # await client.get('/path')
        r"return\s+\w+\.(get|post|put|delete|patch)\s*\(\s*['\"]([^'\"]+)['\"]",  # return client.get('/path')
        r"(GET|POST|PUT|DELETE|PATCH)['\"],\s*url:\s*['\"]([^'\"]+)['\"]",  # method: 'GET', url: '/path'
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, content, re.IGNORECASE)
        for match in matches:
            if len(match.groups()) == 2:
                method = match.group(1).upper()
                path = match.group(2)
                # Only include paths that start with / (relative API paths)
                if path.startswith('/'):
                    calls.append({
                        'method': method,
                        'path': path,
                        'file': file_path.name
                    })
    
    return calls

def main():
    frontends_dir = Path(__file__).parent.parent / "frontends"
    
    user_frontend_api = frontends_dir / "user-frontend" / "src" / "api"
    admin_frontend_api = frontends_dir / "admin-frontend" / "src" / "api"
    
    all_calls = {
        'user-frontend': [],
        'admin-frontend': []
    }
    
    # Scan user-frontend
    if user_frontend_api.exists():
        for ts_file in user_frontend_api.glob("*.ts"):
            calls = extract_api_calls(ts_file)
            for call in calls:
                all_calls['user-frontend'].append(call)
        for tsx_file in user_frontend_api.glob("*.tsx"):
            calls = extract_api_calls(tsx_file)
            for call in calls:
                all_calls['user-frontend'].append(call)
    
    # Scan admin-frontend
    if admin_frontend_api.exists():
        for ts_file in admin_frontend_api.glob("*.ts"):
            calls = extract_api_calls(ts_file)
            for call in calls:
                all_calls['admin-frontend'].append(call)
        for tsx_file in admin_frontend_api.glob("*.tsx"):
            calls = extract_api_calls(tsx_file)
            for call in calls:
                all_calls['admin-frontend'].append(call)
    
    # Save to JSON
    output_file = Path(__file__).parent / "frontend_api_calls.json"
    with open(output_file, 'w') as f:
        json.dump(all_calls, f, indent=2)
    
    # Print summary
    print("=" * 80)
    print("FRONTEND API CALLS INVENTORY")
    print("=" * 80)
    print()
    
    for frontend, calls in all_calls.items():
        print(f"\n{frontend.upper()}:")
        print("-" * 80)
        
        # Remove duplicates and sort
        unique_calls = {}
        for call in calls:
            key = f"{call['method']} {call['path']}"
            if key not in unique_calls:
                unique_calls[key] = call
        
        for key in sorted(unique_calls.keys()):
            call = unique_calls[key]
            print(f"  {call['method']:6} {call['path']:60} ({call['file']})")
        
        print(f"\n  Total unique calls: {len(unique_calls)}")
    
    print(f"\n\nOutput saved to: {output_file}")

if __name__ == "__main__":
    main()
