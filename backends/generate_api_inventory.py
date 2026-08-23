"""
Generate comprehensive API inventory documentation from collected route data.
Creates:
- ROUTES_user.md
- ROUTES_bot.md
- FRONTEND_CALLS.md
- BREAKING_RISK_LIST.md
"""
import json
from pathlib import Path
from collections import defaultdict

def load_data():
    """Load route and frontend call data."""
    base_dir = Path(__file__).parent
    
    with open(base_dir / "user_backend_routes.json", 'r') as f:
        user_routes = json.load(f)
    
    with open(base_dir / "bot_backend_routes.json", 'r') as f:
        bot_routes = json.load(f)
    
    with open(base_dir / "frontend_api_calls.json", 'r') as f:
        frontend_calls = json.load(f)
    
    return user_routes, bot_routes, frontend_calls

def generate_routes_md(routes, service_name, output_file):
    """Generate ROUTES_{service}.md documentation."""
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"# {service_name} API Routes Inventory\n\n")
        f.write(f"**Total Routes:** {len(routes)}\n\n")
        f.write("## Route Summary\n\n")
        f.write("| Method | Path | Name | Tag | Module |\n")
        f.write("|--------|------|------|-----|--------|\n")
        
        for route in routes:
            methods = ', '.join(sorted(route['methods']))
            path = route['path']
            name = route['name']
            tags = ', '.join(route['tags']) if route['tags'] else 'no-tag'
            module = route['module']
            
            f.write(f"| `{methods}` | `{path}` | {name} | {tags} | {module} |\n")
        
        # Group by prefix
        f.write("\n## Routes by Prefix\n\n")
        prefix_groups = defaultdict(list)
        
        for route in routes:
            path = route['path']
            if path.startswith('/api/v1'):
                prefix = '/api/v1'
            elif path.startswith('/api/'):
                parts = path.split('/')
                if len(parts) > 2:
                    prefix = f"/{parts[1]}/{parts[2]}"
                else:
                    prefix = f"/{parts[1]}"
            elif path.startswith('/'):
                parts = path.split('/')
                if len(parts) > 1 and parts[1]:
                    prefix = f"/{parts[1]}"
                else:
                    prefix = '/'
            else:
                prefix = 'other'
            
            prefix_groups[prefix].append(route)
        
        for prefix in sorted(prefix_groups.keys()):
            routes_in_prefix = prefix_groups[prefix]
            f.write(f"\n###  `{prefix}` ({len(routes_in_prefix)} routes)\n\n")
           
            for route in routes_in_prefix:
                methods = ', '.join(sorted(route['methods']))
                f.write(f"- **{methods}** `{route['path']}` — {route['name']}\n")

def generate_frontend_calls_md(user_routes, bot_routes, frontend_calls, output_file):
    """Generate FRONTEND_CALLS.md mapping."""
    
    # Create lookup dictionaries
    user_lookup = {r['path']: r for r in user_routes}
    bot_lookup = {r['path']: r for r in bot_routes}
    
    # Get unique frontend calls
    user_fe_calls = {}
    for call in frontend_calls['user-frontend']:
        key = f"{call['method']} {call['path']}"
        user_fe_calls[key] = call
    
    admin_fe_calls = {}
    for call in frontend_calls['admin-frontend']:
        key = f"{call['method']} {call['path']}"
        admin_fe_calls[key] = call
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# Frontend API Calls Mapping\n\n")
        f.write("## Summary\n\n")
        f.write(f"- **User Frontend**: {len(user_fe_calls)} unique endpoints called\n")
        f.write(f"- **Admin Frontend**: {len(admin_fe_calls)} unique endpoints called\n\n")
        
        f.write("## User Frontend Calls\n\n")
        f.write("| Method | Path | Found In | Status |\n")
        f.write("|--------|------|----------|--------|\n")
        
        for key in sorted(user_fe_calls.keys()):
            call = user_fe_calls[key]
            path = call['path']
            
            # Check if exists in backends
            status = "[OK] user-backend" if path in user_lookup else ""
            if not status:
                status = "[OK] bot-backend" if path in bot_lookup else ""
            if not status:
                status = "[MISSING] NOT FOUND"
            
            f.write(f"| `{call['method']}` | `{path}` | {call['file']} | {status} |\n")
        
        f.write("\n## Admin Frontend Calls\n\n")
        f.write("| Method | Path | Found In | Status |\n")
        f.write("|--------|------|----------|--------|\n")
        
        for key in sorted(admin_fe_calls.keys()):
            call = admin_fe_calls[key]
            path = call['path']
            
            # Check if exists in backends
            status = "[OK] user-backend" if path in user_lookup else ""
            if not status:
                status = "[OK] bot-backend" if path in bot_lookup else ""
            if not status:
                status = "[MISSING] NOT FOUND"
            
            f.write(f"| `{call['method']}` | `{path}` | {call['file']} | {status} |\n")

def analyze_issues(user_routes, bot_routes, frontend_calls):
    """Analyze and identify API issues."""
    issues = {
        'prefix_mismatches': [],
        'duplicate_endpoints': [],
        'frontend_missing': [],
        'proxy_gaps': []
    }
    
    # Check for prefix mismatches
    for route in user_routes + bot_routes:
        path = route['path']
        # Check if it's an API route without standard prefix
        if '/api/' in path and not path.startswith('/api/'):
            issues['prefix_mismatches'].append({
                'path': path,
                'service': 'user' if route in user_routes else 'bot'
            })
        # Non-standard prefixes
        if path.startswith('/api/') and not (path.startswith('/api/v1') or path.startswith('/api/admin') 
                                              or path.startswith('/api/analytics') or path.startswith('/api/billing')
                                              or path.startswith('/api/brokers') or path.startswith('/api/onboarding')
                                              or path.startswith('/api/strategies') or path.startswith('/api/risk')
                                              or path.startswith('/api/portfolio') or path.startswith('/api/strategy')):
            # This could be interesting
            pass
    
    # Check for duplicate endpoints across backends
    user_paths = {(r['path'], ','.join(sorted(r['methods']))): r for r in user_routes}
    bot_paths = {(r['path'], ','.join(sorted(r['methods']))): r for r in bot_routes}
    
    for key in user_paths:
        if key in bot_paths:
            issues['duplicate_endpoints'].append({
                'path': key[0],
                'methods': key[1],
                'user_module': user_paths[key]['module'],
                'bot_module': bot_paths[key]['module']
            })
    
    # Check frontend calls that don't match backend routes
    all_backend_paths = set(r['path'] for r in user_routes + bot_routes)
    all_frontend_paths = set(call['path'] for call in frontend_calls['user-frontend'] + frontend_calls['admin-frontend'])
    
    for path in all_frontend_paths:
        if path not in all_backend_paths:
            issues['frontend_missing'].append(path)
    
    # Check for endpoints that should be proxied but aren't
    # Look for /api/v1/ endpoints which suggest proxying
    v1_endpoints = [r for r in user_routes if r['path'].startswith('/api/v1')]
    for endpoint in v1_endpoints:
        # Check if it's a proxy (module name contains 'proxy')
        if 'proxy' not in endpoint['module'].lower():
            issues['proxy_gaps'].append({
                'path': endpoint['path'],
                'module': endpoint['module']
            })
    
    return issues

def generate_breaking_risk_list(user_routes, bot_routes, frontend_calls, issues, output_file):
    """Generate top 10 breaking risk list."""
    
    # Collect all frontend-used endpoints
    all_fe_calls = frontend_calls['user-frontend'] + frontend_calls['admin-frontend']
    fe_paths = defaultdict(int)
    for call in all_fe_calls:
        fe_paths[call['path']] += 1
   
    # Create risk list
    risks = []
    
    # High risk: Endpoints called by frontend
    for path, count in fe_paths.items():
        risks.append({
            'path': path,
            'risk_level': 'CRITICAL',
            'reason': f'Called by frontend ({count} times)',
            'recommendation': 'Version this endpoint before making changes'
        })
    
    # Medium risk: Duplicate endpoints
    for dup in issues['duplicate_endpoints']:
        risks.append({
            'path': dup['path'],
            'risk_level': 'HIGH',
            'reason': f'Exists in both backends: {dup["user_module"]} & {dup["bot_module"]}',
            'recommendation': 'Consolidate or clearly document which should be used'
        })
    
    # Prefix mismatches
    for mismatch in issues['prefix_mismatches']:
        risks.append({
            'path': mismatch['path'],
            'risk_level': 'MEDIUM',
            'reason': f'Inconsistent prefix pattern in {mismatch["service"]}-backend',
            'recommendation': 'Standardize API prefix pattern'
        })
    
    # Sort by risk level
    risk_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
    risks.sort(key=lambda x: risk_order[x['risk_level']])
    
    # Take top 10
    top_risks = risks[:10]
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# Breaking Change Risk Assessment\n\n")
        f.write("## Top 10 Endpoints to Protect\n\n")
        f.write("These endpoints are at highest risk of causing breaking changes if modified:\n\n")
        
        for i, risk in enumerate(top_risks, 1):
            f.write(f"### {i}. `{risk['path']}`\n\n")
            f.write(f"- **Risk Level**: {risk['risk_level']}\n")
            f.write(f"- **Reason**: {risk['reason']}\n")
            f.write(f"- **Recommendation**: {risk['recommendation']}\n\n")
        
        f.write("## Issue Summary\n\n")
        f.write(f"- **Duplicate Endpoints**: {len(issues['duplicate_endpoints'])}\n")
        f.write(f"- **Prefix Mismatches**: {len(issues['prefix_mismatches'])}\n")
        f.write(f"- **Frontend Missing**: {len(issues['frontend_missing'])}\n")
        f.write(f"- **Potential Proxy Gaps**: {len(issues['proxy_gaps'])}\n\n")
        
        if issues['duplicate_endpoints']:
            f.write("### Duplicate Endpoints Detail\n\n")
            for dup in issues['duplicate_endpoints']:
                f.write(f"- `{dup['methods']}` `{dup['path']}`: user={dup['user_module']}, bot={dup['bot_module']}\n")
            f.write("\n")
        
        if issues['frontend_missing']:
            f.write("### Frontend Calls Not Found in Backend\n\n")
            for path in issues['frontend_missing']:
                f.write(f"- `{path}`\n")
            f.write("\n")

def main():
    print("Loading route data...")
    user_routes, bot_routes, frontend_calls = load_data()
    
    base_dir = Path(__file__).parent
    
    print(f"Generating ROUTES_user.md...")
    generate_routes_md(user_routes, "User Backend (Port 8000)", base_dir / "ROUTES_user.md")
    
    print(f"Generating ROUTES_bot.md...")
    generate_routes_md(bot_routes, "Bot Backend (Port 9000)", base_dir / "ROUTES_bot.md")
    
    print(f"Generating FRONTEND_CALLS.md...")
    generate_frontend_calls_md(user_routes, bot_routes, frontend_calls, base_dir / "FRONTEND_CALLS.md")
    
    print(f"Analyzing issues...")
    issues = analyze_issues(user_routes, bot_routes, frontend_calls)
    
    print(f"Generating BREAKING_RISK_LIST.md...")
    generate_breaking_risk_list(user_routes, bot_routes, frontend_calls, issues, base_dir / "BREAKING_RISK_LIST.md")
    
    print("\n" + "=" * 80)
    print("API INVENTORY DOCUMENTATION COMPLETE")
    print("=" * 80)
    print(f"\nGenerated files:")
    print(f"  - ROUTES_user.md ({len(user_routes)} routes)")
    print(f"  - ROUTES_bot.md ({len(bot_routes)} routes)")
    print(f"  - FRONTEND_CALLS.md")
    print(f"  - BREAKING_RISK_LIST.md")
    print()

if __name__ == "__main__":
    main()
