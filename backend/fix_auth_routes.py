import re

# Read the file
with open(r'app\api\auth.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace /auth/ with / in all route decorators
content_fixed = re.sub(r'@router\.(get|post|put|delete)\("/auth/', r'@router.\1("/', content)

# Write back
with open(r'app\api\auth.py', 'w', encoding='utf-8') as f:
    f.write(content_fixed)

print("Fixed auth routes - stripped /auth/ prefixes")
