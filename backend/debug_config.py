try:
    from app.core.config import settings
    print("Settings loaded successfully")
except Exception as e:
    print(f"Error loading settings: {e}")
    if hasattr(e, 'errors'):
        import json
        print(json.dumps(e.errors(), indent=2))
