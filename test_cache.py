import sys

# mock streamlit
sys.modules['streamlit'] = type('MockStreamlit', (), {
    'secrets': type('MockSecrets', (), {'get': lambda *args, **kwargs: ''})(),
    'cache_resource': lambda f: f
})

import keiba_bot

print("Testing caching engine")
for obj in keiba_bot.run_races_iter(2026, "03", "02", "11", [5], mode="dify"):
    print(obj.get("type"), str(obj.get("data", ""))[:100])
