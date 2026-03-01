import sys

# mock streamlit
sys.modules['streamlit'] = type('MockStreamlit', (), {
    'secrets': type('MockSecrets', (), {'get': lambda *args, **kwargs: ''})(),
    'cache_resource': lambda f: f
})

import requests
import keiba_bot

print("=== HTTP GET test ===")
url = 'https://www.nankankeiba.com/uma_shosai/2026030221130101.do'
sess = keiba_bot.get_http_session()
res = sess.get(url, timeout=15)
res.encoding = "cp932"
print(f"Status: {res.status_code}, Length: {len(res.text)}")

print("\n=== parse_nankankeiba_detail test ===")
nk_data = keiba_bot.parse_nankankeiba_detail(res.text, '川崎', {})
print(f"Horses count: {len(nk_data['horses'])}")

if nk_data['horses']:
    for u in sorted(nk_data['horses'].keys(), key=int)[:3]:
        h = nk_data['horses'][u]
        print(f"  #{u} {h.get('name', '?')} hist={len(h.get('hist', []))}")
        for hi in h.get('hist', [])[:2]:
            if isinstance(hi, dict):
                print(f"    -> url={hi.get('url','N/A')[:50]}  place={hi.get('place','?')} dist={hi.get('dist','?')}")
            else:
                print(f"    -> (str) {str(hi)[:60]}")

    print("\n=== predict_pace_python test ===")
    pace = keiba_bot.predict_pace_python(nk_data['horses'], {}, '1400')
    print(pace)
else:
    print("ERROR: No horses parsed!")
