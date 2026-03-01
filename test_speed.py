import sys

# mock streamlit
sys.modules['streamlit'] = type('MockStreamlit', (), {
    'secrets': type('MockSecrets', (), {'get': lambda *args, **kwargs: ''})(),
    'cache_resource': lambda f: f
})

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import keiba_bot

options = Options()
options.add_argument('--headless')
driver = webdriver.Chrome(options=options)
print("Loading page...")
driver.get('https://www.nankankeiba.com/uma_shosai/2026030221130101.do')
driver.execute_script("if(typeof changeShosai === 'function'){ changeShosai('s1'); }")

time.sleep(2)
html = driver.page_source
driver.quit()

print("Parsing...")
nk_data = keiba_bot.parse_nankankeiba_detail(html, '川崎', {})
print('Horses count:', len(nk_data['horses']))

top_horse = nk_data['horses'].get('1')
if top_horse:
    print('Horse 1 history dicts:')
    for h in top_horse['hist']:
        print(h)
    
print("\n--- PACE PREDICTION ---")
pace = keiba_bot.predict_pace_python(nk_data['horses'], {}, '1400')
print(pace)
