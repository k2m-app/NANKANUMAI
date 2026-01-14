import time
import re
import requests
import streamlit as st

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ==================================================
# 設定・セッション管理
# ==================================================
KEIBA_ID = st.secrets.get("KEIBA_ID", "")
KEIBA_PASS = st.secrets.get("KEIBA_PASS", "")
DIFY_API_KEY = st.secrets.get("DIFY_API_KEY", "")
DIFY_BASE_URL = st.secrets.get("DIFY_BASE_URL", "https://api.dify.ai")

def _build_session() -> requests.Session:
    sess = requests.Session()
    # リトライ設定: 接続エラーや50xエラー時に3回まで再試行
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(500, 502, 503, 504))
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    return sess

@st.cache_resource
def get_http_session() -> requests.Session:
    return _build_session()

def build_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,1024")
    return webdriver.Chrome(options=options)

def login_keibabook(driver: webdriver.Chrome, wait: WebDriverWait):
    driver.get("https://s.keibabook.co.jp/login/login")
    if "logout" in driver.current_url: return # 既にログイン済み
    try:
        wait.until(EC.visibility_of_element_located((By.NAME, "login_id"))).send_keys(KEIBA_ID)
        driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
        driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
        time.sleep(1)
    except:
        pass 

# ==================================================
# スクレイピング関数群
# ==================================================
def fetch_race_ids(driver, year, month, day, place_code):
    date_str = f"{year}{month}{day}"
    url = f"https://s.keibabook.co.jp/chihou/nittei/{date_str}10"
    driver.get(url)
    try: WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "a")))
    except: pass
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    race_ids = []
    seen = set()
    for a in soup.find_all("a", href=True):
        m = re.search(r"(\d{16})", a["href"])
        if m:
            rid = m.group(1)
            # 指定した競馬場コード(6,7桁目)と一致するものだけ
            if rid[6:8] == place_code and rid not in seen:
                race_ids.append(rid)
                seen.add(rid)
    return sorted(race_ids)

def get_keibago_data(year, month, day, race_no, baba_code):
    date_str = f"{year}/{month}/{day}"
    url = f"https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTableSmall?k_raceDate={date_str}&k_raceNo={race_no}&k_babaCode={baba_code}"
    
    sess = get_http_session()
    try:
        r = sess.get(url, timeout=15) # タイムアウト設定
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")
        
        horses = {}
        tbl = soup.select_one("table.bs[border='1']")
        if not tbl: return {}, ""
        
        for tr in tbl.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 4: continue
            try:
                txts = [td.get_text(strip=True) for td in tds]
                umaban = next((t for t in txts if t.isdigit()), None)
                if not umaban: continue
                
                row_text = tr.get_text(" ", strip=True)
                is_change = "替" in row_text or "☆" in row_text or "▲" in row_text 
                
                bamei_tag = tr.select_one(".bamei")
                horse_name = bamei_tag.get_text(strip=True) if bamei_tag else "不明"

                horses[umaban] = {"name": horse_name, "is_change": is_change}
            except: continue
        return horses, ""
    except Exception:
        return {}, ""

# ==================================================
# ローカル対戦表生成ロジック
# ==================================================
def _get_kai_nichi(target_month, target_day, target_place):
    url = "https://www.nankankeiba.com/bangumi_menu/bangumi.do"
    sess = get_http_session()
    try:
        res = sess.get(url, timeout=10)
        res.encoding = 'cp932'
        soup = BeautifulSoup(res.text, 'html.parser')
        
        for tr in soup.find_all('tr'):
            text = tr.get_text()
            if target_place in text and "競馬" in text:
                m = re.search(r'第(\d+)回.*?(\d+)月\s*(.*?)日', text)
                if m:
                    mon = int(m.group(2))
                    if mon != int(target_month): continue
                    days = [int(d) for d in re.findall(r'\d+', m.group(3))]
                    if int(target_day) in days:
                        return int(m.group(1)), days.index(int(target_day)) + 1, None
        return None, None, "開催情報特定不可"
    except Exception as e:
        return None, None, str(e)

def _parse_grades(text):
    grades = {}
    if not text: return grades
    for line in text.split('\n'):
        if '|' in line and ('①' in line or '②' in line or '1' in line):
            parts = [p.strip() for p in line.split('|') if p.strip()]
            if len(parts) >= 2:
                grade_cand = parts[-1]
                if grade_cand in ['S','A','B','C','D','E']:
                    name_part = parts[0]
                    name_clean = re.sub(r'[①-⑳0-9\(\)（）]', '', name_part).split('(')[0]
                    grades[name_clean.strip()] = grade_cand
    return grades

def _fetch_history_data(year, month, day, place_name, race_num, grades):
    kai, nichi, err = _get_kai_nichi(month, day, place_name)
    if err: kai, nichi = 15, 1

    p_code = {'浦和': '18', '船橋': '19', '大井': '20', '川崎': '21'}.get(place_name, '20')
    race_id = f"{year}{int(month):02}{int(day):02}{p_code}{int(kai):02}{int(nichi):02}{int(race_num):02}"
    url = f"https://www.nankankeiba.com/taisen/{race_id}.do"
    
    sess = get_http_session()

    try:
        res = sess.get(url, timeout=15)
        res.encoding = 'cp932'
        soup = BeautifulSoup(res.text, 'html.parser')
        
        tbl = soup.find('table', class_='nk23_c-table08__table')
        if not tbl: return f"\n(対戦データなし: {url})"

        tbody = tbl.find('tbody')
        thead = tbl.find('thead')
        if not (thead and tbody): return ""

        races = []
        for th in thead.find_all('th')[1:]:
            link = th.find('a')
            if link:
                title = th.get_text(strip=True).replace('競走成績', '').replace('対戦表', '')
                r_url = "https://www.nankankeiba.com" + link.get('href', '')
                races.append({"title": title, "url": r_url, "results": []})

        if not races: return "\n(初対戦)"

        for tr in tbody.find_all('tr'):
            cells = tr.find_all(['td', 'th'])
            if not cells: continue
            
            uma_tag = cells[0].find('a')
            if not uma_tag: continue
            h_name = uma_tag.get_text(strip=True)
            h_grade = _parse_grades_fuzzy(h_name, grades)

            for i, cell in enumerate(cells[1:]):
                if i >= len(races): break
                rank_text = cell.get_text(strip=True).split('｜')[0].strip()
                if rank_text and (rank_text.isdigit() or rank_text in ['除外','取消']):
                    sort_k = int(rank_text) if rank_text.isdigit() else 999
                    races[i]["results"].append({
                        "rank": rank_text,
                        "name": h_name,
                        "grade": h_grade,
                        "sort": sort_k
                    })

        output = ["###注目対戦"]
        has_content = False
        
        for r in races:
            if not r["results"]: continue
            has_content = True
            r["results"].sort(key=lambda x: x["sort"])
            
            line_items = []
            for res in r["results"]:
                g_str = f"({res['grade']})" if res['grade'] else ""
                rank_disp = f"{res['rank']}着" if res['rank'].isdigit() else res['rank']
                line_items.append(f"{rank_disp} {res['name']}{g_str}")
            
            output.append(f"**・ {r['title']}**")
            output.append(" / ".join(line_items))
            output.append(f"[詳細]({r['url']})\n")

        return "\n".join(output) if has_content else "\n(該当データなし)"

    except Exception as e:
        return f"\n(対戦表エラー: {e})"

def _parse_grades_fuzzy(horse_name, grades):
    if horse_name in grades: return grades[horse_name]
    for k, v in grades.items():
        if k in horse_name or horse_name in k:
            return v
    return ""

# ==================================================
# Dify連携 & メイン実行
# ==================================================
def run_dify_simple(prompt):
    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "inputs": {"text": prompt},
        "response_mode": "blocking",
        "user": "streamlit-user"
    }
    sess = get_http_session() # リトライ付きセッションを使用
    try:
        # ★ここを修正: timeoutを300秒(5分)に設定
        res = sess.post(f"{DIFY_BASE_URL}/v1/workflows/run", headers=headers, json=payload, timeout=300)
        
        if res.status_code == 200:
            return res.json().get('data', {}).get('outputs', {}).get('text', "Error: No text output")
        return f"Dify Error: {res.status_code} {res.text}"
    except requests.exceptions.Timeout:
        return "⚠️ Dify Timeout: 処理に時間がかかりすぎたため中断されました(300秒超過)。"
    except Exception as e:
        return f"Conn Error: {e}"

def run_races_iter(year, month, day, place_code, target_races):
    place_map = {"10": "大井", "11": "川崎", "12": "船橋", "13": "浦和"}
    place_name = place_map.get(place_code, "地方")
    baba_code = {"10":"20", "11":"21", "12":"19", "13":"18"}.get(place_code, "20")

    driver = build_driver()
    wait = WebDriverWait(driver, 10)

    try:
        login_keibabook(driver, wait)
        race_ids = fetch_race_ids(driver, year, month, day, place_code)
        
        if not race_ids:
            yield 0, "レースID取得失敗。開催日を確認してください。"
            return

        for i, race_id in enumerate(race_ids):
            race_num = i + 1
            if target_races and race_num not in target_races: continue

            # データ取得
            driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{race_id}")
            html_danwa = driver.page_source
            driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{race_id}")
            html_cyokyo = driver.page_source
            
            kg_horses, _ = get_keibago_data(year, month, day, race_num, baba_code)

            # テキスト整形
            soup_d = BeautifulSoup(html_danwa, "html.parser")
            soup_c = BeautifulSoup(html_cyokyo, "html.parser")
            
            # 談話データの抽出
            danwa_map = {}
            tbl_d = soup_d.find("table", class_="danwa")
            if tbl_d and tbl_d.tbody:
                curr_u = None
                for row in tbl_d.tbody.find_all("tr"):
                    ud = row.find("td", class_="umaban")
                    if ud:
                        curr_u = ud.get_text(strip=True)
                        continue
                    td = row.find("td", class_="danwa")
                    if td and curr_u:
                        danwa_map[curr_u] = td.get_text(strip=True)
                        curr_u = None

            # 調教データの抽出
            cyokyo_map = {}
            for t in soup_c.find_all("table", class_="cyokyo"):
                tb = t.find("tbody")
                if not tb: continue
                trs = tb.find_all("tr", recursive=False)
                if not trs: continue
                u_td = trs[0].find("td", class_="umaban")
                if not u_td: continue
                ub = u_td.get_text(strip=True)
                # 簡易的に結合
                cyokyo_map[ub] = trs[0].get_text(" ", strip=True) + (trs[1].get_text(" ", strip=True) if len(trs)>1 else "")

            # レース名
            rt = soup_d.find("div", class_="racetitle")
            race_header = rt.get_text(" ", strip=True) if rt else f"{place_name} {race_num}R"

            # プロンプト作成
            lines = [f"{race_header} (日時:{year}/{month}/{day})"]
            all_uma = sorted(set(danwa_map.keys()) | set(cyokyo_map.keys()) | set(kg_horses.keys()), key=lambda x: int(x) if x.isdigit() else 999)

            for u in all_uma:
                kg = kg_horses.get(u, {})
                d_txt = danwa_map.get(u, "なし")
                c_txt = cyokyo_map.get(u, "なし")
                base_info = f"【馬番{u}】{kg.get('name','')} (変:{'有' if kg.get('is_change') else '無'})"
                lines.append(f"{base_info}\n談話:{d_txt}\n調教:{c_txt}")
            
            final_prompt = "\n".join(lines)

            # Dify実行
            dify_res = run_dify_simple(final_prompt)

            # ローカル対戦表生成
            grades = _parse_grades(dify_res)
            history_text = _fetch_history_data(year, month, day, place_name, race_num, grades)

            header_info = f"📅 自動判定: {year}年{month}月{day}日 {place_name} {race_num}R"
            full_output = f"{header_info}\n\n{dify_res}\n\n{history_text}"

            yield race_num, full_output
            time.sleep(2)

    except Exception as e:
        yield 0, f"Critical Error: {e}"
    finally:
        driver.quit()
