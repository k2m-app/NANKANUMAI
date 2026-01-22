import time
import re
import os
import csv
import json
import requests
import streamlit as st
import pandas as pd
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==================================================
# 【設定】ファイルパス
# ==================================================
DATA_DIR = "2025data"
JOCKEY_FILE = os.path.join(DATA_DIR, "2025_NARJockey.csv")
TRAINER_FILE = os.path.join(DATA_DIR, "2025_NankanTrainer.csv")
POWER_FILE = os.path.join(DATA_DIR, "2025_騎手パワー.csv")

# ==================================================
# 【設定】アカウント・API
# ==================================================
KEIBA_ID = st.secrets.get("KEIBA_ID", "")
KEIBA_PASS = st.secrets.get("KEIBA_PASS", "")
DIFY_API_KEY = st.secrets.get("DIFY_API_KEY", "")
DIFY_BASE_URL = st.secrets.get("DIFY_BASE_URL", "https://api.dify.ai")

# ==================================================
# 共通関数・データロード (変更なし)
# ==================================================
@st.cache_resource
def get_http_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
    })
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

def login_keibabook_robust(driver):
    try:
        driver.get("https://s.keibabook.co.jp/login/login")
        if "logout" not in driver.current_url:
            WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.NAME, "login_id"))).send_keys(KEIBA_ID)
            driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
            driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
            time.sleep(1)
    except Exception as e:
        print(f"Login Warning: {e}")

def run_dify_prediction(full_text):
    if not DIFY_API_KEY: return "⚠️ DIFY_API_KEY未設定"
    url = f"{(DIFY_BASE_URL or '').strip().rstrip('/')}/v1/workflows/run"
    payload = {
        "inputs": {"text": full_text}, 
        "response_mode": "streaming", 
        "user": "keiba-bot"
    }
    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}", 
        "Content-Type": "application/json"
    }
    sess = get_http_session()
    full_response = ""
    try:
        with sess.post(url, headers=headers, json=payload, stream=True, timeout=120) as res:
            if res.status_code != 200:
                return f"⚠️ Dify Error ({res.status_code}): {res.text[:200]}..."
            for line in res.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    if decoded_line.startswith('data:'):
                        json_str = decoded_line[5:].strip()
                        if not json_str: continue
                        try:
                            data = json.loads(json_str)
                            event = data.get('event')
                            if event == 'workflow_finished':
                                outputs = data.get('data', {}).get('outputs', {})
                                if 'text' in outputs: return outputs['text']
                            elif event == 'text_chunk' or event == 'message':
                                chunk = data.get('data', {}).get('text', '')
                                full_response += chunk
                        except: pass
        return full_response if full_response else "（回答生成エラー）"
    except Exception as e:
        return f"⚠️ API Connection Error: {e}"

@st.cache_resource
def load_resources():
    res = {"jockeys": [], "trainers": [], "power": {}, "power_data": {}}
    if os.path.exists(JOCKEY_FILE):
        try:
            with open(JOCKEY_FILE, "r", encoding="utf-8-sig") as f:
                res["jockeys"] = [l.strip().replace(",","").replace(" ","").replace("　","") for l in f if l.strip()]
        except: pass
    if os.path.exists(TRAINER_FILE):
        try:
            with open(TRAINER_FILE, "r", encoding="utf-8-sig") as f:
                res["trainers"] = [l.strip().replace(",","").replace(" ","").replace("　","") for l in f if l.strip()]
        except: pass
    if os.path.exists(POWER_FILE):
        try:
            df = pd.read_csv(POWER_FILE, encoding="utf-8-sig")
            place_col = df.columns[0]
            for _, row in df.iterrows():
                p = str(row[place_col]).strip()
                j = str(row.get("騎手名", "")).replace(" ","").replace("　","")
                if p and j:
                    power_val = row.get('騎手パワー','-')
                    win = row.get('勝率','-')
                    fuku = row.get('複勝率','-')
                    info = f"P:{power_val}(勝{win} 複{fuku})" 
                    key = (p, j)
                    res["power"][key] = info
                    res["power_data"][key] = {"power": power_val, "win": win, "fuku": fuku}
        except: pass
    return res

def normalize_name(abbrev, full_list):
    if not abbrev: return ""
    clean = re.sub(r"[ 　▲△☆◇★\d\.]+", "", abbrev)
    if not clean: return ""
    if not full_list: return clean
    if clean in full_list: return clean
    candidates = []
    for full in full_list:
        it = iter(full)
        if all(char in it for char in clean):
            diff = len(full) - len(clean)
            candidates.append((diff, full))
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]
    return clean

# ==================================================
# 開催特定 & URL生成 (変更なし)
# ==================================================
def get_nankan_kai_nichi(month, day, place_name):
    url = "https://www.nankankeiba.com/bangumi_menu/bangumi.do"
    sess = get_http_session()
    try:
        res = sess.get(url, timeout=10)
        res.encoding = 'cp932'
        soup = BeautifulSoup(res.text, 'html.parser')
        target_m, target_d = int(month), int(day)
        for tr in soup.find_all('tr'):
            text = tr.get_text(" ", strip=True)
            if place_name not in text: continue
            kai_m = re.search(r'第\s*(\d+)\s*回', text)
            if not kai_m: continue
            kai = int(kai_m.group(1))
            mon_m = re.search(r'(\d+)\s*月', text)
            if not mon_m: continue
            if int(mon_m.group(1)) != target_m: continue
            days_part = text.split("月")[1]
            days_match = re.findall(r'(\d+)', days_part)
            days_list = [int(d) for d in days_match if 1 <= int(d) <= 31]
            if target_d in days_list:
                return kai, days_list.index(target_d) + 1
        return None, None
    except: return None, None

def get_kb_url_id(year, month, day, place_code, nichi, race_num):
    mm, dd = str(month).zfill(2), str(day).zfill(2)
    p, n, r = str(place_code).zfill(2), str(nichi).zfill(2), str(race_num).zfill(2)
    return f"{year}{mm}{p}{n}{r}{mm}{dd}"

# ==================================================
# データ取得ロジック (変更なし)
# ==================================================
def parse_kb_danwa_cyokyo(driver, kb_id):
    d_danwa, d_cyokyo = {}, {}
    try:
        driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{kb_id}")
        if "login" in driver.current_url:
            login_keibabook_robust(driver)
            driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{kb_id}")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for tbl in soup.select("table.danwa"):
            curr = None
            for tr in tbl.select("tbody tr"):
                u = tr.select_one("td.umaban")
                if u: curr = u.get_text(strip=True); continue
                t = tr.select_one("td.danwa")
                if curr and t: 
                    raw_text = t.get_text(" ", strip=True)
                    m = re.search(r'[―-]+(.*)', raw_text)
                    if m: d_danwa[curr] = m.group(1).strip()
                    else: d_danwa[curr] = raw_text 
                    curr = None
        driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{kb_id}")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for tbl in soup.select("table.cyokyo"):
            rows = tbl.select("tbody tr")
            if not rows: continue
            r1 = rows[0]
            u_td = r1.select_one("td.umaban")
            if not u_td: continue
            uma = u_td.get_text(strip=True)
            tp_txt = r1.select_one("td.tanpyo").get_text(strip=True) if r1.select_one("td.tanpyo") else ""
            dt_txt = ""
            if len(rows) > 1:
                dt_raw = rows[1].get_text(" ", strip=True)
                dt_txt = re.sub(r'\s+', ' ', dt_raw)
            d_cyokyo[uma] = f"【短評】{tp_txt} 【詳細】{dt_txt}"
    except: pass
    return d_danwa, d_cyokyo

def parse_nankankeiba_detail(html, place_name, resources):
    soup = BeautifulSoup(html, "html.parser")
    data = {"meta": {}, "horses": {}}
    h3 = soup.find("h3", class_="nk23_c-tab1__title")
    data["meta"]["race_name"] = h3.get_text(strip=True) if h3 else ""
    if data["meta"]["race_name"]:
        parts = re.split(r'[ 　]+', data["meta"]["race_name"])
        data["meta"]["grade"] = parts[-1] if len(parts) > 1 else ""
    cond = soup.select_one("a.nk23_c-tab1__subtitle__text.is-blue")
    data["meta"]["course"] = f"{place_name} {cond.get_text(strip=True)}" if cond else ""
    table = soup.select_one("#shosai_aria table.nk23_c-table22__table")
    if not table: return data
    for row in table.select("tbody tr"):
        try:
            u_tag = row.select_one("td.umaban") or row.select_one("td.is-col02")
            if not u_tag: continue
            umaban = u_tag.get_text(strip=True)
            if not umaban.isdigit(): continue
            horse_name = "不明"
            h_link = row.select_one("td.is-col03 a.is-link") or row.select_one("td.pr-umaName-textRound a.is-link")
            if h_link: horse_name = h_link.get_text(strip=True)
            else:
                td3 = row.select_one("td.is-col03")
                if td3:
                    txt = td3.get_text(" ", strip=True)
                    m_nm = re.search(r'[ァ-ンー]{2,9}', txt)
                    if m_nm: horse_name = m_nm.group(0)
            jg_td = row.select_one("td.cs-g1")
            j_raw, t_raw = "", ""
            if jg_td:
                links = jg_td.select("a")
                if len(links) >= 1: j_raw = links[0].get_text(strip=True)
                if len(links) >= 2: t_raw = links[1].get_text(strip=True)
            j_full = normalize_name(j_raw, resources["jockeys"])
            t_full = normalize_name(t_raw, resources["trainers"])
            power_info = resources["power"].get((place_name, j_full), "P:不明")
            ai2 = row.select_one("td.cs-ai2 .graph_text_div")
            pair_stats = "-"
            if ai2 and "データ" not in ai2.get_text():
                r = ai2.select_one(".is-percent").get_text(strip=True)
                w = ai2.select_one(".is-number").get_text(strip=True)
                t = ai2.select_one(".is-total").get_text(strip=True)
                pair_stats = f"勝{r}({w}/{t})" 
            history = []
            prev_power_info = ""
            for i in range(1, 4):
                z = row.select_one(f"td.cs-z{i}")
                if not z or not z.get_text(strip=True): continue
                d_spans = z.select("p.nk23_u-d-flex span.nk23_u-text10")
                d_txt = ""
                if d_spans:
                    for s in d_spans:
                        if re.search(r"\d+\.\d+\.\d+", s.get_text()): d_txt = s.get_text(strip=True); break
                ymd, place_short = "", ""
                m = re.match(r"([^\d]+)(\d+)\.(\d+)\.(\d+)", d_txt)
                if m:
                    place_short = m.group(1)
                    ymd = f"{m.group(2)}/{int(m.group(3))}/{int(m.group(4))}"
                cond_txt = d_spans[-1].get_text(strip=True) if len(d_spans)>=2 else ""
                dist_m = re.search(r"\d{4}", cond_txt)
                dist = dist_m.group(0) if dist_m else ""
                course_s = f"{place_short}{dist}" if m else cond_txt
                r_a = z.select_one("a.is-link")
                r_ti = r_a.get("title", "") if r_a else ""
                rp = re.split(r'[ 　]+', r_ti)
                r_cl = rp[1] if len(rp)>1 else ""
                p_lines = z.select("p.nk23_u-text10")
                j_prev, pop, agari = "", "", ""
                rank = z.select_one(".nk23_u-text19").get_text(strip=True).replace("着","") if z.select_one(".nk23_u-text19") else ""
                pos_p = z.select_one("p.position")
                pas = "-".join([s.get_text(strip=True) for s in pos_p.find_all("span")]) if pos_p else ""
                for p in p_lines:
                    pt = p.get_text(strip=True)
                    if "人気" in pt:
                        pm = re.search(r"(\d+)人気", pt)
                        if pm: pop = f"{pm.group(1)}人"
                        sps = p.find_all("span")
                        if len(sps) > 1: j_prev = sps[1].get_text(strip=True)
                    if "3F" in pt:
                        am = re.search(r"\(([\d]+)\)", pt)
                        if am: agari = f"3F{am.group(1)}位"
                j_prev_full = normalize_name(j_prev, resources["jockeys"])
                if i == 1:
                    p_data = resources["power_data"].get((place_short, j_prev_full))
                    if p_data: prev_power_info = f"前P:{p_data['power']}"
                h_str = f"{ymd} {course_s} {r_cl} {j_prev_full} {pas}({agari})→{rank}着({pop})"
                history.append(h_str)
            data["horses"][umaban] = {
                "name": horse_name, "jockey": j_full, "trainer": t_full,
                "power": power_info, "prev_power": prev_power_info,
                "compat": pair_stats, "hist": history, 
                "prev_jockey_name": history[0].split(" ")[3] if history else ""
            }
        except Exception: continue
    return data

def _parse_grades_from_ai(text):
    grades = {}
    for line in text.split('\n'):
        m = re.search(r'([SABCDE])\s*[:：]?\s*([^\s　]+)', line)
        if m:
            g, n = m.group(1), re.sub(r'[（\(].*?[）\)]', '', m.group(2)).strip()
            if n: grades[n] = g
    return grades

def _fetch_matchup_table(nankan_id, grades):
    url = f"https://www.nankankeiba.com/taisen/{nankan_id}.do"
    sess = get_http_session()
    try:
        soup = BeautifulSoup(sess.get(url, timeout=10).content, 'html.parser', from_encoding='cp932')
        tbl = soup.find('table', class_='nk23_c-table08__table')
        if not tbl: return "\n(対戦データなし)"
        races = []
        if tbl.find('thead'):
            for col in tbl.find('thead').find_all(['th','td'])[2:]:
                det = col.find(class_='nk23_c-table08__detail')
                if det:
                    link = col.find('a')
                    races.append({
                        "title": det.get_text(" ", strip=True),
                        "url": "https://www.nankankeiba.com" + link.get('href','') if link else "",
                        "results": []
                    })
        if not races: return "\n(初対戦)"
        if tbl.find('tbody'):
            for tr in tbl.find('tbody').find_all('tr'):
                u = tr.find('a', class_='nk23_c-table08__text')
                if not u: continue
                name = u.get_text(strip=True)
                grade = grades.get(name, "")
                if not grade:
                    for k,v in grades.items():
                        if k in name or name in k: grade = v; break
                cells = tr.find_all(['td','th'])
                idx_st = -1
                for i, c in enumerate(cells):
                    if c.find('a', class_='nk23_c-table08__text'): idx_st=i; break
                if idx_st == -1: continue
                for i, c in enumerate(cells[idx_st+1:]):
                    if i >= len(races): break
                    rp = c.find('p', class_='nk23_c-table08__number')
                    rnk = ""
                    if rp:
                        sp = rp.find('span')
                        rnk = sp.get_text(strip=True) if sp else rp.get_text(strip=True).split('｜')[0].strip()
                    if rnk and (rnk.isdigit() or rnk in ['除外','中止']):
                        races[i]["results"].append({"rank":rnk, "name":name, "grade":grade, "sort":int(rnk) if rnk.isdigit() else 999})
        out = ["\n【対戦表（AI評価付き）】"]
        for r in races:
            if not r["results"]: continue
            r["results"].sort(key=lambda x:x["sort"])
            line_parts = []
            for x in r["results"]:
                g = f"[{x['grade']}]" if x['grade'] else ""
                line_parts.append(f"{x['rank']}着 {x['name']}{g}")
            out.append(f"◆ {r['title']}\n" + " / ".join(line_parts) + f"\n詳細: {r['url']}\n")
        return "\n".join(out)
    except: return "(対戦表エラー)"

# ==================================================
# メイン処理 (変更部分: Generator)
# ==================================================
def run_races_iter(year, month, day, place_code, target_races):
    """
    UI依存を排除し、純粋にデータ取得状況と結果をYieldするジェネレータ
    yield (type, content)
    type: 'status', 'error', 'result'
    """
    resources = load_resources()
    kb_input_map = {"10":"大井", "11":"川崎", "12":"船橋", "13":"浦和"}
    nk_code_map = {"10":"20", "11":"21", "12":"19", "13":"18"}
    place_name = kb_input_map.get(place_code, "地方")
    nk_place_code = nk_code_map.get(place_code)

    ops = Options()
    ops.add_argument("--headless=new")
    ops.add_argument("--no-sandbox")
    ops.add_argument("--disable-dev-shm-usage")
    ops.add_argument("user-agent=Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1")
    
    driver = webdriver.Chrome(options=ops)
    
    try:
        yield ("status", f"📅 {place_name} 開催特定中...")
        kai, nichi = get_nankan_kai_nichi(month, day, place_name)
        if not kai: 
            yield ("error", f"開催特定失敗: {month}/{day} {place_name}")
            return
        
        yield ("status", f"✅ {place_name} 第{kai}回 {nichi}日目 - ログイン中...")
        login_keibabook_robust(driver)

        prog_url = f"https://www.nankankeiba.com/program/{year}{month}{day}{nk_place_code}.do"
        driver.get(prog_url)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        r_nums = []
        for a in soup.find_all("a", href=True):
            if f"{year}{month}{day}{nk_place_code}" in a["href"] and "uma_shosai" not in a["href"]:
                f = a["href"].split("/")[-1].replace(".do","")
                if len(f)==16: r_nums.append(int(f[14:16]))
        r_nums = sorted(list(set(r_nums))) or range(1, 13)

        for r_num in r_nums:
            if target_races and r_num not in target_races: continue
            
            yield ("status", f"🏇 {r_num}R データ収集中...")
            
            try:
                nk_id = f"{year}{month}{day}{nk_place_code}{kai:02}{nichi:02}{r_num:02}"
                kb_id = get_kb_url_id(year, month, day, place_code, nichi, r_num)
                
                danwa, cyokyo = parse_kb_danwa_cyokyo(driver, kb_id)
                driver.get(f"https://www.nankankeiba.com/uma_shosai/{nk_id}.do")
                nk_data = parse_nankankeiba_detail(driver.page_source, place_name, resources)
                
                if not nk_data["horses"]: 
                    yield ("result", (r_num, f"⚠️ {r_num}R データなし"))
                    continue

                header = f"レース名:{r_num}R {nk_data['meta'].get('race_name','')} 格:{nk_data['meta'].get('grade','')} コース:{nk_data['meta'].get('course','')}"
                horse_texts = []
                
                for u in sorted(nk_data["horses"].keys(), key=int):
                    h = nk_data["horses"][u]
                    p_jockey = h.get("prev_jockey_name", "")
                    p_info = f"(前:{p_jockey})" if p_jockey else ""
                    power_line = f"【騎手】{h['power']}、{h['prev_power']} 相性:{h['compat']}"
                    block = [
                        f"[{u}]{h['name']} 騎:{h['jockey']}{p_info} 師:{h['trainer']}",
                        f"話:{danwa.get(u,'なし')}", 
                        f"調:{cyokyo.get(u,'データなし')}",
                        power_line,
                        "【近走】"
                    ]
                    cn_map = {0:"[前走]", 1:"[2走前]", 2:"[3走前]"}
                    for idx, hs in enumerate(h["hist"]):
                        prefix = cn_map.get(idx, f"[{idx+1}走]")
                        block.append(f"{prefix} {hs}")
                    horse_texts.append("\n".join(block))
                
                full_prompt = header + "\n\n" + "\n\n".join(horse_texts)
                
                yield ("status", f"🤖 {r_num}R AI分析中...")
                ai_out = run_dify_prediction(full_prompt)
                
                grades = _parse_grades_from_ai(ai_out)
                match_txt = _fetch_matchup_table(nk_id, grades)
                
                ai_out_clean = re.sub(r'^\s*-{3,}\s*$', '', ai_out, flags=re.MULTILINE)
                ai_out_clean = re.sub(r'\n{3,}', '\n\n', ai_out_clean).strip()

                final = f"📅 {year}/{month}/{day} {place_name}{r_num}R\n\n=== 🤖AI予想 ===\n{ai_out_clean}\n\n{match_txt}"
                
                yield ("result", (r_num, final))
                time.sleep(1)

            except Exception as e:
                yield ("result", (r_num, f"Error {r_num}R: {e}"))
    except Exception as e:
        yield ("error", f"Global Error: {e}")
    finally:
        driver.quit()

# ==================================================
# Streamlit UI (ここが重要：State管理版)
# ==================================================
def main():
    st.set_page_config(page_title="NANKAN AI Master", layout="wide")
    st.title("🏇 NANKAN AI Prediction System")

    # --- Sidebar ---
    st.sidebar.header("設定")
    today = datetime.now()
    col1, col2 = st.sidebar.columns(2)
    s_year = col1.number_input("年", value=today.year)
    s_month = col2.number_input("月", value=today.month)
    s_day = st.sidebar.number_input("日", value=today.day)
    s_place = st.sidebar.selectbox("場所", 
        options=["10","11","12","13"], 
        format_func=lambda x: {"10":"大井","11":"川崎","12":"船橋","13":"浦和"}.get(x)
    )
    
    # 対象レース選択
    target_race_input = st.sidebar.text_input("レース指定 (例: 1,2,11 / 空白で全R)", "")
    
    # --- Session State 初期化 (データ永続化のため) ---
    if "results" not in st.session_state:
        st.session_state.results = {}  # {r_num: text}
    if "logs" not in st.session_state:
        st.session_state.logs = []

    # --- 実行ボタン ---
    if st.sidebar.button("分析開始", type="primary"):
        target_races = [int(x.strip()) for x in target_race_input.split(",")] if target_race_input.strip() else []
        
        # ログクリアは任意
        st.session_state.logs = []
        
        # プログレス表示用のコンテナ
        status_container = st.status("🚀 処理を開始します...", expanded=True)
        
        # ジェネレータを実行
        for type_, content in run_races_iter(s_year, s_month, s_day, s_place, target_races):
            if type_ == "status":
                status_container.write(content)
            elif type_ == "error":
                status_container.error(content)
            elif type_ == "result":
                r_num, text = content
                # 【重要】Stateに保存する
                st.session_state.results[r_num] = text
                status_container.write(f"✅ {r_num}R 完了")
        
        status_container.update(label="✨ 全処理完了！", state="complete", expanded=False)

    # --- 結果表示エリア (Stateから読み込むためリロードしても消えない) ---
    st.divider()
    st.subheader(f"📊 分析結果一覧 ({len(st.session_state.results)}レース済)")

    # 1R〜12Rまで順番に表示
    # タブで分けるか、Expanderで分けるかはお好みで。ここではExpander推奨
    if not st.session_state.results:
        st.info("👈 サイドバーから分析を開始してください")
    else:
        # 結果があるものだけソートして表示
        sorted_keys = sorted(st.session_state.results.keys())
        for r in sorted_keys:
            txt = st.session_state.results[r]
            # レースごとにExpanderを作成
            with st.expander(f"🏁 {r}R の予想結果を見る", expanded=False):
                st.code(txt, language="text") # コピーしやすいようにcodeブロックを使用

    # 履歴クリアボタン
    if st.sidebar.button("結果クリア"):
        st.session_state.results = {}
        st.rerun()

if __name__ == "__main__":
    main()
