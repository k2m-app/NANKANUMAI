import time
import re
import os
import csv
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
# HTTPセッション / Dify連携
# ==================================================
@st.cache_resource
def get_http_session() -> requests.Session:
    sess = requests.Session()
    # 競馬ブックなどはスマホサイトへアクセスするためUAを偽装
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
    })
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

def run_dify_prediction(full_text):
    """ Dify APIにレース情報を送信して予想を取得する """
    if not DIFY_API_KEY: return "⚠️ DIFY_API_KEY未設定"
    
    url = f"{(DIFY_BASE_URL or '').strip().rstrip('/')}/v1/workflows/run"
    payload = {
        "inputs": {"text": full_text}, 
        "response_mode": "blocking", 
        "user": "keiba-bot"
    }
    headers = {"Authorization": f"Bearer {DIFY_API_KEY}", "Content-Type": "application/json"}
    
    sess = get_http_session()
    try:
        # 推論には時間がかかるためタイムアウトを長めに
        res = sess.post(url, headers=headers, json=payload, timeout=120)
        if res.status_code != 200: return f"⚠️ Dify Error: {res.status_code} {res.text}"
        j = res.json()
        return j.get("data", {}).get("outputs", {}).get("text", "") or str(j)
    except Exception as e: return f"⚠️ API Error: {e}"

# ==================================================
# データ読み込み・正規化
# ==================================================
@st.cache_resource
def load_resources():
    res = {"jockeys": [], "trainers": [], "power": {}}
    
    # 騎手リスト
    if os.path.exists(JOCKEY_FILE):
        try:
            with open(JOCKEY_FILE, "r", encoding="utf-8-sig") as f:
                res["jockeys"] = [l.strip().replace(",","").replace(" ","").replace("　","") for l in f if l.strip()]
        except: pass
    
    # 調教師リスト
    if os.path.exists(TRAINER_FILE):
        try:
            with open(TRAINER_FILE, "r", encoding="utf-8-sig") as f:
                res["trainers"] = [l.strip().replace(",","").replace(" ","").replace("　","") for l in f if l.strip()]
        except: pass
    
    # 騎手パワー
    if os.path.exists(POWER_FILE):
        try:
            df = pd.read_csv(POWER_FILE, encoding="utf-8-sig")
            place_col = df.columns[0]
            for _, row in df.iterrows():
                p = str(row[place_col]).strip()
                j = str(row.get("騎手名", "")).replace(" ","").replace("　","")
                if p and j:
                    info = f"騎手パワー:{row.get('騎手パワー','-')}(勝率{row.get('勝率','-')} 複勝率{row.get('複勝率','-')})"
                    res["power"][(p, j)] = info
        except: pass
    return res

def normalize_name(abbrev, full_list):
    """ 名寄せロジック """
    if not abbrev: return ""
    clean = abbrev.replace(" ","").replace("　","")
    if not full_list: return clean
    if clean in full_list: return clean
    # 前方一致で候補を探す (例: 木間龍 -> 木間塚龍馬)
    matches = [n for n in full_list if n.startswith(clean) or (len(clean)>=2 and n.startswith(clean[0]) and clean[1] in n)]
    return sorted(matches, key=len)[0] if matches else clean

# ==================================================
# 開催回・日次 特定ロジック (nankankeiba)
# ==================================================
def get_nankan_kai_nichi(month, day, place_name):
    """ 
    nankankeibaの番組表から「第〇回・〇日目」を特定
    ※この「〇日目(nichi)」は競馬ブックのURL生成にも使用します
    """
    url = "https://www.nankankeiba.com/bangumi_menu/bangumi.do"
    sess = get_http_session()
    try:
        res = sess.get(url, timeout=10)
        res.encoding = 'cp932'
        soup = BeautifulSoup(res.text, 'html.parser')
        
        target_m = int(month)
        target_d = int(day)
        
        for tr in soup.find_all('tr'):
            text = tr.get_text(" ", strip=True)
            if place_name not in text: continue
            
            # 回数の特定
            kai_match = re.search(r'第\s*(\d+)\s*回', text)
            if not kai_match: continue
            kai = int(kai_match.group(1))
            
            # 月の特定
            m_match = re.search(r'(\d+)\s*月', text)
            if not m_match: continue
            if int(m_match.group(1)) != target_m: continue
            
            # 日付リスト抽出 (例: 19, 20, 21...)
            if "月" in text:
                days_part = text.split("月")[1]
                days_match = re.findall(r'(\d+)', days_part)
                # 妥当な日付のみリスト化
                days_list = [int(d) for d in days_match if 1 <= int(d) <= 31]
                
                if target_d in days_list:
                    nichi = days_list.index(target_d) + 1
                    return kai, nichi
        return None, None
    except: return None, None

# ==================================================
# 競馬ブック 解析ロジック (URL計算・HTML解析)
# ==================================================
def get_kb_url_id(year, month, day, place_code, nichi, race_num):
    """
    競馬ブックのURL IDを計算で生成
    Format: YYYY(4) + MM(2) + Place(2) + Nichi(2) + Race(2) + MMDD(4)
    """
    mm = str(month).zfill(2)
    dd = str(day).zfill(2)
    p_code = str(place_code).zfill(2)
    n_code = str(nichi).zfill(2)
    r_code = str(race_num).zfill(2)
    
    return f"{year}{mm}{p_code}{n_code}{r_code}{mm}{dd}"

def parse_kb_danwa_cyokyo(driver, kb_id):
    """ 
    競馬ブックから談話・調教を取得 
    """
    d_danwa, d_cyokyo = {}, {}
    
    # ログイン状態維持のためのリトライ
    def ensure_login():
        if "login" in driver.current_url:
            try:
                driver.find_element(By.NAME, "login_id").send_keys(KEIBA_ID)
                driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
                driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
                time.sleep(1)
            except: pass

    try:
        # --- 談話 (Danwa) ---
        url_danwa = f"https://s.keibabook.co.jp/chihou/danwa/1/{kb_id}"
        driver.get(url_danwa)
        ensure_login() # リダイレクト対応
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        # table class="default danwa" を探す
        for tbl in soup.select("table.danwa"):
            current_horse = None
            for tr in tbl.select("tbody tr"):
                # 馬番行
                u_td = tr.select_one("td.umaban")
                if u_td:
                    current_horse = u_td.get_text(strip=True)
                    continue
                
                # 談話行
                t_td = tr.select_one("td.danwa")
                if current_horse and t_td:
                    # <p>タグ内のテキストを取得
                    text = t_td.get_text(strip=True)
                    d_danwa[current_horse] = text
                    current_horse = None # リセット

        # --- 調教 (Cyokyo) ---
        url_cyokyo = f"https://s.keibabook.co.jp/chihou/cyokyo/1/{kb_id}"
        driver.get(url_cyokyo)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # 複数の table class="default cyokyo" がある
        for tbl in soup.select("table.cyokyo"):
            rows = tbl.select("tbody tr")
            if not rows: continue
            
            # 1行目: 基本情報
            r1 = rows[0]
            u_td = r1.select_one("td.umaban")
            if not u_td: continue
            
            uma_num = u_td.get_text(strip=True)
            tanpyo = ""
            tp_td = r1.select_one("td.tanpyo")
            if tp_td: tanpyo = tp_td.get_text(strip=True)
            
            # 2行目以降: 詳細 (dlやnested table)
            detail_text = ""
            if len(rows) > 1:
                # 2行目のテキストをまるごと取得して整形
                raw_text = rows[1].get_text(" ", strip=True)
                # 連続する空白を1つに
                detail_text = re.sub(r'\s+', ' ', raw_text)
            
            d_cyokyo[uma_num] = f"【短評】{tanpyo} 【詳細】{detail_text}"

    except Exception as e:
        print(f"KB Parse Error: {e}")
        
    return d_danwa, d_cyokyo

# ==================================================
# nankankeiba 詳細解析
# ==================================================
def parse_nankankeiba_detail(html, place_name, resources):
    """ nankankeiba詳細出走表解析 """
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
            
            h_tag = row.select_one("td.is-col03 a.is-link")
            horse_name = h_tag.get_text(strip=True) if h_tag else ""

            jg_td = row.select_one("td.cs-g1")
            j_raw, t_raw = "", ""
            if jg_td:
                links = jg_td.select("a")
                if len(links) >= 1: j_raw = links[0].get_text(strip=True)
                if len(links) >= 2: t_raw = links[1].get_text(strip=True)
            
            j_full = normalize_name(j_raw, resources["jockeys"])
            t_full = normalize_name(t_raw, resources["trainers"])
            power = resources["power"].get((place_name, j_full), "騎手パワー:不明")

            # 相性データ
            ai2 = row.select_one("td.cs-ai2 .graph_text_div")
            pair_stats = "データなし"
            if ai2 and "データ" not in ai2.get_text():
                r = ai2.select_one(".is-percent").get_text(strip=True)
                w = ai2.select_one(".is-number").get_text(strip=True)
                t = ai2.select_one(".is-total").get_text(strip=True)
                pair_stats = f"勝率{r}({w}勝/{t}回)"

            # 近走3走
            history = []
            for i in range(1, 4):
                z = row.select_one(f"td.cs-z{i}")
                if not z or not z.get_text(strip=True): continue
                
                # 日付・場所
                d_txt = ""
                d_spans = z.select("p.nk23_u-d-flex span.nk23_u-text10")
                if d_spans:
                    for s in d_spans:
                        if re.search(r"\d+\.\d+\.\d+", s.get_text()): d_txt = s.get_text(strip=True); break
                
                ymd = ""
                m = re.match(r"([^\d]+)(\d+)\.(\d+)\.(\d+)", d_txt)
                if m:
                    place_short = m.group(1)
                    ymd = f"20{m.group(2)}/{int(m.group(3)):02}/{int(m.group(4)):02}"
                
                cond_txt = d_spans[-1].get_text(strip=True) if len(d_spans)>=2 else ""
                dist_m = re.search(r"\d{4}", cond_txt)
                dist = dist_m.group(0) if dist_m else ""
                course_s = f"{place_short}{dist}m" if m else cond_txt

                r_a = z.select_one("a.is-link")
                r_ti = r_a.get("title", "") if r_a else ""
                rp = re.split(r'[ 　]+', r_ti)
                r_nm = rp[0] if rp else ""
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
                        if pm: pop = f"{pm.group(1)}人気"
                        sps = p.find_all("span")
                        if len(sps)>1: j_prev = re.sub(r"[\d\.]+", "", sps[1].get_text(strip=True))
                    if "3F" in pt:
                        am = re.search(r"\(([\d]+)\)", pt)
                        if am: agari = f"上がり3F:{am.group(1)}位"

                h_str = f"開催日：{ymd}　コース：{course_s} レース：{r_nm}　クラス：{r_cl} 騎手：{j_prev}　通過順{pas}({agari})→{rank}着（{pop}）"
                history.append(h_str)

            data["horses"][umaban] = {
                "name": horse_name, "jockey": j_full, "trainer": t_full,
                "power": power, "compat": pair_stats, "hist": history
            }
        except: continue
    return data

# ==================================================
# 対戦表 & 評価解析
# ==================================================
def _parse_grades_from_ai(text):
    grades = {}
    lines = text.split('\n')
    for line in lines:
        m = re.search(r'([SABCDE])\s*[:：]?\s*([^\s　]+)', line)
        if m:
            grade, name = m.group(1), m.group(2)
            name = re.sub(r'[（\(].*?[）\)]', '', name).strip()
            if name: grades[name] = grade
    return grades

def _fetch_matchup_table(nankan_id, grades):
    url = f"https://www.nankankeiba.com/taisen/{nankan_id}.do"
    sess = get_http_session()
    try:
        res = sess.get(url, timeout=10)
        res.encoding = 'cp932'
        soup = BeautifulSoup(res.text, 'html.parser')
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
                u_link = tr.find('a', class_='nk23_c-table08__text')
                if not u_link: continue
                h_name = u_link.get_text(strip=True)
                grade = grades.get(h_name, "")
                if not grade:
                    for k, v in grades.items():
                        if k in h_name or h_name in k: grade = v; break
                
                cells = tr.find_all(['td','th'])
                st_idx = -1
                for idx, c in enumerate(cells):
                    if c.find('a', class_='nk23_c-table08__text'): st_idx=idx; break
                if st_idx == -1: continue

                for i, cell in enumerate(cells[st_idx+1:]):
                    if i >= len(races): break
                    rp = cell.find('p', class_='nk23_c-table08__number')
                    rnk = ""
                    if rp:
                        sp = rp.find('span')
                        rnk = sp.get_text(strip=True) if sp else rp.get_text(strip=True).split('｜')[0].strip()
                    
                    if rnk and (rnk.isdigit() or rnk in ['除外','中止']):
                        races[i]["results"].append({
                            "rank": rnk, "name": h_name, "grade": grade,
                            "sort": int(rnk) if rnk.isdigit() else 999
                        })

        out = ["\n【対戦表（AI評価付き）】"]
        for r in races:
            if not r["results"]: continue
            r["results"].sort(key=lambda x: x["sort"])
            line_parts = []
            for x in r["results"]:
                g_str = f"[{x['grade']}]" if x['grade'] else ""
                line_parts.append(f"{x['rank']}着 {x['name']}{g_str}")
            out.append(f"◆ {r['title']}")
            out.append(" / ".join(line_parts))
            out.append(f"詳細: {r['url']}\n")
            
        return "\n".join(out)

    except: return "(対戦表作成エラー)"

# ==================================================
# メイン実行関数
# ==================================================
def run_races_iter(year, month, day, place_code, target_races, ui=False):
    resources = load_resources()
    
    # ユーザー入力(KBコード) -> Nankanコード
    # 10:大井, 11:川崎, 12:船橋, 13:浦和
    kb_input_map = {"10":"大井", "11":"川崎", "12":"船橋", "13":"浦和"}
    nk_code_map = {"10":"20", "11":"21", "12":"19", "13":"18"}
    
    place_name = kb_input_map.get(place_code, "地方")
    nk_place_code = nk_code_map.get(place_code)

    if not nk_place_code: yield (0, "⚠️ 場所コードエラー"); return

    # スマホエミュレーション用オプション (Selenium)
    ops = Options()
    ops.add_argument("--headless=new")
    ops.add_argument("--no-sandbox")
    ops.add_argument("--disable-dev-shm-usage")
    ops.add_argument("user-agent=Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1")
    
    driver = webdriver.Chrome(options=ops)
    wait = WebDriverWait(driver, 10)

    try:
        # 1. 開催情報特定 (Kai, Nichi)
        if ui: st.info("📅 開催情報を特定中...")
        kai, nichi = get_nankan_kai_nichi(month, day, place_name)
        if not kai or not nichi:
            yield (0, f"⚠️ 開催情報が見つかりませんでした ({month}/{day} {place_name})")
            return
        
        if ui: st.success(f"✅ {place_name} 第{kai}回 {nichi}日目")

        # 2. ログイン (KeibaBook)
        if ui: st.info("🔑 ログイン中(KeibaBook)...")
        login_keibabook_robust(driver)

        # 3. レース一覧取得 (nankankeiba)
        prog_url = f"https://www.nankankeiba.com/program/{year}{month}{day}{nk_place_code}.do"
        driver.get(prog_url)
        soup_prog = BeautifulSoup(driver.page_source, "html.parser")
        
        race_nums = []
        for a in soup_prog.find_all("a", href=True):
            if f"{year}{month}{day}{nk_place_code}" in a["href"] and "uma_shosai" not in a["href"]:
                fname = a["href"].split("/")[-1].replace(".do","")
                if len(fname) == 16: race_nums.append(int(fname[14:16]))
        
        race_nums = sorted(list(set(race_nums)))
        if not race_nums: race_nums = range(1, 13)

        # 4. 各レース処理
        for r_num in race_nums:
            if target_races and r_num not in target_races: continue
            
            if ui: st.markdown(f"## {place_name} {r_num}R")
            
            try:
                # ★ID生成
                nk_id = f"{year}{month}{day}{nk_place_code}{kai:02}{nichi:02}{r_num:02}"
                # KB_ID: YYYY(4)+MM(2)+Place(2)+Nichi(2)+R(2)+MMDD(4)
                # place_codeはユーザー入力のまま(10,11,12,13)
                kb_id = get_kb_url_id(year, month, day, place_code, nichi, r_num)
                
                # A. データ取得
                danwa, cyokyo = parse_kb_danwa_cyokyo(driver, kb_id)
                
                nk_url = f"https://www.nankankeiba.com/uma_shosai/{nk_id}.do"
                driver.get(nk_url)
                nk_data = parse_nankankeiba_detail(driver.page_source, place_name, resources)
                
                if not nk_data["horses"]:
                    yield (r_num, f"⚠️ データ取得失敗: {nk_url}"); continue

                # B. プロンプト作成
                header = f"レース名: {r_num}R {nk_data['meta'].get('race_name','')}　格付け:{nk_data['meta'].get('grade','')}　コース:{nk_data['meta'].get('course','')}"
                horse_texts = []
                for u in sorted(nk_data["horses"].keys(), key=int):
                    h = nk_data["horses"][u]
                    
                    prev_j = ""
                    if h["hist"]:
                        m = re.search(r"騎手：([^　\s]+)", h["hist"][0])
                        if m: prev_j = m.group(1)
                    p_info = f" (前走:{prev_j})" if prev_j else ""
                    
                    lines = [
                        f"[馬番{u}] {h['name']} 騎手:{h['jockey']}{p_info} 調教師:{h['trainer']}",
                        f"談話: {danwa.get(u,'なし')} 調教:{cyokyo.get(u,'調教データなし')}",
                        f"【騎手】{h['power']} 相性:{h['compat']}"
                    ]
                    cn = {0:"①", 1:"②", 2:"③"}
                    for idx, his in enumerate(h["hist"]):
                        lines.append(f"【近走】{cn.get(idx,'')} {his}")
                    
                    horse_texts.append("\n".join(lines))
                
                full_prompt = header + "\n\n" + "\n\n".join(horse_texts)
                
                # C. Dify送信
                if ui: st.info("🤖 AI分析中...")
                ai_output = run_dify_prediction(full_prompt)
                
                # D. 対戦表作成
                grades = _parse_grades_from_ai(ai_output)
                matchup_text = _fetch_matchup_table(nk_id, grades)
                
                # E. 最終出力
                final_res = f"📅 {year}/{month}/{day} {place_name}{r_num}R\n\n=== 🤖AI予想 ===\n{ai_output}\n\n{matchup_text}\n\n=== 📊分析データ(抜粋) ===\n{full_prompt[:300]}..."
                
                if ui: st.success("✅ 完了")
                yield (r_num, final_res)
                time.sleep(2)

            except Exception as e:
                yield (r_num, f"Error: {e}")
    
    finally:
        driver.quit()
