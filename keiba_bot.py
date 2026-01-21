import time
import re
import os
import csv
import requests
import streamlit as st

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==================================================
# 【設定】ファイルパス設定
# ※フォルダ階層がある場合は適宜 "data/2025_NARJockey.csv" 等に変更してください
# ==================================================
JOCKEY_FILE = "2025data/2025_NARJockey.csv"        # 騎手名リスト
TRAINER_FILE = "2025data/2025_NankanTrainer.csv"   # 調教師名リスト
STATS_FILE = "2025data/騎手調教師_2025.csv"        # 相性データCSV

# ==================================================
# 【設定】Secrets読み込み
# ==================================================
KEIBA_ID = st.secrets.get("KEIBA_ID", "")
KEIBA_PASS = st.secrets.get("KEIBA_PASS", "")
DIFY_API_KEY = st.secrets.get("DIFY_API_KEY", "")
DIFY_BASE_URL = st.secrets.get("DIFY_BASE_URL", "https://api.dify.ai")

# ==================================================
# ★名前＆相性データ読み込みロジック（分離・完全修正版）
# ==================================================
@st.cache_resource
def load_data_resources():
    """
    騎手リスト、調教師リスト、相性データをメモリに読み込む
    Professional Fix: 
    - 騎手と調教師を別々のリストとして管理
    - 内部の空白(全角/半角)を全て除去してキーにする「完全正規化」
    - utf-8-sig対応
    """
    resources = {"jockeys": [], "trainers": [], "stats": {}}
    
    # -------------------------------------------------
    # 1. 騎手リスト (2025_NARJockey.csv)
    # -------------------------------------------------
    if os.path.exists(JOCKEY_FILE):
        try:
            with open(JOCKEY_FILE, "r", encoding="utf-8-sig") as f:
                for line in f:
                    # カンマ、スペースを除去してリスト化
                    clean_line = line.strip().replace("，", "").replace(",", "").replace("　", "").replace(" ", "")
                    if clean_line:
                        resources["jockeys"].append(clean_line)
            print(f"✅ Jockeys loaded: {len(resources['jockeys'])}")
        except Exception as e:
            print(f"⚠️ Jockey list loading error: {e}")
    else:
        print(f"ℹ️ {JOCKEY_FILE} not found. Skipping jockey normalization.")

    # -------------------------------------------------
    # 2. 調教師リスト (2025_NankanTrainer.csv)
    # -------------------------------------------------
    if os.path.exists(TRAINER_FILE):
        try:
            with open(TRAINER_FILE, "r", encoding="utf-8-sig") as f:
                for line in f:
                    # カンマ、スペースを除去してリスト化
                    clean_line = line.strip().replace("，", "").replace(",", "").replace("　", "").replace(" ", "")
                    if clean_line:
                        resources["trainers"].append(clean_line)
            print(f"✅ Trainers loaded: {len(resources['trainers'])}")
        except Exception as e:
            print(f"⚠️ Trainer list loading error: {e}")
    else:
        print(f"ℹ️ {TRAINER_FILE} not found. Skipping trainer normalization.")

    # -------------------------------------------------
    # 3. 相性データ (騎手調教師_2025.csv)
    # -------------------------------------------------
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                
                # 【重要】ヘッダーのクリーニング
                if reader.fieldnames:
                    reader.fieldnames = [h.strip().replace(' ', '').replace('　', '') for h in reader.fieldnames]

                count = 0
                for row in reader:
                    # 値の取得時に内部の空白もすべて削除してキーにする
                    raw_jockey = row.get('騎手名', '')
                    raw_trainer = row.get('調教師名', '')
                    
                    jockey_key = raw_jockey.replace(" ", "").replace("　", "")
                    trainer_key = raw_trainer.replace(" ", "").replace("　", "")
                    
                    if not jockey_key or not trainer_key: continue
                    
                    try:
                        total = int(row.get('出走回数', 0) or 0)
                        w1 = int(row.get('1着', 0) or 0)
                        w2 = int(row.get('2着', 0) or 0)
                        w3 = int(row.get('3着', 0) or 0)
                        others = total - (w1 + w2 + w3)
                        
                        win_rate = row.get('勝率', '0%').strip()
                        fuku_rate = row.get('複勝率', '0%').strip()
                        
                        record_str = f"{w1}-{w2}-{w3}-{others}"
                        key = (jockey_key, trainer_key)
                        
                        resources["stats"][key] = f"【相性】勝率:{win_rate} 複勝率:{fuku_rate} ({record_str})"
                        count += 1
                    except ValueError:
                        continue 
                        
            print(f"✅ Stats loaded: {count} pairs (Normalized keys)")
        except Exception as e:
            print(f"⚠️ Stats loading error: {e}")
    else:
        print(f"ℹ️ {STATS_FILE} not found. Skipping stats loading.")
            
    return resources

def find_best_match(abbrev, name_list):
    """ 
    略称 -> 正式名称への変換ロジック 
    name_list: 騎手リストまたは調教師リストの特定リストを渡す
    """
    if not abbrev: return "不明"
    abbrev_clean = abbrev.replace(" ", "").replace("　", "")
    if not name_list: return abbrev
    
    # 1. 完全一致チェック
    if abbrev_clean in name_list: return abbrev_clean

    # 2. 正規表現によるあいまいマッチング
    try:
        # "御神訓" -> "御.*神.*訓"
        pattern_str = ".*".join(list(abbrev_clean))
        regex = re.compile(pattern_str)
    except:
        return abbrev

    candidates = []
    for fname in name_list:
        # 文字順序が一致 かつ 先頭文字（苗字の頭）が一致するもの
        if regex.search(fname) and fname.startswith(abbrev_clean[0]):
            candidates.append(fname)
    
    if len(candidates) == 1:
        return candidates[0]
    elif len(candidates) > 1:
        # 複数候補がある場合は最も短い名前（略称に近い）を採用
        # 例: 「森」で「森泰斗」と「森下」がヒットした場合など（文脈によるが短い方を優先）
        return min(candidates, key=len)
    
    return abbrev

def get_compatibility(jockey, trainer, stats_db):
    """ 
    騎手と調教師のペアから相性データを取得 
    """
    if not jockey or not trainer: return "(データ不足)"
    
    j_key = jockey.replace(" ", "").replace("　", "")
    t_key = trainer.replace(" ", "").replace("　", "")
    
    key = (j_key, t_key)
    
    if key in stats_db:
        return stats_db[key]
    else:
        return "(相性データなし)"

# ==================================================
# 内部ユーティリティ・HTTPセッション
# ==================================================
def _ui_info(ui, msg):
    if ui: st.info(msg)
def _ui_success(ui, msg):
    if ui: st.success(msg)
def _ui_warning(ui, msg):
    if ui: st.warning(msg)
def _ui_markdown(ui, msg):
    if ui: st.markdown(msg)

@st.cache_resource
def get_http_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=3, 
        backoff_factor=0.6, 
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["POST", "GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

# ==================================================
# Selenium / スクレイピング
# ==================================================
def build_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1400,2200")
    options.add_argument("--log-level=3")
    return webdriver.Chrome(options=options)

def login_keibabook(driver, wait):
    driver.get("https://s.keibabook.co.jp/login/login")
    if "logout" in driver.current_url: return
    try:
        wait.until(EC.visibility_of_element_located((By.NAME, "login_id"))).send_keys(KEIBA_ID)
        driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
        driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
        time.sleep(1)
    except: pass

def fetch_race_ids_from_schedule(driver, year, month, day, target_place_code, ui=False):
    date_str = f"{year}{month}{day}"
    url = f"https://s.keibabook.co.jp/chihou/nittei/{date_str}10"
    _ui_info(ui, f"📅 日程取得中: {url}")
    driver.get(url)
    try: WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "a")))
    except: pass
    soup = BeautifulSoup(driver.page_source, "html.parser")
    race_ids = []
    seen = set()
    for a in soup.find_all("a", href=True):
        m = re.search(r"(\d{16})", a["href"])
        if not m: continue
        rid = m.group(1)
        if rid[6:8] == target_place_code:
            if rid not in seen:
                race_ids.append(rid)
                seen.add(rid)
    return sorted(race_ids)

# --- スクレイピングデータ解析関数群 ---
def parse_race_info(html):
    soup = BeautifulSoup(html, "html.parser")
    racetitle = soup.find("div", class_="racetitle")
    if not racetitle: return {}
    racemei = racetitle.find("div", class_="racemei")
    p_tags = racemei.find_all("p") if racemei else []
    race_name = p_tags[1].get_text(strip=True) if len(p_tags) >= 2 else (p_tags[0].get_text(strip=True) if p_tags else "")
    sub = racetitle.find("div", class_="racetitle_sub")
    sub_p = sub.find_all("p") if sub else []
    cond = sub_p[1].get_text(" ", strip=True) if len(sub_p) >= 2 else ""
    return {"race_name": race_name, "cond": cond}

def parse_danwa_comments(html):
    soup = BeautifulSoup(html, "html.parser")
    danwa_dict = {}
    table = soup.find("table", class_="danwa")
    if table and table.tbody:
        current_uma = None
        for row in table.tbody.find_all("tr"):
            uma_td = row.find("td", class_="umaban")
            if uma_td:
                current_uma = uma_td.get_text(strip=True)
                continue
            txt_td = row.find("td", class_="danwa")
            if txt_td and current_uma:
                danwa_dict[current_uma] = txt_td.get_text(strip=True)
                current_uma = None
    return danwa_dict

def parse_cyokyo(html):
    soup = BeautifulSoup(html, "html.parser")
    cyokyo_dict = {}
    tables = soup.find_all("table", class_="cyokyo")
    for tbl in tables:
        tbody = tbl.find("tbody")
        if not tbody: continue
        rows = tbody.find_all("tr", recursive=False)
        if not rows: continue
        h_row = rows[0]
        uma_td = h_row.find("td", class_="umaban")
        name_td = h_row.find("td", class_="kbamei")
        if not uma_td or not name_td: continue
        umaban = uma_td.get_text(strip=True)
        bamei = name_td.get_text(" ", strip=True)
        tanpyo_elem = h_row.find("td", class_="tanpyo")
        tanpyo = tanpyo_elem.get_text(strip=True) if tanpyo_elem else ""
        detail = rows[1].get_text(" ", strip=True) if len(rows) > 1 else ""
        cyokyo_dict[umaban] = f"【馬名】{bamei} 【短評】{tanpyo} 【詳細】{detail}"
    return cyokyo_dict

# --- keiba.go.jp 出馬表パース ---
_KEIBAGO_UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
}
_WEIGHT_RE = re.compile(r"^[☆▲△◇]?\s*\d{1,2}\.\d$")
_PREV_JOCKEY_RE = re.compile(r"\d+人\s+([☆▲△◇]?\s*\S+)\s+\d{1,2}\.\d")

def _norm_name(s):
    s = (s or "").strip().replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s)
    return s.replace("▲", "").replace("△", "").replace("☆", "").replace("◇", "").strip()

def _extract_jockey_from_cell(td):
    lines = [x.strip() for x in td.get_text("\n", strip=True).split("\n") if x.strip()]
    lines2 = [ln for ln in lines if not _WEIGHT_RE.match(ln)]
    return lines2[0].replace(" ", "") if lines2 else "不明"

def fetch_keibago_debatable_small(year, month, day, race_no, baba_code):
    date_str = f"{year}/{str(month).zfill(2)}/{str(day).zfill(2)}"
    url = f"https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTableSmall?k_raceDate={requests.utils.quote(date_str)}&k_raceNo={race_no}&k_babaCode={baba_code}"
    sess = get_http_session()
    r = sess.get(url, headers=_KEIBAGO_UA, timeout=25)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")

    header = ""
    top_bs = soup.select_one("table.bs")
    if top_bs: header = top_bs.get_text(" ", strip=True)
    nar_race_level = ""
    title_span = soup.select_one("span.midium")
    if title_span: nar_race_level = title_span.get_text(strip=True)
    
    main_table = soup.select_one("td.dbtbl table.bs[border='1']") or soup.select_one("table.bs[border='1']")
    horses = {}
    if not main_table: return header, horses, url, nar_race_level

    last_waku = ""
    for tr in main_table.find_all("tr"):
        if not tr.select_one("font.bamei"): continue
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 8: continue
        
        first_txt = tds[0].get_text(strip=True)
        waku_present = first_txt.isdigit() and len(tds) >= 9
        if waku_present and not tds[1].get_text(strip=True).isdigit(): waku_present = False

        if waku_present:
            waku = tds[0].get_text(strip=True)
            umaban = tds[1].get_text(strip=True)
            horse_td = tds[2]
            trainer_td = tds[3]
            jockey_td = tds[4]
            zenso_td = tds[8] if len(tds) > 8 else None
            last_waku = waku
        else:
            waku = last_waku
            umaban = tds[0].get_text(strip=True)
            horse_td = tds[1]
            trainer_td = tds[2]
            jockey_td = tds[3]
            zenso_td = tds[7] if len(tds) > 7 else None
        
        if not umaban.isdigit(): continue
        bamei_tag = horse_td.select_one("font.bamei b")
        horse = bamei_tag.get_text(strip=True) if bamei_tag else horse_td.get_text(" ", strip=True)
        trainer_raw = trainer_td.get_text(" ", strip=True)
        trainer = trainer_raw.split("（")[0].strip() if trainer_raw else "不明"
        jockey = _extract_jockey_from_cell(jockey_td)
        prev_jockey = ""
        if zenso_td:
            m = _PREV_JOCKEY_RE.search(zenso_td.get_text(" ", strip=True))
            if m: prev_jockey = m.group(1).strip().replace(" ", "")
        
        is_change = bool(prev_jockey and jockey and _norm_name(prev_jockey) != _norm_name(jockey))
        horses[str(umaban)] = {
            "waku": str(waku), "umaban": str(umaban), "horse": horse, 
            "trainer": trainer, "jockey": jockey, 
            "prev_jockey": prev_jockey, "is_change": is_change
        }
    return header, horses, url, nar_race_level

# --- 開催情報判定 ---
def _get_kai_nichi_from_web(target_month, target_day, target_place_name):
    url = "https://www.nankankeiba.com/bangumi_menu/bangumi.do"
    sess = get_http_session()
    try:
        res = sess.get(url, timeout=10)
        res.encoding = 'cp932'
        soup = BeautifulSoup(res.text, 'html.parser')
        target_row = None
        for tr in soup.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) >= 3 and target_place_name in tds[1].get_text():
                target_row = tr
                break
        if not target_row: return 0, 0, f"開催情報なし: {target_place_name}"
        info_td = target_row.find_all('td')[2]
        info_text = info_td.get_text(" ", strip=True).replace('\u00a0', ' ').replace('\u3000', ' ')
        m = re.search(r'第\s*(\d+)\s*回[^\d]*(\d+)\s*月\s*(.*?)\s*日', info_text)
        if not m: return 0, 0, f"開催情報パース不可: {info_text}"
        kai = int(m.group(1))
        mon = int(m.group(2))
        if mon != int(target_month): return 0, 0, f"月不一致"
        days_str = m.group(3)
        days = [int(d) for d in re.findall(r'\d+', days_str)]
        target_d = int(target_day)
        if target_d in days: return kai, days.index(target_d) + 1, None
        else: return 0, 0, f"指定日なし"
    except Exception as e: return 0, 0, f"Error: {e}"

# --- Dify連携 & 対戦表 ---
def _dify_url(path): 
    return f"{(DIFY_BASE_URL or '').strip().rstrip('/')}{path}"

def run_dify_with_blocking_robust(full_text):
    if not DIFY_API_KEY: return "⚠️ DIFY_API_KEY未設定"
    url = _dify_url("/v1/workflows/run")
    payload = {
        "inputs": {"text": full_text}, 
        "response_mode": "blocking", 
        "user": "keiba-bot"
    }
    headers = {"Authorization": f"Bearer {DIFY_API_KEY}", "Content-Type": "application/json"}
    sess = get_http_session()
    
    for attempt in range(3):
        try:
            res = sess.post(url, headers=headers, json=payload, timeout=(10, 600))
            if res.status_code != 200:
                if res.status_code in [500, 502, 503, 504] and attempt < 2:
                    time.sleep(10)
                    continue
                return f"⚠️ Dify Error: {res.status_code} {res.text}"
            j = res.json() or {}
            outputs = j.get("data", {}).get("outputs", {})
            return outputs.get("text") or str(outputs)
        except Exception as e:
            return f"⚠️ API Error: {e}"
    return "⚠️ Retry Failed"

def _parse_grades(text):
    grades = {}
    if not text: return grades
    for line in text.split('\n'):
        if '|' in line:
            parts = [p.strip() for p in line.split('|') if p.strip()]
            if len(parts) >= 2:
                found_grade = None
                for p in reversed(parts):
                    if p in ['S','A','B','C','D','E'] or (len(p)==1 and p in 'SABCDE'):
                        found_grade = p
                        break
                if found_grade:
                    clean_name = re.sub(r'[①-⑳0-9\(\)（）]', '', parts[0]).split('(')[0].strip()
                    if clean_name: grades[clean_name] = found_grade
    return grades

def _parse_grades_fuzzy(horse_name, grades):
    if horse_name in grades: return grades[horse_name]
    h_clean = horse_name.replace(" ", "").replace("　", "")
    for k, v in grades.items():
        if h_clean == k.replace(" ", "").replace("　", ""): return v
    for k, v in grades.items():
        if k in horse_name or horse_name in k: return v
    return ""

def _fetch_history_data(year, month, day, place_name, race_num, grades, kai, nichi):
    if kai == 0 or nichi == 0: return "\n(対戦表取得スキップ)"
    p_code = {'浦和': '18', '船橋': '19', '大井': '20', '川崎': '21'}.get(place_name, '20')
    race_id = f"{year}{int(month):02}{int(day):02}{p_code}{int(kai):02}{int(nichi):02}{int(race_num):02}"
    url = f"https://www.nankankeiba.com/taisen/{race_id}.do"
    sess = get_http_session()
    try:
        res = sess.get(url, timeout=15)
        res.encoding = 'cp932'
        soup = BeautifulSoup(res.text, 'html.parser')
        tbl = soup.find('table', class_='nk23_c-table08__table')
        if not tbl: return "\n(対戦データなし)"
        
        races = []
        thead = tbl.find('thead')
        if thead:
            header_row = thead.find('tr')
            if header_row:
                for col in header_row.find_all(['th', 'td'])[2:]:
                    det = col.find(class_='nk23_c-table08__detail')
                    if det:
                        link = col.find('a', href=re.compile(r'/result/\d+'))
                        races.append({
                            "title": det.get_text(" ", strip=True), 
                            "url": "https://www.nankankeiba.com" + link.get('href','') if link else "", 
                            "results": []
                        })
        
        if not races: return "\n(初対戦)"
        tbody = tbl.find('tbody')
        if not tbody: return ""

        for tr in tbody.find_all('tr'):
            uma_link = tr.find('a', class_='nk23_c-table08__text')
            if not uma_link: continue
            horse_name = uma_link.get_text(strip=True)
            h_grade = _parse_grades_fuzzy(horse_name, grades)
            cells = tr.find_all(['td', 'th'])
            name_idx = -1
            for idx, c in enumerate(cells):
                if c.find('a', class_='nk23_c-table08__text'): name_idx = idx; break
            if name_idx == -1: continue
            
            for i, cell in enumerate(cells[name_idx+1:]):
                if i >= len(races): break
                rank = ""
                num_p = cell.find('p', class_='nk23_c-table08__number')
                if num_p:
                    span = num_p.find('span')
                    rank = span.get_text(strip=True) if span else num_p.get_text(strip=True).split('｜')[0].strip()
                if rank and (rank.isdigit() or rank in ['除外','中止','取消']):
                    races[i]["results"].append({
                        "rank": rank, 
                        "name": horse_name, 
                        "grade": h_grade, 
                        "sort": int(rank) if rank.isdigit() else 999
                    })
        
        output = ["==注目の対戦=="]
        has_content = False
        for r in races:
            if not r["results"]: continue
            has_content = True
            r["results"].sort(key=lambda x: x["sort"])
            line = " / ".join([f"{x['rank']}着 {x['name']}" + (f"({x['grade']})" if x['grade'] else "") for x in r["results"]])
            title = re.sub(r'\s+', ' ', r['title'])
            output.append(f"##{title}\n{line}\n[詳細]({r['url']})\n")
        return "\n".join(output) if has_content else "\n(該当データなし)"
    except Exception as e: return f"\n(対戦表エラー: {e})"

# ==================================================
# メイン処理
# ==================================================
def run_races_iter(year, month, day, place_code, target_races, ui=False):
    # --- 【初期化】名前＆相性リストを一括読み込み（修正済み） ---
    resources = load_data_resources()
    jockey_list = resources["jockeys"]   # 騎手専用リスト
    trainer_list = resources["trainers"] # 調教師専用リスト
    stats_db = resources["stats"]
    # ---------------------------------------------
    
    place_names = {"10": "大井", "11": "川崎", "12": "船橋", "13": "浦和"}
    place_name = place_names.get(place_code, "地方")
    baba_code = {"10": "20", "11": "21", "12": "19", "13": "18"}.get(place_code)

    if not baba_code: yield (0, "⚠️ babaCode mapping error"); return

    driver = build_driver()
    wait = WebDriverWait(driver, 12)

    try:
        _ui_info(ui, "🔑 ログイン中...")
        login_keibabook(driver, wait)
        race_ids = fetch_race_ids_from_schedule(driver, year, month, day, place_code, ui=ui)
        if not race_ids: yield (0, "⚠️ レースID取得失敗"); return

        _ui_info(ui, f"📅 開催情報解析: {place_name} {month}/{day}")
        kai_val, nichi_val, date_err = _get_kai_nichi_from_web(month, day, place_name)
        if date_err: _ui_warning(ui, f"⚠️ {date_err}")
        else: _ui_success(ui, f"✅ 第{kai_val}回 {nichi_val}日目")

        for i, race_id in enumerate(race_ids):
            race_num = i + 1
            if target_races and race_num not in target_races: continue
            _ui_markdown(ui, f"## {place_name} {race_num}R")
            
            try:
                header, keibago_dict, _, nar_race_level = fetch_keibago_debatable_small(str(year), str(month), str(day), race_num, str(baba_code))
                _ui_info(ui, "📡 データ収集中...")
                driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{race_id}")
                html_danwa = driver.page_source
                driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{race_id}")
                html_cyokyo = driver.page_source
                meta_info = parse_race_info(html_danwa)
                danwa_dict = parse_danwa_comments(html_danwa)
                cyokyo_dict = parse_cyokyo(html_cyokyo)

                all_uma = sorted(set(danwa_dict) | set(cyokyo_dict) | set(keibago_dict), key=lambda x: int(x) if x.isdigit() else 999)
                merged_text = []
                
                for uma in all_uma:
                    kg = keibago_dict.get(uma, {})
                    
                    # 1. 名前変換 (騎手は騎手リスト、調教師は調教師リストから検索)
                    # ★修正: 適切なリストを渡すことで精度向上
                    full_jockey = find_best_match(kg.get('jockey', ''), jockey_list)
                    full_trainer = find_best_match(kg.get('trainer', ''), trainer_list)

                    # 2. 相性データ取得
                    compatibility_info = get_compatibility(full_jockey, full_trainer, stats_db)
                    
                    # 表示用テキスト作成
                    prev_info = ""
                    if kg.get('is_change'):
                        pj = kg.get('prev_jockey', '')
                        # ★修正: 前走騎手も騎手リストから検索
                        pj_full = find_best_match(pj, jockey_list)
                        prev_info = f" 【⚠️乗り替わり】(前走:{pj_full})" if pj else " 【⚠️乗り替わり】"

                    # 3. テキストに相性情報を埋め込む
                    info = f"▼[馬番{uma}] {kg.get('horse','')} 騎手:{full_jockey}{prev_info} 調教師:{full_trainer} {compatibility_info}"
                    merged_text.append(f"{info}\n談話: {danwa_dict.get(uma,'なし')}\n調教: {cyokyo_dict.get(uma,'なし')}")

                if not merged_text: yield (race_num, f"⚠️ データなし"); continue

                prompt = f"レース名: {meta_info.get('race_name','')}\nレースレベル: {nar_race_level}\n条件: {meta_info.get('cond','')}\n\n" + "\n".join(merged_text)

                _ui_info(ui, "🤖 AI分析中...")
                dify_res = run_dify_with_blocking_robust(prompt)
                dify_res = (dify_res or "").strip()

                grades = _parse_grades(dify_res)
                history_text = _fetch_history_data(year, month, day, place_name, race_num, grades, kai_val, nichi_val)
                final_output = f"📅 {year}年{month}月{day}日 {place_name} 第{kai_val}回 {nichi_val}日目 {race_num}R\n\n{dify_res}\n\n{history_text}"
                
                _ui_success(ui, "✅ 完了")
                yield (race_num, final_output)
                time.sleep(3)

            except Exception as e: yield (race_num, f"⚠️ Error: {e}"); time.sleep(3)
    finally:
        try: driver.quit()
        except: pass
