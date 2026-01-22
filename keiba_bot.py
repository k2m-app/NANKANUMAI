import time
import re
import os
import json
import requests
import streamlit as st
import pandas as pd
import traceback  # ★エラー詳細追跡用

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException

# HTML Parsing & Network
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==================================================
# 1. 設定 & 定数
# ==================================================
DATA_DIR = "2025data"
JOCKEY_FILE = os.path.join(DATA_DIR, "2025_NARJockey.csv")
TRAINER_FILE = os.path.join(DATA_DIR, "2025_NankanTrainer.csv")
POWER_FILE = os.path.join(DATA_DIR, "2025_騎手パワー.csv")

# Secrets (Streamlit Cloud対策でgetを使用)
KEIBA_ID = st.secrets.get("KEIBA_ID", "")
KEIBA_PASS = st.secrets.get("KEIBA_PASS", "")
DIFY_API_KEY = st.secrets.get("DIFY_API_KEY", "")
DIFY_BASE_URL = st.secrets.get("DIFY_BASE_URL", "https://api.dify.ai")


# ==================================================
# 2. 共通関数
# ==================================================
@st.cache_resource
def get_http_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
    })
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

def get_driver():
    ops = Options()
    # ヘッドレスモード設定（newモード推奨）
    ops.add_argument("--headless=new")
    ops.add_argument("--no-sandbox")
    ops.add_argument("--disable-dev-shm-usage")
    ops.add_argument("--disable-gpu")
    ops.add_argument("--window-size=1920,1080")
    ops.add_argument("--lang=ja-JP")
    ops.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # ★安定性重視のため eager は削除し、デフォルト(normal)を使用
    # ops.page_load_strategy = 'eager' 
    
    # 画像ロード抑制（通信量削減と高速化）
    ops.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    
    # Bot検知回避
    ops.add_argument("--disable-blink-features=AutomationControlled")
    
    return webdriver.Chrome(options=ops)

def login_keibabook_robust(driver):
    """
    keibabook smart ログイン（失敗してもアプリは続行できる設計）
    """
    try:
        driver.get("https://s.keibabook.co.jp/login/login")
        # 既にログイン済みかチェック
        time.sleep(1)
        if "logout" in driver.current_url or driver.find_elements(By.XPATH, "//a[contains(@href,'logout')]"):
            return True

        # 要素が出るまで待機
        wait = WebDriverWait(driver, 10)
        id_input = wait.until(EC.visibility_of_element_located((By.NAME, "login_id")))
        id_input.clear()
        id_input.send_keys(KEIBA_ID)
        
        pass_input = driver.find_element(By.CSS_SELECTOR, "input[type='password']")
        pass_input.clear()
        pass_input.send_keys(KEIBA_PASS)
        
        # クリック動作をJSで確実に行う
        submit = driver.find_element(By.CSS_SELECTOR, "input[type='submit']")
        driver.execute_script("arguments[0].click();", submit)
        
        time.sleep(2)
        return True
    except Exception:
        # ログイン失敗しても他サイト（南関）の処理は止めない
        return False


# ==================================================
# 3. Dify API
# ==================================================
def run_dify_prediction(full_text):
    if not DIFY_API_KEY:
        return "⚠️ DIFY_API_KEY未設定"

    url = f"{(DIFY_BASE_URL or '').strip().rstrip('/')}/v1/workflows/run"
    payload = {"inputs": {"text": full_text}, "response_mode": "streaming", "user": "keiba-bot"}
    headers = {"Authorization": f"Bearer {DIFY_API_KEY}", "Content-Type": "application/json"}
    sess = get_http_session()

    max_retries = 3
    for _ in range(max_retries):
        full_response = ""
        try:
            with sess.post(url, headers=headers, json=payload, stream=True, timeout=120) as res:
                if res.status_code == 429:
                    time.sleep(60)
                    continue
                if res.status_code != 200:
                    return f"⚠️ Dify Error: {res.status_code}"

                for line in res.iter_lines():
                    if not line:
                        continue
                    decoded_line = line.decode("utf-8", errors="ignore")
                    if not decoded_line.startswith("data:"):
                        continue

                    json_str = decoded_line[5:].strip()
                    if not json_str:
                        continue

                    try:
                        data = json.loads(json_str)
                        event = data.get("event")
                        if event == "workflow_finished":
                            outputs = data.get("data", {}).get("outputs", {})
                            if "text" in outputs:
                                return outputs["text"]
                        elif event in ("text_chunk", "message"):
                            chunk = data.get("data", {}).get("text", "")
                            full_response += chunk
                    except Exception:
                        pass

                return full_response if full_response else "（回答生成エラー）"

        except Exception:
            time.sleep(5)

    return "⚠️ エラー: リトライ上限を超えました"


# ==================================================
# 4. データロード & 解析
# ==================================================
@st.cache_resource
def load_resources():
    res = {"jockeys": [], "trainers": [], "power": {}, "power_data": {}}

    for fpath, key in [(JOCKEY_FILE, "jockeys"), (TRAINER_FILE, "trainers")]:
        if os.path.exists(fpath):
            try:
                with open(fpath, "r", encoding="utf-8-sig") as f:
                    res[key] = [l.strip().replace(",", "").replace(" ", "").replace("　", "") for l in f if l.strip()]
            except Exception:
                pass

    if os.path.exists(POWER_FILE):
        try:
            df = pd.read_csv(POWER_FILE, encoding="utf-8-sig")
            # 列名の揺らぎに対応（1列目を場所、騎手名を探索）
            place_col = df.columns[0]
            jockey_col = "騎手名"
            if jockey_col not in df.columns and len(df.columns) > 2:
                jockey_col = df.columns[2] # C列相当

            for _, row in df.iterrows():
                place = str(row.get(place_col, "")).strip()
                jockey = str(row.get(jockey_col, "")).replace(" ", "").replace("　", "").strip()
                if not place or not jockey:
                    continue

                power = str(row.get("騎手パワー", "")).strip()
                win = str(row.get("勝率", "")).strip()
                fuku = str(row.get("複勝率", "")).strip()

                key_t = (place, jockey)
                res["power"][key_t] = f"P:{power}" if power else "P:不明"
                res["power_data"][key_t] = {
                    "power": power,
                    "win_rate": win,
                    "fuku_rate": fuku
                }
        except Exception:
            pass

    return res

def normalize_name(abbrev, full_list):
    if not abbrev: return ""
    clean = re.sub(r"[ 　▲△☆◇★\d\.]+", "", abbrev)
    if not clean: return ""
    if not full_list: return clean
    if clean in full_list: return clean
    # 部分一致探索
    candidates = []
    for full in full_list:
        if all(c in full for c in clean):
            candidates.append((len(full) - len(clean), full))
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]
    return clean

def get_nankan_kai_nichi(month, day, place_name):
    url = "https://www.nankankeiba.com/bangumi_menu/bangumi.do"
    sess = get_http_session()
    try:
        res = sess.get(url, timeout=10)
        res.encoding = "cp932"
        soup = BeautifulSoup(res.text, "html.parser")

        target_m, target_d = int(month), int(day)
        for tr in soup.find_all("tr"):
            text = tr.get_text(" ", strip=True)
            if place_name not in text:
                continue
            kai_m = re.search(r"第\s*(\d+)\s*回", text)
            mon_m = re.search(r"(\d+)\s*月", text)
            if kai_m and mon_m and int(mon_m.group(1)) == target_m:
                days_part = text.split("月", 1)[1]
                days_match = re.findall(r"(\d+)", days_part)
                days_list = [int(d) for d in days_match if 1 <= int(d) <= 31]
                if target_d in days_list:
                    return int(kai_m.group(1)), days_list.index(target_d) + 1
        return None, None
    except Exception:
        return None, None

def get_kb_url_id(year, month, day, place_code, nichi, race_num):
    return f"{year}{str(month).zfill(2)}{str(place_code).zfill(2)}{str(nichi).zfill(2)}{str(race_num).zfill(2)}{str(month).zfill(2)}{str(day).zfill(2)}"

def parse_kb_danwa_cyokyo(driver, kb_id):
    d_danwa, d_cyokyo = {}, {}
    try:
        # 談話
        driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{kb_id}")
        # ログイン画面に飛ばされたら再ログインを試みる
        if "login" in driver.current_url:
            if login_keibabook_robust(driver):
                driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{kb_id}")

        soup = BeautifulSoup(driver.page_source, "html.parser")
        for tbl in soup.select("table.danwa"):
            curr = None
            for tr in tbl.select("tbody tr"):
                u = tr.select_one("td.umaban")
                if u:
                    curr = u.get_text(strip=True)
                    continue
                t = tr.select_one("td.danwa")
                if curr and t:
                    raw_text = t.get_text(" ", strip=True)
                    m = re.search(r"[―-]+(.*)", raw_text)
                    d_danwa[curr] = m.group(1).strip() if m else raw_text
                    curr = None

        # 調教
        driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{kb_id}")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for tbl in soup.select("table.cyokyo"):
            rows = tbl.select("tbody tr")
            if not rows: continue
            r1 = rows[0]
            u_td = r1.select_one("td.umaban")
            if not u_td: continue
            uma = u_td.get_text(strip=True)
            
            tanpyo = r1.select_one("td.tanpyo")
            tp_txt = tanpyo.get_text(strip=True) if tanpyo else ""
            
            dt_txt = ""
            if len(rows) > 1:
                dt_raw = rows[1].get_text(" ", strip=True)
                dt_txt = re.sub(r"\s+", " ", dt_raw)
            d_cyokyo[uma] = f"【短評】{tp_txt} 【詳細】{dt_txt}"
            
    except Exception:
        # 競馬ブック取得失敗は致命的ではないのでスルー
        pass
    return d_danwa, d_cyokyo


# ==================================================
# 5. nankankeiba パース
# ==================================================
PLACE_MAP = {
    "船": "船橋", "船橋": "船橋", "大": "大井", "大井": "大井",
    "川": "川崎", "川崎": "川崎", "浦": "浦和", "浦和": "浦和", "門": "門別", "門別": "門別",
}

def _normalize_place_token(raw_p: str, fallback_place: str) -> str:
    if not raw_p: return fallback_place
    s = re.sub(r"\s+", " ", str(raw_p)).strip()
    s = s.replace("着", "").strip()
    for k, v in PLACE_MAP.items():
        if k and k in s: return v
    s1 = s[:1]
    if s1 in PLACE_MAP: return PLACE_MAP[s1]
    return fallback_place

def _format_rate(val) -> str:
    if val is None: return ""
    s = str(val).strip()
    if not s or s in ("-", "nan", "NaN", "None"): return ""
    if "%" in s:
        try:
            m = re.search(r"([\d\.]+)", s)
            return f"{round(float(m.group(1)))}%" if m else s
        except: return s
    try:
        x = float(s)
        if x <= 1.0: x *= 100.0
        return f"{round(x)}%"
    except: return ""

def _parse_date_place(text, fallback_place):
    s = re.sub(r"\s+", " ", (text or "")).strip()
    # "川 23.10.12" 形式
    m = re.search(r"([^\d]{0,12})\s*(\d{2,4})\.(\d{1,2})\.(\d{1,2})", s)
    if m:
        raw_p = (m.group(1) or "").strip()
        y = int(m.group(2))
        if y < 100: y = 2000 + y
        return _normalize_place_token(raw_p, fallback_place), f"{y}/{m.group(3)}/{m.group(4)}"
    
    # "2023/10/12" 形式
    m = re.search(r"([^\d]{0,12})\s*(\d{4})/(\d{1,2})/(\d{1,2})", s)
    if m:
        raw_p = (m.group(1) or "").strip()
        return _normalize_place_token(raw_p, fallback_place), f"{m.group(2)}/{m.group(3)}/{m.group(4)}"
    return fallback_place, "不明"

def _parse_one_history(z_cell, fallback_place, resources):
    """
    セルごとの解析処理（安全策強化版）
    """
    if not z_cell:
        return {"place": "", "ymd": "", "dist": "", "rank": "", "pop": "", "agari": "", "pas": "", "jockey_full": "", "raw": ""}
        
    z_text = z_cell.get_text(" ", strip=True)
    place, ymd = _parse_date_place(z_text, fallback_place)
    
    # 距離
    m_dist = re.search(r"(?:ダ|芝)\s*([0-9]{3,4})", z_text) or re.search(r"([0-9]{3,4})m", z_text)
    dist = m_dist.group(1) if m_dist else ""
    
    # 着順
    m_rank = re.search(r"(\d{1,2})\s*着", z_text)
    rank = m_rank.group(1) if m_rank else ""
    
    # 人気
    m_pop = re.search(r"(\d+)\s*人気", z_text)
    pop = f"{m_pop.group(1)}人" if m_pop else ""
    
    # 上がり
    m_agari = re.search(r"3F\s*[\d\.]+\s*\((\d+)\)", z_text)
    agari = f"3F{m_agari.group(1)}位" if m_agari else ""
    
    # 通過順
    pas = ""
    pos_p = z_cell.select_one("p.position")
    if pos_p:
        spans = [s.get_text(strip=True) for s in pos_p.find_all("span") if s.get_text(strip=True)]
        if spans: pas = "-".join(spans)
    else:
        m_pas = re.search(r"(\d{1,2}-\d{1,2}(?:-\d{1,2})*)", z_text)
        if m_pas: pas = m_pas.group(1)

    # 騎手（人気行から抽出）
    j_prev = ""
    for p in z_cell.select("p.nk23_u-text10"):
        pt = p.get_text(" ", strip=True)
        # "2人気 吉原寛 56.0" のパターン
        m = re.search(r"(?:\d+人気)?.*?([^\s\d]{1,8})\s*(\d{2}\.\d)", pt)
        if m:
            j_prev = m.group(1).strip()
            break

    j_prev_full = normalize_name(j_prev, resources["jockeys"])

    return {
        "place": place, "ymd": (ymd or "").replace("着", "").strip(),
        "dist": dist, "rank": rank, "pop": pop, "agari": agari, "pas": pas,
        "jockey_full": j_prev_full, "raw": z_text,
    }

def parse_nankankeiba_detail(html, place_name, resources):
    soup = BeautifulSoup(html, "html.parser")
    data = {"meta": {}, "horses": {}}

    try:
        h3 = soup.find("h3", class_="nk23_c-tab1__title")
        data["meta"]["race_name"] = h3.get_text(strip=True) if h3 else ""
        if data["meta"]["race_name"]:
            parts = re.split(r"[ 　]+", data["meta"]["race_name"])
            data["meta"]["grade"] = parts[-1] if len(parts) > 1 else ""

        cond = soup.select_one("a.nk23_c-tab1__subtitle__text.is-blue")
        data["meta"]["course"] = f"{place_name} {cond.get_text(strip=True)}" if cond else ""
    except:
        pass

    shosai_area = soup.select_one("#shosai_aria")
    if not shosai_area: return data
    table = shosai_area.select_one("table.nk23_c-table22__table")
    if not table: return data

    for row in table.select("tbody tr"):
        try:
            # 馬番
            u_tag = row.select_one("td.umaban") or row.select_one("td.is-col02")
            if not u_tag: continue
            umaban = u_tag.get_text(strip=True)
            if not umaban.isdigit(): continue

            # 馬名
            h_link = row.select_one("td.is-col03 a.is-link") or row.select_one("td.pr-umaName-textRound a.is-link")
            horse_name = h_link.get_text(strip=True) if h_link else "不明"

            # 騎手・調教師
            jg_td = row.select_one("td.cs-g1")
            j_raw, t_raw = "", ""
            if jg_td:
                links = jg_td.select("a")
                if len(links) >= 1: j_raw = links[0].get_text(strip=True)
                if len(links) >= 2: t_raw = links[1].get_text(strip=True)

            j_full = normalize_name(j_raw, resources["jockeys"])
            t_full = normalize_name(t_raw, resources["trainers"])

            # 相性
            ai2 = row.select_one("td.cs-ai2 .graph_text_div")
            pair_stats = "-"
            if ai2 and "データ" not in ai2.get_text():
                try:
                    r = ai2.select_one(".is-percent").get_text(strip=True) if ai2.select_one(".is-percent") else ""
                    w = ai2.select_one(".is-number").get_text(strip=True) if ai2.select_one(".is-number") else ""
                    tt = ai2.select_one(".is-total").get_text(strip=True) if ai2.select_one(".is-total") else ""
                    if r and w and tt: pair_stats = f"勝{r}({w}/{tt})"
                except: pass

            # 過去走
            history_strs = []
            prev_power_val = ""
            prev_place = ""
            prev_jockey_full = ""

            for i in range(1, 4):
                z = row.select_one(f"td.cs-z{i}")
                if not z: continue
                one = _parse_one_history(z, place_name, resources)

                # 前走情報（1走目）
                if i == 1:
                    prev_place = one["place"]
                    prev_jockey_full = one["jockey_full"]
                    p_prev = resources["power_data"].get((prev_place, prev_jockey_full))
                    if p_prev and str(p_prev.get("power", "")).strip():
                        prev_power_val = str(p_prev["power"]).strip()

                ymd = one["ymd"]
                pl = one["place"]
                dist = one["dist"]
                jk = one["jockey_full"]
                pas = one["pas"] or "-"
                ag = one["agari"]
                rk = one["rank"] or ""
                pop = one["pop"] or ""
                ag_part = f"{ag}" if ag else ""
                
                if ymd: # 日付がある（＝走っている）場合のみ追加
                    history_strs.append(f"{ymd} {pl}{dist} {jk} {pas}({ag_part})→{rk}着({pop})")

            # 今回騎手パワー
            curr = resources["power_data"].get((place_name, j_full), {})
            curr_p = str(curr.get("power", "")).strip()
            curr_win = _format_rate(curr.get("win_rate"))
            curr_fuku = _format_rate(curr.get("fuku_rate"))
            p_disp = f"P:{curr_p}" if curr_p and curr_p not in ("-", "nan", "NaN") else "P:不明"
            stats_part = f"（勝{curr_win}複{curr_fuku}）" if (curr_win or curr_fuku) else ""
            prev_disp = prev_power_val if prev_power_val else "-"

            power_line = f"【騎手】{p_disp}{stats_part} 前P:{prev_disp} 相性:{pair_stats}"

            data["horses"][umaban] = {
                "name": horse_name, "jockey": j_full, "trainer": t_full,
                "compat": pair_stats, "hist": history_strs, "display_power": power_line,
                "prev_place": prev_place, "prev_jockey_full": prev_jockey_full, "prev_power_val": prev_power_val,
            }
        except Exception:
            # 個別の馬データのパース失敗はスキップして次へ
            traceback.print_exc()
            continue
            
    return data


# ==================================================
# 6. 対戦表
# ==================================================
def _parse_grades_from_ai(text):
    grades = {}
    for line in (text or "").split("\n"):
        m = re.search(r"([SABCDE])\s*[:：]?\s*([^\s　]+)", line)
        if m:
            g, n = m.group(1), re.sub(r"[（\(].*?[）\)]", "", m.group(2)).strip()
            if n: grades[n] = g
    return grades

def _fetch_matchup_table_selenium(driver, nankan_id, grades):
    url = f"https://www.nankankeiba.com/taisen/{nankan_id}.do"
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 5)
        # テーブルが出るか、データなしメッセージが出るまで待つ
        try:
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "nk23_c-table08__table")))
        except:
            return "\n(対戦データなし)"

        soup = BeautifulSoup(driver.page_source, "html.parser")
        tbl = soup.find("table", class_="nk23_c-table08__table")
        if not tbl: return "\n(対戦データなし)"

        races = []
        # ヘッダー解析
        if tbl.find("thead"):
            # 先頭2列は馬情報なのでスキップ
            for col in tbl.find("thead").find_all(["th", "td"])[2:]:
                det = col.find(class_="nk23_c-table08__detail")
                if det:
                    link = col.find("a")
                    href = link.get("href", "") if link else ""
                    full_url = ""
                    if href:
                        id_match = re.search(r"(\d{10,})", href)
                        if id_match: full_url = f"https://www.nankankeiba.com/result/{id_match.group(1)}.do"
                        elif href.startswith("/"): full_url = "https://www.nankankeiba.com" + href
                        else: full_url = href
                    races.append({"title": det.get_text(" ", strip=True), "url": full_url, "results": []})

        if not races: return "\n(初対戦)"

        # ボディ解析
        if tbl.find("tbody"):
            for tr in tbl.find("tbody").find_all("tr"):
                u = tr.find("a", class_="nk23_c-table08__text")
                if not u: continue
                name = u.get_text(strip=True)
                
                # グレード紐付け
                grade = grades.get(name, "")
                if not grade:
                    for k, v in grades.items():
                        if k in name or name in k:
                            grade = v; break

                cells = tr.find_all(["td", "th"])
                idx_st = -1
                for i, c in enumerate(cells):
                    if c.find("a", class_="nk23_c-table08__text"): idx_st = i; break
                if idx_st == -1: continue

                # 各レースの結果
                for i, c in enumerate(cells[idx_st + 1:]):
                    if i >= len(races): break
                    rp = c.find("p", class_="nk23_c-table08__number")
                    rnk = ""
                    if rp:
                        sp = rp.find("span")
                        rnk = sp.get_text(strip=True) if sp else rp.get_text(strip=True).split("｜")[0].strip()
                    if rnk and (rnk.isdigit() or rnk in ["除外", "中止"]):
                        races[i]["results"].append({
                            "rank": rnk, "name": name, "grade": grade,
                            "sort": int(rnk) if rnk.isdigit() else 999
                        })

        out = ["\n【対戦表（AI評価付き）】"]
        for r in races:
            if not r["results"]: continue
            r["results"].sort(key=lambda x: x["sort"])
            line_parts = []
            for x in r["results"]:
                g = f"[{x['grade']}]" if x["grade"] else ""
                line_parts.append(f"{x['rank']}着 {x['name']}{g}")
            out.append(f"◆ {r['title']}\n" + " / ".join(line_parts) + (f"\nLink: {r['url']}" if r["url"] else ""))
        return "\n".join(out)
    except Exception as e:
        return f"(対戦表取得エラー: {e})"


# ==================================================
# 7. uma_shosai 遅延ロード対策（★最重要修正点）
# ==================================================
def _force_shosai_loaded(driver, timeout=30):
    """
    JSによるデータロード完了を「要素の中身」で判定する鉄壁ロジック
    """
    # 1. まずJSトリガーを確実に引く
    try:
        driver.execute_script("if(typeof changeShosai === 'function'){ changeShosai('s1'); }")
    except: pass

    # 2. WebDriverWaitで「データが入ったセル」が出現するのを待つ
    #    (td.cs-z1 内に文字が5文字以上入っている要素が1つでも見つかればOKとする)
    wait = WebDriverWait(driver, timeout)
    
    def data_is_populated(d):
        try:
            # テーブルが存在するか
            tbl = d.find_element(By.CSS_SELECTOR, "table.nk23_c-table22__table")
            if not tbl: return False
            
            # 過去走データセル(cs-z1)を取得
            cells = d.find_elements(By.CSS_SELECTOR, "td.cs-z1")
            
            # 「Loading...」ではなく、かつ空でもないセルがあるか確認
            valid_cells = 0
            for c in cells:
                txt = c.text.strip()
                if txt and "Loading" not in txt and len(txt) > 3:
                    valid_cells += 1
            
            # 全頭が初出走のレースは稀だが、少なくとも1頭分あればロード完了とみなす
            return valid_cells > 0
        except:
            return False

    try:
        wait.until(data_is_populated)
        time.sleep(0.5) # 念押しの安定待機
        return True
    except TimeoutException:
        # 新馬戦などで全頭データがない場合もありうるので、エラーにはせずログだけ出す
        print("Wait timeout or No history data found.")
        return False


# ==================================================
# 8. ジェネレータ（メイン）
# ==================================================
def run_races_iter(year, month, day, place_code, target_races, mode="dify"):
    resources = load_resources()

    kb_input_map = {"10": "大井", "11": "川崎", "12": "船橋", "13": "浦和"}
    nk_code_map = {"10": "20", "11": "21", "12": "19", "13": "18"}

    place_name = kb_input_map.get(place_code, "地方")
    nk_place_code = nk_code_map.get(place_code)
    driver = get_driver()

    try:
        yield {"type": "status", "data": f"📅 開催特定中 ({place_name})..."}
        kai, nichi = get_nankan_kai_nichi(month, day, place_name)
        if not kai:
            yield {"type": "error", "data": "開催特定失敗（日付・場所を確認してください）"}
            return
        yield {"type": "status", "data": f"✅ {place_name} 第{kai}回 {nichi}日目"}

        yield {"type": "status", "data": "🔑 競馬ブック ログイン中..."}
        login_keibabook_robust(driver)

        prog_url = f"https://www.nankankeiba.com/program/{year}{month}{day}{nk_place_code}.do"
        driver.get(prog_url)
        
        # プログラム一覧待ち
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "program_list")))
        except TimeoutException:
            yield {"type": "error", "data": "レース一覧の取得にタイムアウトしました"}
            return

        soup = BeautifulSoup(driver.page_source, "html.parser")
        r_nums = []
        for a in soup.find_all("a", href=True):
            if f"{year}{month}{day}{nk_place_code}" in a["href"] and "uma_shosai" not in a["href"]:
                f = a["href"].split("/")[-1].replace(".do", "")
                if len(f) == 16:
                    r_nums.append(int(f[14:16]))
        r_nums = sorted(list(set(r_nums))) or list(range(1, 13))

        for r_num in r_nums:
            if target_races and r_num not in target_races:
                continue

            yield {"type": "status", "data": f"🏇 {r_num}R データ解析中..."}
            
            # リトライ処理
            max_race_retries = 3
            success = False
            
            for attempt in range(max_race_retries):
                try:
                    nk_id = f"{year}{month}{day}{nk_place_code}{kai:02}{nichi:02}{r_num:02}"
                    kb_id = get_kb_url_id(year, month, day, place_code, nichi, r_num)
                    result_url = f"https://www.nankankeiba.com/result/{nk_id}.do"

                    danwa, cyokyo = parse_kb_danwa_cyokyo(driver, kb_id)

                    # uma_shosai
                    shosai_url = f"https://www.nankankeiba.com/uma_shosai/{nk_id}.do"
                    driver.get(shosai_url)
                    
                    # ロード待機
                    _force_shosai_loaded(driver, timeout=30)

                    nk_data = parse_nankankeiba_detail(driver.page_source, place_name, resources)
                    
                    if not nk_data["horses"]:
                        # データが空ならリトライ
                        if attempt < max_race_retries - 1:
                            driver.refresh()
                            time.sleep(2)
                            continue
                        else:
                            yield {"type": "error", "data": f"{r_num}R データ取得失敗(空データ)"}
                            break

                    header = (
                        f"レース名:{r_num}R {nk_data['meta'].get('race_name','')} "
                        f"格:{nk_data['meta'].get('grade','')} "
                        f"コース:{nk_data['meta'].get('course','')}"
                    )

                    horse_texts = []
                    for u in sorted(nk_data["horses"].keys(), key=int):
                        h = nk_data["horses"][u]
                        block = [
                            f"[{u}]{h['name']} 騎:{h['jockey']} 師:{h['trainer']}",
                            f"話:{danwa.get(u,'なし')}",
                            f"調:{cyokyo.get(u,'データなし')}",
                            h["display_power"],
                            "【近走】",
                        ]
                        block.extend(h["hist"])
                        horse_texts.append("\n".join(block))

                    full_prompt = header + "\n\n" + "\n\n".join(horse_texts)

                    if mode == "raw":
                        final_text = f"{full_prompt}\n\n詳細リンク: {result_url}"
                        yield {"type": "result", "race_num": r_num, "data": final_text}
                        time.sleep(1)
                        success = True
                        break

                    yield {"type": "status", "data": f"🤖 {r_num}R AI予測中..."}
                    ai_out = run_dify_prediction(full_prompt)
                    grades = _parse_grades_from_ai(ai_out)
                    match_txt = _fetch_matchup_table_selenium(driver, nk_id, grades)

                    ai_out_clean = re.sub(r"^\s*-{3,}\s*$", "", ai_out or "", flags=re.MULTILINE)
                    ai_out_clean = re.sub(r"\n{3,}", "\n\n", ai_out_clean).strip()

                    final_text = (
                        f"📅 {year}/{month}/{day} {place_name}{r_num}R\n\n"
                        f"=== 🤖AI予想 ===\n{ai_out_clean}\n\n"
                        f"{match_txt}\n\n詳細リンク: {result_url}"
                    )
                    yield {"type": "result", "race_num": r_num, "data": final_text}
                    
                    success = True
                    time.sleep(5) 
                    break 

                except Exception as e:
                    # エラーログ詳細表示
                    traceback.print_exc()
                    if attempt < max_race_retries - 1:
                        yield {"type": "status", "data": f"⚠️ {r_num}R エラーによりリトライ({attempt+1}/{max_race_retries})..."}
                        driver.delete_all_cookies() # クッキー削除でリセット試行
                        time.sleep(3)
                    else:
                        yield {"type": "error", "data": f"{r_num}R 取得断念: {e}"}

    except Exception as e:
        traceback.print_exc()
        yield {"type": "error", "data": f"Fatal Error: {e}"}
    finally:
        try:
            driver.quit()
        except:
            pass
