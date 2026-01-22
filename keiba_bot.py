import time
import re
import os
import json
import requests
import streamlit as st
import pandas as pd
from datetime import datetime

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

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

# Secrets
KEIBA_ID = st.secrets.get("KEIBA_ID", "")
KEIBA_PASS = st.secrets.get("KEIBA_PASS", "")
DIFY_API_KEY = st.secrets.get("DIFY_API_KEY", "")
DIFY_BASE_URL = st.secrets.get("DIFY_BASE_URL", "https://api.dify.ai")

# ==================================================
# ★ 先に normalize_name を定義（load_resourcesで使うため）
# ==================================================
def normalize_name(abbrev, full_list, priority_set=None):
    """
    略称をフルネームに正規化する。
    priority_setが指定されている場合、そこに含まれる名前を優先する（priority_setはフルネーム集合であること）。
    """
    if not abbrev:
        return ""

    # 余計な記号・数字・空白など除去（最大3文字でもここで整う）
    clean = re.sub(r"[ 　▲△☆◇★\d\.]+", "", str(abbrev))
    clean = clean.strip()
    if not clean:
        return ""

    if not full_list:
        return clean

    # 完全一致（フルネームが来たときはそのまま）
    if clean in full_list:
        return clean

    candidates = []
    for full in full_list:
        # 1) 連続一致（最優先）
        # 2) 文字が全部含まれる（次点、2～3文字でも拾える）
        if clean in full:
            diff = len(full) - len(clean)
            is_priority = 1 if (priority_set and full in priority_set) else 0
            # 連続一致は強く優先するため、contig=0 を最優先に
            candidates.append((0, -is_priority, diff, full))
        elif all(c in full for c in clean):
            diff = len(full) - len(clean)
            is_priority = 1 if (priority_set and full in priority_set) else 0
            candidates.append((1, -is_priority, diff, full))

    if candidates:
        # contig(0が最強) → priority(1が強いので-優先) → diff(短いほど) で決定
        candidates.sort(key=lambda x: (x[0], x[1], x[2]))
        return candidates[0][3]

    return clean

# ==================================================
# 2. 共通関数
# ==================================================
@st.cache_resource
def get_http_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    })
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

def get_driver():
    ops = Options()
    ops.add_argument("--headless=new")
    ops.add_argument("--no-sandbox")
    ops.add_argument("--disable-dev-shm-usage")
    ops.add_argument("--disable-gpu")
    ops.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=ops)

def login_keibabook_robust(driver):
    try:
        driver.get("https://s.keibabook.co.jp/login/login")
        time.sleep(1)
        if "logout" in driver.current_url or driver.find_elements(By.XPATH, "//a[contains(@href,'logout')]"):
            return True
        WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.NAME, "login_id"))).send_keys(KEIBA_ID)
        driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
        driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
        time.sleep(2)
        return True
    except Exception:
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
    for attempt in range(max_retries):
        full_response = ""
        try:
            with sess.post(url, headers=headers, json=payload, stream=True, timeout=120) as res:
                if res.status_code == 429:
                    time.sleep(60)
                    continue
                if res.status_code != 200:
                    return f"⚠️ Dify Error: {res.status_code}"

                for line in res.iter_lines():
                    if line:
                        decoded_line = line.decode("utf-8")
                        if decoded_line.startswith("data:"):
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
                                elif event == "text_chunk" or event == "message":
                                    chunk = data.get("data", {}).get("text", "")
                                    full_response += chunk
                            except:
                                pass
                return full_response if full_response else "（回答生成エラー）"
        except Exception:
            time.sleep(5)
    return "⚠️ エラー: リトライ上限を超えました"

# ==================================================
# 4. データロード & 解析 (★近走騎手名フルネーム化が確実に叶う版)
# ==================================================
@st.cache_resource
def load_resources():
    res = {
        "jockeys": [],        # ★フルネームのみ（JOCKEY_FILE由来）
        "trainers": [],
        "power_data": {},     # (場所, 騎手フル名) -> {power, win, fuku}
        "power_jockeys": set()  # ★フルネーム集合（priority用）
    }

    # パス解決ヘルパー
    def get_valid_path(target_path):
        if os.path.exists(target_path):
            return target_path
        basename = os.path.basename(target_path)
        p2 = os.path.join(DATA_DIR, basename)
        if os.path.exists(p2):
            return p2
        if os.path.exists(basename):
            return basename
        return None

    # 1. 騎手・調教師リスト読み込み（フルネーム想定）
    j_path = get_valid_path(JOCKEY_FILE)
    if j_path:
        for enc in ["utf-8-sig", "cp932"]:
            try:
                with open(j_path, "r", encoding=enc) as f:
                    # ★1行1名の前提でフルネームだけを作る
                    res["jockeys"] = [
                        l.strip().replace(" ", "").replace("　", "")
                        for l in f if l.strip()
                    ]
                break
            except:
                continue

    t_path = get_valid_path(TRAINER_FILE)
    if t_path:
        for enc in ["utf-8-sig", "cp932"]:
            try:
                with open(t_path, "r", encoding=enc) as f:
                    res["trainers"] = [
                        l.strip().replace(",", "").replace(" ", "").replace("　", "")
                        for l in f if l.strip()
                    ]
                break
            except:
                continue

    # 2. 騎手パワーCSV読み込み
    # ★ここが重要：POWER_FILE側の騎手名（短縮表記の可能性あり）をフルネームに正規化して保存する
    p_path = get_valid_path(POWER_FILE)
    if p_path:
        df = None
        for enc in ["utf-8-sig", "cp932"]:
            try:
                df = pd.read_csv(p_path, encoding=enc)
                break
            except:
                continue

        if df is not None:
            try:
                place_col = df.columns[0]
                has_win = "勝率" in df.columns
                has_fuku = "複勝率" in df.columns
                has_power = "騎手パワー" in df.columns
                has_name = "騎手名" in df.columns

                for _, row in df.iterrows():
                    p = str(row[place_col]).strip()
                    j_raw = row["騎手名"] if has_name else ""
                    j_raw = str(j_raw).replace(" ", "").replace("　", "").strip()

                    if not j_raw or not p:
                        continue

                    # ★POWER側の名前を「フルネーム候補」に正規化（ここで最大3文字→フル名へ）
                    j_full = normalize_name(j_raw, res["jockeys"], priority_set=None)

                    # power_jockeys はフルネーム集合（priority用）
                    if j_full:
                        res["power_jockeys"].add(j_full)

                    val_power = str(row["騎手パワー"]) if has_power else "-"
                    val_win = str(row["勝率"]) if has_win else "-"
                    val_fuku = str(row["複勝率"]) if has_fuku else "-"

                    # ★キー: (場所, 騎手フル名) に統一
                    key_t = (p, j_full if j_full else j_raw)
                    res["power_data"][key_t] = {
                        "power": val_power,
                        "win": val_win,
                        "fuku": val_fuku
                    }

                # ★ここは削除：POWER_FILE由来の「短い騎手名」を jockeys に混ぜない
                # （混ぜると normalize_name が短い方で確定してしまい、フルネーム化が失敗する）
                # current_jockeys = set(res["jockeys"])
                # for j in res["power_jockeys"]:
                #     if j not in current_jockeys:
                #         res["jockeys"].append(j)

            except Exception:
                pass

    return res

def parse_nankankeiba_detail(html, place_name, resources):
    soup = BeautifulSoup(html, "html.parser")
    data = {"meta": {}, "horses": {}}

    h3 = soup.find("h3", class_="nk23_c-tab1__title")
    data["meta"]["race_name"] = h3.get_text(strip=True) if h3 else ""
    if data["meta"]["race_name"]:
        parts = re.split(r"[ 　]+", data["meta"]["race_name"])
        data["meta"]["grade"] = parts[-1] if len(parts) > 1 else ""
    cond = soup.select_one("a.nk23_c-tab1__subtitle__text.is-blue")
    data["meta"]["course"] = f"{place_name} {cond.get_text(strip=True)}" if cond else ""

    shosai_area = soup.select_one("#shosai_aria")
    if not shosai_area:
        return data

    table = shosai_area.select_one("table.nk23_c-table22__table")
    if not table:
        return data

    PLACE_MAP = {"船": "船橋", "大": "大井", "川": "川崎", "浦": "浦和", "門": "門別", "盛": "盛岡", "水": "水沢", "笠": "笠松", "名": "名古屋", "園": "園田", "姫": "姫路", "高": "高知", "佐": "佐賀"}
    KNOWN_PLACES = list(PLACE_MAP.values()) + ["JRA"]

    for row in table.select("tbody tr"):
        try:
            u_tag = row.select_one("td.umaban") or row.select_one("td.is-col02")
            if not u_tag:
                continue
            umaban = u_tag.get_text(strip=True)
            if not umaban.isdigit():
                continue
            h_link = row.select_one("td.is-col03 a.is-link") or row.select_one("td.pr-umaName-textRound a.is-link")
            horse_name = h_link.get_text(strip=True) if h_link else "不明"

            # --- 今回の騎手・調教師 ---
            jg_td = row.select_one("td.cs-g1")
            j_raw, t_raw = "", ""
            if jg_td:
                links = jg_td.select("a")
                if len(links) >= 1:
                    j_raw = links[0].get_text(strip=True)
                if len(links) >= 2:
                    t_raw = links[1].get_text(strip=True)

            # 正規化
            j_full = normalize_name(j_raw, resources["jockeys"], resources["power_jockeys"])
            t_full = normalize_name(t_raw, resources["trainers"], None)

            # --- 今回の騎手データ ---
            p_data_curr = resources["power_data"].get((place_name, j_full))
            curr_power_str = "P:不明"
            if p_data_curr:
                cp = p_data_curr["power"]
                cw = p_data_curr["win"].replace("%", "")
                cf = p_data_curr["fuku"].replace("%", "")
                curr_power_str = f"P:{cp}(勝{cw}%複{cf}%)"

            ai2 = row.select_one("td.cs-ai2 .graph_text_div")
            pair_stats = "-"
            if ai2 and "データ" not in ai2.get_text():
                r = ai2.select_one(".is-percent").get_text(strip=True)
                w = ai2.select_one(".is-number").get_text(strip=True)
                t = ai2.select_one(".is-total").get_text(strip=True)
                pair_stats = f"勝{r}({w}/{t})"

            history = []
            prev_power_val = None

            # --- 近走データ (最大3走) ---
            for i in range(1, 4):
                z = row.select_one(f"td.cs-z{i}")
                if not z:
                    continue
                z_full_text = z.get_text(" ", strip=True)
                if not z_full_text:
                    continue

                # 1. 日付と開催場
                d_txt = ""
                place_short = ""
                d_div = z.select_one("p.nk23_u-d-flex")

                if d_div:
                    d_raw = d_div.get_text(" ", strip=True)
                    m_dt = re.search(r"(\d+\.\d+\.\d+)", d_raw)
                    if m_dt:
                        d_txt = m_dt.group(1)

                    rem_text = d_raw.replace(d_txt, "") if d_txt else d_raw
                    for kp in KNOWN_PLACES:
                        if kp in rem_text:
                            place_short = kp
                            break
                    if not place_short:
                        for k, v in PLACE_MAP.items():
                            if k in rem_text:
                                place_short = v
                                break

                if not d_txt:
                    d_txt = "不明"
                if not place_short:
                    place_short = place_name

                # 2. 距離
                dm = re.search(r"(\d{3,4})m?", z_full_text)
                dist = dm.group(1) if dm else ""

                # ==================================================
                # ★ 3. 着順 (修正：能試・取消・除外に対応)
                # ==================================================
                rank = ""
                # 通常の着順タグ (例: 1着, 2着...)
                r_tag = z.select_one(".nk23_u-text19")
                
                if r_tag:
                    # 数字のみを取り出す
                    rank = r_tag.get_text(strip=True).replace("着", "")
                else:
                    # 着順がない場合、特殊タグ(能試、取消、除外など)を探す
                    special_tag = z.select_one(".nk23_u-text16")
                    if special_tag:
                        # "能試" や "取消" という文字をそのまま取得
                        rank = special_tag.get_text(strip=True)
                # ==================================================

                # 4. 騎手(略称)・人気
                j_prev, pop = "", ""
                p_lines = z.select("p.nk23_u-text10")
                for p in p_lines:
                    txt = p.get_text(strip=True)
                    if "人気" in txt:
                        pm = re.search(r"(\d+)人気", txt)
                        if pm:
                            pop = f"{pm.group(1)}人"
                        spans = p.find_all("span")
                        if len(spans) >= 2:
                            j_cand = spans[1].get_text(strip=True)
                            j_prev = re.sub(r"[\d\.]+", "", j_cand)
                        break

                # 5. 上がり3F (タグ取得版)
                agari = ""
                ft_elem = z.select_one(".furlongtime")
                if ft_elem:
                    raw_agari = ft_elem.get_text(strip=True)
                    if raw_agari:
                        agari = raw_agari

                # 6. 通過順
                pos_p = z.select_one("p.position")
                pas = ""
                if pos_p:
                    pas_spans = [s.get_text(strip=True) for s in pos_p.find_all("span")]
                    pas = "-".join(pas_spans)

                # 7. 騎手名の正規化
                j_prev_full = normalize_name(j_prev, resources["jockeys"], resources["power_jockeys"])
                if not j_prev_full and j_prev:
                    j_prev_full = j_prev

                # ★ 前走(i=1)のP取得 ★
                if i == 1:
                    p_key = (place_short, j_prev_full)
                    p_data_prev = resources["power_data"].get(p_key)
                    if p_data_prev:
                        prev_power_val = p_data_prev["power"]

                # ==================================================
                # ★ 文字列生成 (修正：着順の表示分け)
                # ==================================================
                agari_part = f"({agari})" if agari else ""
                pop_part = f"({pop})" if pop else ""
                
                # rankが数字なら「着」をつける。それ以外（能試・取消など）ならそのまま表示。
                if rank.isdigit():
                    rank_part = f"{rank}着"
                elif rank:
                    rank_part = rank  # "能試", "取消" など
                else:
                    rank_part = "着不明"

                h_str = f"{d_txt} {place_short}{dist} {j_prev_full} {pas}{agari_part}→{rank_part}{pop_part}"
                history.append(h_str)
                # ==================================================
            # --- 最終表示用 ---
            if prev_power_val:
                power_line = f"【騎手】{curr_power_str}(前P:{prev_power_val})、 相性:{pair_stats}"
            else:
                power_line = f"【騎手】{curr_power_str}、 相性:{pair_stats}"

            data["horses"][umaban] = {
                "name": horse_name, "jockey": j_full, "trainer": t_full,
                "power": curr_power_str,
                "compat": pair_stats, "hist": history,
                "display_power": power_line
            }

        except Exception:
            continue
    return data
# ==================================================
# 5. ヘルパー関数 (URL, カレンダー等)
# ==================================================
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
                days_part = text.split("月")[1]
                days_match = re.findall(r"(\d+)", days_part)
                days_list = [int(d) for d in days_match if 1 <= int(d) <= 31]
                if target_d in days_list:
                    return int(kai_m.group(1)), days_list.index(target_d) + 1
        return None, None
    except:
        return None, None

def get_kb_url_id(year, month, day, place_code, nichi, race_num):
    return f"{year}{str(month).zfill(2)}{str(place_code).zfill(2)}{str(nichi).zfill(2)}{str(race_num).zfill(2)}{str(month).zfill(2)}{str(day).zfill(2)}"

def parse_kb_danwa_cyokyo(driver, kb_id):
    d_danwa, d_cyokyo = {}, {}
    try:
        driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{kb_id}")
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

        driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{kb_id}")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for tbl in soup.select("table.cyokyo"):
            rows = tbl.select("tbody tr")
            if not rows:
                continue
            r1 = rows[0]
            u_td = r1.select_one("td.umaban")
            if not u_td:
                continue
            uma = u_td.get_text(strip=True)
            tp_txt = r1.select_one("td.tanpyo").get_text(strip=True) if r1.select_one("td.tanpyo") else ""
            dt_txt = ""
            if len(rows) > 1:
                dt_raw = rows[1].get_text(" ", strip=True)
                dt_txt = re.sub(r"\s+", " ", dt_raw)
            d_cyokyo[uma] = f"【短評】{tp_txt} 【詳細】{dt_txt}"
    except:
        pass
    return d_danwa, d_cyokyo

def _parse_grades_from_ai(text):
    grades = {}
    for line in text.split("\n"):
        m = re.search(r"([SABCDE])\s*[:：]?\s*([^\s　]+)", line)
        if m:
            g, n = m.group(1), re.sub(r"[（\(].*?[）\)]", "", m.group(2)).strip()
            if n:
                grades[n] = g
    return grades

def _fetch_matchup_table_selenium(driver, nankan_id, grades):
    url = f"https://www.nankankeiba.com/taisen/{nankan_id}.do"
    try:
        driver.get(url)
        time.sleep(0.5)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        tbl = soup.find("table", class_="nk23_c-table08__table")
        if not tbl:
            return "\n(対戦データなし)"

        races = []
        if tbl.find("thead"):
            for col in tbl.find("thead").find_all(["th", "td"])[2:]:
                det = col.find(class_="nk23_c-table08__detail")
                if det:
                    link = col.find("a")
                    href = link.get("href", "") if link else ""
                    full_url = ""
                    if href:
                        id_match = re.search(r"(\d{10,})", href)
                        if id_match:
                            full_url = f"https://www.nankankeiba.com/result/{id_match.group(1)}.do"
                        elif href.startswith("/"):
                            full_url = "https://www.nankankeiba.com" + href
                        else:
                            full_url = href

                    races.append({
                        "title": det.get_text(" ", strip=True),
                        "url": full_url,
                        "results": []
                    })

        if not races:
            return "\n(初対戦)"

        if tbl.find("tbody"):
            for tr in tbl.find("tbody").find_all("tr"):
                u = tr.find("a", class_="nk23_c-table08__text")
                if not u:
                    continue
                name = u.get_text(strip=True)
                grade = grades.get(name, "")
                if not grade:
                    for k, v in grades.items():
                        if k in name or name in k:
                            grade = v
                            break
                cells = tr.find_all(["td", "th"])
                idx_st = -1
                for i, c in enumerate(cells):
                    if c.find("a", class_="nk23_c-table08__text"):
                        idx_st = i
                        break
                if idx_st == -1:
                    continue
                for i, c in enumerate(cells[idx_st + 1:]):
                    if i >= len(races):
                        break
                    rp = c.find("p", class_="nk23_c-table08__number")
                    rnk = ""
                    if rp:
                        sp = rp.find("span")
                        rnk = sp.get_text(strip=True) if sp else rp.get_text(strip=True).split("｜")[0].strip()
                    if rnk and (rnk.isdigit() or rnk in ["除外", "中止"]):
                        races[i]["results"].append({"rank": rnk, "name": name, "grade": grade, "sort": int(rnk) if rnk.isdigit() else 999})

        out = ["\n【対戦表（AI評価付き）】"]
        for r in races:
            if not r["results"]:
                continue
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
# 6. ジェネレータ
# ==================================================
def run_races_iter(year, month, day, place_code, target_races, mode="dify", **kwargs):
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
            yield {"type": "error", "data": "開催特定失敗"}
            return
        yield {"type": "status", "data": f"✅ {place_name} 第{kai}回 {nichi}日目"}

        yield {"type": "status", "data": "🔑 競馬ブック ログイン中..."}
        login_keibabook_robust(driver)

        prog_url = f"https://www.nankankeiba.com/program/{year}{month}{day}{nk_place_code}.do"
        driver.get(prog_url)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        r_nums = []
        for a in soup.find_all("a", href=True):
            if f"{year}{month}{day}{nk_place_code}" in a["href"] and "uma_shosai" not in a["href"]:
                f = a["href"].split("/")[-1].replace(".do", "")
                if len(f) == 16:
                    r_nums.append(int(f[14:16]))
        r_nums = sorted(list(set(r_nums))) or range(1, 13)

        for r_num in r_nums:
            if target_races and r_num not in target_races:
                continue

            yield {"type": "status", "data": f"🏇 {r_num}R データ解析中..."}

            try:
                nk_id = f"{year}{month}{day}{nk_place_code}{kai:02}{nichi:02}{r_num:02}"
                kb_id = get_kb_url_id(year, month, day, place_code, nichi, r_num)

                result_url = f"https://www.nankankeiba.com/result/{nk_id}.do"

                danwa, cyokyo = parse_kb_danwa_cyokyo(driver, kb_id)

                driver.get(f"https://www.nankankeiba.com/uma_shosai/{nk_id}.do")
                try:
                    driver.execute_script("if(typeof changeShosai === 'function'){ changeShosai('s1'); }")
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "shosai_aria")))
                    time.sleep(1.0)
                except TimeoutException:
                    yield {"type": "error", "data": f"{r_num}R 詳細データ読み込みタイムアウト"}
                    continue

                nk_data = parse_nankankeiba_detail(driver.page_source, place_name, resources)

                # リトライロジック
                if not nk_data["horses"]:
                    for _ in range(2):
                        time.sleep(1)
                        driver.execute_script("if(typeof changeShosai === 'function'){ changeShosai('s1'); }")
                        time.sleep(1)
                        nk_data = parse_nankankeiba_detail(driver.page_source, place_name, resources)
                        if nk_data["horses"]:
                            break

                if not nk_data["horses"]:
                    yield {"type": "error", "data": f"{r_num}R データなし (HTML解析失敗)"}
                    continue

                header = f"レース名:{r_num}R {nk_data['meta'].get('race_name','')} 格:{nk_data['meta'].get('grade','')} コース:{nk_data['meta'].get('course','')}"
                horse_texts = []
                for u in sorted(nk_data["horses"].keys(), key=int):
                    h = nk_data["horses"][u]

                    power_line = h.get("display_power", f"【騎手】{h['power']}、 相性:{h['compat']}")

                    block = [
                        f"[{u}]{h['name']} 騎:{h['jockey']} 師:{h['trainer']}",
                        f"話:{danwa.get(u,'なし')}",
                        f"調:{cyokyo.get(u,'データなし')}",
                        power_line,
                        "【近走】"
                    ]
                    for idx, hs in enumerate(h["hist"]):
                        block.append(f"{hs}")
                    horse_texts.append("\n".join(block))

                full_prompt = header + "\n\n" + "\n\n".join(horse_texts)

                if mode == "raw":
                    yield {"type": "status", "data": f"🔍 {r_num}R 対戦データを取得中..."}
                    match_txt = _fetch_matchup_table_selenium(driver, nk_id, grades={})
                    final_text = f"{full_prompt}\n\n{match_txt}\n\n詳細リンク: {result_url}"
                    yield {"type": "result", "race_num": r_num, "data": final_text}
                    time.sleep(1)
                    continue

                yield {"type": "status", "data": f"🤖 {r_num}R AI予測中..."}
                ai_out = run_dify_prediction(full_prompt)
                grades = _parse_grades_from_ai(ai_out)
                match_txt = _fetch_matchup_table_selenium(driver, nk_id, grades)
                ai_out_clean = re.sub(r"^\s*-{3,}\s*$", "", ai_out, flags=re.MULTILINE)
                ai_out_clean = re.sub(r"\n{3,}", "\n\n", ai_out_clean).strip()

                final_text = f"📅 {year}/{month}/{day} {place_name}{r_num}R\n\n=== 🤖AI予想 ===\n{ai_out_clean}\n\n{match_txt}\n\n詳細リンク: {result_url}"
                yield {"type": "result", "race_num": r_num, "data": final_text}
                time.sleep(15)

            except Exception as e:
                yield {"type": "error", "data": f"{r_num}R Error: {e}"}

    except Exception as e:
        yield {"type": "error", "data": f"Fatal: {e}"}
    finally:
        driver.quit()
