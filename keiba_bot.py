            # keiba_bot.py
import time
import re
import os
import html
import json
import requests
import streamlit as st
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException

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

KEIBA_ID = st.secrets.get("KEIBA_ID", "")
KEIBA_PASS = st.secrets.get("KEIBA_PASS", "")
DIFY_API_KEY = st.secrets.get("DIFY_API_KEY", "")
DIFY_BASE_URL = st.secrets.get("DIFY_BASE_URL", "https://api.dify.ai")


# ==================================================
# 0. 名前正規化
# ==================================================
def normalize_name(abbrev, full_list, priority_set=None):
    """
    略称をフルネームに正規化する。
    priority_setが指定されている場合、そこに含まれる名前を優先する（priority_setはフルネーム集合であること）。
    """
    if not abbrev:
        return ""

    clean = re.sub(r"[ 　▲△☆◇★\d\.]+", "", str(abbrev)).strip()
    if not clean:
        return ""

    if not full_list:
        return clean

    if clean in full_list:
        return clean

    candidates = []
    for full in full_list:
        if clean in full:
            diff = len(full) - len(clean)
            is_priority = 1 if (priority_set and full in priority_set) else 0
            candidates.append((0, -is_priority, diff, full))
        elif all(c in full for c in clean):
            diff = len(full) - len(clean)
            is_priority = 1 if (priority_set and full in priority_set) else 0
            candidates.append((1, -is_priority, diff, full))

    if candidates:
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


def login_keibabook_robust(driver) -> bool:
    try:
        driver.get("https://s.keibabook.co.jp/login/login")
        time.sleep(1)

        # 既ログイン判定（ざっくり）
        if "logout" in driver.current_url or driver.find_elements(By.XPATH, "//a[contains(@href,'logout')]"):
            return True

        WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located((By.NAME, "login_id"))
        ).send_keys(KEIBA_ID)

        driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
        driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
        time.sleep(2)
        return True
    except Exception:
        return False


# ==================================================
# 3. Dify API
# ==================================================
def run_dify_prediction(full_text: str) -> str:
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
                            if isinstance(outputs, dict) and "text" in outputs:
                                return outputs["text"] or ""

                        elif event in ("text_chunk", "message"):
                            chunk = data.get("data", {}).get("text", "")
                            if chunk:
                                full_response += chunk
                    except Exception:
                        pass

                return full_response if full_response else "（回答生成エラー）"
        except Exception:
            time.sleep(5)

    return "⚠️ エラー: リトライ上限を超えました"


# ==================================================
# 4. データロード
# ==================================================
@st.cache_resource
def load_resources():
    res = {
        "jockeys": [],
        "trainers": [],
        "power_data": {},         # (場所, 騎手フル名) -> {power, win, fuku}
        "power_jockeys": set()    # フルネーム集合（priority用）
    }

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

    # 騎手リスト（フルネーム）
    j_path = get_valid_path(JOCKEY_FILE)
    if j_path:
        for enc in ("utf-8-sig", "cp932"):
            try:
                with open(j_path, "r", encoding=enc) as f:
                    res["jockeys"] = [l.strip().replace(" ", "").replace("　", "") for l in f if l.strip()]
                break
            except Exception:
                continue

    # 調教師
    t_path = get_valid_path(TRAINER_FILE)
    if t_path:
        for enc in ("utf-8-sig", "cp932"):
            try:
                with open(t_path, "r", encoding=enc) as f:
                    res["trainers"] = [l.strip().replace(",", "").replace(" ", "").replace("　", "") for l in f if l.strip()]
                break
            except Exception:
                continue

    # 騎手パワー
    p_path = get_valid_path(POWER_FILE)
    if p_path:
        df = None
        for enc in ("utf-8-sig", "cp932"):
            try:
                df = pd.read_csv(p_path, encoding=enc)
                break
            except Exception:
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
                    j_raw = str(row["騎手名"]).replace(" ", "").replace("　", "").strip() if has_name else ""
                    if not p or not j_raw:
                        continue

                    j_full = normalize_name(j_raw, res["jockeys"], priority_set=None)
                    if j_full:
                        res["power_jockeys"].add(j_full)

                    val_power = str(row["騎手パワー"]) if has_power else "-"
                    val_win = str(row["勝率"]) if has_win else "-"
                    val_fuku = str(row["複勝率"]) if has_fuku else "-"

                    key_t = (p, j_full if j_full else j_raw)
                    res["power_data"][key_t] = {"power": val_power, "win": val_win, "fuku": val_fuku}

            except Exception:
                pass

    return res


# ==================================================
# 5. 南関：馬詳細ページ解析（前走騎手を取る）
# ==================================================
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

    PLACE_MAP = {"船": "船橋", "大": "大井", "川": "川崎", "浦": "浦和", "門": "門別", "盛": "盛岡", "水": "水沢",
                 "笠": "笠松", "名": "名古屋", "園": "園田", "姫": "姫路", "高": "高知", "佐": "佐賀"}
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

            # 今回の騎手・調教師
            jg_td = row.select_one("td.cs-g1")
            j_raw, t_raw = "", ""
            if jg_td:
                links = jg_td.select("a")
                if len(links) >= 1:
                    j_raw = links[0].get_text(strip=True)
                if len(links) >= 2:
                    t_raw = links[1].get_text(strip=True)

            j_full = normalize_name(j_raw, resources["jockeys"], resources["power_jockeys"])
            t_full = normalize_name(t_raw, resources["trainers"], None)

            # 今回の騎手データ
            p_data_curr = resources["power_data"].get((place_name, j_full))
            curr_power_str = "P:不明"
            if p_data_curr:
                cp = p_data_curr["power"]
                cw = str(p_data_curr["win"]).replace("%", "")
                cf = str(p_data_curr["fuku"]).replace("%", "")
                curr_power_str = f"P:{cp}(勝{cw}%複{cf}%)"

            # 相性
            ai2 = row.select_one("td.cs-ai2 .graph_text_div")
            pair_stats = "-"
            if ai2 and "データ" not in ai2.get_text():
                r = ai2.select_one(".is-percent").get_text(strip=True)
                w = ai2.select_one(".is-number").get_text(strip=True)
                t = ai2.select_one(".is-total").get_text(strip=True)
                pair_stats = f"勝{r}({w}/{t})"

            history = []
            prev_power_val = None
            prev_jockey_latest = ""

            for i in range(1, 4):
                z = row.select_one(f"td.cs-z{i}")
                if not z:
                    continue
                z_full_text = z.get_text(" ", strip=True)
                if not z_full_text:
                    continue
                
                # ULR取得
                a_tag = z.select_one("a.is-link.event_stop")
                past_url = ""
                if a_tag and a_tag.get("href"):
                    past_url = "https://www.nankankeiba.com" + a_tag.get("href")

                # 日付と開催場
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

                # 距離
                dm = re.search(r"(\d{3,4})m?", z_full_text)
                dist = dm.group(1) if dm else ""

                # 着順（能試/取消/除外対応）
                rank = ""
                r_tag = z.select_one(".nk23_u-text19")
                if r_tag:
                    rank = r_tag.get_text(strip=True).replace("着", "")
                else:
                    special_tag = z.select_one(".nk23_u-text16")
                    if special_tag:
                        rank = special_tag.get_text(strip=True)

                # 騎手(略称)・人気
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

                # 上がり3F
                agari = ""
                ft_elem = z.select_one(".furlongtime")
                if ft_elem:
                    agari = ft_elem.get_text(strip=True) or ""

                # 通過順
                pos_p = z.select_one("p.position")
                pas = ""
                if pos_p:
                    pas_spans = [s.get_text(strip=True) for s in pos_p.find_all("span")]
                    pas = "-".join(pas_spans)

                # 騎手名正規化
                j_prev_full = normalize_name(j_prev, resources["jockeys"], resources["power_jockeys"])
                if not j_prev_full and j_prev:
                    j_prev_full = j_prev

                # 最新近走(i=1)の騎手＆P
                if i == 1:
                    prev_jockey_latest = j_prev_full
                    p_key = (place_short, j_prev_full)
                    p_data_prev = resources["power_data"].get(p_key)
                    if p_data_prev:
                        prev_power_val = p_data_prev["power"]

                agari_part = f"({agari})" if agari else ""
                pop_part = f"({pop})" if pop else ""
                if str(rank).isdigit():
                    rank_part = f"{rank}着"
                elif rank:
                    rank_part = rank
                else:
                    rank_part = "着不明"

                h_str = f"{d_txt} {place_short}{dist} {j_prev_full} {pas}{agari_part}→{rank_part}{pop_part}"
                history.append({
                    "text": h_str,
                    "url": past_url,
                    "place": place_short,
                    "dist": dist,
                    "pas": pas
                })

            # 表示用騎手行
            if prev_power_val:
                power_line = f"【騎手】{curr_power_str}(前P:{prev_power_val})、 相性:{pair_stats}"
            else:
                power_line = f"【騎手】{curr_power_str}、 相性:{pair_stats}"

            data["horses"][umaban] = {
                "name": horse_name,
                "jockey": j_full,
                "trainer": t_full,
                "power": curr_power_str,
                "prev_jockey": prev_jockey_latest,
                "compat": pair_stats,
                "hist": history,
                "display_power": power_line,
            }

        except Exception:
            continue

    return data


# ==================================================
# 6. 開催特定など
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
    except Exception:
        return None, None


def get_kb_url_id(year, month, day, place_code, nichi, race_num):
    return f"{year}{str(month).zfill(2)}{str(place_code).zfill(2)}{str(nichi).zfill(2)}{str(race_num).zfill(2)}{str(month).zfill(2)}{str(day).zfill(2)}"


# ==================================================
# 7. 競馬ブック（談話/調教）
# ==================================================
def parse_kb_danwa_cyokyo(driver, kb_id):
    d_danwa, d_cyokyo = {}, {}
    try:
        # --- 談話 ---
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

        # --- 調教 ---
        driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{kb_id}")
        if "login" in driver.current_url:
            if login_keibabook_robust(driver):
                driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{kb_id}")

        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.default.cyokyo"))
            )
        except Exception:
            pass

        soup = BeautifulSoup(driver.page_source, "html.parser")
        for tbl in soup.select("table.default.cyokyo"):
            try:
                u_td = tbl.select_one("td.umaban")
                if not u_td:
                    continue
                uma = u_td.get_text(strip=True)

                tp_td = tbl.select_one("td.tanpyo")
                tp_txt = tp_td.get_text(strip=True) if tp_td else ""

                tbody_rows = tbl.select("tbody tr")
                if len(tbody_rows) < 2:
                    d_cyokyo[uma] = f"【短評】{tp_txt}"
                    continue

                content_td = tbody_rows[1].find("td")
                if not content_td:
                    d_cyokyo[uma] = f"【短評】{tp_txt}"
                    continue

                cyokyo_lines = []
                dls = content_td.select("dl.dl-table")

                for dl in dls:
                    first_dt = dl.find("dt")
                    first_dt_text = first_dt.get_text(strip=True) if first_dt else ""
                    label = "前走調教" if "(前回)" in first_dt_text else "今走調教"

                    dt_left = dl.select_one("dt.left")
                    info_text = dt_left.get_text(" ", strip=True) if dt_left else ""
                    dt_right = dl.select_one("dt.right")
                    cond_text = dt_right.get_text(strip=True) if dt_right else ""

                    next_node = dl.find_next_sibling()
                    while next_node and next_node.name != "table":
                        next_node = next_node.find_next_sibling()

                    time_data = ""
                    if next_node and "cyokyodata" in (next_node.get("class") or []):
                        data_rows = []
                        for tr in next_node.select("tr"):
                            cells = [td.get_text(strip=True) for td in tr.find_all("td") if td.get_text(strip=True)]
                            if cells:
                                data_rows.append(" ".join(cells))
                        time_data = " / ".join(data_rows)

                    line = f"{label}：{info_text} {cond_text} {time_data}".strip()
                    line = re.sub(r"\s{2,}", " ", line)
                    cyokyo_lines.append(line)

                full_text = f"【短評】{tp_txt}".strip()
                if cyokyo_lines:
                    full_text += "\n" + "\n".join(cyokyo_lines)

                d_cyokyo[uma] = full_text

            except Exception:
                continue

    except Exception:
        pass

    return d_danwa, d_cyokyo

# ==================================================
# 7.5 Dify出力 × Python調教 注入（ランク列の直前に確実に入れる版）
# ==================================================
def _flatten_for_md_cell(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\n", " / ")
    s = re.sub(r"\s{2,}", " ", s).strip()
    s = s.replace("|", "｜")  # セル破壊防止
    return s


def _split_md_row_4cols_safe(line: str):
    """
    Markdown表の1行を「4列」として安全に分解する。
    途中の本文に '|' が含まれても壊さない。
    戻り値: (col1, col2, col3, col4) or None
    """
    s = line.strip()
    if not s.startswith("|"):
        return None

    # 両端の '|' を落とす（末尾に無いケースもあるのでrstripはしない）
    core = s[1:]
    # 末尾に '|' があるなら外す
    if core.endswith("|"):
        core = core[:-1]

    # 最初の2つの区切り '|' の位置
    p1 = core.find("|")
    if p1 == -1:
        return None
    p2 = core.find("|", p1 + 1)
    if p2 == -1:
        return None

    # 最後の区切り（ランク列の前）
    plast = core.rfind("|")
    if plast == -1 or plast <= p2:
        return None

    c1 = core[:p1].strip()
    c2 = core[p1 + 1:p2].strip()
    c3 = core[p2 + 1:plast].strip()   # ←ここは '|' を含んでもOK（本文全体）
    c4 = core[plast + 1:].strip()

    return c1, c2, c3, c4


def inject_cyokyo_before_rank(ai_text: str, cyokyo: dict) -> str:
    """
    各馬行の「スコア・詳細（3列目）」の末尾に【調教】を追記する。
    → 結果として、行末の「| ランク |」の直前に必ず【調教】が入る。
    """
    if not ai_text:
        return ai_text

    cy_map = {str(k).strip(): v for k, v in (cyokyo or {}).items()}

    out = []
    for line in ai_text.splitlines():
        s = line.strip()

        # 表じゃない行はそのまま
        if not s.startswith("|"):
            out.append(line)
            continue

        # 罫線行はそのまま
        if set(s.replace("|", "").strip()) <= set("-: "):
            out.append(line)
            continue

        cols = _split_md_row_4cols_safe(line)
        if not cols:
            # 想定外の表行 → そのまま
            out.append(line)
            continue

        c1, c2, c3, c4 = cols

        # ヘッダ行はそのまま
        if c1 in ("馬番", "馬 番", "#", "No", "No.", "番号"):
            out.append(line)
            continue

        # 馬番抽出
        m = re.search(r"\d+", c1)
        umaban = m.group(0) if m else ""

        # 調教追記（ランク列の直前＝3列目の末尾）
        if umaban and umaban in cy_map:
            cy = _flatten_for_md_cell(cy_map.get(umaban, ""))
            if cy:
                # すでに【調教】が入ってたら二重に入れない
                if "【調教】" not in c3:
                    c3 = (c3.rstrip() + f" 【調教】{cy}").strip()

        out.append(f"| {c1} | {c2} | {c3} | {c4} |")

    return "\n".join(out)


# ==================================================
# 8. Dify出力からランク抽出（JSONでもMarkdownでもOK）
# ==================================================
import unicodedata

def _norm_horse_name(s: str) -> str:
    """
    馬名照合用に正規化（NFKC + 空白除去 + 括弧書き除去 + 記号除去）
    """
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = s.strip()
    # 全角/半角スペースを含む空白を全部落とす（馬名は基本スペース無し想定）
    s = re.sub(r"\s+", "", s.replace("　", " "))
    # 括弧内の注記を除去
    s = re.sub(r"[（(].*?[）)]", "", s)
    # 表装飾系を軽く除去
    s = re.sub(r"[*_`]", "", s)
    return s

def _parse_grades_from_ai(ai_out: str):
    """
    Dify出力（JSON/Markdown/パイプ区切り）から馬名→ランク(S～G)を抽出して返す。
    返すdictのキーは _norm_horse_name() 済み。
    """
    grades = {}

    text = ai_out or ""

    # JSON {"text": "..."} 形式なら展開
    try:
        j = json.loads(text)
        if isinstance(j, dict) and isinstance(j.get("text"), str):
            text = j["text"]
    except Exception:
        pass

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # 既存のパイプ切り等の処理
        if "|" in line:
            tmp = line.replace("|", "").strip()
            if not tmp or set(tmp) <= set("-: "):
                continue
            core = line[1:] if line.startswith("|") else line
            core = core[:-1] if core.endswith("|") else core
            cols = [c.strip() for c in core.split("|")]
            if len(cols) >= 4:
                rank = cols[-1].strip()
                if re.fullmatch(r"[SABCDEFG]", rank):
                    horse = re.split(r"\s*騎[:：]", cols[1], maxsplit=1)[0].strip()
                    horse = _norm_horse_name(horse)
                    if horse:
                        grades[horse] = rank
                        continue

        # 新フォーマット対応: "①アルマリアルト　D" または "[1]アルマリアルト D"
        # マーク（丸文字やカッコ数字）や「馬名」＋空白＋「アルファベット1文字」を探す
        m = re.search(r"(?:[①-⑳]|\[\d+\])?\s*([^\s　]+)[\s　]+([SABCDEFG])\s*$", line)
        if m:
            horse = _norm_horse_name(m.group(1))
            rank = m.group(2)
            if horse:
                grades[horse] = rank
                continue
                
    # フォールバック（例: "D:ナツハヤテ" みたいな形式）
    if not grades:
        for line in text.split("\n"):
            m = re.search(r"([SABCDEFG])\s*[:：]?\s*([^\s　|]+)", line)
            if m:
                g = m.group(1)
                n = _norm_horse_name(m.group(2))
                if n:
                    grades[n] = g

    return grades



# ==================================================
# 8.5 過去レースからのテン速度取得
# ==================================================
def fetch_past_race_2f_times(url, driver=None):
    """
    過去レースURLからラップタイムを取得し、最初の2区間(約2F)の速度(km/h)を返す
    """
    sess = get_http_session()
    try:
        import time
        time.sleep(0.5) # サーバー負荷・IP制限回避のためのウェイト
        res = sess.get(url, timeout=5)
        res.encoding = "cp932"
        # ラップタイムの抽出 (例: 6.1-12.0-14.3-...)
        m = re.search(r'([0-9]{1,2}\.[0-9]-[0-9]{1,2}\.[0-9](?:-[0-9]{1,2}\.[0-9])*)', res.text)
        if not m:
            return None
            
        parts = m.group(1).split('-')
        if len(parts) < 2:
            return None
            
        p0 = float(parts[0])
        p1 = float(parts[1])
        
        # 距離の推定: 時間が8秒未満なら100m、それ以上なら200m
        dist_0 = 100 if p0 < 8.0 else 200
        dist_1 = 100 if p1 < 8.0 else 200
        total_dist = dist_0 + dist_1
        total_time = p0 + p1
        
        if total_time <= 0:
            return None
            
        # km/h に変換
        speed_kmh = (total_dist / total_time) * 3.6
        return speed_kmh
    except Exception:
        return None

# ==================================================
# 9. 対戦表（AI評価付き） ※ここが今回の肝：out/return をループ外へ
# ==================================================
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
        # ヘッダ（過去レース列）取得
        thead = tbl.find("thead")
        if thead:
            for col in thead.find_all(["th", "td"])[2:]:
                det = col.find(class_="nk23_c-table08__detail")
                if not det:
                    continue

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

        # ボディ（各馬の着順）取得
        tbody = tbl.find("tbody")
        if tbody:
            for tr in tbody.find_all("tr"):
                u = tr.find("a", class_="nk23_c-table08__text")
                if not u:
                    continue

                # === 修正箇所: ここを堅牢にしました ===
                name = u.get_text(strip=True)

                # gradesが None の場合の対策
                safe_grades = grades if isinstance(grades, dict) else {}

                # grade を付与（正規化完全一致 → 正規化部分一致）
                name_norm = _norm_horse_name(name)
                grade = safe_grades.get(name_norm, "")

                if not grade and name_norm:
                    for k_norm, v in safe_grades.items():
                        if k_norm and (k_norm in name_norm or name_norm in k_norm):
                            grade = v
                            break
                # ========================================

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
                        races[i]["results"].append({
                            "rank": rnk,
                            "name": name,
                            "grade": grade,
                            "sort": int(rnk) if rnk.isdigit() else 999
                        })

        # ===== 整形処理 =====
        out = ["\n【対戦表（AI評価付き）】"]

        for r in races:
            if not r.get("results"):
                continue

            r["results"].sort(key=lambda x: x.get("sort", 999))

            line_parts = []
            for x in r["results"]:
                g = f"({x.get('grade')})" if x.get("grade") else ""
                line_parts.append(f"{x['rank']}着 {x['name']}{g}")

            if line_parts:
                title = r.get("title", "")
                link = f"\nLink: {r['url']}" if r.get("url") else ""
                out.append(f"◆ {title}\n" + " / ".join(line_parts) + link)

        if len(out) == 1:
            return "\n(初対戦)"

        return "\n".join(out)

    except Exception as e:
        return f"(対戦表取得エラー: {e})"


def predict_pace_python(horses_data, danwa_data, current_distance_str):
    predictions = []
    
    # 距離の抽出
    dm = re.search(r'(\d{1,3}(?:,\d{3})*|\d+)(?=m)', str(current_distance_str).replace(',', ''))
    curr_dist = int(dm.group(1)) if dm else 1400

    for umaban, data in horses_data.items():
        name = data.get("name", "")
        hist = data.get("hist", [])
        danwa = danwa_data.get(umaban, "")
        
        base_aggro_score = 0
        if hist:
            latest_hist = hist[0]
            latest_text = latest_hist.get("text", "") if isinstance(latest_hist, dict) else latest_hist
            pos_match = re.search(r'(\d+-\d+(?:-\d+)*)', latest_text)
            if pos_match:
                positions = [int(p) for p in pos_match.group(1).split('-') if p.isdigit()]
                if positions:
                    calc_pos = positions[0] 
                    base_aggro_score = max(0, 11 - calc_pos) * 2 
            
            dist_match = re.search(r'(\d{3,4})m?', latest_text)
            if dist_match:
                prev_dist = int(dist_match.group(1))
                if prev_dist < curr_dist:
                    base_aggro_score += 5  
                elif prev_dist > curr_dist:
                    base_aggro_score -= 3  

        comment_mod = 0
        if danwa:
            if re.search(r'(前走|前回).*(逃げ|前).*(苦し|厳し|バテ|甘く)', danwa):
                comment_mod -= 5
            if re.search(r'(ハナ.*こだわらない|控える|溜める|番手.(いい|競馬)|中団)', danwa):
                comment_mod -= 10
            if re.search(r'(ハナ.(切|行|主張|立)|前.(行け|つけ|行きた))', danwa):
                comment_mod += 15

        waku_mod = max(0, 5 - int(umaban) if str(umaban).isdigit() else 0)
        final_score = base_aggro_score + comment_mod + waku_mod
        
        predictions.append({
            "umaban": umaban,
            "name": name,
            "score": final_score,
            "danwa_reason": comment_mod,
            "hist": hist
        })

    # 一時ソートして上位5頭を抽出
    predictions.sort(key=lambda x: x["score"], reverse=True)
    top_5 = predictions[:5]
    
    # 南関4競馬場のテン速度を計算
    nankan_places = ["浦和", "船橋", "大井", "川崎"]
    speeds_log = []
    url_cache = {}
    
    for p in top_5:
        p["speed_avg"] = None
        speeds = []
        for h in p["hist"][:3]:
            if not isinstance(h, dict) or not h.get("url"):
                continue
            if h.get("place") not in nankan_places:
                continue
                
            past_url = h["url"]
            if past_url in url_cache:
                raw_speed = url_cache[past_url]
            else:
                raw_speed = fetch_past_race_2f_times(past_url)
                url_cache[past_url] = raw_speed
                
            if raw_speed:
                # 通過順位によるペナルティ(1番手以外は減速)
                pas_str = h.get("pas", "")
                penalty = 0
                if pas_str:
                    first_pos = pas_str.split('-')[0]
                    if first_pos.isdigit():
                        pos = int(first_pos)
                        if pos > 1:
                            # 2番手なら-0.5km/h、3番手なら-1.0km/h、10番手なら-4.5km/h 程度
                            penalty = (pos - 1) * 0.5
                
                adj_speed = raw_speed - penalty
                
                # 距離とコースによる補正値を加算
                place_mod = 0.0
                dist_mod = 0.0
                
                past_place = h.get("place", "")
                past_dist_str = str(h.get("dist", ""))
                
                if past_place in ["川崎", "浦和"]:
                    place_mod = 0.5
                    
                if past_dist_str.isdigit():
                    pd = int(past_dist_str)
                    if pd <= 1200:
                        dist_mod = -1.0
                    elif pd >= 1500:
                        dist_mod = 1.0
                        
                adj_speed = adj_speed + place_mod + dist_mod
                speeds.append(adj_speed)
                
        if speeds:
            p["speed_avg"] = sum(speeds) / len(speeds)
            speeds_log.append(f"[{p['umaban']}]{p['name']}: {p['speed_avg']:.1f}km/h")
        else:
            speeds_log.append(f"[{p['umaban']}]{p['name']}: データなし")

    # テン速度が取れた馬の中で最終的に逃げ馬の優先度を決定 (上位ほど逃げる)
    def sort_key(x):
        return (x["speed_avg"] if x["speed_avg"] is not None else 0, x["score"])

    predictions[:5] = sorted(top_5, key=sort_key, reverse=True)

    escape_horses = [p for p in predictions if p["score"] >= 15]
    
    if len(escape_horses) >= 3:
        pace = "ハイペース"
        explanation = "逃げ・先行意欲の高い馬が複数おり、序盤から激しいポジション争いが予想されるため、差し馬の台頭に注意。"
    elif len(escape_horses) == 0:
        pace = "スローペース"
        explanation = "明確にハナを主張する馬がおらず、押し出されるように隊列が落ち着く可能性が高い。前残りの展開に注意。"
    else:
        pace = "ミドルペース"
        explanation = "ハナ候補がすんなり隊列を先導し、淀みのない平均的なペースで流れると予想される。"

    leaders = " ".join([f"[{h['umaban']}]{h['name']}" for h in predictions[:2]])
    
    tenkai_text = f"【展開予想】\n"
    tenkai_text += f"◆ペース予想：{pace}\n"
    tenkai_text += f"◆ハナ・先行候補：{leaders}\n"
    tenkai_text += f"◆展開解説：{explanation}\n"
    if speeds_log:
        tenkai_text += f"◆先手候補テン速度 (近走平均)：{' / '.join(speeds_log)}\n"
    tenkai_text += "◆想定隊列順: " + " → ".join([f"[{p['umaban']}]{p['name']}" for p in predictions])
    
    return tenkai_text

def build_evaluation_list(grades, horses_data):
    rank_map = {"S": [], "A": [], "B": [], "C": [], "D": [], "E": [], "F": [], "G": [], "無": []}
    
    for u, hd in horses_data.items():
        name_norm = _norm_horse_name(hd["name"])
        grade = grades.get(name_norm, "")
        if not grade:
            for k_norm, v in grades.items():
                if k_norm and (k_norm in name_norm or name_norm in k_norm):
                    grade = v
                    break
        if not grade:
            grade = "無"
            
        if grade in rank_map:
            rank_map[grade].append(u)
        else:
            rank_map[grade] = [u]
            
    eval_text = "【評価一覧】"
    parts = []
    for r in ["S", "A", "B", "C", "D", "E", "F", "G", "無"]:
        if rank_map.get(r):
            nums = "".join([f"[{x}]" for x in rank_map[r]])
            if r == "無":
                parts.append(f"評価なし{nums}")
            else:
                parts.append(f"{r}{nums}")
    
    if parts:
        eval_text += "  " + "  ".join(parts)
    else:
        eval_text += "  (評価データなし)"
    return eval_text

def generate_html_output(year, month, day, place_name, r_num, header1, pace_text, eval_list_text, match_txt, ai_out_clean, details_text):
    
    # ランク別に色付けするためのHTML整形関数（スマホ向けコンパクト化＋色付け）
    def format_rank_text(text):
        t = html.escape(text)
        t = re.sub(r'([SABCDEFG])([①-⑳]*)', lambda m: f'<span class="rank-{m.group(1)}">{m.group(1)}{m.group(2)}</span>', t)
        return t

    def format_detailed_text(text):
        # Ai詳細や馬別詳細の中の【結論】や【調教】を強調し、調教を小さくする
        t = html.escape(text)
        t = re.sub(r'([SABCDEFG])(?![\w<>])', lambda m: f'<span class="rank-{m.group(1)}">{m.group(1)}</span>', t)
        t = t.replace('【調教】', '<br><span class="chokyo-label">【調教】</span><div class="chokyo-text">')
        t = t.replace('\n----------------------------------------\n', '</div><hr>')
        return t

    html_content = f'''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>{year}/{month}/{day} {place_name}{r_num}R 予想</title>
    <style>
        :root {{
            --primary: #2c3e50;
            --bg: #f5f7fa;
            --box-bg: #ffffff;
            --text: #333333;
            --border: #e2e8f0;
            --s-color: #d4af37; /* 金色 */
            --a-color: #ff69b4; /* ピンク */
            --b-color: #ff4500; /* 赤色 */
            --c-color: #ff8c00; /* オレンジ */
            --d-color: #ffd700; /* 黄色 */
            --e-color: #32cd32; /* 緑 */
            --f-color: #999999;
            --g-color: #cccccc;
        }}
        body {{
            font-family: "Hiragino Kaku Gothic ProN", "Meiryo", sans-serif;
            background-color: var(--bg);
            color: var(--text);
            margin: 0;
            padding: 0;
            font-size: 14px;
            line-height: 1.5;
        }}
        .header {{ 
            background-color: var(--primary); 
            color: white; 
            padding: 12px 15px; 
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
            position: sticky;
            top: 0;
            z-index: 100;
        }}
        .header h2 {{ margin: 0; font-size: 1.1em; }}
        .header p {{ margin: 5px 0 0 0; font-size: 0.85em; opacity: 0.9; }}
        
        /* ランクの色 */
        .rank-S {{ color: var(--s-color); font-weight: bold; font-size: 1.2em; text-shadow: 0 0 1px rgba(0,0,0,0.3); }}
        .rank-A {{ color: var(--a-color); font-weight: bold; font-size: 1.1em; }}
        .rank-B {{ color: var(--b-color); font-weight: bold; font-size: 1.1em; }}
        .rank-C {{ color: var(--c-color); font-weight: bold; font-size: 1.1em; }}
        .rank-D {{ color: var(--d-color); font-weight: bold; font-size: 1.0em; text-shadow: 0 0 1px rgba(0,0,0,0.3); }}
        .rank-E {{ color: var(--e-color); font-weight: bold; font-size: 1.0em; }}
        .rank-F {{ color: var(--f-color); font-weight: bold; font-size: 1.0em; }}
        .rank-G {{ color: var(--g-color); font-weight: bold; font-size: 1.0em; }}
        
        .eval-list-container {{
            background: var(--box-bg);
            margin: 10px;
            padding: 12px;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .eval-title {{ font-weight: bold; border-bottom: 2px solid var(--primary); padding-bottom: 3px; margin-bottom: 8px; font-size: 1.1em;}}
        .eval-list {{ font-size: 1.1em; line-height: 1.6; word-break: break-all; }}

        /* タブシステム */
        .tabs {{
            display: flex;
            background: #fff;
            border-bottom: 1px solid var(--border);
            overflow-x: auto;
            position: sticky;
            top: 54px;
            z-index: 90;
        }}
        .tab-button {{
            flex: 1;
            min-width: 80px;
            background: none;
            border: none;
            padding: 12px 5px;
            font-size: 0.95em;
            font-weight: bold;
            color: #666;
            cursor: pointer;
            border-bottom: 3px solid transparent;
            white-space: nowrap;
        }}
        .tab-button.active {{
            color: var(--primary);
            border-bottom: 3px solid var(--primary);
        }}
        .tab-content {{
            display: none;
            padding: 10px;
        }}
        .tab-content.active {{
            display: block;
        }}
        
        .content-box {{
            background: var(--box-bg);
            padding: 12px;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            margin-bottom: 15px;
            overflow-x: auto;
        }}
        
        pre {{ 
            white-space: pre-wrap; 
            font-family: inherit; 
            margin: 0;
            font-size: 0.95em;
        }}
        
        /* 調教テキスト小さく */
        .chokyo-label {{ font-size: 0.85em; font-weight: bold; color: #555; }}
        .chokyo-text {{ 
            font-size: 0.8em; 
            color: #666; 
            background: #f8f9fa; 
            padding: 6px; 
            border-radius: 4px;
            margin-top: 3px;
        }}
        hr {{ border: 0; border-top: 1px dashed var(--border); margin: 15px 0; }}
        
    </style>
</head>
<body>
    <div class="header">
        <h2>{year}/{month}/{day} {place_name}{r_num}R</h2>
        <p>{html.escape(header1)}</p>
    </div>
    
    <div class="eval-list-container">
        <div class="eval-title">📊 評価一覧</div>
        <div class="eval-list">{format_rank_text(eval_list_text)}</div>
    </div>

    <div class="tabs">
        <button class="tab-button active" onclick="openTab(event, 'tab-pace')">展開予想</button>
        <button class="tab-button" onclick="openTab(event, 'tab-ai')">AI詳細</button>
        <button class="tab-button" onclick="openTab(event, 'tab-detail')">馬別詳細</button>
        <button class="tab-button" onclick="openTab(event, 'tab-match')">対戦表</button>
    </div>

    <div id="tab-pace" class="tab-content active">
        <div class="content-box">
            <pre>{html.escape(pace_text)}</pre>
        </div>
    </div>

    <div id="tab-ai" class="tab-content">
        <div class="content-box">
            <pre>{format_detailed_text(ai_out_clean)}</pre>
            <!-- 最後閉じタグ補完用 -->
            </div>
        </div>
    </div>

    <div id="tab-detail" class="tab-content">
        <div class="content-box">
            <pre>{html.escape(details_text).replace('【近走】', '<hr>【近走】')}</pre>
        </div>
    </div>

    <div id="tab-match" class="tab-content">
        <div class="content-box">
            <pre>{format_rank_text(match_txt.strip())}</pre>
        </div>
    </div>

    <script>
        function openTab(evt, tabName) {{
            var i, tabcontent, tablinks;
            tabcontent = document.getElementsByClassName("tab-content");
            for (i = 0; i < tabcontent.length; i++) {{
                tabcontent[i].style.display = "none";
                tabcontent[i].classList.remove("active");
            }}
            tablinks = document.getElementsByClassName("tab-button");
            for (i = 0; i < tablinks.length; i++) {{
                tablinks[i].classList.remove("active");
            }}
            var targetTab = document.getElementById(tabName);
            if (targetTab) {{
                targetTab.style.display = "block";
                targetTab.classList.add("active");
            }}
            evt.currentTarget.classList.add("active");
            
            // 状態をlocalStorageに保存（レース番号ごとに記録）
            localStorage.setItem('keiba_active_tab_{r_num}', tabName);
        }}
        
        // ページ読み込み時に前回のタブを復元
        document.addEventListener('DOMContentLoaded', (event) => {{
            const savedTab = localStorage.getItem('keiba_active_tab_{r_num}');
            if (savedTab) {{
                const tabButton = document.querySelector(`button[onclick*="'${{savedTab}}'"]`);
                if (tabButton) {{
                    tabButton.click();
                }}
            }}
        }});
    </script>
</body>
</html>'''
    return html_content

# ==================================================
# 10. ジェネレータ（全レース処理）
# ==================================================
def run_races_iter(year, month, day, place_code, target_races, mode="dify", manual_kai_nichi=None, **kwargs):
    resources = load_resources()

    kb_input_map = {"10": "大井", "11": "川崎", "12": "船橋", "13": "浦和"}
    nk_code_map = {"10": "20", "11": "21", "12": "19", "13": "18"}

    place_name = kb_input_map.get(place_code, "地方")
    nk_place_code = nk_code_map.get(place_code)

    driver = get_driver()

    try:
        kai, nichi = None, None
        if manual_kai_nichi and manual_kai_nichi.get("kai") and manual_kai_nichi.get("nichi"):
            kai = manual_kai_nichi["kai"]
            nichi = manual_kai_nichi["nichi"]
            yield {"type": "status", "data": f"📅 手動指定を使用: {place_name} 第{kai}回 {nichi}日目"}
        else:
            yield {"type": "status", "data": f"📅 開催特定中 ({place_name})..."}
            kai, nichi = get_nankan_kai_nichi(month, day, place_name)
            
        if not kai:
            yield {"type": "error", "data": "開催特定失敗（左のメニューから手動で第何回・何日目か入力してください）"}
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
        r_nums = sorted(set(r_nums)) or list(range(1, 13))

        for r_num in r_nums:
            if target_races and r_num not in target_races:
                continue

            yield {"type": "status", "data": f"🏇 {r_num}R データ解析中..."}

            try:
                nk_id = f"{year}{month}{day}{nk_place_code}{kai:02}{nichi:02}{r_num:02}"
                kb_id = get_kb_url_id(year, month, day, place_code, nichi, r_num)

                danwa, cyokyo = parse_kb_danwa_cyokyo(driver, kb_id)

                # 南関競馬の詳細ページをHTTPで直接取得（サーバーサイドレンダリング済みのためSelenium不要）
                detail_url = f"https://www.nankankeiba.com/uma_shosai/{nk_id}.do"
                sess = get_http_session()
                try:
                    detail_res = sess.get(detail_url, timeout=15)
                    detail_res.encoding = "cp932"
                    detail_html = detail_res.text
                except Exception as e:
                    yield {"type": "error", "data": f"{r_num}R 詳細ページ取得失敗: {e}"}
                    continue

                nk_data = parse_nankankeiba_detail(detail_html, place_name, resources)

                if not nk_data["horses"]:
                    # フォールバック: Seleniumで再試行
                    try:
                        driver.get(detail_url)
                        time.sleep(2)
                        for attempt in range(3):
                            try:
                                driver.execute_script("if(typeof changeShosai === 'function'){ changeShosai('s1'); }")
                                time.sleep(1.5)
                                break
                            except Exception:
                                time.sleep(1)
                        nk_data = parse_nankankeiba_detail(driver.page_source, place_name, resources)
                    except Exception:
                        pass

                if not nk_data["horses"]:
                    yield {"type": "error", "data": f"{r_num}R データなし (HTML解析失敗)"}
                    continue

                header = (
                    f"レース名:{r_num}R {nk_data['meta'].get('race_name','')} "
                    f"格:{nk_data['meta'].get('grade','')} "
                    f"コース:{nk_data['meta'].get('course','')}"
                )

                horse_texts = []
                for u in sorted(nk_data["horses"].keys(), key=int):
                    h = nk_data["horses"][u]
                    power_line = h.get("display_power", f"【騎手】{h.get('power','P:不明')}、 相性:{h.get('compat','-')}")

                    curr_j = (h.get("jockey") or "").strip()
                    prev_j = (h.get("prev_jockey") or "").strip()

                    if prev_j:
                        if curr_j == prev_j:
                            jockey_disp = f"{curr_j}(同)"
                        else:
                            jockey_disp = f"{curr_j}←{prev_j}"
                    else:
                        jockey_disp = curr_j

                    block = [
                        f"[{u}]{h['name']} 騎:{jockey_disp} 師:{h.get('trainer','')}",
                        f"話:{danwa.get(u,'なし')}",
                        f"調:{cyokyo.get(u,'データなし')}",
                        power_line,
                        "【近走】",
                    ]
                    for hs in h.get("hist", []):
                        hs_text = hs.get("text", "") if isinstance(hs, dict) else hs
                        block.append(hs_text)

                    horse_texts.append("\n".join(block))

                full_prompt = header + "\n\n" + "\n\n".join(horse_texts)

                # RAWモード
                if mode == "raw":
                    yield {"type": "status", "data": f"🔍 {r_num}R 対戦データを取得中..."}
                    match_txt = _fetch_matchup_table_selenium(driver, nk_id, grades={})
                    
                    header1 = f"{nk_data['meta'].get('race_name','')}  {nk_data['meta'].get('course','')}  {nk_data['meta'].get('grade','')}"
                    pace_text = predict_pace_python(nk_data["horses"], danwa, nk_data['meta'].get('course',''))
                    details_text = "【馬別詳細結果】\n" + "\n\n".join(horse_texts)

                    final_text = (
                        f"📅 {year}/{month}/{day} {place_name}{r_num}R\n\n"
                        f"{header1}\n\n"
                        f"{pace_text}\n\n"
                        f"{match_txt}\n\n"
                        f"{details_text}"
                    )
                    final_html = generate_html_output(year, month, day, place_name, r_num, header1, pace_text, "【評価一覧】  (AI未実行)", match_txt, "(AI未実行)", details_text)

                    yield {"type": "result", "race_num": r_num, "data_text": final_text, "data_html": final_html}
                    time.sleep(1)
                    continue

                header1 = f"{nk_data['meta'].get('race_name','')}  {nk_data['meta'].get('course','')}  {nk_data['meta'].get('grade','')}"
                pace_text = predict_pace_python(nk_data["horses"], danwa, nk_data['meta'].get('course',''))
                details_text = "【馬別詳細結果】\n" + "\n\n".join(horse_texts)

                # PACEモード (展開のみ)
                if mode == "pace":
                    yield {"type": "status", "data": f"⏱️ {r_num}R 展開予想を生成中..."}
                    final_text = (
                        f"📅 {year}/{month}/{day} {place_name}{r_num}R\n\n"
                        f"{header1}\n\n"
                        f"{pace_text}\n\n"
                        f"{details_text}"
                    )
                    final_html = generate_html_output(
                        year, month, day, place_name, r_num, header1, pace_text, 
                        "【評価一覧】 (展開のみモードのため省略)", 
                        "【対戦表】 (展開のみモードのため省略)", 
                        "AI評価なし", 
                        details_text
                    )
                    yield {"type": "result", "race_num": r_num, "data_text": final_text, "data_html": final_html}
                    time.sleep(1)
                    continue

                # Difyモード
                yield {"type": "status", "data": f"🤖 {r_num}R AI予測中... (展開のみを先行表示しています)"}
                
                early_text = (
                    f"📅 {year}/{month}/{day} {place_name}{r_num}R\n\n"
                    f"{header1}\n\n"
                    f"{pace_text}\n\n"
                    f"【現在AIの評価を生成中...約1〜3分かかります】"
                )
                yield {"type": "early_result", "race_num": r_num, "data_text": early_text}
                
                ai_out = run_dify_prediction(full_prompt)

                grades = _parse_grades_from_ai(ai_out)
                match_txt = _fetch_matchup_table_selenium(driver, nk_id, grades)

                # ai_out が {"text": "..."} のJSON文字列で来る場合に備えて展開
                ai_text = ai_out
                try:
                    j = json.loads(ai_out)
                    if isinstance(j, dict) and isinstance(j.get("text"), str):
                        ai_text = j["text"]
                except Exception:
                    pass

                # 余計な罫線/空行を軽く整形
                ai_out_clean = re.sub(r"^\s*-{3,}\s*$", "", ai_text, flags=re.MULTILINE)
                ai_out_clean = re.sub(r"\n{3,}", "\n\n", ai_out_clean).strip()

                ai_out_clean = inject_cyokyo_before_rank(ai_out_clean, cyokyo)


                # ユーザー指定のフォーマットに組み立て
                eval_list_text = build_evaluation_list(grades, nk_data["horses"])

                final_text = (
                    f"📅 {year}/{month}/{day} {place_name}{r_num}R\n\n"
                    f"{header1}\n\n"
                    f"{pace_text}\n\n"
                    f"{eval_list_text}\n\n"
                    f"{match_txt}\n\n"
                    f"【AI評価詳細】\n{ai_out_clean}\n\n"
                    f"{details_text}"
                )
                
                final_html = generate_html_output(year, month, day, place_name, r_num, header1, pace_text, eval_list_text, match_txt, ai_out_clean, details_text)

                yield {"type": "result", "race_num": r_num, "data_text": final_text, "data_html": final_html}
                time.sleep(15)


            except Exception as e:
                yield {"type": "error", "data": f"{r_num}R Error: {e}"}

    except Exception as e:
        yield {"type": "error", "data": f"Fatal: {e}"}
    finally:
        try:
            driver.quit()
        except Exception:
            pass
