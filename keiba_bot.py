import time
import json
import re
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
# 【設定】Secrets読み込み
# ==================================================
KEIBA_ID = st.secrets.get("KEIBA_ID", "")
KEIBA_PASS = st.secrets.get("KEIBA_PASS", "")
DIFY_API_KEY = st.secrets.get("DIFY_API_KEY", "")
DIFY_BASE_URL = st.secrets.get("DIFY_BASE_URL", "https://api.dify.ai")

# ==================================================
# 内部ユーティリティ
# ==================================================
def _ui_info(ui: bool, msg: str):
    if ui: st.info(msg)

def _ui_success(ui: bool, msg: str):
    if ui: st.success(msg)

def _ui_warning(ui: bool, msg: str):
    if ui: st.warning(msg)

def _ui_error(ui: bool, msg: str):
    if ui: st.error(msg)

def _ui_caption(ui: bool, msg: str):
    if ui: st.caption(msg)

def _ui_markdown(ui: bool, msg: str):
    if ui: st.markdown(msg)

def _ui_divider(ui: bool):
    if ui: st.divider()

# ==================================================
# requests session
# ==================================================
def _build_requests_session(total: int = 3, backoff: float = 0.6) -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=total,
        connect=total,
        read=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

@st.cache_resource
def get_http_session() -> requests.Session:
    return _build_requests_session(total=3, backoff=0.6)

# ==================================================
# Selenium Driver
# ==================================================
def build_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1400,2200")
    return webdriver.Chrome(options=options)

def login_keibabook(driver: webdriver.Chrome, wait: WebDriverWait):
    driver.get("https://s.keibabook.co.jp/login/login")
    if "logout" in driver.current_url: return
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
def fetch_race_ids_from_schedule(driver, year, month, day, target_place_code, ui: bool = False):
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

def parse_race_info(html: str):
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

def parse_danwa_comments(html: str):
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

def parse_cyokyo(html: str):
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
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}
_WEIGHT_RE = re.compile(r"^[☆▲△◇]?\s*\d{1,2}\.\d$")
_PREV_JOCKEY_RE = re.compile(r"\d+人\s+([☆▲△◇]?\s*\S+)\s+\d{1,2}\.\d")

def _norm_name(s: str) -> str:
    s = (s or "").strip().replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s)
    s = s.replace("▲", "").replace("△", "").replace("☆", "").replace("◇", "")
    return s.strip()

def _extract_jockey_from_cell(td) -> str:
    lines = [x.strip() for x in td.get_text("\n", strip=True).split("\n") if x.strip()]
    lines2 = [ln for ln in lines if not _WEIGHT_RE.match(ln)]
    return lines2[0].replace(" ", "") if lines2 else "不明"

def fetch_keibago_debatable_small(year: str, month: str, day: str, race_no: int, baba_code: str):
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
        
        # ★前走騎手取得
        prev_jockey = ""
        if zenso_td:
            m = _PREV_JOCKEY_RE.search(zenso_td.get_text(" ", strip=True))
            if m: prev_jockey = m.group(1).strip().replace(" ", "")
        
        is_change = bool(prev_jockey and jockey and _norm_name(prev_jockey) != _norm_name(jockey))

        horses[str(umaban)] = {
            "waku": str(waku), "umaban": str(umaban), "horse": horse,
            "trainer": trainer, "jockey": jockey, "prev_jockey": prev_jockey, "is_change": is_change
        }
    return header, horses, url, nar_race_level

# ==================================================
# ★対戦表生成ロジック
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

def _parse_grades_fuzzy(horse_name, grades):
    if horse_name in grades: return grades[horse_name]
    for k, v in grades.items():
        if k in horse_name or horse_name in k:
            return v
    return ""

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

# ==================================================
# Dify連携：堅牢なリトライ機能
# ==================================================
def _dify_url(path: str) -> str:
    base = (DIFY_BASE_URL or "").strip().rstrip("/")
    return f"{base}{path}"

def _format_http_error(res: requests.Response) -> str:
    try:
        return f"⚠️ Dify HTTP {res.status_code}: {res.json()}"
    except:
        return f"⚠️ Dify HTTP {res.status_code}: {res.text[:800]}"

def run_dify_workflow_blocking(full_text: str) -> str:
    if not DIFY_API_KEY: return "⚠️ DIFY_API_KEY未設定"
    
    url = _dify_url("/v1/workflows/run")
    payload = {
        "inputs": {"text": full_text},
        "response_mode": "blocking",
        "user": "keiba-bot",
    }
    headers = {"Authorization": f"Bearer {DIFY_API_KEY}", "Content-Type": "application/json"}
    sess = get_http_session()

    try:
        # ★タイムアウトを300秒(5分)に設定して粘る
        res = sess.post(url, headers=headers, json=payload, timeout=(10, 300))
        if res.status_code != 200: return _format_http_error(res)
        
        j = res.json() or {}
        outputs = j.get("data", {}).get("outputs", {})
        # textフィールドを優先取得
        return outputs.get("text") or str(outputs)
    except Exception as e:
        return f"⚠️ blocking API Error: {str(e)}"

def run_dify_with_fallback(full_text: str) -> str:
    max_retries = 3
    for attempt in range(max_retries):
        res = run_dify_workflow_blocking(full_text)
        
        is_error = False
        # エラーワードが含まれていたらリトライ対象とする
        if "⚠️" in res and ("503" in res or "overloaded" in res or "PluginInvokeError" in res):
            is_error = True
        
        if is_error:
            if attempt < max_retries - 1:
                wait_time = 10 + (attempt * 5)
                st.warning(f"⚠️ AI混雑中... {wait_time}秒後リトライ ({attempt+1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                return f"⚠️ {max_retries}回失敗: {res}"
        
        return res
    return "⚠️ リトライ上限超過"

# ==================================================
# メイン処理 (Iterator)
# ==================================================
def run_races_iter(year, month, day, place_code, target_races, ui=False):
    place_names = {"10": "大井", "11": "川崎", "12": "船橋", "13": "浦和"}
    place_name = place_names.get(place_code, "地方")
    baba_map = {"10": "20", "11": "21", "12": "19", "13": "18"}
    baba_code = baba_map.get(place_code)

    if not baba_code:
        yield (0, "⚠️ babaCode mapping error")
        return

    driver = build_driver()
    wait = WebDriverWait(driver, 12)

    try:
        _ui_info(ui, "🔑 ログイン中...")
        login_keibabook(driver, wait)
        
        race_ids = fetch_race_ids_from_schedule(driver, year, month, day, place_code, ui=ui)
        if not race_ids:
            yield (0, "⚠️ レースID取得失敗")
            return

        for i, race_id in enumerate(race_ids):
            race_num = i + 1
            if target_races and race_num not in target_races: continue

            _ui_markdown(ui, f"## {place_name} {race_num}R")
            
            try:
                # 1. データ取得
                header, keibago_dict, _, nar_race_level = fetch_keibago_debatable_small(
                    str(year), str(month), str(day), race_num, str(baba_code)
                )
                
                _ui_info(ui, "📡 データ収集中...")
                driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{race_id}")
                html_danwa = driver.page_source
                driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{race_id}")
                html_cyokyo = driver.page_source
                
                race_meta = parse_race_info(html_danwa)
                danwa_dict = parse_danwa_comments(html_danwa)
                cyokyo_dict = parse_cyokyo(html_cyokyo)

                # 2. プロンプト作成
                all_uma = sorted(set(danwa_dict) | set(cyokyo_dict) | set(keibago_dict), key=lambda x: int(x) if x.isdigit() else 999)
                merged_text = []
                
                for uma in all_uma:
                    kg = keibago_dict.get(uma, {})
                    
                    # ★修正点: 前走騎手をプロンプトに含める
                    prev_info = ""
                    if kg.get('is_change'):
                        pj = kg.get('prev_jockey', '')
                        prev_info = f" 【⚠️乗り替わり】(前走:{pj})" if pj else " 【⚠️乗り替わり】"

                    info = f"▼[馬番{uma}] {kg.get('horse','')} 騎手:{kg.get('jockey','')}{prev_info} 調教師:{kg.get('trainer','')}"
                    
                    merged_text.append(f"{info}\n談話: {danwa_dict.get(uma,'なし')}\n調教: {cyokyo_dict.get(uma,'なし')}")

                if not merged_text:
                    yield (race_num, f"⚠️ データなし: {place_name}{race_num}R")
                    continue

                prompt = (
                    f"レース名: {race_meta.get('race_name','')}\n"
                    f"レースレベル: {nar_race_level}\n"
                    f"条件: {race_meta.get('cond','')}\n\n"
                    + "\n".join(merged_text)
                )

                # 3. AI実行
                _ui_info(ui, "🤖 AI分析中...")
                # UIなしのイテレータでもリトライ機能付き関数を呼ぶ
                dify_res = run_dify_with_fallback(prompt)
                dify_res = (dify_res or "").strip()

                # 4. 対戦表生成
                grades = _parse_grades(dify_res)
                history_text = _fetch_history_data(year, month, day, place_name, race_num, grades)

                # 5. 結合出力
                header_info = f"📅 自動判定: {year}年{month}月{day}日 {place_name} {race_num}R"
                final_output = f"{header_info}\n\n{dify_res}\n\n{history_text}"
                
                _ui_success(ui, "✅ 完了")
                yield (race_num, final_output)
                time.sleep(3) # 連続アクセス防止の待機

            except Exception as e:
                yield (race_num, f"⚠️ Error: {e}")
                time.sleep(3)

    finally:
        try: driver.quit()
        except: pass
