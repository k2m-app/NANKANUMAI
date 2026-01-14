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

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ==================================================
# 設定
# ==================================================
KEIBA_ID = st.secrets.get("KEIBA_ID", "")
KEIBA_PASS = st.secrets.get("KEIBA_PASS", "")
DIFY_API_KEY = st.secrets.get("DIFY_API_KEY", "")
DIFY_BASE_URL = st.secrets.get("DIFY_BASE_URL", "https://api.dify.ai")

# ==================================================
# 共通ツール・セッション管理
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

def build_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1400,2200")
    return webdriver.Chrome(options=options)

def login_keibabook(driver: webdriver.Chrome, wait: WebDriverWait):
    driver.get("https://s.keibabook.co.jp/login/login")
    wait.until(EC.visibility_of_element_located((By.NAME, "login_id"))).send_keys(KEIBA_ID)
    driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
    driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
    time.sleep(1)

# ==================================================
# 1. 競馬ブック等のスクレイピング関数
# ==================================================
def fetch_race_ids_from_schedule(driver, year, month, day, target_place_code, ui: bool = False):
    date_str = f"{year}{month}{day}"
    url = f"https://s.keibabook.co.jp/chihou/nittei/{date_str}10"
    driver.get(url)
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "a")))
    except:
        pass
    soup = BeautifulSoup(driver.page_source, "html.parser")
    race_ids = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"(\d{16})", href)
        if not m: continue
        rid = m.group(1)
        if rid[6:8] == target_place_code:
            if rid not in seen:
                race_ids.append(rid)
                seen.add(rid)
    race_ids.sort()
    return race_ids

def parse_race_info(html: str):
    soup = BeautifulSoup(html, "html.parser")
    racetitle = soup.find("div", class_="racetitle")
    if not racetitle: return {}
    racemei = racetitle.find("div", class_="racemei")
    p_tags = racemei.find_all("p") if racemei else []
    race_name = ""
    if len(p_tags) >= 2: race_name = p_tags[1].get_text(strip=True)
    elif len(p_tags) == 1: race_name = p_tags[0].get_text(strip=True)
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
    if lines2: return lines2[0].replace(" ", "")
    return "不明"

def fetch_keibago_debatable_small(year: str, month: str, day: str, race_no: int, baba_code: str):
    date_str = f"{year}/{str(month).zfill(2)}/{str(day).zfill(2)}"
    url = (
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTableSmall"
        f"?k_raceDate={requests.utils.quote(date_str)}&k_raceNo={race_no}&k_babaCode={baba_code}"
    )
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

    main_table = soup.select_one("td.dbtbl table.bs[border='1']")
    if not main_table: main_table = soup.select_one("table.bs[border='1']")

    horses = {}
    last_waku = ""
    if not main_table: return header, horses, url, nar_race_level

    for tr in main_table.find_all("tr"):
        if not tr.select_one("font.bamei"): continue
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 8: continue

        first_txt = tds[0].get_text(strip=True)
        waku_present = first_txt.isdigit() and len(tds) >= 9
        if waku_present:
            if not tds[1].get_text(strip=True).isdigit(): waku_present = False

        if waku_present:
            waku = tds[0].get_text(strip=True)
            umaban = tds[1].get_text(strip=True)
            horse_td = tds[2]
            trainer_td = tds[3]
            jockey_td = tds[4]
            zenso_td = tds[8] if len(tds) > 8 else None
            last_waku = waku
        else:
            waku = last_waku or ""
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
        
        cj = _norm_name(jockey)
        pj = _norm_name(prev_jockey)
        is_change = bool(pj and cj and pj != cj)

        horses[str(umaban)] = {
            "waku": str(waku), "umaban": str(umaban), "horse": horse,
            "trainer": trainer, "jockey": jockey, "prev_jockey": prev_jockey,
            "is_change": is_change,
        }
    return header, horses, url, nar_race_level

# ==================================================
# 2. ★対戦表作成ロジック (Streamlit側)
# ==================================================
def generate_battle_table_local(llm_text, year, month, day, place_name, race_num):
    # 1. 回・日目の自動取得
    kai, nichi, error_msg = _get_kai_nichi(month, day, place_name)
    
    header_info = ""
    if error_msg:
        header_info = f"開催情報エラー: {error_msg}\n"
        if not kai: kai = 15 # 仮
        if not nichi: nichi = 1
    else:
        header_info = f"自動判定: {year}年{month}月{day}日 {place_name} 第{kai}回 {nichi}日目\n"

    # 2. LLMテキストから評価(S,A...)を読み取る
    grade_map = _parse_grades(llm_text)

    # 3. 対戦データを取得・生成
    history_text = _fetch_history_data(year, month, day, place_name, kai, nichi, race_num, grade_map)

    return f"{header_info}\n{llm_text}\n\n{history_text}"

def _clean_text_strict(text):
    if not text: return ""
    text = re.sub(r'<[^>]+>', ' ', text)
    text = text.replace('&nbsp;', ' ').replace('&amp;', '&')
    text = re.sub(r'[\r\n\t]+', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()

def _get_kai_nichi(target_month, target_day, target_place):
    url = "https://www.nankankeiba.com/bangumi_menu/bangumi.do"
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        res.encoding = 'cp932'
        soup = BeautifulSoup(res.text, 'html.parser')
        
        target_row = None
        for tr in soup.find_all('tr'):
            text = tr.get_text()
            if target_place in text and "競馬" in text:
                target_row = tr
                break
        
        if not target_row: return None, None, f"{target_place}の開催情報なし"

        info_text = ""
        link = target_row.find('a')
        if link: info_text = _clean_text_strict(link.get_text())
        else: info_text = _clean_text_strict(target_row.get_text())

        match = re.search(r'第(\d+)回.*?(\d+)月\s*(.*?)日', info_text)
        if not match: return None, None, f"解析失敗: {info_text}"

        kai_val = int(match.group(1))
        mon_val = int(match.group(2))
        if int(target_month) != mon_val:
             return None, None, f"月不一致(Web:{mon_val}, 指定:{target_month})"

        days_str = match.group(3)
        days_clean = re.sub(r'[^\d,]', '', days_str.replace('，', ','))
        days_list = [int(d) for d in days_clean.split(',') if d]

        if int(target_day) in days_list:
            nichi_val = days_list.index(int(target_day)) + 1
            return kai_val, nichi_val, None
        else:
            return None, None, f"期間外(予定:{days_list})"
    except Exception as e:
        return None, None, str(e)

def _parse_grades(text):
    grades = {}
    if not text: return grades
    for line in text.split('\n'):
        if '|' in line and '---' not in line:
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 4:
                raw_name = parts[1]
                raw_grade = parts[-2]
                match = re.search(r'[①-⑳]?\s*([^(\s]+)', raw_name)
                if match:
                    horse_name = match.group(1)
                    grade = raw_grade.strip()
                    if grade in ['S', 'A', 'B', 'C', 'D']:
                        grades[horse_name] = grade
    return grades

def _fetch_history_data(year, month, day, place_name, kai, nichi, race_num, grade_map):
    place_codes = {'浦和': '18', '船橋': '19', '大井': '20', '川崎': '21'}
    p_code = place_codes.get(place_name, '20')
    race_id = f"{year}{int(month):02}{int(day):02}{p_code}{int(kai):02}{int(nichi):02}{int(race_num):02}"
    url = f"https://www.nankankeiba.com/taisen/{race_id}.do"
    
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        res.encoding = 'cp932'
        soup = BeautifulSoup(res.text, 'html.parser')
        
        target_table = soup.find('table', class_='nk23_c-table08__table')
        if not target_table:
            for tbl in soup.find_all('table'):
                if tbl.find('a', href=re.compile(r'/result/\d+')):
                    target_table = tbl
                    break
        if not target_table: return f"\n(対戦データなし: {url})"

        past_races = []
        thead = target_table.find('thead')
        if not thead: return "\n(テーブル構造エラー)"
        
        for cell in thead.find('tr').find_all(['th', 'td']):
            link = cell.find('a', href=re.compile(r'/result/\d+'))
            if link:
                detail = cell.find(class_='nk23_c-table08__detail')
                raw = detail.get_text(strip=True) if detail else cell.get_text(strip=True)
                info = _clean_text_strict(raw.replace('競走成績','').replace('対戦表',''))
                
                # resultのURLをそのまま使う (liveonには変換しない)
                result_url = "https://www.nankankeiba.com" + link['href']
                
                # race_id抽出 (ソート用)
                rid_match = re.search(r'/result/(\d+)', link['href'])
                rid = rid_match.group(1) if rid_match else "0"

                past_races.append({
                    'info': info, 
                    'url': result_url, 
                    'id': rid, # ソート用のID
                    'results': [] # (着順, 評価, 馬名) のタプルを入れる
                })

        if not past_races: return "\n(初対戦)"

        tbody = target_table.find('tbody')
        if not tbody: return "\n(データ行なし)"
        
        for row in tbody.find_all('tr'):
            uma_link = row.find('a', href=re.compile(r'/uma_info/'))
            if not uma_link: continue
            
            horse_name = uma_link.get_text(strip=True)
            grade = grade_map.get(horse_name)
            if not grade:
                for k, v in grade_map.items():
                    if k in horse_name or horse_name in k:
                        grade = v; break
            
            cells = row.find_all(['td', 'th'])
            h_idx = -1
            for idx, c in enumerate(cells):
                if c.find('a', href=re.compile(r'/uma_info/')):
                    h_idx = idx; break
            if h_idx == -1: continue
            
            result_cells = cells[h_idx+1:]
            for col_idx, race_obj in enumerate(past_races):
                if col_idx < len(result_cells):
                    cell = result_cells[col_idx]
                    rank_num = 999
                    rank_str = ""
                    
                    num_tag = cell.find(class_='nk23_c-table08__number')
                    if num_tag:
                        span = num_tag.find('span')
                        rank_str = span.get_text(strip=True) if span else num_tag.get_text(strip=True).split('｜')[0].strip()
                    else:
                        txt = cell.get_text(strip=True)
                        if txt:
                            p = txt.split('｜')[0].strip()
                            if p.isdigit() or p in ['除外','中止','取消']: rank_str = p
                    
                    if rank_str:
                        if rank_str.isdigit():
                            rank_num = int(rank_str)
                        # 結果を追加 (ソート用数値, 表示用文字列, 評価, 馬名)
                        race_obj['results'].append({
                            'sort_key': rank_num,
                            'disp_rank': rank_str,
                            'grade': grade,
                            'name': horse_name
                        })

        # 4. ソートと出力生成 (変更点反映)
        # 日付順（新しい順）＝ IDの降順
        past_races.sort(key=lambda x: x['id'], reverse=True)
        
        output = ["###注目対戦"] # タイトル変更
        has_data = False
        
        for race in past_races:
            if race['results']:
                has_data = True
                
                # 着順ソート (数字昇順 -> その他)
                race['results'].sort(key=lambda x: x['sort_key'])
                
                # 表示文字列作成: "1着 馬名(A)" 形式
                res_strs = []
                for r in race['results']:
                    grade_str = f"({r['grade']})" if r['grade'] else ""
                    # 数字の場合: "1着 馬名(A)"
                    # 文字の場合: "取消 馬名(A)"
                    prefix = f"{r['disp_rank']}着" if r['disp_rank'].isdigit() else r['disp_rank']
                    res_strs.append(f"{prefix} {r['name']}{grade_str}")
                
                # URLはそのままresultを使用、表記は[詳細]
                output.append(f"**・ {race['info']}**")
                output.append(" / ".join(res_strs))
                output.append(f"[詳細]({race['url']})\n")
        
        if not has_data: return "\n(初対戦)"
        return "\n".join(output)

    except Exception as e:
        return f"\n(対戦表エラー: {e})"

# ==================================================
# 3. Dify連携
# ==================================================
def run_dify(inputs_dict):
    url = f"{DIFY_BASE_URL}/v1/workflows/run"
    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "inputs": inputs_dict,
        "response_mode": "blocking",
        "user": "streamlit-user"
    }
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=60)
        if res.status_code == 200:
            data = res.json().get('data', {})
            outputs = data.get('outputs', {})
            for v in outputs.values():
                if isinstance(v, str) and len(v) > 10:
                    return v
            return "⚠️ Difyからの応答が空でした"
        else:
            return f"⚠️ Dify Error: {res.status_code} {res.text}"
    except Exception as e:
        return f"⚠️ 通信エラー: {e}"

# ==================================================
# 4. メイン処理 (統合版)
# ==================================================
def run_races_iter(year, month, day, place_code, target_races, ui=False):
    place_names = {"10": "大井", "11": "川崎", "12": "船橋", "13": "浦和"}
    place_name = place_names.get(place_code, "地方")
    baba_map = {"10": "20", "11": "21", "12": "19", "13": "18"}
    baba_code = baba_map.get(place_code, "20")

    driver = build_driver()
    wait = WebDriverWait(driver, 10)
    
    try:
        login_keibabook(driver, wait)
        race_ids = fetch_race_ids_from_schedule(driver, year, month, day, place_code, ui=ui)
        
        if not race_ids:
            yield 0, "⚠️ レースIDが取得できませんでした。日付/コードを確認してください。"
            return

        for i, race_id in enumerate(race_ids):
            race_num = i + 1
            if target_races is not None and race_num not in target_races:
                continue
            
            try:
                # スクレイピング
                header, keibago_dict, keibago_url, nar_race_level = fetch_keibago_debatable_small(
                    year=str(year), month=str(month), day=str(day),
                    race_no=race_num, baba_code=str(baba_code)
                )
                
                driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{race_id}")
                html_danwa = driver.page_source
                race_meta = parse_race_info(html_danwa)
                danwa_dict = parse_danwa_comments(html_danwa)
                
                driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{race_id}")
                cyokyo_dict = parse_cyokyo(driver.page_source)
                
                all_uma = sorted(
                    set(danwa_dict.keys()) | set(cyokyo_dict.keys()) | set(keibago_dict.keys()),
                    key=lambda x: int(x) if str(x).isdigit() else 999,
                )
                
                merged_text = []
                for uma in all_uma:
                    kg = keibago_dict.get(uma, {})
                    d = danwa_dict.get(uma, "（なし）")
                    c = cyokyo_dict.get(uma, "（なし）")
                    info = f"▼[馬番{uma}] {kg.get('horse','')} 騎手:{kg.get('jockey','')} 調教師:{kg.get('trainer','')}"
                    if kg.get('is_change'): info += " 【⚠️乗り替わり】"
                    merged_text.append(f"{info}\n談話: {d}\n調教: {c}")
                
                if not merged_text:
                    yield race_num, "⚠️ データなしのためスキップ"
                    continue

                prompt = (
                    f"レース名: {race_meta.get('race_name','')}\n"
                    f"レースレベル: {nar_race_level}\n"
                    f"条件: {race_meta.get('cond','')}\n\n"
                    + "\n".join(merged_text)
                )
                
                dify_inputs = {
                    "text": prompt,
                    "date": f"{year}/{month}/{day}",
                    "place": place_name,
                    "race_no": str(race_num),
                    "year": str(year),
                    "month": str(month),
                    "day": str(day)
                }
                dify_res = run_dify(dify_inputs)
                
                final_output = generate_battle_table_local(
                    dify_res, year, month, day, place_name, race_num
                )
                
                yield race_num, final_output
                
            except Exception as e:
                yield race_num, f"⚠️ Error: {e}"
            
            time.sleep(2)
    finally:
        driver.quit()
