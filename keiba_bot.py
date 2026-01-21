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
        res = sess.post(url, headers=headers, json=payload, timeout=90)
        if res.status_code != 200: return f"⚠️ Dify Error: {res.status_code}"
        j = res.json()
        return j.get("data", {}).get("outputs", {}).get("text", "") or str(j)
    except Exception as e: return f"⚠️ API Error: {e}"

# ==================================================
# データ読み込み・正規化
# ==================================================
@st.cache_resource
def load_resources():
    res = {"jockeys": [], "trainers": [], "power": {}}
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
                    info = f"騎手パワー:{row.get('騎手パワー','-')}(勝率{row.get('勝率','-')} 複勝率{row.get('複勝率','-')})"
                    res["power"][(p, j)] = info
        except: pass
    return res

def normalize_name(abbrev, full_list):
    if not abbrev: return ""
    clean = abbrev.replace(" ","").replace("　","")
    if not full_list: return clean
    if clean in full_list: return clean
    matches = [n for n in full_list if n.startswith(clean) or (len(clean)>=2 and n.startswith(clean[0]) and clean[1] in n)]
    return sorted(matches, key=len)[0] if matches else clean

# ==================================================
# nankankeiba & KeibaBook 解析
# ==================================================
def parse_nankankeiba_detail(html, place_name, resources):
    """ nankankeiba詳細出走表から、基本情報・相性・近走3走を取得 """
    soup = BeautifulSoup(html, "html.parser")
    data = {"meta": {}, "horses": {}}

    # レース情報
    h3 = soup.find("h3", class_="nk23_c-tab1__title")
    data["meta"]["race_name"] = h3.get_text(strip=True) if h3 else ""
    if data["meta"]["race_name"]:
        parts = re.split(r'[ 　]+', data["meta"]["race_name"])
        data["meta"]["grade"] = parts[-1] if len(parts) > 1 else ""
    
    cond = soup.select_one("a.nk23_c-tab1__subtitle__text.is-blue")
    data["meta"]["course"] = f"{place_name} {cond.get_text(strip=True)}" if cond else ""

    # 馬データ
    table = soup.select_one("#shosai_aria table.nk23_c-table22__table")
    if not table: return data

    for row in table.select("tbody tr"):
        try:
            # 馬番・馬名
            u_tag = row.select_one("td.umaban") or row.select_one("td.is-col02")
            if not u_tag: continue
            umaban = u_tag.get_text(strip=True)
            if not umaban.isdigit(): continue
            
            h_tag = row.select_one("td.is-col03 a.is-link")
            horse_name = h_tag.get_text(strip=True) if h_tag else ""

            # 騎手・調教師
            jg_td = row.select_one("td.cs-g1")
            j_raw, t_raw = "", ""
            if jg_td:
                links = jg_td.select("a")
                if len(links) >= 1: j_raw = links[0].get_text(strip=True)
                if len(links) >= 2: t_raw = links[1].get_text(strip=True)
            
            j_full = normalize_name(j_raw, resources["jockeys"])
            t_full = normalize_name(t_raw, resources["trainers"])
            power = resources["power"].get((place_name, j_full), "騎手パワー:不明")

            # 相性 (騎手x調教師)
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
                
                # 日付・場所・条件
                d_txt = ""
                d_spans = z.select("p.nk23_u-d-flex span.nk23_u-text10")
                if d_spans:
                    for s in d_spans:
                        if re.search(r"\d+\.\d+\.\d+", s.get_text()): d_txt = s.get_text(strip=True); break
                
                # 年月日変換
                ymd = ""
                m = re.match(r"([^\d]+)(\d+)\.(\d+)\.(\d+)", d_txt)
                if m:
                    place_short = m.group(1)
                    ymd = f"20{m.group(2)}/{int(m.group(3)):02}/{int(m.group(4)):02}"
                
                cond_txt = d_spans[-1].get_text(strip=True) if len(d_spans)>=2 else ""
                dist_m = re.search(r"\d{4}", cond_txt)
                dist = dist_m.group(0) if dist_m else ""
                course_s = f"{place_short}{dist}m" if m else cond_txt

                # レース名・クラス
                r_a = z.select_one("a.is-link")
                r_ti = r_a.get("title", "") if r_a else ""
                rp = re.split(r'[ 　]+', r_ti)
                r_nm = rp[0] if rp else ""
                r_cl = rp[1] if len(rp)>1 else ""

                # 騎手・人気・着順・通過・上がり
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

def parse_kb_danwa_cyokyo(driver, kb_rid):
    """ 競馬ブックから談話と調教を一括取得 """
    d_danwa, d_cyokyo = {}, {}
    try:
        driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{kb_rid}")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        tbl = soup.find("table", class_="danwa")
        if tbl and tbl.tbody:
            cur = None
            for row in tbl.tbody.find_all("tr"):
                u = row.find("td", class_="umaban")
                if u: cur = u.get_text(strip=True); continue
                t = row.find("td", class_="danwa")
                if t and cur: d_danwa[cur] = t.get_text(strip=True); cur=None
        
        driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{kb_rid}")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for tbl in soup.find_all("table", class_="cyokyo"):
            if not tbl.tbody: continue
            rs = tbl.tbody.find_all("tr", recursive=False)
            if not rs: continue
            u_td = rs[0].find("td", class_="umaban")
            if u_td:
                u = u_td.get_text(strip=True)
                tp = rs[0].find("td", class_="tanpyo").get_text(strip=True) if rs[0].find("td", class_="tanpyo") else ""
                dt = rs[1].get_text(" ", strip=True) if len(rs)>1 else ""
                d_cyokyo[u] = f"【短評】{tp} 【詳細】{dt}"
    except: pass
    return d_danwa, d_cyokyo

# ==================================================
# 対戦表 & 評価解析 (復活)
# ==================================================
def _parse_grades_from_ai(text):
    """ AIの回答から馬の評価(S/A/B...)を抽出する簡易ロジック """
    grades = {}
    # 行ごとに "◎馬名" や "S 馬名" のようなパターンを探す
    lines = text.split('\n')
    for line in lines:
        # パターン: [S] 馬名 or 評価:S 馬名 など
        m = re.search(r'([SABCDE])\s*[:：]?\s*([^\s　]+)', line)
        if m:
            grade, name = m.group(1), m.group(2)
            # 馬名から括弧などを除去
            name = re.sub(r'[（\(].*?[）\)]', '', name).strip()
            if name: grades[name] = grade
    return grades

def _fetch_matchup_table(nankan_id, grades):
    """ nankankeibaの対戦表ページを取得し、AI評価印を付与して整形 """
    url = f"https://www.nankankeiba.com/taisen/{nankan_id}.do"
    sess = get_http_session()
    try:
        res = sess.get(url, timeout=10)
        res.encoding = 'cp932'
        soup = BeautifulSoup(res.text, 'html.parser')
        tbl = soup.find('table', class_='nk23_c-table08__table')
        if not tbl: return "\n(対戦データなし)"

        races = []
        # ヘッダーからレース名取得
        if tbl.find('thead') and tbl.find('thead').find('tr'):
            for col in tbl.find('thead').find('tr').find_all(['th','td'])[2:]:
                det = col.find(class_='nk23_c-table08__detail')
                if det:
                    link = col.find('a')
                    races.append({
                        "title": det.get_text(" ", strip=True),
                        "url": "https://www.nankankeiba.com" + link.get('href','') if link else "",
                        "results": []
                    })
        
        if not races: return "\n(初対戦)"

        # ボディから着順取得
        if tbl.find('tbody'):
            for tr in tbl.find('tbody').find_all('tr'):
                u_link = tr.find('a', class_='nk23_c-table08__text')
                if not u_link: continue
                
                horse_name = u_link.get_text(strip=True)
                # AI評価を取得 (完全一致または部分一致)
                grade = grades.get(horse_name, "")
                if not grade:
                    # 部分一致検索
                    for k, v in grades.items():
                        if k in horse_name or horse_name in k:
                            grade = v; break
                
                cells = tr.find_all(['td','th'])
                # 馬名セルの次からがレース結果
                start_idx = -1
                for idx, c in enumerate(cells):
                    if c.find('a', class_='nk23_c-table08__text'): start_idx=idx; break
                
                if start_idx == -1: continue

                for i, cell in enumerate(cells[start_idx+1:]):
                    if i >= len(races): break
                    rank_p = cell.find('p', class_='nk23_c-table08__number')
                    rank = ""
                    if rank_p:
                        sp = rank_p.find('span')
                        rank = sp.get_text(strip=True) if sp else rank_p.get_text(strip=True).split('｜')[0].strip()
                    
                    if rank and (rank.isdigit() or rank in ['除外','中止']):
                        races[i]["results"].append({
                            "rank": rank, "name": horse_name, "grade": grade,
                            "sort": int(rank) if rank.isdigit() else 999
                        })

        # 出力生成
        out = ["\n【対戦表（AI評価付き）】"]
        has_data = False
        for r in races:
            if not r["results"]: continue
            has_data = True
            r["results"].sort(key=lambda x: x["sort"])
            # 1着 馬名(S) / 2着 馬名(A)...
            line_parts = []
            for x in r["results"]:
                g_str = f"[{x['grade']}]" if x['grade'] else ""
                line_parts.append(f"{x['rank']}着 {x['name']}{g_str}")
            
            out.append(f"◆ {r['title']}")
            out.append(" / ".join(line_parts))
            out.append(f"詳細: {r['url']}\n")
            
        return "\n".join(out) if has_data else "\n(対戦データなし)"

    except Exception as e: return f"(対戦表エラー: {e})"

# ==================================================
# メイン実行関数
# ==================================================
def run_races_iter(year, month, day, place_code, target_races, ui=False):
    # 1. 準備
    resources = load_resources()
    
    # マッピング
    kb_place_map = {"10":"大井", "11":"川崎", "12":"船橋", "13":"浦和"}
    nk_place_map = {"10":"20", "11":"21", "12":"19", "13":"18"} # KB -> Nankan
    
    place_name = kb_place_map.get(place_code, "地方")
    nk_place_code = nk_place_map.get(place_code)

    if not nk_place_code: yield (0, "⚠️ 場所コードエラー"); return

    # Selenium
    ops = Options()
    ops.add_argument("--headless=new")
    ops.add_argument("--no-sandbox")
    ops.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=ops)
    wait = WebDriverWait(driver, 10)

    try:
        # 2. ログイン
        if ui: st.info("🔑 ログイン中...")
        driver.get("https://s.keibabook.co.jp/login/login")
        if "logout" not in driver.current_url:
            wait.until(EC.visibility_of_element_located((By.NAME, "login_id"))).send_keys(KEIBA_ID)
            driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
            driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
            time.sleep(1)

        # 3. レースIDリスト取得 (nankankeibaのプログラムページから取得が確実)
        # URL: https://www.nankankeiba.com/program/YYYYMMDDpp.do
        prog_url = f"https://www.nankankeiba.com/program/{year}{month}{day}{nk_place_code}.do"
        if ui: st.info(f"📅 開催情報取得: {prog_url}")
        driver.get(prog_url)
        
        # リンクからID抽出 (ID: YYYYMMDDppKkDDRR)
        soup_prog = BeautifulSoup(driver.page_source, "html.parser")
        race_list = [] # (race_num, full_nankan_id)
        
        # "race_one" などのクラスを持つリンクを探す
        for a in soup_prog.find_all("a", href=True):
            # hrefに日付と場所コードが含まれているか
            if f"{year}{month}{day}{nk_place_code}" in a["href"] and "uma_shosai" not in a["href"]:
                # プログラム一覧内のリンク (例: .../2026012119100301.do)
                # ファイル名部分を抽出
                fname = a["href"].split("/")[-1].replace(".do","")
                if fname.isdigit() and len(fname) == 16:
                    r_num = int(fname[14:16])
                    race_list.append((r_num, fname))
        
        # 重複排除してソート
        race_list = sorted(list(set(race_list)))
        
        if not race_list: yield (0, "⚠️ レース一覧が取得できませんでした"); return

        # 4. 各レース処理
        for r_num, nk_id in race_list:
            if target_races and r_num not in target_races: continue
            
            if ui: st.markdown(f"## {place_name} {r_num}R")
            
            try:
                # A. KeibaBook ID生成 (YYYYMMDD + KB_Place + RR)
                kb_id = f"{year}{month}{day}{place_code}{str(r_num).zfill(2)}"
                
                # B. KeibaBookデータ (談話・調教)
                danwa, cyokyo = parse_kb_danwa_cyokyo(driver, kb_id)
                
                # C. Nankanデータ (詳細出走表)
                nk_url = f"https://www.nankankeiba.com/uma_shosai/{nk_id}.do"
                driver.get(nk_url)
                nk_data = parse_nankankeiba_detail(driver.page_source, place_name, resources)
                
                # D. プロンプト作成
                header = f"レース名: {r_num}R {nk_data['meta'].get('race_name','')}　格付け:{nk_data['meta'].get('grade','')}　コース:{nk_data['meta'].get('course','')}"
                
                horse_texts = []
                for u in sorted(nk_data["horses"].keys(), key=int):
                    h = nk_data["horses"][u]
                    
                    # 前走騎手
                    prev_j = ""
                    if h["hist"]:
                        m = re.search(r"騎手：([^　\s]+)", h["hist"][0])
                        if m: prev_j = m.group(1)
                    p_info = f" (前走:{prev_j})" if prev_j else ""
                    
                    # ブロック作成
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
                
                # E. Dify送信
                if ui: st.info("🤖 AI分析中...")
                ai_output = run_dify_prediction(full_prompt)
                
                # F. 対戦表作成 (AIの評価を使って)
                # AIの回答から評価(S/A/B)を抽出
                grades = _parse_grades_from_ai(ai_output)
                # nankankeibaの対戦表URLはIDと同じ
                matchup_text = _fetch_matchup_table(nk_id, grades)
                
                # G. 最終出力
                final_res = f"📅 {year}/{month}/{day} {place_name}{r_num}R\n\n=== 🤖AI予想 ===\n{ai_output}\n\n{matchup_text}\n\n=== 📊分析プロンプト(参考) ===\n{full_prompt[:300]}..."
                
                if ui: st.success("✅ 完了")
                yield (r_num, final_res)
                time.sleep(2)

            except Exception as e:
                yield (r_num, f"Error: {e}")
    
    finally:
        driver.quit()
