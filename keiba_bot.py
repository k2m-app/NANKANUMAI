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
# Dify連携ロジック (復活)
# ==================================================
@st.cache_resource
def get_http_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    return sess

def run_dify_prediction(full_text):
    """ Dify APIにレース情報を送信して予想を取得する """
    if not DIFY_API_KEY: return "⚠️ DIFY_API_KEYが設定されていません。"
    
    url = f"{(DIFY_BASE_URL or '').strip().rstrip('/')}/v1/workflows/run"
    payload = {
        "inputs": {"text": full_text}, 
        "response_mode": "blocking", 
        "user": "keiba-bot"
    }
    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}", 
        "Content-Type": "application/json"
    }
    
    sess = get_http_session()
    try:
        # タイムアウトを長めに設定（AIの思考時間考慮）
        res = sess.post(url, headers=headers, json=payload, timeout=90)
        
        if res.status_code != 200:
            return f"⚠️ Dify Error ({res.status_code}): {res.text}"
            
        json_data = res.json()
        # ワークフローの出力形式に合わせてキーを調整してください（通常は 'text' や 'answer'）
        return json_data.get("data", {}).get("outputs", {}).get("text", "（回答なし）")
        
    except Exception as e:
        return f"⚠️ API Connection Error: {e}"

# ==================================================
# データ読み込み・正規化ロジック
# ==================================================
@st.cache_resource
def load_resources():
    res = {"jockeys": [], "trainers": [], "power": {}}
    
    if os.path.exists(JOCKEY_FILE):
        try:
            with open(JOCKEY_FILE, "r", encoding="utf-8-sig") as f:
                res["jockeys"] = [line.strip().replace("，","").replace(",","").replace(" ","").replace("　","") for line in f if line.strip()]
        except Exception as e: print(f"⚠️ Jockey load error: {e}")

    if os.path.exists(TRAINER_FILE):
        try:
            with open(TRAINER_FILE, "r", encoding="utf-8-sig") as f:
                res["trainers"] = [line.strip().replace("，","").replace(",","").replace(" ","").replace("　","") for line in f if line.strip()]
        except Exception as e: print(f"⚠️ Trainer load error: {e}")

    if os.path.exists(POWER_FILE):
        try:
            df = pd.read_csv(POWER_FILE, encoding="utf-8-sig")
            place_col = df.columns[0]
            for _, row in df.iterrows():
                place = str(row[place_col]).strip()
                jockey = str(row.get("騎手名", "")).replace(" ","").replace("　","")
                if place and jockey:
                    power = row.get("騎手パワー", "0")
                    win = row.get("勝率", "0%")
                    fuku = row.get("複勝率", "0%")
                    res["power"][(place, jockey)] = f"騎手パワー:{power}(勝率{win} 複勝率{fuku})"
        except Exception as e: print(f"⚠️ Power load error: {e}")
    
    return res

def normalize_name(abbrev, full_list):
    if not abbrev: return ""
    clean = abbrev.replace(" ","").replace("　","")
    if not full_list: return clean
    if clean in full_list: return clean
    matches = [n for n in full_list if n.startswith(clean) or (len(clean)>=2 and n.startswith(clean[0]) and clean[1] in n)]
    if matches: return sorted(matches, key=len)[0]
    return clean

# ==================================================
# nankankeiba.com 解析ロジック
# ==================================================
def parse_nankankeiba_html(html, place_name, resources):
    soup = BeautifulSoup(html, "html.parser")
    data = {"meta": {}, "horses": {}}

    title_h3 = soup.find("h3", class_="nk23_c-tab1__title")
    data["meta"]["race_name"] = title_h3.get_text(strip=True) if title_h3 else ""
    if data["meta"]["race_name"]:
        parts = data["meta"]["race_name"].split(" ")
        data["meta"]["grade"] = parts[-1] if len(parts) > 1 else ""
    
    cond_a = soup.select_one("a.nk23_c-tab1__subtitle__text.is-blue")
    if cond_a:
        cond_text = cond_a.get_text(strip=True)
        data["meta"]["course"] = f"{place_name} {cond_text}"

    table = soup.select_one("#shosai_aria table.nk23_c-table22__table")
    if not table: return data

    rows = table.select("tbody tr")
    for row in rows:
        try:
            umaban_tag = row.select_one("td.umaban") or row.select_one("td.is-col02")
            if not umaban_tag: continue
            umaban = umaban_tag.get_text(strip=True)
            if not umaban.isdigit(): continue

            horse_tag = row.select_one("td.is-col03 a.is-link")
            horse_name = horse_tag.get_text(strip=True) if horse_tag else ""

            jg_td = row.select_one("td.cs-g1")
            jockey_raw = ""
            trainer_raw = ""
            if jg_td:
                links = jg_td.select("a")
                if len(links) >= 1: jockey_raw = links[0].get_text(strip=True)
                if len(links) >= 2: trainer_raw = links[1].get_text(strip=True)
            
            jockey_full = normalize_name(jockey_raw, resources["jockeys"])
            trainer_full = normalize_name(trainer_raw, resources["trainers"])
            power_info = resources["power"].get((place_name, jockey_full), f"騎手パワー:不明 (※リスト外: {jockey_full})")

            # 相性データ
            ai2_div = row.select_one("td.cs-ai2 .graph_text_div")
            stats_pair = "データなし"
            if ai2_div and "データ" not in ai2_div.get_text():
                rate = ai2_div.select_one(".is-percent").get_text(strip=True)
                win = ai2_div.select_one(".is-number").get_text(strip=True)
                total = ai2_div.select_one(".is-total").get_text(strip=True)
                stats_pair = f"勝率{rate}({win}勝/{total}回)"

            # 近走データ
            history = []
            for i in range(1, 4):
                z_td = row.select_one(f"td.cs-z{i}")
                if not z_td or not z_td.get_text(strip=True): continue
                
                dp_span = z_td.select("p.nk23_u-d-flex span.nk23_u-text10")
                date_place_text = ""
                if dp_span:
                    for s in dp_span:
                        txt = s.get_text(strip=True)
                        if re.search(r"\d+\.\d+\.\d+", txt): date_place_text = txt; break
                
                date_str = ""
                m = re.match(r"([^\d]+)(\d+)\.(\d+)\.(\d+)", date_place_text)
                if m:
                    yy, mm, dd = m.group(2), m.group(3), m.group(4)
                    date_str = f"20{yy}/{int(mm):02}/{int(dd):02}"
                
                cond_text = dp_span[-1].get_text(strip=True) if len(dp_span) >= 2 else ""
                dist_m = re.search(r"\d{4}", cond_text)
                dist = dist_m.group(0) if dist_m else ""
                place_m = re.match(r"^[^\d]+", date_place_text)
                place_h = place_m.group(0) if place_m else ""
                course_str = f"{place_h}{dist}m"

                race_a = z_td.select_one("a.is-link")
                race_title_full = race_a.get("title", "") if race_a else ""
                r_parts = race_title_full.split(" ")
                race_name = r_parts[0] if r_parts else ""
                race_class = r_parts[1] if len(r_parts) > 1 else ""

                j_p_line = z_td.select("p.nk23_u-text10")
                jockey_prev = ""
                pop = ""
                for p in j_p_line:
                    ptxt = p.get_text(strip=True)
                    if "人気" in ptxt:
                        pop_m = re.search(r"(\d+)人気", ptxt)
                        if pop_m: pop = f"{pop_m.group(1)}人気"
                        spans = p.find_all("span")
                        if len(spans) > 1:
                            j_raw = spans[1].get_text(strip=True)
                            jockey_prev = re.sub(r"[\d\.]+", "", j_raw)

                rank_span = z_td.select_one(".nk23_u-text19")
                rank = rank_span.get_text(strip=True).replace("着", "") if rank_span else ""

                pass_str = ""
                agari_str = ""
                pos_p = z_td.select_one("p.position")
                if pos_p: pass_str = "-".join([s.get_text(strip=True) for s in pos_p.find_all("span")])
                
                time_p = z_td.select("p.nk23_u-text10")
                for p in time_p:
                    if "3F" in p.get_text():
                        ag_m = re.search(r"\(([\d]+)\)", p.get_text())
                        if ag_m: agari_str = f"上がり3F：{ag_m.group(1)}位"

                hist_str = (f"開催日：{date_str}　コース：{course_str} レース：{race_name}　"
                            f"クラス：{race_class} 騎手：{jockey_prev}　"
                            f"通過順{pass_str}({agari_str})→{rank}着（{pop}）")
                history.append(hist_str)

            data["horses"][umaban] = {
                "name": horse_name, "jockey": jockey_full, "trainer": trainer_full,
                "power": power_info, "compatibility": stats_pair, "history": history
            }
        except Exception: continue
    return data

# ==================================================
# 競馬ブック 解析ロジック
# ==================================================
def parse_keibabook_danwa(html):
    soup = BeautifulSoup(html, "html.parser")
    d = {}
    tbl = soup.find("table", class_="danwa")
    if tbl and tbl.tbody:
        cur = None
        for row in tbl.tbody.find_all("tr"):
            u = row.find("td", class_="umaban")
            if u: cur = u.get_text(strip=True); continue
            t = row.find("td", class_="danwa")
            if t and cur: d[cur] = t.get_text(strip=True); cur=None
    return d

def parse_keibabook_cyokyo(html):
    soup = BeautifulSoup(html, "html.parser")
    d = {}
    for tbl in soup.find_all("table", class_="cyokyo"):
        tb = tbl.find("tbody")
        if not tb: continue
        rs = tb.find_all("tr", recursive=False)
        if not rs: continue
        u_td = rs[0].find("td", class_="umaban")
        if u_td:
            u = u_td.get_text(strip=True)
            tp = rs[0].find("td", class_="tanpyo").get_text(strip=True) if rs[0].find("td", class_="tanpyo") else ""
            dt = rs[1].get_text(" ", strip=True) if len(rs)>1 else ""
            d[u] = f"【短評】{tp} 【詳細】{dt}"
    return d

# ==================================================
# メイン処理
# ==================================================
def run_races_iter(year, month, day, place_code, target_races, ui=False):
    # 1. データ読み込み
    resources = load_resources()
    
    # 場所コード対応表
    kb_to_nankan = {"10": "20", "11": "21", "12": "19", "13": "18"}
    nankan_place_code = kb_to_nankan.get(place_code)
    place_names = {"10": "大井", "11": "川崎", "12": "船橋", "13": "浦和"}
    place_name = place_names.get(place_code, "地方")

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    try:
        # A. 競馬ブックログイン
        if ui: st.info("🔑 ログイン中...")
        driver.get("https://s.keibabook.co.jp/login/login")
        if "logout" not in driver.current_url:
            wait.until(EC.visibility_of_element_located((By.NAME, "login_id"))).send_keys(KEIBA_ID)
            driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
            driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
            time.sleep(1)

        # B. 開催ID取得
        date_str = f"{year}{month}{day}"
        driver.get(f"https://s.keibabook.co.jp/chihou/nittei/{date_str}10")
        soup_kb = BeautifulSoup(driver.page_source, "html.parser")
        kb_race_ids = [] 
        for a in soup_kb.find_all("a", href=True):
            m = re.search(r"(\d{16})", a["href"])
            if m:
                rid = m.group(1)
                if rid[6:8] == place_code:
                    kb_race_ids.append((int(rid[14:16]), rid))
        kb_race_ids.sort()
        
        # C. レース処理ループ
        for r_num, kb_rid in kb_race_ids:
            if target_races and r_num not in target_races: continue
            
            if ui: st.markdown(f"## {place_name} {r_num}R")
            
            try:
                # 1. 競馬ブック情報
                driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{kb_rid}")
                danwa_dict = parse_keibabook_danwa(driver.page_source)
                driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{kb_rid}")
                cyokyo_dict = parse_keibabook_cyokyo(driver.page_source)

                # 2. nankankeiba情報 (URL特定ロジック)
                if r_num == 1 or 'nk_base_id' not in locals():
                    driver.get(f"https://www.nankankeiba.com/program/{year}{month}{day}{nankan_place_code}.do")
                    lnk = driver.find_element(By.XPATH, f"//a[contains(@href, '{year}{month}{day}{nankan_place_code}') and contains(@href, '{str(r_num).zfill(2)}.do')]")
                    nk_base_id = lnk.get_attribute('href').split("/")[-1].replace(".do", "")[:-2]
                
                driver.get(f"https://www.nankankeiba.com/uma_shosai/{nk_base_id}{str(r_num).zfill(2)}.do")
                nk_data = parse_nankankeiba_html(driver.page_source, place_name, resources)
                
                # 3. プロンプト作成 (指定フォーマット)
                header = f"レース名: {r_num}R {nk_data['meta'].get('race_name','')}　格付け:{nk_data['meta'].get('grade','')}　コース:{nk_data['meta'].get('course','')}"
                horse_texts = []
                
                for u in sorted(nk_data["horses"].keys(), key=int):
                    h = nk_data["horses"][u]
                    danwa = danwa_dict.get(u, "なし")
                    cyokyo = cyokyo_dict.get(u, "調教データなし")
                    
                    # 前走騎手
                    p_jockey = ""
                    if h["history"]:
                        m = re.search(r"騎手：([^　\s]+)", h["history"][0])
                        if m: p_jockey = m.group(1)
                    p_info = f" (前走:{p_jockey})" if p_jockey else ""
                    
                    block = [
                        f"[馬番{u}] {h['name']} 騎手:{h['jockey']}{p_info} 調教師:{h['trainer']}",
                        f"談話: {danwa} 調教:{cyokyo}",
                        f"【騎手】{h['power']} 相性:{h['compatibility']}"
                    ]
                    cn_map = {0:"①", 1:"②", 2:"③"}
                    for idx, hs in enumerate(h["history"]):
                        block.append(f"【近走】{cn_map.get(idx,'')} {hs}")
                    
                    horse_texts.append("\n".join(block))
                
                full_prompt = header + "\n\n" + "\n\n".join(horse_texts)
                
                # 4. Dify送信
                if ui: st.info("🤖 AI分析中...")
                dify_res = run_dify_prediction(full_prompt)
                
                # 5. 結果表示
                # Difyの回答 + 元のプロンプト(確認用)
                final_output = f"📅 {year}/{month}/{day} {place_name}{r_num}R\n\n=== 🤖AI予想 ===\n{dify_res}\n\n=== 📊使用データ ===\n{full_prompt}"
                
                yield (r_num, final_output)
                time.sleep(2)

            except Exception as e:
                yield (r_num, f"Error: {e}")
                
    finally:
        driver.quit()
