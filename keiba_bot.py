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
# Streamlit Cloudなどの環境に合わせてパスを調整してください
DATA_DIR = "2025data"
JOCKEY_FILE = os.path.join(DATA_DIR, "2025_NARJockey.csv")
TRAINER_FILE = os.path.join(DATA_DIR, "2025_NankanTrainer.csv")
POWER_FILE = os.path.join(DATA_DIR, "2025_騎手パワー.csv")

# ==================================================
# 【設定】アカウント・API (Secretsから読み込み)
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
        # 出力キーはワークフローの設定に依存します（'text', 'answer', 'result'など）
        # ここでは一般的な 'text' を取得し、なければjson全体を返します
        outputs = json_data.get("data", {}).get("outputs", {})
        return outputs.get("text") or outputs.get("result") or str(outputs)
        
    except Exception as e:
        return f"⚠️ API Connection Error: {e}"

# ==================================================
# データ読み込み・正規化ロジック
# ==================================================
@st.cache_resource
def load_resources():
    res = {"jockeys": [], "trainers": [], "power": {}}
    
    # 1. 騎手リスト
    if os.path.exists(JOCKEY_FILE):
        try:
            with open(JOCKEY_FILE, "r", encoding="utf-8-sig") as f:
                res["jockeys"] = [line.strip().replace("，","").replace(",","").replace(" ","").replace("　","") for line in f if line.strip()]
        except Exception as e: print(f"⚠️ Jockey list load error: {e}")

    # 2. 調教師リスト
    if os.path.exists(TRAINER_FILE):
        try:
            with open(TRAINER_FILE, "r", encoding="utf-8-sig") as f:
                res["trainers"] = [line.strip().replace("，","").replace(",","").replace(" ","").replace("　","") for line in f if line.strip()]
        except Exception as e: print(f"⚠️ Trainer list load error: {e}")

    # 3. 騎手パワーCSV
    if os.path.exists(POWER_FILE):
        try:
            df = pd.read_csv(POWER_FILE, encoding="utf-8-sig")
            # 1列目が競馬場名と仮定 (カラム名が Unnamed: 0 になっている可能性があるため)
            place_col = df.columns[0]
            
            for _, row in df.iterrows():
                place = str(row[place_col]).strip()
                jockey = str(row.get("騎手名", "")).replace(" ","").replace("　","")
                
                if place and jockey:
                    power = row.get("騎手パワー", "-")
                    win = row.get("勝率", "-")
                    fuku = row.get("複勝率", "-")
                    
                    # 検索キー: (競馬場, 騎手名)
                    key = (place, jockey)
                    res["power"][key] = f"騎手パワー:{power}(勝率{win} 複勝率{fuku})"
                    
            print(f"✅ Power data loaded: {len(res['power'])} records")
        except Exception as e: print(f"⚠️ Power data load error: {e}")
    
    return res

def normalize_name(abbrev, full_list):
    """ 略称 -> 正式名称への変換 (名寄せ) """
    if not abbrev: return ""
    clean = abbrev.replace(" ","").replace("　","")
    if not full_list: return clean
    if clean in full_list: return clean
    
    # 前方一致検索 (例: "木間龍" -> "木間塚龍馬")
    # 2文字以上一致、かつ先頭が一致するもの
    matches = [n for n in full_list if n.startswith(clean) or (len(clean)>=2 and n.startswith(clean[0]) and clean[1] in n)]
    
    if matches:
        # 最も短いもの（あるいはリスト順）を返す
        return sorted(matches, key=len)[0]
    return clean

# ==================================================
# nankankeiba.com 解析ロジック (BeautifulSoup)
# ==================================================
def parse_nankankeiba_html(html, place_name, resources):
    soup = BeautifulSoup(html, "html.parser")
    data = {"meta": {}, "horses": {}}

    # --- 1. レース情報 ---
    title_h3 = soup.find("h3", class_="nk23_c-tab1__title")
    data["meta"]["race_name"] = title_h3.get_text(strip=True) if title_h3 else ""
    
    # 格付け抽出 (タイトル文字列の末尾などから推測)
    if data["meta"]["race_name"]:
        # 全角スペースや半角スペースで区切られていることが多い
        parts = re.split(r'[ 　]+', data["meta"]["race_name"])
        data["meta"]["grade"] = parts[-1] if len(parts) > 1 else ""
    
    cond_a = soup.select_one("a.nk23_c-tab1__subtitle__text.is-blue")
    if cond_a:
        cond_text = cond_a.get_text(strip=True)
        data["meta"]["course"] = f"{place_name} {cond_text}"

    # --- 2. 詳細出走表解析 ---
    table = soup.select_one("#shosai_aria table.nk23_c-table22__table")
    if not table: return data

    rows = table.select("tbody tr")
    for row in rows:
        try:
            # 馬番
            umaban_tag = row.select_one("td.umaban") or row.select_one("td.is-col02")
            if not umaban_tag: continue
            umaban = umaban_tag.get_text(strip=True)
            if not umaban.isdigit(): continue

            # 馬名
            horse_tag = row.select_one("td.is-col03 a.is-link")
            horse_name = horse_tag.get_text(strip=True) if horse_tag else ""

            # 騎手・調教師（HTML上の略称）
            jg_td = row.select_one("td.cs-g1")
            jockey_raw = ""
            trainer_raw = ""
            if jg_td:
                links = jg_td.select("a")
                if len(links) >= 1: jockey_raw = links[0].get_text(strip=True)
                if len(links) >= 2: trainer_raw = links[1].get_text(strip=True)
            
            # 正規化 (CSVリストを使ってフルネームへ)
            jockey_full = normalize_name(jockey_raw, resources["jockeys"])
            trainer_full = normalize_name(trainer_raw, resources["trainers"])

            # 騎手パワー取得 (場所名と騎手名で検索)
            # マッチしない場合はデフォルト値を設定
            power_info = resources["power"].get((place_name, jockey_full), f"騎手パワー:不明")

            # 相性データ (騎手x調教師: cs-ai2)
            # nankankeibaは「勝率」と「勝利数/騎乗数」を持っている
            ai2_div = row.select_one("td.cs-ai2 .graph_text_div")
            stats_pair = "データなし"
            if ai2_div and "データ" not in ai2_div.get_text():
                rate = ai2_div.select_one(".is-percent").get_text(strip=True)
                win = ai2_div.select_one(".is-number").get_text(strip=True)
                total = ai2_div.select_one(".is-total").get_text(strip=True)
                stats_pair = f"勝率{rate}({win}勝/{total}回)"

            # --- 近走データ (過去3走: cs-z1 ~ cs-z3) ---
            history = []
            for i in range(1, 4): # 1, 2, 3
                z_td = row.select_one(f"td.cs-z{i}")
                if not z_td or not z_td.get_text(strip=True): continue
                
                # 日付・場所取得 (例: 浦和26.1.7)
                dp_span = z_td.select("p.nk23_u-d-flex span.nk23_u-text10")
                date_place_text = ""
                if dp_span:
                    for s in dp_span:
                        txt = s.get_text(strip=True)
                        if re.search(r"\d+\.\d+\.\d+", txt): date_place_text = txt; break
                
                # 日付整形
                date_str = ""
                m = re.match(r"([^\d]+)(\d+)\.(\d+)\.(\d+)", date_place_text)
                if m:
                    # 年号26 -> 2026
                    yy, mm, dd = m.group(2), m.group(3), m.group(4)
                    date_str = f"20{yy}/{int(mm):02}/{int(dd):02}"
                
                # コース条件 (例: 稍外ダ1400)
                cond_text = dp_span[-1].get_text(strip=True) if len(dp_span) >= 2 else ""
                
                # 距離と場所を結合
                dist_m = re.search(r"\d{4}", cond_text)
                dist = dist_m.group(0) if dist_m else ""
                place_m = re.match(r"^[^\d]+", date_place_text)
                place_h = place_m.group(0) if place_m else ""
                course_str = f"{place_h}{dist}m"

                # レース名・クラス
                race_a = z_td.select_one("a.is-link")
                race_title_full = race_a.get("title", "") if race_a else ""
                r_parts = re.split(r'[ 　]+', race_title_full) # 空白区切り
                race_name = r_parts[0] if r_parts else ""
                race_class = r_parts[1] if len(r_parts) > 1 else ""

                # 騎手・人気・着順
                j_p_line = z_td.select("p.nk23_u-text10")
                jockey_prev = ""
                pop = ""
                for p in j_p_line:
                    ptxt = p.get_text(strip=True)
                    if "人気" in ptxt:
                        pop_m = re.search(r"(\d+)人気", ptxt)
                        if pop_m: pop = f"{pop_m.group(1)}人気"
                        # 同じPタグ内、あるいは兄弟要素から騎手名を取得
                        spans = p.find_all("span")
                        if len(spans) > 1:
                            # 数字を除去して名前だけにする (小杉亮55.0 -> 小杉亮)
                            j_raw = spans[1].get_text(strip=True)
                            jockey_prev = re.sub(r"[\d\.]+", "", j_raw)

                rank_span = z_td.select_one(".nk23_u-text19")
                rank = rank_span.get_text(strip=True).replace("着", "") if rank_span else ""

                # 通過順
                pass_str = ""
                pos_p = z_td.select_one("p.position")
                if pos_p: 
                    pass_str = "-".join([s.get_text(strip=True) for s in pos_p.find_all("span")])
                
                # 上がり3F
                agari_str = ""
                for p in j_p_line: # すでに取得したpリストを再利用
                    if "3F" in p.get_text():
                        # "3F 39.9(10)" -> "(10)"を抽出
                        ag_m = re.search(r"\(([\d]+)\)", p.get_text())
                        if ag_m: agari_str = f"上がり3F：{ag_m.group(1)}位"

                # 指定フォーマットへ整形
                hist_str = (f"開催日：{date_str}　コース：{course_str} レース：{race_name}　"
                            f"クラス：{race_class} 騎手：{jockey_prev}　"
                            f"通過順{pass_str}({agari_str})→{rank}着（{pop}）")
                history.append(hist_str)

            data["horses"][umaban] = {
                "name": horse_name, 
                "jockey": jockey_full, 
                "trainer": trainer_full,
                "power": power_info, 
                "compatibility": stats_pair, 
                "history": history
            }
        except Exception: continue
            
    return data

# ==================================================
# 競馬ブック 解析ロジック (談話・調教)
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
# メイン処理 (データ収集 -> プロンプト作成 -> AI連携)
# ==================================================
def run_races_iter(year, month, day, place_code, target_races, ui=False):
    # 1. リソース読み込み
    resources = load_resources()
    
    # 2. 場所コード対応 (KeibaBook: 10~13 -> Nankan: 20,21,19,18)
    kb_to_nankan = {"10": "20", "11": "21", "12": "19", "13": "18"}
    nankan_place_code = kb_to_nankan.get(place_code)
    place_names = {"10": "大井", "11": "川崎", "12": "船橋", "13": "浦和"}
    place_name = place_names.get(place_code, "地方")

    # 3. ブラウザ起動
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    try:
        # A. 競馬ブックにログイン
        if ui: st.info("🔑 ログイン中 (KeibaBook)...")
        driver.get("https://s.keibabook.co.jp/login/login")
        if "logout" not in driver.current_url:
            wait.until(EC.visibility_of_element_located((By.NAME, "login_id"))).send_keys(KEIBA_ID)
            driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
            driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
            time.sleep(1)

        # B. 開催レースIDリスト取得 (KeibaBookの日程ページから)
        date_str = f"{year}{month}{day}"
        driver.get(f"https://s.keibabook.co.jp/chihou/nittei/{date_str}10")
        soup_kb = BeautifulSoup(driver.page_source, "html.parser")
        
        kb_race_ids = [] 
        for a in soup_kb.find_all("a", href=True):
            m = re.search(r"(\d{16})", a["href"])
            if m:
                rid = m.group(1)
                # 場所コードが一致するものだけ
                if rid[6:8] == place_code:
                    kb_race_ids.append((int(rid[14:16]), rid))
        kb_race_ids.sort()
        
        # C. レースごとのループ処理
        for r_num, kb_rid in kb_race_ids:
            if target_races and r_num not in target_races: continue
            
            if ui: st.markdown(f"## {place_name} {r_num}R")
            
            try:
                # --- [Step 1] 競馬ブックから談話・調教を取得 ---
                driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{kb_rid}")
                danwa_dict = parse_keibabook_danwa(driver.page_source)
                driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{kb_rid}")
                cyokyo_dict = parse_keibabook_cyokyo(driver.page_source)

                # --- [Step 2] nankankeiba.comから詳細データを取得 ---
                # URLを特定 (nankankeibaのIDには「回・日次」が含まれるため、プログラムページからリンクを探す)
                if r_num == 1 or 'nk_base_id' not in locals():
                    # 当日のプログラム一覧へアクセス
                    prog_url = f"https://www.nankankeiba.com/program/{year}{month}{day}{nankan_place_code}.do"
                    driver.get(prog_url)
                    # 該当レース番号のリンクを探してIDのベース部分(YYYYMMDDppKkDD)を抽出
                    try:
                        lnk = driver.find_element(By.XPATH, f"//a[contains(@href, '{year}{month}{day}{nankan_place_code}') and contains(@href, '{str(r_num).zfill(2)}.do')]")
                        href = lnk.get_attribute('href')
                        nk_id_full = href.split("/")[-1].replace(".do", "")
                        nk_base_id = nk_id_full[:-2] # 末尾のレース番号を除去
                    except:
                        yield (r_num, "⚠️ nankankeiba URL特定失敗"); continue
                
                # 詳細出走表へアクセス
                nk_race_url = f"https://www.nankankeiba.com/uma_shosai/{nk_base_id}{str(r_num).zfill(2)}.do"
                driver.get(nk_race_url)
                
                # データ解析
                nk_data = parse_nankankeiba_html(driver.page_source, place_name, resources)
                
                # --- [Step 3] AI用プロンプトの作成 ---
                header = f"レース名: {r_num}R {nk_data['meta'].get('race_name','')}　格付け:{nk_data['meta'].get('grade','')}　コース:{nk_data['meta'].get('course','')}"
                horse_texts = []
                
                for u in sorted(nk_data["horses"].keys(), key=int):
                    h = nk_data["horses"][u]
                    danwa = danwa_dict.get(u, "なし")
                    cyokyo = cyokyo_dict.get(u, "調教データなし")
                    
                    # 前走騎手情報の抽出 (近走履歴の1行目から)
                    p_jockey = ""
                    if h["history"]:
                        m = re.search(r"騎手：([^　\s]+)", h["history"][0])
                        if m: p_jockey = m.group(1)
                    p_info = f" (前走:{p_jockey})" if p_jockey else ""
                    
                    # ブロック構築
                    block = [
                        f"[馬番{u}] {h['name']} 騎手:{h['jockey']}{p_info} 調教師:{h['trainer']}",
                        f"談話: {danwa} 調教:{cyokyo}",
                        f"【騎手】{h['power']} 相性:{h['compatibility']}"
                    ]
                    
                    # 近走履歴追加
                    cn_map = {0:"①", 1:"②", 2:"③"}
                    for idx, hs in enumerate(h["history"]):
                        block.append(f"【近走】{cn_map.get(idx,'')} {hs}")
                    
                    horse_texts.append("\n".join(block))
                
                # 完成したプロンプト
                full_prompt = header + "\n\n" + "\n\n".join(horse_texts)
                
                # --- [Step 4] Difyへ送信 & 予想取得 ---
                if ui: st.info("🤖 AI分析中 (Dify)...")
                dify_res = run_dify_prediction(full_prompt)
                
                # --- [Step 5] 結果出力 ---
                final_output = f"📅 {year}/{month}/{day} {place_name}{r_num}R\n\n=== 🤖AI予想 ===\n{dify_res}\n\n=== 📊使用データ(抜粋) ===\n{full_prompt[:500]}..."
                
                if ui: st.success("✅ 予想完了")
                yield (r_num, final_output)
                time.sleep(2)

            except Exception as e:
                yield (r_num, f"Error: {e}")
                
    finally:
        driver.quit()
