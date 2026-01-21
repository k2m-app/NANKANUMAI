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
        except Exception as e: print(f"⚠️ Jockey load error: {e}")

    # 2. 調教師リスト
    if os.path.exists(TRAINER_FILE):
        try:
            with open(TRAINER_FILE, "r", encoding="utf-8-sig") as f:
                res["trainers"] = [line.strip().replace("，","").replace(",","").replace(" ","").replace("　","") for line in f if line.strip()]
        except Exception as e: print(f"⚠️ Trainer load error: {e}")

    # 3. 騎手パワー (CSV: 1列目=場所, C列=騎手名, ... 騎手パワー, 勝率, 複勝率)
    if os.path.exists(POWER_FILE):
        try:
            df = pd.read_csv(POWER_FILE, encoding="utf-8-sig")
            # 1列目が場所(Unnamed: 0の場合あり)、騎手名カラムを探す
            place_col = df.columns[0]
            for _, row in df.iterrows():
                place = str(row[place_col]).strip()
                jockey = str(row.get("騎手名", "")).replace(" ","").replace("　","")
                if place and jockey:
                    power = row.get("騎手パワー", "")
                    win = row.get("勝率", "")
                    fuku = row.get("複勝率", "")
                    res["power"][(place, jockey)] = f"騎手パワー:{power}(勝率{win} 複勝率{fuku})"
        except Exception as e: print(f"⚠️ Power load error: {e}")
    
    return res

def normalize_name(abbrev, full_list):
    """ 略称(木間龍) -> 正式名称(木間塚龍馬) """
    if not abbrev: return ""
    clean = abbrev.replace(" ","").replace("　","")
    if not full_list: return clean
    # 完全一致
    if clean in full_list: return clean
    # 前方一致検索
    matches = [n for n in full_list if n.startswith(clean) or (len(clean)>=2 and n.startswith(clean[0]) and clean[1] in n)]
    if matches:
        # 最も文字数が近い、あるいはリスト順で最初のものを返す
        return sorted(matches, key=len)[0]
    return clean

# ==================================================
# nankankeiba.com 解析ロジック (BeautifulSoup)
# ==================================================
def parse_nankankeiba_html(html, place_name, resources):
    soup = BeautifulSoup(html, "html.parser")
    data = {"meta": {}, "horses": {}}

    # --- 1. レース情報取得 ---
    title_h3 = soup.find("h3", class_="nk23_c-tab1__title")
    data["meta"]["race_name"] = title_h3.get_text(strip=True) if title_h3 else ""
    
    # 格付け (タイトルから抽出、または別の場所)
    # 例: "笑門来福賞 Ｂ３(二)" -> 格付けは "Ｂ３(二)"
    # タイトル文字列をスペースで分割して後ろを取得する簡易ロジック
    if data["meta"]["race_name"]:
        parts = data["meta"]["race_name"].split(" ")
        data["meta"]["grade"] = parts[-1] if len(parts) > 1 else ""
    
    # コース条件
    cond_a = soup.select_one("a.nk23_c-tab1__subtitle__text.is-blue")
    if cond_a:
        # "ダ1,600m（外）" のような形式
        cond_text = cond_a.get_text(strip=True)
        data["meta"]["course"] = f"{place_name} {cond_text}"

    # --- 2. 各馬データ取得 (詳細出走表) ---
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

            # 騎手・調教師
            jg_td = row.select_one("td.cs-g1")
            jockey_raw = ""
            trainer_raw = ""
            if jg_td:
                links = jg_td.select("a")
                if len(links) >= 1: jockey_raw = links[0].get_text(strip=True)
                if len(links) >= 2: trainer_raw = links[1].get_text(strip=True)
            
            # 正規化
            jockey_full = normalize_name(jockey_raw, resources["jockeys"])
            trainer_full = normalize_name(trainer_raw, resources["trainers"])

            # 騎手パワー取得
            power_info = resources["power"].get((place_name, jockey_full), "騎手パワー:不明")

            # 相性データ (騎手x調教師)
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
                
                # 開催日・場所 (例: 浦和26.1.7)
                date_place_text = ""
                dp_span = z_td.select("p.nk23_u-d-flex span.nk23_u-text10")
                if dp_span:
                    # 複数ある場合、日付っぽいものを探す
                    for s in dp_span:
                        txt = s.get_text(strip=True)
                        if re.search(r"\d+\.\d+\.\d+", txt):
                            date_place_text = txt
                            break
                
                # 日付変換 (浦和26.1.7 -> 2026/01/07)
                # 年号26は2026年と仮定
                date_str = ""
                course_short = "" # 浦和1400m
                
                m = re.match(r"([^\d]+)(\d+)\.(\d+)\.(\d+)", date_place_text)
                if m:
                    place_short = m.group(1)
                    yy, mm, dd = m.group(2), m.group(3), m.group(4)
                    date_str = f"20{yy}/{int(mm):02}/{int(dd):02}"
                
                # コース・条件 (稍外ダ1400)
                cond_text = ""
                if len(dp_span) >= 2:
                    cond_text = dp_span[-1].get_text(strip=True)
                
                # 距離抽出 (1400)
                dist_m = re.search(r"\d{4}", cond_text)
                dist = dist_m.group(0) if dist_m else ""
                # 場所名抽出 (日付の頭についてるやつ)
                place_m = re.match(r"^[^\d]+", date_place_text)
                place_h = place_m.group(0) if place_m else ""
                
                course_str = f"{place_h}{dist}m"

                # レース名・クラス
                race_a = z_td.select_one("a.is-link")
                race_title_full = race_a.get("title", "") if race_a else ""
                # "初夢（はつゆめ）特別 Ｂ２(二)Ｂ３(一)" -> 分割
                # 空白で区切られていることが多い
                r_parts = race_title_full.split(" ")
                race_name = r_parts[0] if r_parts else ""
                race_class = r_parts[1] if len(r_parts) > 1 else ""

                # 騎手 (小杉亮55.0) -> 名前だけ抽出
                j_p_line = z_td.select("p.nk23_u-text10")
                jockey_prev = ""
                pop = ""
                rank = ""
                
                # 人気・騎手情報の行を探す
                for p in j_p_line:
                    ptxt = p.get_text(strip=True)
                    if "人気" in ptxt:
                        # "13頭 6番 9人気"
                        pop_m = re.search(r"(\d+)人気", ptxt)
                        if pop_m: pop = f"{pop_m.group(1)}人気"
                        
                        # 同じ行のspanに騎手がいる場合があるが、構造上別タグの場合も
                        # nankankeibaは <p><span>人気</span><span>騎手</span></p>
                        spans = p.find_all("span")
                        if len(spans) > 1:
                            j_raw = spans[1].get_text(strip=True)
                            jockey_prev = re.sub(r"[\d\.]+", "", j_raw) # 数字除去

                # 着順 (10着)
                rank_span = z_td.select_one(".nk23_u-text19")
                if rank_span:
                    rank = rank_span.get_text(strip=True).replace("着", "")

                # 通過順・上がり (7-6-7-11 / 3F 39.9(10))
                pass_str = ""
                agari_str = ""
                
                pos_p = z_td.select_one("p.position")
                if pos_p:
                    pass_str = "-".join([s.get_text(strip=True) for s in pos_p.find_all("span")])
                
                time_p = z_td.select("p.nk23_u-text10")
                for p in time_p:
                    if "3F" in p.get_text():
                        # "1:30.3(1.2) 3F 39.9(10)"
                        ft = p.select_one(".furlongtime")
                        if ft:
                            ft_text = ft.get_text(strip=True)
                            # (10) を抽出
                            ag_m = re.search(r"\(([\d]+)\)", ft_text)
                            if ag_m:
                                agari_str = f"上がり3F：{ag_m.group(1)}位"

                # 整形
                # ①開催日：...
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

        except Exception as e:
            print(f"Parse error at row: {e}")
            continue
            
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
    
    # 場所コード変換 (KeibaBook -> Nankan)
    # 浦和:18, 船橋:19, 大井:20, 川崎:21
    # KB: 10=大井, 11=川崎, 12=船橋, 13=浦和
    kb_to_nankan = {"10": "20", "11": "21", "12": "19", "13": "18"}
    nankan_place_code = kb_to_nankan.get(place_code)
    
    place_names = {"10": "大井", "11": "川崎", "12": "船橋", "13": "浦和"}
    place_name = place_names.get(place_code, "地方")

    # Selenium起動
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    try:
        # 1. 競馬ブックログイン
        if ui: st.info("🔑 ログイン中...")
        driver.get("https://s.keibabook.co.jp/login/login")
        if "logout" not in driver.current_url:
            wait.until(EC.visibility_of_element_located((By.NAME, "login_id"))).send_keys(KEIBA_ID)
            driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
            driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
            time.sleep(1)

        # 2. 開催回・日次取得 (nankankeiba URL生成用)
        # 競馬ブックの日程ページからリンクを取得してIDを解析するのが確実
        date_str = f"{year}{month}{day}"
        kb_url = f"https://s.keibabook.co.jp/chihou/nittei/{date_str}10"
        driver.get(kb_url)
        
        # 競馬ブックのレースIDリストを取得
        soup_kb = BeautifulSoup(driver.page_source, "html.parser")
        kb_race_ids = [] # (race_num, kb_id)
        for a in soup_kb.find_all("a", href=True):
            m = re.search(r"(\d{16})", a["href"])
            if m:
                rid = m.group(1)
                # 場所コードチェック
                if rid[6:8] == place_code:
                    r_num = int(rid[14:16])
                    kb_race_ids.append((r_num, rid))
        
        kb_race_ids.sort()
        
        # nankankeibaの開催回・日次特定 (簡易的にnankankeibaの日程ページを見るか、スクレイピングで取得)
        # ここではnankankeibaのトップページ等から当日のリンクを探すロジックが複雑なため、
        # ユーザー提供情報にある「_get_kai_nichi_from_web」相当の処理が必要ですが、
        # 簡略化のため、URL生成に必要な「回・日」をスクレイピングで取得します。
        
        # 3. レースループ
        for r_num, kb_rid in kb_race_ids:
            if target_races and r_num not in target_races: continue
            
            if ui: st.markdown(f"## {place_name} {r_num}R")
            
            try:
                # --- A. 競馬ブック情報取得 ---
                # 談話
                driver.get(f"https://s.keibabook.co.jp/chihou/danwa/1/{kb_rid}")
                danwa_dict = parse_keibabook_danwa(driver.page_source)
                # 調教
                driver.get(f"https://s.keibabook.co.jp/chihou/cyokyo/1/{kb_rid}")
                cyokyo_dict = parse_keibabook_cyokyo(driver.page_source)

                # --- B. nankankeiba情報取得 ---
                # URLを特定する必要がある。競馬ブックのIDには回・日が含まれない(YYYYMMDDppRR00)
                # nankankeibaは YYYYMMDDppKkDDRR (Kk=回, DD=日)
                # 開催情報ページからリンクを取得する
                if r_num == 1 or 'nk_base_url' not in locals():
                    # 1Rの時に開催情報を取得し、ベースURLフォーマットを特定
                    nk_sched_url = "https://www.nankankeiba.com/calendar/000000.do"
                    driver.get(nk_sched_url)
                    # カレンダーから当日のリンクを探す...のは大変なので
                    # 直接当日の出走表一覧へアクセス (YYYYMMDDpp.do)
                    nk_prog_url = f"https://www.nankankeiba.com/program/{year}{month}{day}{nankan_place_code}.do"
                    driver.get(nk_prog_url)
                    # 該当レースのリンクを探す
                    lnk = driver.find_element(By.XPATH, f"//a[contains(@href, '{year}{month}{day}{nankan_place_code}') and contains(@href, '{str(r_num).zfill(2)}.do')]")
                    href = lnk.get_attribute('href')
                    # href = .../2026012119100301.do -> ID抽出
                    nk_id_full = href.split("/")[-1].replace(".do", "")
                    # ベース部分 (YYYYMMDDppKkDD)
                    nk_base_id = nk_id_full[:-2]
                
                # 対象レースのURL
                nk_race_url = f"https://www.nankankeiba.com/uma_shosai/{nk_base_id}{str(r_num).zfill(2)}.do"
                driver.get(nk_race_url)
                
                # 詳細データ解析
                nk_data = parse_nankankeiba_html(driver.page_source, place_name, resources)
                
                # --- C. データ統合・出力生成 ---
                
                # ヘッダー出力
                header_text = f"レース名: {r_num}R {nk_data['meta'].get('race_name','')}　格付け:{nk_data['meta'].get('grade','')}　コース:{nk_data['meta'].get('course','')}"
                output_lines = [header_text, ""]
                
                # 馬ごとの出力
                # nk_data['horses'] は umaban(str) がキー
                for u in sorted(nk_data["horses"].keys(), key=int):
                    h_data = nk_data["horses"][u]
                    
                    # 競馬ブックデータ
                    danwa = danwa_dict.get(u, "なし")
                    cyokyo = cyokyo_dict.get(u, "調教データなし")
                    
                    # 前走騎手を取得 (Historyの1番目から抽出)
                    prev_jockey = ""
                    if h_data["history"]:
                        # "騎手：小杉亮" を探す
                        m = re.search(r"騎手：([^　\s]+)", h_data["history"][0])
                        if m: prev_jockey = m.group(1)
                    
                    # 前走情報文字列 (前走:小杉亮)
                    prev_info = f" (前走:{prev_jockey})" if prev_jockey else ""
                    
                    # 基本情報行
                    line1 = f"[馬番{u}] {h_data['name']} 騎手:{h_data['jockey']}{prev_info} 調教師:{h_data['trainer']}"
                    
                    # 談話・調教行
                    line2 = f"談話: {danwa} 調教:{cyokyo}"
                    
                    # 騎手データ行
                    line3 = f"【騎手】{h_data['power']} 相性:{h_data['compatibility']}"
                    
                    # 近走データ行
                    hist_lines = []
                    cn_map = {0:"①", 1:"②", 2:"③"}
                    for idx, h_str in enumerate(h_data["history"]):
                        hist_lines.append(f"【近走】{cn_map.get(idx,'')} {h_str}")
                    
                    # 結合
                    block = "\n".join([line1, line2, line3] + hist_lines)
                    output_lines.append(block + "\n") # 空行区切り
                
                final_output = "\n".join(output_lines)
                
                if ui: st.text_area(f"{r_num}R 出力結果", final_output, height=300)
                yield (r_num, final_output)
                
                time.sleep(2)

            except Exception as e:
                yield (r_num, f"Error: {e}")
                
    finally:
        driver.quit()
