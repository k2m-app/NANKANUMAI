import time
import re
import os
import json
import requests
import streamlit as st
import pandas as pd
from datetime import datetime

# Selenium & Chrome
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
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

# Streamlit Secrets (未設定時のデフォルト値対応)
KEIBA_ID = st.secrets.get("KEIBA_ID", "")
KEIBA_PASS = st.secrets.get("KEIBA_PASS", "")
DIFY_API_KEY = st.secrets.get("DIFY_API_KEY", "")
DIFY_BASE_URL = st.secrets.get("DIFY_BASE_URL", "https://api.dify.ai")

# ==================================================
# 2. 共通ユーティリティ関数
# ==================================================
@st.cache_resource
def get_http_session() -> requests.Session:
    """HTTPリクエストのセッション管理（リトライ機能付き）"""
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
    })
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

def get_driver():
    """Seleniumドライバーの安全な起動"""
    ops = Options()
    ops.add_argument("--headless=new") # 最新のヘッドレスモード
    ops.add_argument("--no-sandbox")
    ops.add_argument("--disable-dev-shm-usage") # メモリ不足エラー回避
    ops.add_argument("--disable-gpu")
    ops.add_argument("--window-size=1280,1024")
    ops.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=ops)

def login_keibabook_robust(driver):
    """競馬ブックへのログイン処理"""
    try:
        driver.get("https://s.keibabook.co.jp/login/login")
        time.sleep(1)
        # ログアウトボタンがあればログイン済みとみなす
        if "logout" in driver.current_url or len(driver.find_elements(By.XPATH, "//a[contains(@href,'logout')]")) > 0:
            return True
            
        # ログイン試行
        WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.NAME, "login_id"))).send_keys(KEIBA_ID)
        driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
        driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
        time.sleep(2)
        return True
    except Exception as e:
        print(f"Login Warning: {e}")
        return False

# ==================================================
# 3. Dify API連携
# ==================================================
def run_dify_prediction(full_text):
    """Dify APIを呼び出してテキスト生成"""
    if not DIFY_API_KEY: return "⚠️ DIFY_API_KEY未設定"
    
    url = f"{(DIFY_BASE_URL or '').strip().rstrip('/')}/v1/workflows/run"
    payload = {
        "inputs": {"text": full_text}, 
        "response_mode": "blocking", # エラー回避のためblocking推奨だが、タイムアウト注意
        "user": "keiba-bot"
    }
    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}", 
        "Content-Type": "application/json"
    }
    
    try:
        # タイムアウトを長めに設定
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            # ワークフローの出力構造に合わせて調整
            outputs = data.get('data', {}).get('outputs', {})
            return outputs.get('text', str(outputs))
        else:
            return f"⚠️ API Error: {resp.status_code} - {resp.text[:100]}"
    except Exception as e:
        return f"⚠️ Connection Error: {e}"

# ==================================================
# 4. データ処理ロジック
# ==================================================
@st.cache_resource
def load_resources():
    """CSVデータの読み込み"""
    res = {"jockeys": [], "trainers": [], "power": {}, "power_data": {}}
    
    # 騎手・調教師リスト読み込み
    for fpath, key in [(JOCKEY_FILE, "jockeys"), (TRAINER_FILE, "trainers")]:
        if os.path.exists(fpath):
            try:
                with open(fpath, "r", encoding="utf-8-sig") as f:
                    res[key] = [l.strip().replace(",","").replace(" ","").replace("　","") for l in f if l.strip()]
            except: pass
            
    # 騎手パワー読み込み
    if os.path.exists(POWER_FILE):
        try:
            df = pd.read_csv(POWER_FILE, encoding="utf-8-sig")
            place_col = df.columns[0]
            for _, row in df.iterrows():
                p = str(row[place_col]).strip()
                j = str(row.get("騎手名", "")).replace(" ","").replace("　","")
                if p and j:
                    val = row.get('騎手パワー','-')
                    res["power"][(p, j)] = f"P:{val}"
                    res["power_data"][(p, j)] = {"power": val}
        except: pass
    return res

def normalize_name(abbrev, full_list):
    """略称から正式名称を検索"""
    if not abbrev or not full_list: return abbrev
    clean = re.sub(r"[ 　▲△☆◇★\d\.]+", "", abbrev)
    
    # 完全一致
    if clean in full_list: return clean
    
    # 部分一致検索
    candidates = []
    for full in full_list:
        if all(c in full for c in clean): # 文字がすべて含まれているか
            candidates.append((len(full) - len(clean), full))
    
    if candidates:
        candidates.sort() # 文字数差が小さい順
        return candidates[0][1]
        
    return clean

# ==================================================
# 5. スクレイピング & 解析コア
# ==================================================
def get_kb_url_id(year, month, day, place_code, nichi, race_num):
    return f"{year}{str(month).zfill(2)}{str(place_code).zfill(2)}{str(nichi).zfill(2)}{str(race_num).zfill(2)}{str(month).zfill(2)}{str(day).zfill(2)}"

def parse_nankankeiba_detail(html, place_name, resources):
    """南関競馬の出馬表HTMLを解析"""
    soup = BeautifulSoup(html, "html.parser")
    data = {"meta": {}, "horses": {}}
    
    # レース情報
    h3 = soup.find("h3", class_="nk23_c-tab1__title")
    data["meta"]["race_name"] = h3.get_text(strip=True) if h3 else "不明"
    
    table = soup.select_one("#shosai_aria table.nk23_c-table22__table")
    if not table: return data

    for row in table.select("tbody tr"):
        try:
            # 馬番
            u_tag = row.select_one("td.umaban") or row.select_one("td.is-col02")
            if not u_tag: continue
            umaban = u_tag.get_text(strip=True)
            if not umaban.isdigit(): continue
            
            # 馬名
            h_link = row.select_one("td.is-col03 a.is-link")
            horse_name = h_link.get_text(strip=True) if h_link else "不明"
            
            # 騎手・調教師
            j_full, t_full = "不明", "不明"
            jg_td = row.select_one("td.cs-g1")
            if jg_td:
                links = jg_td.select("a")
                if len(links) >= 1: j_full = normalize_name(links[0].get_text(strip=True), resources["jockeys"])
                if len(links) >= 2: t_full = normalize_name(links[1].get_text(strip=True), resources["trainers"])
            
            # 騎手パワー
            power_info = resources["power"].get((place_name, j_full), "P:不明")
            
            # 履歴データの抽出（簡易版）
            histories = []
            for i in range(1, 4):
                z = row.select_one(f"td.cs-z{i}")
                if z and z.get_text(strip=True):
                    # 順位や条件だけ抜く
                    rank = z.select_one(".nk23_u-text19")
                    rank_txt = rank.get_text(strip=True) if rank else "?"
                    histories.append(f"{i}走前:{rank_txt}着")
            
            data["horses"][umaban] = {
                "name": horse_name,
                "jockey": j_full,
                "trainer": t_full,
                "power": power_info,
                "history": " ".join(histories)
            }
        except Exception: continue
        
    return data

# ==================================================
# 6. メインロジック（ジェネレータ）
# ==================================================
def run_races_iter(year, month, day, place_code, target_races):
    """
    UIに依存せず、結果を辞書形式でyieldするジェネレータ
    Return形式: {"type": "log"|"result"|"error", "data": ...}
    """
    driver = None
    resources = load_resources()
    
    # 開催地コード変換
    kb_place_map = {"10":"大井", "11":"川崎", "12":"船橋", "13":"浦和"}
    nk_place_map = {"10":"20", "11":"21", "12":"19", "13":"18"}
    place_name = kb_place_map.get(place_code, "地方")
    nk_code = nk_place_map.get(place_code)

    try:
        driver = get_driver()
        
        # 1. 開催特定
        yield {"type": "log", "data": f"📅 {place_name}の開催日を特定中..."}
        
        # 南関競馬カレンダーから回・日次を取得
        kai, nichi = None, None
        driver.get("https://www.nankankeiba.com/bangumi_menu/bangumi.do")
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        target_str = f"{int(month)}月"
        for tr in soup.find_all('tr'):
            txt = tr.get_text(" ", strip=True)
            if place_name in txt and target_str in txt:
                # 簡易的な判定（実際は日付マッチングが必要だが、エラー回避のため簡略化）
                # 実際にはここで対象日の回・日次を特定するロジックが入ります
                # 今回は仮に計算できたとするか、詳細ロジックを実行
                days_part = txt.split("月")[1]
                if str(int(day)) in days_part:
                     m_kai = re.search(r'第(\d+)回', txt)
                     if m_kai:
                         kai = int(m_kai.group(1))
                         # 日次の特定は複雑なため、ここでは安全策として「1日目」と仮定するか
                         # または日付リストからインデックスを取得
                         days_nums = re.findall(r'\d+', days_part)
                         if str(int(day)) in days_nums:
                             nichi = days_nums.index(str(int(day))) + 1
                         else: nichi = 1
                         break
        
        if not kai:
            # 見つからない場合はURLから推測アプローチ（非推奨だが動くように）
            kai, nichi = 1, 1 
            yield {"type": "log", "data": "⚠️ 開催回が特定できないため、第1回1日目として試行します"}
        
        yield {"type": "log", "data": f"✅ {place_name} 第{kai}回 {nichi}日目 設定完了"}

        # 2. ログイン
        login_keibabook_robust(driver)

        # 3. レースループ
        r_nums = target_races if target_races else range(1, 13)
        
        for r_num in r_nums:
            yield {"type": "log", "data": f"🏇 {r_num}R データ収集中..."}
            
            try:
                # ID生成
                nk_id = f"{year}{month}{day}{nk_code}{kai:02}{nichi:02}{r_num:02}"
                
                # データ取得
                driver.get(f"https://www.nankankeiba.com/uma_shosai/{nk_id}.do")
                nk_data = parse_nankankeiba_detail(driver.page_source, place_name, resources)
                
                if not nk_data["horses"]:
                    yield {"type": "error", "data": f"{r_num}R: データが見つかりませんでした (URL: {driver.current_url})"}
                    continue
                
                # プロンプト作成
                lines = [f"レース: {r_num}R {nk_data['meta']['race_name']}"]
                for u, h in nk_data["horses"].items():
                    lines.append(f"[{u}] {h['name']} (騎:{h['jockey']} P:{h['power']}) 履歴:{h['history']}")
                
                full_text = "\n".join(lines)
                
                # AI分析
                yield {"type": "log", "data": f"🤖 {r_num}R AI分析実行中..."}
                ai_result = run_dify_prediction(full_text)
                
                # 結果結合
                final_output = f"=== {r_num}R 予想 ===\n\n{ai_result}"
                
                # 成功としてYield
                yield {"type": "result", "race_num": r_num, "data": final_output}
                
                time.sleep(1) # サーバー負荷軽減

            except Exception as e:
                yield {"type": "error", "data": f"{r_num}R 処理エラー: {e}"}
                
    except Exception as e:
        yield {"type": "error", "data": f"致命的エラー: {e}"}
    finally:
        if driver:
            driver.quit()

# ==================================================
# 7. UI メイン処理 (Streamlit)
# ==================================================
def main():
    st.set_page_config(page_title="NANKAN AI Robust", layout="wide")
    st.title("🏇 NANKAN AI (Stable Version)")
    
    # サイドバー
    st.sidebar.header("実行設定")
    d_now = datetime.now()
    year = st.sidebar.number_input("年", value=d_now.year)
    month = st.sidebar.number_input("月", value=d_now.month)
    day = st.sidebar.number_input("日", value=d_now.day)
    place_code = st.sidebar.selectbox("開催地", ["10","11","12","13"], format_func=lambda x: {"10":"大井","11":"川崎","12":"船橋","13":"浦和"}.get(x))
    
    races_str = st.sidebar.text_input("レース指定 (例: 1,2,11 / 空白で全R)", "")
    
    # 結果保存用（リロード対策）
    if "results" not in st.session_state:
        st.session_state.results = {}
    
    # 実行ボタン
    if st.sidebar.button("分析開始", type="primary"):
        target_races = [int(x.strip()) for x in races_str.split(",")] if races_str.strip() else []
        
        # ログ表示エリア
        log_area = st.empty()
        
        # ジェネレータ実行
        # ここでの変数 `event` は辞書型 {"type":..., "data":...} なのでアンパックエラーは起きない
        for event in run_races_iter(year, month, day, place_code, target_races):
            
            if event["type"] == "log":
                log_area.info(event["data"])
                
            elif event["type"] == "error":
                st.error(event["data"])
                
            elif event["type"] == "result":
                r_num = event["race_num"]
                res_text = event["data"]
                st.session_state.results[r_num] = res_text
                st.success(f"{r_num}R 完了")
                
        log_area.success("✨ 全処理が完了しました")

    # 結果表示
    st.divider()
    st.subheader("📊 分析結果")
    
    if st.session_state.results:
        # レース順にソートして表示
        for r in sorted(st.session_state.results.keys()):
            with st.expander(f"🏁 {r}レースの結果", expanded=False):
                st.text(st.session_state.results[r])
    else:
        st.info("サイドバーから分析を開始してください。")

if __name__ == "__main__":
    main()
