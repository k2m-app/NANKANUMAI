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
# 共通ツール
# ==================================================
def get_http_session():
    sess = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500,502,503,504])
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    return sess

def build_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)

def login_keibabook(driver, wait):
    driver.get("https://s.keibabook.co.jp/login/login")
    wait.until(EC.visibility_of_element_located((By.NAME, "login_id"))).send_keys(KEIBA_ID)
    driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(KEIBA_PASS)
    driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
    time.sleep(1)

# ==================================================
# ★新機能: Streamlit側で対戦表を作る関数 (BeautifulSoup版)
# ==================================================
def generate_battle_table_local(llm_text, year, month, day, place_name, race_num):
    """
    Difyから返ってきたテキストをもとに、ローカルで対戦表を作成してくっつける関数
    """
    
    # 1. 回・日目の自動取得
    kai, nichi, error_msg = _get_kai_nichi(month, day, place_name)
    
    header_info = ""
    if error_msg:
        header_info = f"⚠️ 開催情報取得エラー: {error_msg}\n"
    else:
        header_info = f"📅 自動判定: {year}年{month}月{day}日 {place_name} 第{kai}回 {nichi}日目\n"

    # 2. LLMテキストから評価(S,A...)を読み取る
    grade_map = _parse_grades(llm_text)

    # 3. 南関サイトから対戦データを取ってくる
    history_text = _fetch_history_data(year, month, day, place_name, kai, nichi, race_num, grade_map)

    # 4. 全部合体させて返す
    return f"{header_info}\n{llm_text}\n\n{history_text}"

# --- 以下、対戦表作成のための裏方機能 ---

def _get_kai_nichi(target_month, target_day, target_place):
    """
    南関競馬の番組表ページをスクレイピングして、日付から「回・日目」を特定する
    """
    url = "https://www.nankankeiba.com/bangumi_menu/bangumi.do"
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        res.encoding = 'cp932' # 南関はCP932(Shift_JIS)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        target_row = None
        # 「大井競馬」などの文字が含まれる行を探す
        for tr in soup.find_all('tr'):
            text = tr.get_text()
            if target_place in text and "競馬" in text:
                target_row = tr
                break
        
        if not target_row:
            return 15, 4, f"{target_place}の開催情報が見つかりませんでした(仮値で続行)"

        # リンクや画像altの中から開催情報を探す
        info_text = ""
        link = target_row.find('a')
        if link:
            info_text = link.get_text(strip=True)
        
        # 正規表現で「第15回 1月 12, 13...」を解析
        match = re.search(r'第(\d+)回.*?(\d+)月\s*(.*?)日', info_text)
        if not match:
            return 15, 4, f"開催テキスト解析失敗: {info_text}"

        kai_val = int(match.group(1))
        # 日付リスト作成 "12, 13, 14" -> [12, 13, 14]
        days_str = match.group(3)
        days_clean = re.sub(r'[^\d,]', '', days_str.replace('，', ','))
        days_list = [int(d) for d in days_clean.split(',') if d]

        target_day_int = int(target_day)
        if target_day_int in days_list:
            nichi_val = days_list.index(target_day_int) + 1
            return kai_val, nichi_val, None
        else:
            return 15, 4, f"指定日({target_day})が期間外です"

    except Exception as e:
        return 15, 4, str(e)

def _parse_grades(text):
    """
    LLMの出力テキストから馬名と評価(S,A...)を辞書化する
    """
    grades = {}
    if not text: return grades
    
    for line in text.split('\n'):
        if '|' in line and '---' not in line:
            parts = [p.strip() for p in line.split('|')]
            # テーブルの列数に合わせて調整（馬名が2列目、評価が最後から2列目と仮定）
            if len(parts) >= 4:
                raw_name = parts[1]
                raw_grade = parts[-2] # 最後が空文字になることがあるので -2 を推奨
                
                # ①馬名(騎手) の形式から馬名のみ抽出
                match = re.search(r'[①-⑳]?\s*([^(\s]+)', raw_name)
                if match:
                    horse_name = match.group(1)
                    grade = raw_grade.strip()
                    if grade in ['S', 'A', 'B', 'C', 'D']:
                        grades[horse_name] = grade
    return grades

def _fetch_history_data(year, month, day, place_name, kai, nichi, race_num, grade_map):
    """
    BeautifulSoupを使って南関の対戦表を正確に取得する
    """
    place_codes = {'浦和': '18', '船橋': '19', '大井': '20', '川崎': '21'}
    p_code = place_codes.get(place_name, '20')
    
    # ID生成
    race_id = f"{year}{int(month):02}{int(day):02}{p_code}{int(kai):02}{int(nichi):02}{int(race_num):02}"
    url = f"https://www.nankankeiba.com/taisen/{race_id}.do"
    
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        res.encoding = 'cp932'
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 1. テーブル特定 (新しいクラス名優先)
        target_table = soup.find('table', class_='nk23_c-table08__table')
        if not target_table:
            # 見つからない場合のフォールバック（resultリンクを含むテーブルを探す）
            for tbl in soup.find_all('table'):
                if tbl.find('a', href=re.compile(r'/result/\d+')):
                    target_table = tbl
                    break
        
        if not target_table:
            return f"\n(対戦表データが見つかりませんでした: {url})"

        # 2. ヘッダー解析（過去レース情報の抽出）
        past_races = []
        thead = target_table.find('thead')
        if not thead: return "\n(テーブル構造エラー: theadなし)"
        
        header_row = thead.find('tr')
        header_cells = header_row.find_all(['th', 'td'])
        
        for i, cell in enumerate(header_cells):
            link = cell.find('a', href=re.compile(r'/result/\d+'))
            if link:
                # 詳細テキスト取得 (クラス名 nk23_c-table08__detail があればそこから)
                detail_tag = cell.find(class_='nk23_c-table08__detail')
                raw_info = detail_tag.get_text(strip=True) if detail_tag else cell.get_text(strip=True)
                
                # 不要な文字を削除して整形
                info_text = raw_info.replace('競走成績', '').replace('対戦表', '')
                info_text = re.sub(r'\s+', ' ', info_text).strip()
                
                full_url = "https://www.nankankeiba.com" + link['href']
                
                past_races.append({
                    'info': info_text, 
                    'url': full_url, 
                    'results': [], 
                    'max_score': 0,
                    'grades': []
                })

        if not past_races:
            return "\n(過去の対戦履歴がありません)"

        # 3. データ行解析
        tbody = target_table.find('tbody')
        data_rows = tbody.find_all('tr')
        
        rank_score = {'S': 5, 'A': 4, 'B': 3, 'C': 2, 'D': 1}
        
        for row in data_rows:
            # 馬名セルを探す
            uma_link = row.find('a', href=re.compile(r'/uma_info/'))
            if not uma_link: continue
            
            horse_name = uma_link.get_text(strip=True)
            
            # 評価のマッチング (完全一致優先、なければ部分一致)
            grade = grade_map.get(horse_name)
            if not grade:
                for k, v in grade_map.items():
                    if k in horse_name or horse_name in k:
                        grade = v
                        break
            
            # 行内の全セルを取得
            cells = row.find_all(['td', 'th'])
            
            # 馬名セルのインデックス特定
            h_idx = -1
            for idx, c in enumerate(cells):
                if c.find('a', href=re.compile(r'/uma_info/')):
                    h_idx = idx
                    break
            
            if h_idx == -1: continue
            
            # 結果セルは馬名の次から
            result_cells = cells[h_idx+1:]
            
            # ヘッダーで取得した past_races の並び順と照合
            for col_idx, race_obj in enumerate(past_races):
                if col_idx < len(result_cells):
                    cell = result_cells[col_idx]
                    
                    # 着順抽出ロジック (BS4ならクラス指定で正確に取れる)
                    rank = ""
                    # パターン1: class="nk23_c-table08__number" 内の span
                    num_tag = cell.find(class_='nk23_c-table08__number')
                    if num_tag:
                        span = num_tag.find('span')
                        if span:
                            rank = span.get_text(strip=True)
                        else:
                            # パイプで区切られている場合
                            txt = num_tag.get_text(strip=True)
                            rank = txt.split('｜')[0].strip()
                    else:
                        # パターン2: セル直下のテキスト
                        txt = cell.get_text(strip=True)
                        if txt:
                            first_part = txt.split('｜')[0].split('|')[0].strip()
                            if first_part.isdigit() or first_part in ['除外', '中止', '取消']:
                                rank = first_part
                    
                    if rank:
                        mark = f"【{grade}】" if grade else ""
                        race_obj['results'].append(f"{rank}着 {mark}{horse_name}")
                        
                        if grade:
                            s = rank_score.get(grade, 0)
                            if s > race_obj['max_score']:
                                race_obj['max_score'] = s
                            race_obj['grades'].append(grade)

        # 4. ソートと出力生成
        # 重要度(max_score)が高い順 > その評価馬の数が多い順
        past_races.sort(key=lambda x: (x['max_score'], len(x['grades'])), reverse=True)
        
        output = ["### 📊 注目対戦 (Streamlit自動生成)"]
        has_data = False
        
        for race in past_races:
            if race['results']:
                has_data = True
                # 重要度アイコン
                icon = "🔥" if race['max_score'] >= 5 else ("✨" if race['max_score'] >= 4 else "🔹")
                # 動画リンク変換
                liveon_url = race['url'].replace('result', 'liveon')
                
                output.append(f"**{icon} {race['info']}**")
                output.append(" / ".join(race['results']))
                output.append(f"[映像・詳細]({liveon_url})\n")
        
        if not has_data:
            return "\n(該当する対戦データが見つかりませんでした)"

        return "\n".join(output)

    except Exception as e:
        return f"\n(対戦表生成エラー: {str(e)})"

# ==================================================
# Dify連携 (シンプル版)
# ==================================================
def run_dify(inputs):
    """
    Difyにテキストを送って、予想コメントだけを返してもらう
    """
    url = f"{DIFY_BASE_URL}/v1/workflows/run"
    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}",
        "Content-Type": "application/json"
    }
    # Dify側は 'text' 変数を受け取る設定になっている想定
    payload = {
        "inputs": inputs,
        "response_mode": "blocking",
        "user": "streamlit-user"
    }
    
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=60)
        
        if res.status_code == 200:
            data = res.json().get('data', {})
            outputs = data.get('outputs', {})
            
            # 結果を探して返す
            for v in outputs.values():
                if isinstance(v, str) and len(v) > 10:
                    return v
            return "⚠️ Difyからの応答が空でした"
        else:
            # エラー時
            return f"⚠️ Dify Error: {res.status_code} {res.text}"
            
    except Exception as e:
        return f"⚠️ 通信エラー: {e}"

# ==================================================
# メイン処理 (イテレータ)
# ==================================================
def run_races_iter(year, month, day, place_code, target_races, ui=False):
    # (注意) ここではScraping部分は省略せず、ユーザーの環境に合わせて
    #       fetch_race_ids_from_schedule などを呼び出す必要があります。
    #       以下の実装は「Dify連携と対戦表結合」の流れを示すものです。
    
    place_names = {"10": "大井", "11": "川崎", "12": "船橋", "13": "浦和"}
    place_name = place_names.get(place_code, "地方")
    
    # ドライバー起動
    driver = build_driver()
    wait = WebDriverWait(driver, 10)
    
    try:
        # 1. 競馬ブックログイン
        # _ui_info(ui, "🔑 ログイン中...") 
        login_keibabook(driver, wait)
        
        # 2. レースID取得 (既存の関数を使用する想定)
        # race_ids = fetch_race_ids_from_schedule(driver, year, month, day, place_code, ui=ui)
        # ここではデモ用にダミーIDリストを使いますが、実際は上の行を有効化してください
        # ---------------------------------------------------------------
        # ★★★ ここに元のスクレイピングロジック(fetch_race_ids...)を入れてください ★★★
        # ---------------------------------------------------------------
        
        # 仮: ユーザーが指定したレース番号だけ回すループ
        for race_num in sorted(list(target_races)):
            
            # 3. 馬データのスクレイピング (省略・既存コード利用)
            # prompt = "..." 
            prompt = f"（{place_name}{race_num}R の馬データがここに入ります）" # ダミー
            
            # 4. Dify実行 (予想テキスト生成)
            dify_res = run_dify({"text": prompt})
            
            # 5. ★Streamlit側で対戦表を作成＆結合 (BeautifulSoup版)
            final_output = generate_battle_table_local(
                dify_res, year, month, day, place_name, race_num
            )
            
            yield race_num, final_output
            
            time.sleep(1)

    finally:
        driver.quit()
