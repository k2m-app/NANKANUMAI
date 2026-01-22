# ==================================================
# 4. データロード & 解析 (修正版)
# ==================================================
@st.cache_resource
def load_resources():
    res = {"jockeys": [], "trainers": [], "power": {}, "power_data": {}}
    
    # 騎手・調教師リスト
    for fpath, key in [(JOCKEY_FILE, "jockeys"), (TRAINER_FILE, "trainers")]:
        if os.path.exists(fpath):
            try:
                with open(fpath, "r", encoding="utf-8-sig") as f:
                    res[key] = [l.strip().replace(",","").replace(" ","").replace("　","") for l in f if l.strip()]
            except: pass
    
    # 騎手パワーCSV (1列目:競馬場, 3列目:騎手名, 列名'騎手パワー':値 と仮定)
    if os.path.exists(POWER_FILE):
        try:
            df = pd.read_csv(POWER_FILE, encoding="utf-8-sig")
            # 1列目が競馬場と推測されるため取得
            place_col = df.columns[0]
            
            for _, row in df.iterrows():
                # 競馬場と騎手名をキーにする
                p = str(row[place_col]).strip()
                j = str(row.get("騎手名", "")).replace(" ","").replace("　","")
                
                if p and j:
                    val = row.get('騎手パワー', '-')
                    key_t = (p, j)
                    res["power"][key_t] = f"P:{val}"
                    # 前走P取得用に数値データも保持
                    res["power_data"][key_t] = {"power": val}
        except Exception as e:
            pass
    return res

def normalize_name(abbrev, full_list):
    if not abbrev: return ""
    clean = re.sub(r"[ 　▲△☆◇★\d\.]+", "", abbrev)
    if not clean: return ""
    if not full_list: return clean
    if clean in full_list: return clean
    candidates = []
    for full in full_list:
        if all(c in full for c in clean):
            candidates.append((len(full) - len(clean), full))
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]
    return clean

def parse_nankankeiba_detail(html, place_name, resources):
    soup = BeautifulSoup(html, "html.parser")
    data = {"meta": {}, "horses": {}}
    
    # --- レース情報取得 ---
    h3 = soup.find("h3", class_="nk23_c-tab1__title")
    data["meta"]["race_name"] = h3.get_text(strip=True) if h3 else ""
    if data["meta"]["race_name"]:
        parts = re.split(r'[ 　]+', data["meta"]["race_name"])
        data["meta"]["grade"] = parts[-1] if len(parts) > 1 else ""
    cond = soup.select_one("a.nk23_c-tab1__subtitle__text.is-blue")
    data["meta"]["course"] = f"{place_name} {cond.get_text(strip=True)}" if cond else ""
    
    shosai_area = soup.select_one("#shosai_aria")
    if not shosai_area: return data
    
    table = shosai_area.select_one("table.nk23_c-table22__table")
    if not table: return data
    
    # 競馬場判定用マップ
    PLACE_MAP = {"船":"船橋", "大":"大井", "川":"川崎", "浦":"浦和", "門":"門別", "盛":"盛岡", "水":"水沢", "笠":"笠松", "名":"名古屋", "園":"園田", "姫":"姫路", "高":"高知", "佐":"佐賀"}
    KNOWN_PLACES = list(PLACE_MAP.values()) + ["JRA"]

    for row in table.select("tbody tr"):
        try:
            # --- 馬番・馬名 ---
            u_tag = row.select_one("td.umaban") or row.select_one("td.is-col02")
            if not u_tag: continue
            umaban = u_tag.get_text(strip=True)
            if not umaban.isdigit(): continue
            h_link = row.select_one("td.is-col03 a.is-link") or row.select_one("td.pr-umaName-textRound a.is-link")
            horse_name = h_link.get_text(strip=True) if h_link else "不明"
            
            # --- 騎手・調教師 ---
            jg_td = row.select_one("td.cs-g1")
            j_raw, t_raw = "", ""
            if jg_td:
                links = jg_td.select("a")
                if len(links) >= 1: j_raw = links[0].get_text(strip=True)
                if len(links) >= 2: t_raw = links[1].get_text(strip=True)
            
            j_full = normalize_name(j_raw, resources["jockeys"])
            t_full = normalize_name(t_raw, resources["trainers"])
            
            # 今回の騎手パワー取得
            curr_power_info = resources["power"].get((place_name, j_full), "P:不明")
            
            # --- 相性データ ---
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
                if not z: continue
                # 全体テキスト
                z_full_text = z.get_text(" ", strip=True)
                if not z_full_text: continue

                # 1. 日付と開催場の特定
                d_txt = ""
                place_short = ""
                d_div = z.select_one("p.nk23_u-d-flex")
                
                if d_div:
                    d_raw = d_div.get_text(" ", strip=True)
                    m_dt = re.search(r"(\d+\.\d+\.\d+)", d_raw)
                    if m_dt: d_txt = m_dt.group(1)
                    
                    # 競馬場名を抽出（日付以外の部分から探す）
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
                
                if not d_txt: d_txt = "不明"
                if not place_short: place_short = place_name # 見つからなければ今の開催地と仮定

                # 2. 距離
                dm = re.search(r"(\d{3,4})m?", z_full_text)
                dist = dm.group(1) if dm else ""

                # 3. 着順
                rank = ""
                r_tag = z.select_one(".nk23_u-text19")
                if r_tag: rank = r_tag.get_text(strip=True).replace("着","")

                # 4. 騎手・人気・斤量など
                j_prev, pop = "", ""
                p_lines = z.select("p.nk23_u-text10")
                
                # 人気と騎手の抽出ロジック
                for p in p_lines:
                    txt = p.get_text(strip=True)
                    if "人気" in txt:
                        pm = re.search(r"(\d+)人気", txt)
                        if pm: pop = f"{pm.group(1)}人"
                        
                        # 同行または別spanにある騎手名を探す
                        spans = p.find_all("span")
                        if len(spans) >= 2:
                            # 2つ目のspanが騎手名であるケースが多い
                            j_cand = spans[1].get_text(strip=True)
                            # 数字(斤量)を除去して騎手名だけにする
                            j_prev = re.sub(r"[\d\.]+", "", j_cand)
                        break

                # 5. 上がり3F
                agari = ""
                for p in p_lines:
                    ptxt = p.get_text(" ", strip=True)
                    if "3F" in ptxt:
                        # (5) のような順位を抽出
                        am = re.search(r"3F.*?\((\d+)\)", ptxt)
                        if am: 
                            agari = f"3F:{am.group(1)}位"

                # 6. 通過順
                pos_p = z.select_one("p.position")
                pas = ""
                if pos_p:
                    pas_spans = [s.get_text(strip=True) for s in pos_p.find_all("span")]
                    pas = "-".join(pas_spans)
                
                # 7. 騎手名正規化と前走P取得
                j_prev_full = normalize_name(j_prev, resources["jockeys"])
                if not j_prev_full and j_prev: j_prev_full = j_prev

                # ★ 前走(i=1)の場合、CSVから騎手パワーを取得 ★
                if i == 1:
                    # キー: (開催場, 騎手名)
                    p_key = (place_short, j_prev_full)
                    p_data = resources["power_data"].get(p_key)
                    if p_data:
                        prev_power_val = p_data['power']

                # --- 文字列生成 (レース名は含めない) ---
                # フォーマット: 日付 場所距離 騎手名 通過順(3F順位)→着順(人気)
                agari_part = f"({agari})" if agari else "()"
                pop_part = f"({pop})" if pop else ""
                rank_part = f"{rank}着" if rank else "着不明"
                
                h_str = f"{d_txt} {place_short}{dist} {j_prev_full} {pas}{agari_part}→{rank_part}{pop_part}"
                history.append(h_str)
            
            # --- 最終表示用文字列 ---
            if prev_power_val:
                power_line = f"【騎手】{curr_power_info}(前P:{prev_power_val})、 相性:{pair_stats}"
            else:
                power_line = f"【騎手】{curr_power_info}、 相性:{pair_stats}"

            data["horses"][umaban] = {
                "name": horse_name, "jockey": j_full, "trainer": t_full,
                "power": curr_power_info, 
                "compat": pair_stats, "hist": history,
                "display_power": power_line
            }

        except Exception: continue
    return data
