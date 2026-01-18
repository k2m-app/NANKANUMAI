import streamlit as st
import datetime
import time
import keiba_bot

st.set_page_config(page_title="南関競馬AI予想くん", layout="wide")

st.title("🐎 南関競馬 AI予想生成 & 対戦表")

# --- サイドバー設定 ---
with st.sidebar:
    st.header("開催設定")
    
    today = datetime.date.today()
    target_date = st.date_input("開催日", today)
    
    place_options = {"大井": "10", "川崎": "11", "船橋": "12", "浦和": "13"}
    selected_place = st.selectbox("競馬場", list(place_options.keys()))
    place_code = place_options[selected_place]
    
    st.divider()
    st.subheader("対象レース選択")
    
    # 1. セッションステート初期化（リスト）
    if "selected_races" not in st.session_state:
        st.session_state.selected_races = [10, 11, 12]

    # 2. リストの内容を個別のチェックボックス用ステート(chk_N)に同期
    for r in range(1, 13):
        key_name = f"chk_{r}"
        if key_name not in st.session_state:
            st.session_state[key_name] = (r in st.session_state.selected_races)

    # 3. コールバック関数（全選択・全解除用）
    def update_all_checkboxes(state: bool):
        for r in range(1, 13):
            st.session_state[f"chk_{r}"] = state

    # 4. 全選択/解除ボタン
    col_a, col_c = st.columns(2)
    with col_a:
        st.button("全選択", on_click=update_all_checkboxes, args=(True,))
    with col_c:
        st.button("全解除", on_click=update_all_checkboxes, args=(False,))

    # 5. チェックボックスグリッド
    selected_races_final = []
    cols = st.columns(3)
    for r in range(1, 13):
        with cols[(r-1)%3]:
            # keyを指定してstateを直接管理
            checked = st.checkbox(f"{r}R", key=f"chk_{r}")
            if checked:
                selected_races_final.append(r)
    
    # 最新の状態を保存
    st.session_state.selected_races = selected_races_final

    # 結果保存用のステート初期化
    if "results_cache" not in st.session_state:
        st.session_state.results_cache = {}

    st.caption("※Dify生成待機: 最大10分/レース")
    
    start_btn = st.button("予想開始", type="primary", key="btn_start")
    
    if st.button("結果クリア"):
        st.session_state.results_cache = {}
        st.rerun()

# --- 内部関数: 全結果のテキスト結合 ---
def get_combined_results(cache, place_name, date_obj):
    if not cache:
        return ""
    text_list = []
    text_list.append(f"【{date_obj.strftime('%Y/%m/%d')} {place_name} AI予想 & 対戦表まとめ】\n")
    
    for r_num, content in sorted(cache.items()):
        text_list.append(f"\n{'='*35}\n {place_name} {r_num}R\n{'='*35}\n")
        text_list.append(content)
        text_list.append("\n") # 読みやすく改行
        
    return "\n".join(text_list)

# --- メイン処理エリア ---
result_container = st.container()

# 1. 既に計算済みの結果があれば表示（+ 全結果まとめ）
if st.session_state.results_cache:
    with result_container:
        st.success("📝 生成結果を表示しています")

        # --- ▼ 追加機能: 全レース結果まとめ表示エリア ▼ ---
        full_text = get_combined_results(st.session_state.results_cache, selected_place, target_date)
        with st.expander("📚 全レース結果をまとめてコピーする", expanded=True):
            st.text_area(
                "全レース結果 (Ctrl+A -> Ctrl+C)",
                value=full_text,
                height=300,
                key="res_all_summary"
            )
        st.divider()
        # --------------------------------------------------

        for r_num, text in sorted(st.session_state.results_cache.items()):
            st.subheader(f"{selected_place} {r_num}R")
            st.text_area(
                label=f"{r_num}R 結果 (Ctrl+A -> Ctrl+C)",
                value=text,
                height=500,
                key=f"res_cache_{r_num}"
            )
            st.divider()

# 2. ボタンが押されたら新規実行
if start_btn:
    if not selected_races_final:
        st.warning("レースを選択してください。")
        st.stop()

    # キャッシュをクリアして再実行
    st.session_state.results_cache = {}
    
    year = target_date.year
    month = f"{target_date.month:02}"
    day = f"{target_date.day:02}"
    
    st.info(f"🚀 {year}/{month}/{day} {selected_place}競馬 ({len(selected_races_final)}レース) の予想を開始します...")

    # ジェネレータ実行
    for race_num, output_text in keiba_bot.run_races_iter(year, month, day, place_code, set(selected_races_final), ui=True):
        
        if race_num == 0:
            st.error(output_text)
        else:
            # 結果をキャッシュに保存
            st.session_state.results_cache[race_num] = output_text
            
            with result_container:
                st.subheader(f"{selected_place} {race_num}R")
                st.text_area(
                    label=f"{race_num}R 結果 (Ctrl+A -> Ctrl+C)",
                    value=output_text,
                    height=500,
                    key=f"res_new_{race_num}"
                )
                st.divider()

    st.success("✅ 全ての処理が完了しました！画面を更新してまとめを表示します...")
    time.sleep(1) # 少し待ってからリロード
    st.rerun()    # リロードして上部の「まとめ表示」を有効化する
