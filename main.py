import streamlit as st
import datetime
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
    
    # セッションステート初期化
    if "selected_races" not in st.session_state:
        st.session_state.selected_races = [10, 11, 12]

    # 全選択/解除ボタン
    col_a, col_c = st.columns(2)
    if col_a.button("全選択"):
        st.session_state.selected_races = list(range(1, 13))
    if col_c.button("全解除"):
        st.session_state.selected_races = []

    # チェックボックスグリッド
    selected_races_final = []
    cols = st.columns(3)
    for r in range(1, 13):
        with cols[(r-1)%3]:
            checked = st.checkbox(f"{r}R", value=(r in st.session_state.selected_races), key=f"chk_{r}")
            if checked:
                selected_races_final.append(r)
    
    st.caption("※Dify生成待機: 最大300秒/レース")
    start_btn = st.button("予想開始", type="primary")

# --- メイン処理 ---
if start_btn:
    if not selected_races_final:
        st.warning("レースを選択してください。")
        st.stop()

    year = target_date.year
    month = f"{target_date.month:02}"
    day = f"{target_date.day:02}"
    
    st.info(f"🚀 {year}/{month}/{day} {selected_place}競馬 ({len(selected_races_final)}レース) の予想を開始します...")

    result_container = st.container()

    # ジェネレータ実行
    for race_num, output_text in keiba_bot.run_races_iter(year, month, day, place_code, set(selected_races_final), ui=True):
        
        if race_num == 0:
            st.error(output_text)
        else:
            with result_container:
                st.subheader(f"{selected_place} {race_num}R")
                st.text_area(
                    label=f"{race_num}R 結果 (Ctrl+A -> Ctrl+C)",
                    value=output_text,
                    height=500,
                    key=f"res_{race_num}"
                )
                st.divider()

    st.success("✅ 全ての処理が完了しました！")
