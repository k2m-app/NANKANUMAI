import streamlit as st
import datetime
import keiba_bot

st.set_page_config(page_title="南関競馬AI予想くん", layout="wide")

st.title("🐎 南関競馬 AI予想生成 & 対戦表")

# --- サイドバー設定 ---
with st.sidebar:
    st.header("開催設定")
    
    # 日付選択
    today = datetime.date.today()
    target_date = st.date_input("開催日", today)
    
    # 場所選択
    place_options = {"大井": "10", "川崎": "11", "船橋": "12", "浦和": "13"}
    selected_place = st.selectbox("競馬場", list(place_options.keys()))
    place_code = place_options[selected_place]
    
    st.divider()
    
    # レース選択 (チェックボックス化)
    st.subheader("対象レース選択")
    
    # セッションステートで選択状態を管理
    if "selected_races" not in st.session_state:
        st.session_state.selected_races = [i for i in range(10, 13)] # デフォルト10-12R

    # 全選択/全解除ボタン
    col_all, col_clear = st.columns(2)
    if col_all.button("全レース選択"):
        st.session_state.selected_races = [i for i in range(1, 13)]
    if col_clear.button("全解除"):
        st.session_state.selected_races = []

    # 1〜12Rのチェックボックスを配置
    selected_races = []
    cols = st.columns(3) # 3列で表示
    for r in range(1, 13):
        with cols[(r-1)%3]:
            if st.checkbox(f"{r}R", value=(r in st.session_state.selected_races), key=f"chk_{r}"):
                selected_races.append(r)
    
    st.caption("※Dify生成待機: 最大300秒/レース")
    start_btn = st.button("予想開始", type="primary")

# --- メイン処理 ---
if start_btn:
    if not selected_races:
        st.warning("レースが選択されていません。")
        st.stop()

    year = target_date.year
    month = f"{target_date.month:02}"
    day = f"{target_date.day:02}"
    
    st.info(f"🚀 {year}/{month}/{day} {selected_place}競馬 ({len(selected_races)}レース) の予想を開始します...")

    result_container = st.container()

    # ジェネレータから順次取得
    for race_num, output_text in keiba_bot.run_races_iter(year, month, day, place_code, selected_races):
        
        if race_num == 0:
            st.error(output_text)
        else:
            with result_container:
                st.subheader(f"{selected_place} {race_num}R")
                st.text_area(
                    label=f"{race_num}R 結果 (コピー用)",
                    value=output_text,
                    height=500,
                    key=f"res_{race_num}"
                )
                st.divider()

    st.success("✅ 全ての処理が完了しました！")
