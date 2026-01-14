import streamlit as st
import datetime
import keiba_bot

st.set_page_config(page_title="南関競馬AI予想くん", layout="wide")

st.title("🐎 南関競馬 AI予想生成 & 対戦表")

# --- サイドバー設定 ---
with st.sidebar:
    st.header("開催設定")
    
    # 日付選択 (デフォルトは今日)
    today = datetime.date.today()
    target_date = st.date_input("開催日", today)
    
    # 場所選択
    place_options = {"大井": "10", "川崎": "11", "船橋": "12", "浦和": "13"}
    selected_place = st.selectbox("競馬場", list(place_options.keys()))
    place_code = place_options[selected_place]
    
    # レース選択
    st.subheader("対象レース")
    target_races_input = st.text_input("レース番号 (例: 10,11,12)", "10,11,12")
    
    start_btn = st.button("予想開始", type="primary")

# --- メイン処理 ---
if start_btn:
    year = target_date.year
    month = f"{target_date.month:02}"
    day = f"{target_date.day:02}"
    
    # レース番号のパース
    try:
        if not target_races_input.strip():
            target_races = None # 全レース
        else:
            target_races = [int(x.strip()) for x in target_races_input.replace("、", ",").split(",") if x.strip()]
    except:
        st.error("レース番号の形式が不正です。カンマ区切りで入力してください。")
        st.stop()

    st.info(f"🚀 {year}/{month}/{day} {selected_place}競馬 の予想を開始します...")

    # 結果表示用コンテナ
    result_container = st.container()

    # ジェネレータから順次取得して表示
    for race_num, output_text in keiba_bot.run_races_iter(year, month, day, place_code, target_races):
        
        if race_num == 0:
            # エラー等のシステムメッセージ
            st.error(output_text)
        else:
            with result_container:
                st.markdown(f"### {selected_place} {race_num}R")
                
                # コピーしやすいようにコードブロックではなく、テキストエリアを使用
                # 高さを自動調整できないため、少し大きめに確保
                st.text_area(
                    label=f"{race_num}R 出力結果 (コピー用)",
                    value=output_text,
                    height=400,
                    key=f"res_{race_num}"
                )
                st.divider()

    st.success("✅ 全ての処理が完了しました！")
