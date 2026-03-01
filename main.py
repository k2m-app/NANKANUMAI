import streamlit as st

# 【重要】これが必ずファイルの先頭（importよりも前）になければなりません
st.set_page_config(page_title="南関競馬AI予想くん", layout="wide")

import datetime
import time
import traceback

# 外部ファイル読み込み（エラーハンドリング付き）
try:
    import keiba_bot
except ImportError as e:
    st.error(f"❌ 'keiba_bot.py' が見つかりません: {e}")
    st.stop()
except Exception as e:
    st.error(f"❌ 'keiba_bot.py' の読み込み中にエラーが発生しました:\n{e}")
    st.stop()

def main():
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
        st.subheader("モード選択")
        exec_mode = st.radio(
            "実行モード",
            ("dify", "raw"),
            format_func=lambda x: "🤖 AIで予想する(Dify)" if x == "dify" else "📋 データのみ取得(コピペ用)"
        )
        
        st.divider()
        st.subheader("対象レース選択")
        
        if "selected_races" not in st.session_state:
            st.session_state.selected_races = [10, 11, 12]

        def update_all_checkboxes(state: bool):
            for r in range(1, 13):
                st.session_state[f"chk_{r}"] = state

        col_a, col_c = st.columns(2)
        with col_a:
            st.button("全選択", on_click=update_all_checkboxes, args=(True,))
        with col_c:
            st.button("全解除", on_click=update_all_checkboxes, args=(False,))

        selected_races_final = []
        cols = st.columns(3)
        for r in range(1, 13):
            # 初期値の設定
            key_name = f"chk_{r}"
            if key_name not in st.session_state:
                st.session_state[key_name] = (r in st.session_state.selected_races)
            
            with cols[(r-1)%3]:
                if st.checkbox(f"{r}R", key=key_name):
                    selected_races_final.append(r)
        
        st.session_state.selected_races = selected_races_final

        if "results_cache" not in st.session_state:
            st.session_state.results_cache = {}

        st.caption("※AI予想は最大10分/レース程度かかる場合があります")
        start_btn = st.button("実行開始", type="primary", key="btn_start")
        
        if st.button("結果クリア"):
            st.session_state.results_cache = {}
            st.rerun()

    # --- メイン処理エリア ---
    result_container = st.container()

    # 1. 既存の結果表示
    if st.session_state.results_cache and not start_btn:
        with result_container:
            st.success("📝 生成結果を表示しています")
            
            # 全結果まとめ
            full_text = []
            full_text.append(f"【{target_date.strftime('%Y/%m/%d')} {selected_place} データまとめ】\n")
            for r_num, content_dict in sorted(st.session_state.results_cache.items()):
                text_val = content_dict.get("text", "") if isinstance(content_dict, dict) else content_dict
                full_text.append(f"\n{'='*35}\n {selected_place} {r_num}R\n{'='*35}\n{text_val}\n")
            
            with st.expander("📚 全レース結果をまとめてコピーする", expanded=True):
                st.text_area("全レース結果", value="\n".join(full_text), height=300, key="res_all_summary")
            
            st.divider()

            for r_num, content_dict in sorted(st.session_state.results_cache.items()):
                st.subheader(f"{selected_place} {r_num}R")
                text_val = content_dict.get("text", "") if isinstance(content_dict, dict) else content_dict
                html_val = content_dict.get("html", "") if isinstance(content_dict, dict) else ""
                
                st.text_area(
                    label=f"{r_num}R 結果",
                    value=text_val,
                    height=500,
                    key=f"res_cache_{r_num}"
                )
                if html_val:
                    st.download_button(
                        label=f"📥 {r_num}R HTMLをダウンロード",
                        data=html_val,
                        file_name=f"{target_date.strftime('%Y%m%d')}_{selected_place}{r_num}R.html",
                        mime="text/html",
                        key=f"dl_cache_{r_num}"
                    )
                st.divider()

    # 2. 新規実行
    if start_btn:
        if not selected_races_final:
            st.warning("レースを選択してください。")
            st.stop()

        # キャッシュクリア
        st.session_state.results_cache = {}
        
        year = target_date.year
        month = f"{target_date.month:02}"
        day = f"{target_date.day:02}"
        
        status_area = st.empty()
        mode_text = "AI予想" if exec_mode == "dify" else "データ取得"
        status_area.info(f"🚀 {year}/{month}/{day} {selected_place}競馬 ({len(selected_races_final)}レース) の【{mode_text}】を開始します...")

        try:
            # ジェネレータ実行
            for event in keiba_bot.run_races_iter(year, month, day, place_code, set(selected_races_final), mode=exec_mode):
                
                e_type = event.get("type")
                e_data = event.get("data")

                if e_type == "status":
                    status_area.info(e_data)
                
                elif e_type == "error":
                    st.error(e_data)
                    
                elif e_type == "result":
                    race_num = event.get("race_num")
                    output_text = event.get("data_text", event.get("data", ""))
                    output_html = event.get("data_html", "")
                    
                    st.session_state.results_cache[race_num] = {
                        "text": output_text,
                        "html": output_html
                    }
                    
                    with result_container:
                        st.subheader(f"{selected_place} {race_num}R")
                        st.text_area(
                            label=f"{race_num}R 結果 (速報)",
                            value=output_text,
                            height=500,
                            key=f"res_live_{race_num}"
                        )
                        if output_html:
                            st.download_button(
                                label=f"📥 {race_num}R HTMLをダウンロード",
                                data=output_html,
                                file_name=f"{year}{month}{day}_{selected_place}{race_num}R.html",
                                mime="text/html",
                                key=f"dl_live_{race_num}"
                            )
                        st.divider()
                    
                    status_area.success(f"✅ {race_num}R 完了")

            status_area.success("✅ 全ての処理が完了しました！画面を更新してまとめを表示します...")
            time.sleep(2)
            st.rerun()
            
        except Exception as e:
            st.error(f"予期せぬエラーが発生しました:\n{e}")
            st.code(traceback.format_exc())

if __name__ == "__main__":
    main()
