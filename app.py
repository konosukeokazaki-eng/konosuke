import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import sqlite3
import os
import tempfile
import yaml
from yaml.loader import SafeLoader
import streamlit_authenticator as stauth
from data_processor import DataProcessor

# ページ設定
st.set_page_config(page_title="財務予測シミュレーター", layout="wide")

# ログイン機能の実装
# --------------------------------------------------------------------------------
# 1. 認証情報のロード
try:
    with open('config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)
except FileNotFoundError:
    # config.yamlがない場合はデフォルト設定で作成
    # デフォルトパスワードは 'password'
    default_password_hash = stauth.Hasher.generate(['password'])[0]
    config = {
        'cookie': {
            'expiry_days': 30,
            'key': 'some_random_key',
            'name': 'some_cookie_name'
        },
        'credentials': {
            'usernames': {
                'admin': {
                    'email': 'admin@example.com',
                    'name': '管理者',
                    'password': default_password_hash
                }
            }
        }
    }
    with open('config.yaml', 'w') as file:
        yaml.dump(config, file, default_flow_style=False)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
)

# 2. ログインフォームの表示
name, authentication_status, username = authenticator.login('財務予測アプリ ログイン', 'main')

if authentication_status:
    # ログイン成功時
    st.session_state["authentication_status"] = authentication_status
    st.session_state["name"] = name
    st.session_state["username"] = username
    
    # ログアウトボタンをサイドバーに表示
    authenticator.logout('ログアウト', 'sidebar')
    
    # メインアプリの処理を続行
    
    # 初期化
    if 'processor' not in st.session_state:
        st.session_state.processor = DataProcessor()
    processor = st.session_state.processor
    
    if 'page' not in st.session_state:
        st.session_state.page = "着地予測ダッシュボード"
    if 'display_mode' not in st.session_state:
        st.session_state.display_mode = "要約"
    if 'scenario' not in st.session_state:
        st.session_state.scenario = "現実"
    
    # サイドバー
    st.sidebar.title("財務予測シミュレーター")
    
    # 会社選択
    companies = processor.get_companies()
    if companies.empty:
        st.error("会社データがありません。システム設定から会社を追加してください。")
        st.session_state.page = "システム設定"
        selected_comp_name = ""
    else:
        comp_names = companies['name'].tolist()
        selected_comp_name = st.sidebar.selectbox("会社を選択", comp_names, key="comp_select")
        selected_comp_id = int(companies[companies['name'] == selected_comp_name]['id'].iloc[0])
        st.session_state.selected_comp_id = selected_comp_id
        st.session_state.selected_comp_name = selected_comp_name
    
        # 期選択
        periods = processor.get_company_periods(selected_comp_id)
        if periods.empty:
            st.sidebar.warning("この会社の期データがありません。")
            selected_period_num = 0
        else:
            period_options = [f"第{row['period_num']}期: {row['start_date']} - {row['end_date']}" for _, row in periods.iterrows()]
            selected_period_str = st.sidebar.selectbox("期を選択", period_options, key="period_select")
            selected_period_num = int(selected_period_str.split('第')[1].split('期')[0])
            # 列名を小文字に統一
            periods.columns = [c.lower() for c in periods.columns]
            
            # フィルタリング
            period_match = periods[periods['period_num'] == selected_period_num]
            if not period_match.empty:
                # 列の存在を再確認
                if 'id' in period_match.columns:
                    selected_period_id = int(period_match['id'].iloc[0])
                else:
                    # 列名が取得できない場合は最初の列をIDとみなす
                    selected_period_id = int(period_match.iloc[0, 0])
                    
                st.session_state.selected_period_id = selected_period_id
                st.session_state.selected_period_num = selected_period_num
                st.session_state.start_date = period_match['start_date'].iloc[0]
                st.session_state.end_date = period_match['end_date'].iloc[0]
            else:
                st.error("選択された期が見つかりません。")
                selected_period_id = None
    
        # 予測シナリオ
        st.sidebar.subheader("予測シナリオ")
        st.session_state.scenario = st.sidebar.radio("シナリオを選択してください", ["現実", "楽観", "悲観"], horizontal=True)
        
        # シナリオ設定（現実シナリオをベースに動的に計算するための設定）
        if 'scenario_rates' not in st.session_state:
            st.session_state.scenario_rates = {
                "現実": 0.0,
                "楽観": 0.1, # +10%
                "悲観": -0.1 # -10%
            }
        
        if st.session_state.scenario != "現実":
            st.sidebar.markdown("---")
            st.sidebar.subheader(f"{st.session_state.scenario} シナリオ設定")
            rate_key = f"{st.session_state.scenario}_rate"
            
            # 初期値はセッションステートから取得
            initial_rate = st.session_state.scenario_rates[st.session_state.scenario] * 100
            
            new_rate = st.sidebar.number_input(
                f"増減率 (%)", 
                value=initial_rate, 
                min_value=-100.0, 
                max_value=100.0, 
                step=1.0,
                key=rate_key
            ) / 100.0
            
            # セッションステートを更新
            st.session_state.scenario_rates[st.session_state.scenario] = new_rate
    
        # 実績データ最終月
        months = processor.get_fiscal_months(selected_comp_id, st.session_state.get('selected_period_id'))
        current_month = st.sidebar.selectbox("実績データ最終月", months, key="month_select")
        st.session_state.current_month = current_month
    
        # 表示設定
        st.sidebar.subheader("表示設定")
        st.session_state.display_mode = st.sidebar.radio("表示モード", ["要約", "詳細"], horizontal=True)
    
    # メニュー
    st.sidebar.subheader("メニュー")
    menu = ["着地予測ダッシュボード", "比較分析レポート", "全体予測PL & 補助科目入力", "実績データ入力", "データインポート", "シナリオ一括設定", "システム設定"]
    st.session_state.page = st.sidebar.radio("移動先を選択", menu, index=menu.index(st.session_state.page) if st.session_state.page in menu else 0)
    
    # 通貨フォーマット
    def format_currency(val):
        # 数値型（int, float, numpy.numberなど）の場合のみフォーマットを適用
        if isinstance(val, (int, float, complex)) and not isinstance(val, bool):
            # NaNチェック
            if pd.isna(val):
                return ""
            return f"¥{int(val):,}"
        # それ以外（文字列など）はそのまま返す
        return val
    
    # データの読み込み
    if 'selected_period_id' in st.session_state:
        actuals_df = processor.load_actual_data(st.session_state.selected_period_id)
        # 予測データは常に「現実」シナリオをロード
        forecasts_df = processor.load_forecast_data(st.session_state.selected_period_id, "現実")
        
        # 選択されたシナリオに応じて予測データを動的に調整
        if st.session_state.scenario != "現実":
            rate = st.session_state.scenario_rates[st.session_state.scenario]
            
            # 予測月のみに増減率を適用
            split_idx = processor.get_split_index(st.session_state.selected_comp_id, st.session_state.current_month, st.session_state.selected_period_id)
            forecast_months = months[split_idx:]
            
            # 売上高と費用項目にのみ増減率を適用（計算項目は除く）
            # 売上高は増加、費用は減少（楽観の場合）またはその逆
            for item in processor.all_items:
                if item == "売上高":
                    forecasts_df.loc[forecasts_df['項目名'] == item, forecast_months] *= (1 + rate)
                elif item == "売上原価":
                    forecasts_df.loc[forecasts_df['項目名'] == item, forecast_months] *= (1 - rate)
                    
            st.session_state.adjusted_forecasts_df = forecasts_df.copy() # 調整後のDFを保存
        
        # 補助科目合計の反映
        # 補助科目合計は常に「現実」シナリオの予測データに反映させる
        for item in processor.all_items:
            # 補助科目合計は、選択されたシナリオではなく、常に「現実」シナリオの予測データに反映させる
            sub_totals = processor.calculate_sub_account_totals(st.session_state.selected_period_id, "現実", item)
            if sub_totals.sum() != 0:
                # 予測月のみ上書き
                split_idx = processor.get_split_index(st.session_state.selected_comp_id, st.session_state.current_month, st.session_state.selected_period_id)
                forecast_months = months[split_idx:]
                for m in forecast_months:
                    if m in forecasts_df.columns:
                        forecasts_df.loc[forecasts_df['項目名'] == item, m] = sub_totals.get(m, 0)
        
        # シナリオ調整後のDFをPL計算に使用
        pl_forecasts_df = st.session_state.get('adjusted_forecasts_df', forecasts_df)
        
        current_pl = processor.calculate_pl(
            actuals_df, 
            pl_forecasts_df, 
            processor.get_split_index(st.session_state.selected_comp_id, st.session_state.current_month, st.session_state.selected_period_id),
            months
        )
        st.session_state.actuals_df = actuals_df
        st.session_state.forecasts_df = forecasts_df # 現実シナリオのDFを保存
        st.session_state.current_pl = current_pl
    
    # メインコンテンツ
    if st.session_state.page == "着地予測ダッシュボード":
        st.title("着地予測ダッシュボード")
        st.subheader(f"分析対象: {selected_comp_name} 第{selected_period_num}期 ({st.session_state.scenario}シナリオ)")
        
        # サマリーカード
        cols = st.columns(5)
        summary_items = ["売上高", "売上総損益金額", "営業損益金額", "経常損益金額", "当期純損益金額"]
        for i, item in enumerate(summary_items):
            val = current_pl[current_pl['項目名'] == item]['合計'].iloc[0]
            cols[i].metric(item, format_currency(val))
        
        # グラフ
        st.subheader("月次推移グラフ")
        plot_df = current_pl[current_pl['項目名'].isin(["売上高", "営業損益金額"])].melt(id_vars=['項目名'], value_vars=months, var_name='月', value_name='金額')
        fig = px.line(plot_df, x='月', y='金額', color='項目名', markers=True)
        st.plotly_chart(fig, use_container_width=True)
        
        # P/L表示
        st.subheader(f"P/L ({st.session_state.display_mode}表示)")
        display_df = current_pl[current_pl['タイプ'] == st.session_state.display_mode] if st.session_state.display_mode == "要約" else current_pl
        
        # フォーマット
        formatted_df = display_df.copy()
        for m in months + ['合計']:
            formatted_df[m] = formatted_df[m].apply(format_currency)
        st.dataframe(formatted_df.set_index('項目名')[months + ['合計']], use_container_width=True)
    
    elif st.session_state.page == "全体予測PL & 補助科目入力":
        st.title("全体予測PL & 補助科目入力")
        st.info("各科目の下の「詳細入力」ボタンを押すと、内訳（補助科目）の入力フィールドが表示されます。")
        
        split_idx = processor.get_split_index(st.session_state.selected_comp_id, st.session_state.current_month, st.session_state.selected_period_id)
        forecast_months = months[split_idx:]
        
        # PLの各項目をループ
        for _, row in current_pl.iterrows():
            item_name = row['項目名']
            if st.session_state.display_mode == "要約" and row['タイプ'] == "詳細":
                continue
                
            is_calc = item_name in ["売上総損益金額", "販売管理費計", "営業損益金額", "経常損益金額", "税引前当期純損益金額", "当期純損益金額"]
            
            with st.container():
                c1, c2 = st.columns([1, 4])
                with c1:
                    st.markdown(f"### {item_name}" if is_calc else f"**{item_name}**")
                with c2:
                    # 簡易表示
                    summary_row = row[months].to_frame().T
                    st.dataframe(summary_row.style.format(format_currency), use_container_width=True, hide_index=True)
                
                if not is_calc:
                    with st.expander(f"📝 {item_name} の内訳・予測を入力する"):
                        
                        # 固定費/変動費属性の設定
                        st.markdown("#### 📊 固定費/変動費属性")
                        col_attr_1, col_attr_2 = st.columns(2)
                        
                        # 現在の属性を取得
                        current_attr = processor.get_item_attributes(st.session_state.selected_period_id, item_name)
                        
                        with col_attr_1:
                            is_variable = st.checkbox(
                                "変動費に設定します",
                                value=current_attr["is_variable"],
                                key=f"is_var_{item_name}"
                            )
                        
                        with col_attr_2:
                            if is_variable:
                                variable_rate = st.number_input(
                                    "変動費率（売上に対する割合）",
                                    min_value=0.0,
                                    max_value=1.0,
                                    value=current_attr["variable_rate"],
                                    step=0.01,
                                    key=f"var_rate_{item_name}"
                                )
                            else:
                                variable_rate = 0.0
                        
                        # 属性を保存するボタン
                        if st.button(f"属性を保存: {item_name}", key=f"btn_attr_{item_name}"):
                            processor.save_item_attribute(st.session_state.selected_period_id, item_name, is_variable, variable_rate)
                            st.success(f"{item_name} の属性を保存しました。")
                        
                        st.markdown("---")
                        
                        # 成長率予測ボタン
                        st.markdown("#### 📈 過去実績ベースの予測を自動生成")
                        col_growth_1, col_growth_2 = st.columns([1, 3])
                        with col_growth_1:
                            past_months = st.number_input("過去何ヶ月の実績を基にするか", min_value=2, value=3, key=f"past_months_{item_name}")
                        with col_growth_2:
                            if st.button(f"予測を自動生成: {item_name}", key=f"btn_growth_{item_name}"):
                                forecast_values = processor.calculate_growth_forecast(
                                    st.session_state.selected_period_id,
                                    st.session_state.current_month,
                                    item_name,
                                    past_months
                                )
                                if forecast_values:
                                    # 現実シナリオの予測データを更新
                                    processor.update_forecast_data(st.session_state.selected_period_id, "現実", item_name, forecast_values)
                                    st.success(f"{item_name} の予測を自動生成し、保存しました。")
                                    st.rerun()
                                else:
                                    st.warning("過去の実績データが不足しているため、予測を生成できませんでした。")
                        
                        st.markdown("---")
                        
                        # 補助科目
                        st.write("■ 補助科目（内訳）の入力")
                        # 補助科目のデータは常に「現実」シナリオからロード
                        sub_df = processor.load_sub_accounts(st.session_state.selected_period_id, "現実", item_name)
                        
                        # 補助科目入力フィールド
                        if sub_df.empty:
                            st.warning("補助科目が登録されていません。")
                        else:
                            # 予測月のみを表示
                            sub_df_edit = sub_df[['sub_account_name'] + forecast_months].copy()
                            
                            # Streamlitのデータエディタで編集
                            edited_df = st.data_editor(
                                sub_df_edit,
                                column_config={
                                    "sub_account_name": st.column_config.TextColumn("補助科目名", disabled=True),
                                    **{m: st.column_config.NumberColumn(m, format="¥%d") for m in forecast_months}
                                },
                                hide_index=True,
                                num_rows="dynamic",
                                key=f"sub_edit_{item_name}"
                            )
                            
                            if st.button(f"補助科目予測を保存: {item_name}", key=f"btn_sub_save_{item_name}"):
                                # 編集されたデータを保存
                                processor.save_sub_accounts(st.session_state.selected_period_id, "現実", item_name, edited_df)
                                st.success(f"{item_name} の補助科目予測を保存しました。")
                                st.rerun()
                                
                        st.markdown("---")
                        
                        # 直接予測入力
                        st.write("■ 直接予測入力（補助科目がない場合）")
                        
                        # 予測月のみを表示
                        forecast_row = forecasts_df[forecasts_df['項目名'] == item_name][forecast_months].copy()
                        
                        # Streamlitのデータエディタで編集
                        edited_forecast_row = st.data_editor(
                            forecast_row,
                            column_config={
                                **{m: st.column_config.NumberColumn(m, format="¥%d") for m in forecast_months}
                            },
                            hide_index=True,
                            key=f"forecast_edit_{item_name}"
                        )
                        
                        if st.button(f"直接予測を保存: {item_name}", key=f"btn_forecast_save_{item_name}"):
                            # 編集されたデータを保存
                            forecast_values = edited_forecast_row.iloc[0].to_dict()
                            processor.update_forecast_data(st.session_state.selected_period_id, "現実", item_name, forecast_values)
                            st.success(f"{item_name} の直接予測を保存しました。")
                            st.rerun()
    
    elif st.session_state.page == "実績データ入力":
        st.title("実績データ入力")
        st.warning("この画面は現在開発中です。データインポート機能をご利用ください。")
    
    elif st.session_state.page == "データインポート":
        st.title("実績データインポート")
        st.info("弥生会計からエクスポートしたExcelファイルをアップロードしてください。")
        
        uploaded_file = st.file_uploader("Excelファイルをアップロード", type=["xlsx"])
        
        if uploaded_file is not None:
            # 一時ファイルに保存 (OS依存しない方法)
            import tempfile
            import os
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                tmp_file.write(uploaded_file.read())
                temp_path = tmp_file.name
                st.session_state.temp_path_to_delete = temp_path
                
            st.success(f"ファイル名: {uploaded_file.name} を読み込みました。")
            
            if 'imported_df' not in st.session_state:
                # プレビューのためにデータを抽出
                st.session_state.imported_df, info = processor.import_yayoi_excel(temp_path, preview_only=True)
                st.session_state.show_import_button = True
                
            if st.session_state.show_import_button:
                st.subheader("インポートデータ プレビュー")
                
                imported_df = st.session_state.imported_df
                
                # プレビュー表示
                st.dataframe(imported_df.style.format(format_currency), use_container_width=True)
                
                st.warning("上記の内容でインポートを実行しますか？実行すると、現在の実績データは上書きされます。")
                
                if st.button("上記内容でインポートを実行"):
                    # 実際のインポート処理
                    success, info = processor.save_extracted_data(
                        st.session_state.selected_period_id, 
                        st.session_state.imported_df
                    )
                    if success:
                        st.success("インポートが完了しました！")
                        # セッションステートをクリア
                        del st.session_state.imported_df
                        del st.session_state.show_import_button
                        
                        # 一時ファイルを削除
                        if 'temp_path_to_delete' in st.session_state:
                            os.unlink(st.session_state.temp_path_to_delete)
                            del st.session_state.temp_path_to_delete
                            
                        st.rerun()
                    else:
                        st.error(f"インポートに失敗しました: {info}")
    
    elif st.session_state.page == "シナリオ一括設定":
        st.title("シナリオ一括設定")
        st.info("「現実」シナリオの予測値をベースに、「楽観」「悲観」シナリオの増減率を設定します。")
        
        st.subheader("楽観シナリオ設定")
        col_opt_1, col_opt_2 = st.columns(2)
        with col_opt_1:
            st.markdown("現在の増減率:")
        with col_opt_2:
            new_opt_rate = st.number_input(
                "楽観シナリオ増減率 (%)",
                value=st.session_state.scenario_rates["楽観"] * 100,
                min_value=-100.0,
                max_value=100.0,
                step=1.0,
                key="opt_rate_input"
            ) / 100.0
            
        if st.button("楽観シナリオ増減率を保存"):
            st.session_state.scenario_rates["楽観"] = new_opt_rate
            st.success(f"楽観シナリオの増減率を {new_opt_rate * 100:.1f}% に設定しました。")
            st.rerun()
            
        st.markdown("---")
        
        st.subheader("悲観シナリオ設定")
        col_pes_1, col_pes_2 = st.columns(2)
        with col_pes_1:
            st.markdown("現在の増減率:")
        with col_pes_2:
            new_pes_rate = st.number_input(
                "悲観シナリオ増減率 (%)",
                value=st.session_state.scenario_rates["悲観"] * 100,
                min_value=-100.0,
                max_value=100.0,
                step=1.0,
                key="pes_rate_input"
            ) / 100.0
            
        if st.button("悲観シナリオ増減率を保存"):
            st.session_state.scenario_rates["悲観"] = new_pes_rate
            st.success(f"悲観シナリオの増減率を {new_pes_rate * 100:.1f}% に設定しました。")
            st.rerun()
    
    elif st.session_state.page == "比較分析レポート":
        st.title("比較分析レポート")
        
        # 1. シナリオ間比較
        st.subheader("1. シナリオ間比較 (着地予測)")
        
        # 現実シナリオのPLを取得
        actuals_df = processor.load_actual_data(st.session_state.selected_period_id)
        forecasts_df = processor.load_forecast_data(st.session_state.selected_period_id, "現実")
        
        # 予測月のみのデータフレームを作成
        split_idx = processor.get_split_index(st.session_state.selected_comp_id, st.session_state.current_month, st.session_state.selected_period_id)
        forecast_months = months[split_idx:]
        
        # シナリオごとのPL合計値を計算
        scenario_results = {}
        for scenario, rate in st.session_state.scenario_rates.items():
            # 現実シナリオの予測データをコピー
            temp_forecasts_df = forecasts_df.copy()
            
            # 予測月のみに増減率を適用
            for item in processor.all_items:
                if item == "売上高":
                    temp_forecasts_df.loc[temp_forecasts_df['項目名'] == item, forecast_months] *= (1 + rate)
                elif item == "売上原価":
                    temp_forecasts_df.loc[temp_forecasts_df['項目名'] == item, forecast_months] *= (1 - rate)
                    
            # PL計算
            pl_df = processor.calculate_pl(
                actuals_df, 
                temp_forecasts_df, 
                processor.get_split_index(st.session_state.selected_comp_id, st.session_state.current_month, st.session_state.selected_period_id),
                months
            )
            
            # 合計列を抽出
            scenario_results[scenario] = pl_df[['項目名', '合計']].set_index('項目名')['合計']
            
        # 結果を結合
        comparison_df = pd.DataFrame(scenario_results)
        
        # 比較分析レポート表示
        st.dataframe(comparison_df.style.format(format_currency), use_container_width=True)
        
        # 2. 実績 vs 予測比較
        st.subheader("2. 実績 vs 当初予測比較 (実績最終月時点)")
        
        # 当初予測（現実シナリオの予測データ）
        initial_forecast_df = processor.load_forecast_data(st.session_state.selected_period_id, "現実")
        
        # 実績最終月までの実績合計
        actual_months = months[:split_idx]
        actual_sum = actuals_df[actual_months].sum(axis=1)
        actual_sum.index = actuals_df['項目名']
        
        # 実績最終月までの当初予測合計
        initial_forecast_sum = initial_forecast_df[actual_months].sum(axis=1)
        initial_forecast_sum.index = initial_forecast_df['項目名']
        
        # 比較DF作成
        comparison_actual_df = pd.DataFrame({
            '実績合計': actual_sum,
            '当初予測合計': initial_forecast_sum
        }).fillna(0)
        
        comparison_actual_df['差異'] = comparison_actual_df['実績合計'] - comparison_actual_df['当初予測合計']
        comparison_actual_df['差異率'] = comparison_actual_df['差異'] / comparison_actual_df['当初予測合計'].replace(0, np.nan)
        
        # PL項目のみに絞る
        comparison_actual_df = comparison_actual_df.loc[processor.all_items]
        
        # 表示
        st.dataframe(
            comparison_actual_df.style.format({
                '実績合計': format_currency,
                '当初予測合計': format_currency,
                '差異': format_currency,
                '差異率': "{:.1%}"
            }),
            use_container_width=True
        )
    
    elif st.session_state.page == "システム設定":
        st.title("システム設定")
        
        # 会社設定
        st.subheader("会社設定")
        with st.form("company_form"):
            company_name = st.text_input("会社名")
            if st.form_submit_button("会社を追加"):
                if company_name:
                    processor.add_company(company_name)
                    st.success(f"会社 {company_name} を追加しました。")
                    st.rerun()
                else:
                    st.error("会社名を入力してください。")
                    
        # 会計期間設定
        st.subheader("会計期間設定")
        with st.form("period_form"):
            period_num = st.number_input("期数", min_value=1, step=1)
            start_date = st.date_input("期首日")
            end_date = st.date_input("期末日")
            
            if st.form_submit_button("会計期間を追加"):
                if st.session_state.selected_comp_id and period_num and start_date and end_date:
                    processor.add_fiscal_period(st.session_state.selected_comp_id, period_num, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                    st.success(f"第{period_num}期を追加しました。")
                    st.rerun()
                else:
                    st.error("会社を選択し、すべてのフィールドを入力してください。")

    else:
        st.warning("会計期間が選択されていません。システム設定から登録してください。")

elif authentication_status == False:
    st.error('ユーザー名/パスワードが間違っています')
elif authentication_status == None:
    st.warning('ユーザー名とパスワードを入力してください')
