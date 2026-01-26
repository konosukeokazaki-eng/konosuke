import streamlit as st
import pandas as pd
import plotly.express as px
from data_processor import DataProcessor
import sqlite3
import os
import tempfile
import logging

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ページ設定
st.set_page_config(page_title="財務予測シミュレーター", layout="wide")

# 簡易認証システム（streamlit-authenticator不使用版）
# --------------------------------------------------------------------------------
def check_password():
    """簡易パスワード認証"""
    
    def password_entered():
        """パスワード入力時のコールバック"""
        if st.session_state["password"] == st.secrets.get("password", "admin123"):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # パスワードをセッションから削除
        else:
            st.session_state["password_correct"] = False

    # 初回またはログアウト後
    if "password_correct" not in st.session_state:
        # パスワード入力フォーム
        st.text_input(
            "パスワード", 
            type="password", 
            on_change=password_entered, 
            key="password"
        )
        st.info("デフォルトパスワード: admin123")
        return False
    
    # パスワードが間違っている場合
    elif not st.session_state["password_correct"]:
        st.text_input(
            "パスワード", 
            type="password", 
            on_change=password_entered, 
            key="password"
        )
        st.error("パスワードが正しくありません")
        return False
    
    # パスワードが正しい場合
    else:
        return True

# 認証チェック
if not check_password():
    st.stop()

# ログアウトボタン
if st.sidebar.button("ログアウト"):
    st.session_state["password_correct"] = False
    st.rerun()

# ========================================================================
# メインアプリケーション
# ========================================================================

# 初期化
if 'processor' not in st.session_state:
    st.session_state.processor = DataProcessor()
processor = st.session_state.processor

# ページ状態の初期化
if 'page' not in st.session_state:
    st.session_state.page = "着地予測ダッシュボード"
if 'display_mode' not in st.session_state:
    st.session_state.display_mode = "要約"
if 'scenario' not in st.session_state:
    st.session_state.scenario = "現実"

# シナリオ設定の初期化
if 'scenario_rates' not in st.session_state:
    st.session_state.scenario_rates = {
        "現実": 0.0,
        "楽観": 0.1,  # +10%
        "悲観": -0.1  # -10%
    }

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
        period_options = [
            f"第{row['period_num']}期: {row['start_date']} - {row['end_date']}" 
            for _, row in periods.iterrows()
        ]
        selected_period_str = st.sidebar.selectbox("期を選択", period_options, key="period_select")
        selected_period_num = int(selected_period_str.split('第')[1].split('期')[0])
        
        # 列名を小文字に統一
        periods.columns = [c.lower() for c in periods.columns]
        
        # フィルタリング
        period_match = periods[periods['period_num'] == selected_period_num]
        if not period_match.empty:
            if 'id' in period_match.columns:
                selected_period_id = int(period_match['id'].iloc[0])
            else:
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
    st.session_state.scenario = st.sidebar.radio(
        "シナリオを選択してください",
        ["現実", "楽観", "悲観"],
        horizontal=True
    )
    
    # シナリオ設定
    if st.session_state.scenario != "現実":
        st.sidebar.markdown("---")
        st.sidebar.subheader(f"{st.session_state.scenario} シナリオ設定")
        rate_key = f"{st.session_state.scenario}_rate"
        
        initial_rate = st.session_state.scenario_rates[st.session_state.scenario] * 100
        
        new_rate = st.sidebar.number_input(
            f"増減率 (%)",
            value=initial_rate,
            min_value=-100.0,
            max_value=100.0,
            step=1.0,
            key=rate_key
        ) / 100.0
        
        st.session_state.scenario_rates[st.session_state.scenario] = new_rate

    # 実績データ最終月
    months = processor.get_fiscal_months(
        selected_comp_id,
        st.session_state.get('selected_period_id')
    )
    current_month = st.sidebar.selectbox("実績データ最終月", months, key="month_select")
    st.session_state.current_month = current_month

    # 表示設定
    st.sidebar.subheader("表示設定")
    st.session_state.display_mode = st.sidebar.radio(
        "表示モード",
        ["要約", "詳細"],
        horizontal=True
    )

# メニュー
st.sidebar.subheader("メニュー")
menu = [
    "着地予測ダッシュボード",
    "比較分析レポート",
    "全体予測PL & 補助科目入力",
    "実績データ入力",
    "データインポート",
    "シナリオ一括設定",
    "システム設定"
]
st.session_state.page = st.sidebar.radio(
    "移動先を選択",
    menu,
    index=menu.index(st.session_state.page) if st.session_state.page in menu else 0
)


# 通貨フォーマット
def format_currency(val):
    """通貨フォーマット関数"""
    if isinstance(val, (int, float, complex)) and not isinstance(val, bool):
        if pd.isna(val):
            return ""
        return f"¥{int(val):,}"
    return val


# データの読み込みと計算
if 'selected_period_id' in st.session_state:
    try:
        # 実績データを読み込み
        actuals_df = processor.load_actual_data(st.session_state.selected_period_id)
        
        # 予測データは常に「現実」シナリオをロード（ベース）
        base_forecasts_df = processor.load_forecast_data(
            st.session_state.selected_period_id,
            "現実"
        )
        
        # 補助科目合計の反映（現実シナリオのみ）
        split_idx = processor.get_split_index(
            st.session_state.selected_comp_id,
            st.session_state.current_month,
            st.session_state.selected_period_id
        )
        forecast_months = months[split_idx:]
        
        for item in processor.all_items:
            sub_totals = processor.calculate_sub_account_totals(
                st.session_state.selected_period_id,
                "現実",
                item
            )
            if not sub_totals.empty and sub_totals.sum() != 0:
                for m in forecast_months:
                    if m in base_forecasts_df.columns:
                        base_forecasts_df.loc[
                            base_forecasts_df['項目名'] == item, m
                        ] = sub_totals.get(m, 0)
        
        # 選択されたシナリオに応じて予測データを調整
        if st.session_state.scenario != "現実":
            rate = st.session_state.scenario_rates[st.session_state.scenario]
            adjusted_forecasts_df = processor.apply_scenario_adjustment(
                base_forecasts_df,
                rate,
                forecast_months
            )
        else:
            adjusted_forecasts_df = base_forecasts_df.copy()
        
        # P/L計算
        current_pl = processor.calculate_pl(
            actuals_df,
            adjusted_forecasts_df,
            split_idx,
            months
        )
        
        # セッションステートに保存
        st.session_state.actuals_df = actuals_df
        st.session_state.base_forecasts_df = base_forecasts_df  # 現実シナリオ
        st.session_state.adjusted_forecasts_df = adjusted_forecasts_df  # 調整後
        st.session_state.current_pl = current_pl
        
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        st.error(f"データの読み込みに失敗しました: {e}")


# ========================================================================
# ページコンテンツ
# ========================================================================

if st.session_state.page == "着地予測ダッシュボード":
    st.title("着地予測ダッシュボード")
    st.subheader(
        f"分析対象: {selected_comp_name} "
        f"第{selected_period_num}期 ({st.session_state.scenario}シナリオ)"
    )
    
    if 'current_pl' in st.session_state:
        current_pl = st.session_state.current_pl
        
        # サマリーカード
        cols = st.columns(5)
        summary_items = [
            "売上高",
            "売上総損益金額",
            "営業損益金額",
            "経常損益金額",
            "当期純損益金額"
        ]
        
        for i, item in enumerate(summary_items):
            val = current_pl[current_pl['項目名'] == item]['合計'].iloc[0]
            cols[i].metric(item, format_currency(val))
        
        # グラフ
        st.subheader("月次推移グラフ")
        plot_items = ["売上高", "営業損益金額"]
        plot_df = current_pl[current_pl['項目名'].isin(plot_items)].melt(
            id_vars=['項目名'],
            value_vars=months,
            var_name='月',
            value_name='金額'
        )
        fig = px.line(plot_df, x='月', y='金額', color='項目名', markers=True)
        st.plotly_chart(fig, use_container_width=True)
        
        # P/L表示
        st.subheader(f"P/L ({st.session_state.display_mode}表示)")
        display_df = current_pl[
            current_pl['タイプ'] == st.session_state.display_mode
        ] if st.session_state.display_mode == "要約" else current_pl
        
        # フォーマット
        formatted_df = display_df.copy()
        for m in months + ['合計']:
            if m in formatted_df.columns:
                formatted_df[m] = formatted_df[m].apply(format_currency)
        
        st.dataframe(
            formatted_df.set_index('項目名')[months + ['合計']],
            use_container_width=True
        )


elif st.session_state.page == "全体予測PL & 補助科目入力":
    st.title("全体予測PL & 補助科目入力")
    st.info(
        "各科目の下の「詳細入力」ボタンを押すと、"
        "内訳(補助科目)の入力フィールドが表示されます。"
    )
    
    if 'current_pl' not in st.session_state:
        st.warning("データが読み込まれていません。")
    else:
        current_pl = st.session_state.current_pl
        split_idx = processor.get_split_index(
            st.session_state.selected_comp_id,
            st.session_state.current_month,
            st.session_state.selected_period_id
        )
        forecast_months = months[split_idx:]
        
        # PLの各項目をループ
        for _, row in current_pl.iterrows():
            item_name = row['項目名']
            
            if st.session_state.display_mode == "要約" and row['タイプ'] == "詳細":
                continue
            
            is_calc = item_name in processor.calculated_items
            
            with st.container():
                c1, c2 = st.columns([1, 4])
                with c1:
                    if is_calc:
                        st.markdown(f"### {item_name}")
                    else:
                        st.markdown(f"**{item_name}**")
                with c2:
                    # 簡易表示
                    summary_row = row[months].to_frame().T
                    st.dataframe(
                        summary_row.style.format(format_currency),
                        use_container_width=True,
                        hide_index=True
                    )
                
                if not is_calc:
                    with st.expander(f"🔍 {item_name} の内訳・予測を入力する"):
                        
                        # 固定費/変動費属性の設定
                        st.markdown("#### 📊 固定費/変動費属性")
                        col_attr_1, col_attr_2 = st.columns(2)
                        
                        current_attr = processor.get_item_attributes(
                            st.session_state.selected_period_id,
                            item_name
                        )
                        
                        with col_attr_1:
                            is_variable = st.checkbox(
                                "変動費に設定します",
                                value=current_attr["is_variable"],
                                key=f"is_var_{item_name}"
                            )
                        
                        with col_attr_2:
                            if is_variable:
                                variable_rate = st.number_input(
                                    "変動費率(売上に対する割合)",
                                    min_value=0.0,
                                    max_value=1.0,
                                    value=current_attr["variable_rate"],
                                    step=0.01,
                                    key=f"var_rate_{item_name}"
                                )
                            else:
                                variable_rate = 0.0
                        
                        if st.button(f"属性を保存: {item_name}", key=f"btn_attr_{item_name}"):
                            try:
                                processor.save_item_attribute(
                                    st.session_state.selected_period_id,
                                    item_name,
                                    is_variable,
                                    variable_rate
                                )
                                st.success(f"{item_name} の属性を保存しました。")
                            except Exception as e:
                                st.error(f"保存に失敗しました: {e}")
                        
                        st.markdown("---")
                        
                        # 成長率予測ボタン
                        st.markdown("#### 📈 過去実績ベースの予測を自動生成")
                        col_growth_1, col_growth_2 = st.columns([1, 3])
                        
                        with col_growth_1:
                            past_months_input = st.number_input(
                                "過去何ヶ月の実績を基にするか",
                                min_value=2,
                                value=3,
                                key=f"past_months_{item_name}"
                            )
                        
                        with col_growth_2:
                            st.write("")
                            st.write("")
                            if st.button(
                                f"予測を自動生成・適用",
                                key=f"btn_growth_{item_name}"
                            ):
                                forecast_values = processor.calculate_growth_forecast(
                                    st.session_state.selected_period_id,
                                    st.session_state.current_month,
                                    item_name,
                                    past_months_input
                                )
                                
                                if forecast_values:
                                    # ベース予測データフレームに適用
                                    base_forecasts_df = st.session_state.base_forecasts_df
                                    for m, val in forecast_values.items():
                                        base_forecasts_df.loc[
                                            base_forecasts_df['項目名'] == item_name, m
                                        ] = val
                                    
                                    # データベースに保存
                                    try:
                                        processor.save_forecast_data(
                                            st.session_state.selected_period_id,
                                            "現実",
                                            base_forecasts_df
                                        )
                                        st.success(
                                            f"{item_name} の予測を自動生成し、保存しました。"
                                        )
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"保存に失敗しました: {e}")
                                else:
                                    st.warning(
                                        "過去の実績データが不足しているため、"
                                        "予測を生成できませんでした。"
                                    )
                        
                        st.markdown("---")
                        
                        # 補助科目
                        st.write("▪ 補助科目(内訳)の入力")
                        sub_df = processor.load_sub_accounts(
                            st.session_state.selected_period_id,
                            "現実",
                            item_name
                        )
                        
                        # 予測月のみを編集対象とする
                        sub_cols = ['補助科目名'] + forecast_months
                        editable_sub_df = sub_df.reindex(columns=sub_cols, fill_value=0.0)
                        
                        edited_sub = st.data_editor(
                            editable_sub_df,
                            num_rows="dynamic",
                            key=f"editor_{item_name}",
                            column_config={
                                m: st.column_config.NumberColumn(format="¥%d")
                                for m in forecast_months
                            }
                        )
                        
                        # 直接入力
                        st.write("▪ 直接予測入力(補助科目がない場合)")
                        base_forecasts_df = st.session_state.base_forecasts_df
                        direct_df = base_forecasts_df[
                            base_forecasts_df['項目名'] == item_name
                        ][forecast_months]
                        
                        edited_direct = st.data_editor(
                            direct_df,
                            key=f"direct_{item_name}",
                            column_config={
                                m: st.column_config.NumberColumn(format="¥%d")
                                for m in forecast_months
                            },
                            hide_index=True
                        )
                        
                        if st.button(f"保存: {item_name}", key=f"btn_{item_name}"):
                            try:
                                # 補助科目の保存
                                processor.save_sub_accounts(
                                    st.session_state.selected_period_id,
                                    "現実",
                                    item_name,
                                    edited_sub
                                )
                                
                                # 直接入力の保存
                                for m in forecast_months:
                                    base_forecasts_df.loc[
                                        base_forecasts_df['項目名'] == item_name, m
                                    ] = edited_direct.iloc[0][m]
                                
                                processor.save_forecast_data(
                                    st.session_state.selected_period_id,
                                    "現実",
                                    base_forecasts_df
                                )
                                
                                st.success(f"{item_name} を保存しました。")
                                st.rerun()
                            except Exception as e:
                                st.error(f"保存に失敗しました: {e}")
            
            st.markdown("---")


elif st.session_state.page == "データインポート":
    st.title("弥生会計データインポート")
    st.write("弥生会計から書き出したExcelファイルをアップロードしてください。")
    
    uploaded_file = st.file_uploader("Excelファイルを選択", type=["xlsx", "xls"])
    
    if uploaded_file:
        # 一時ファイルに保存
        with tempfile.NamedTemporaryFile(
            delete=False,
            suffix=os.path.splitext(uploaded_file.name)[1]
        ) as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            temp_path = tmp_file.name
        
        st.session_state.temp_path_to_delete = temp_path
        
        # プレビューボタン
        if st.button("データ抽出・プレビュー"):
            st.session_state.temp_path = temp_path
            
            success, info, imported_df = processor.extract_yayoi_excel_data(
                temp_path,
                st.session_state.selected_comp_id,
                st.session_state.selected_period_num
            )
            
            if success:
                st.session_state.imported_df = imported_df
                st.success("データ抽出に成功しました。以下の内容でインポートされます。")
                st.dataframe(
                    imported_df.style.format(format_currency),
                    use_container_width=True
                )
                st.session_state.show_import_button = True
            else:
                st.error(f"データ抽出に失敗しました: {info}")
                st.session_state.show_import_button = False
        
        # インポート実行ボタン
        if (st.session_state.get('show_import_button', False) and 
            st.session_state.get('imported_df') is not None):
            st.markdown("---")
            if st.button("上記内容でインポートを実行"):
                success, info = processor.save_extracted_data(
                    st.session_state.selected_period_id,
                    st.session_state.imported_df
                )
                
                if success:
                    st.success("インポートが完了しました!")
                    
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
    st.info(
        "この画面で設定した増減率は、サイドバーで『楽観』または『悲観』"
        "シナリオを選択した際に、**『現実』シナリオの予測値に動的に適用**されます。"
        "データは保存されません。"
    )
    
    if 'selected_period_id' in st.session_state:
        st.subheader("シナリオ増減率設定")
        
        # 楽観シナリオ設定
        st.markdown("#### 楽観シナリオ")
        opt_rate_key = "楽観_rate_input"
        initial_opt_rate = st.session_state.scenario_rates["楽観"] * 100
        
        new_opt_rate = st.number_input(
            "楽観シナリオの増減率 (%)",
            value=initial_opt_rate,
            min_value=-100.0,
            max_value=100.0,
            step=1.0,
            key=opt_rate_key
        ) / 100.0
        
        if new_opt_rate != st.session_state.scenario_rates["楽観"]:
            st.session_state.scenario_rates["楽観"] = new_opt_rate
            st.rerun()
        
        # 悲観シナリオ設定
        st.markdown("#### 悲観シナリオ")
        pess_rate_key = "悲観_rate_input"
        initial_pess_rate = st.session_state.scenario_rates["悲観"] * 100
        
        new_pess_rate = st.number_input(
            "悲観シナリオの増減率 (%)",
            value=initial_pess_rate,
            min_value=-100.0,
            max_value=100.0,
            step=1.0,
            key=pess_rate_key
        ) / 100.0
        
        if new_pess_rate != st.session_state.scenario_rates["悲観"]:
            st.session_state.scenario_rates["悲観"] = new_pess_rate
            st.rerun()
        
        st.markdown("---")
        st.success("設定は自動的に保存されました。サイドバーでシナリオを切り替えてご確認ください。")
    else:
        st.warning("期を選択してください。")


elif st.session_state.page == "比較分析レポート":
    st.title("比較分析レポート")
    st.subheader(f"分析対象: {selected_comp_name} 第{selected_period_num}期")
    
    if 'selected_period_id' not in st.session_state:
        st.warning("期を選択してください。")
    else:
        # シナリオ間比較
        st.markdown("## 1. シナリオ間比較")
        st.info("現実、楽観、悲観の3つのシナリオの着地予測を比較します。")
        
        base_forecasts_df = st.session_state.base_forecasts_df
        split_idx = processor.get_split_index(
            st.session_state.selected_comp_id,
            st.session_state.current_month,
            st.session_state.selected_period_id
        )
        forecast_months = months[split_idx:]
        
        scenarios = ["現実", "楽観", "悲観"]
        scenario_pls = {}
        
        for scenario in scenarios:
            rate = st.session_state.scenario_rates[scenario]
            
            if scenario == "現実":
                adjusted_df = base_forecasts_df.copy()
            else:
                adjusted_df = processor.apply_scenario_adjustment(
                    base_forecasts_df,
                    rate,
                    forecast_months
                )
            
            scenario_pls[scenario] = processor.calculate_pl(
                st.session_state.actuals_df,
                adjusted_df,
                split_idx,
                months
            )
        
        # 比較表の作成
        summary_items = [
            "売上高",
            "売上総損益金額",
            "営業損益金額",
            "経常損益金額",
            "当期純損益金額"
        ]
        comparison_data = []
        
        for item in summary_items:
            row_data = {"項目名": item}
            for scenario in scenarios:
                total = scenario_pls[scenario][
                    scenario_pls[scenario]['項目名'] == item
                ]['合計'].iloc[0]
                row_data[scenario] = total
            comparison_data.append(row_data)
        
        comparison_df = pd.DataFrame(comparison_data).set_index('項目名')
        
        # グラフ表示用のデータ整形
        plot_df = comparison_df.reset_index().melt(
            id_vars='項目名',
            var_name='シナリオ',
            value_name='金額'
        )
        
        # 表示
        st.dataframe(
            comparison_df.style.format(format_currency),
            use_container_width=True
        )
        
        fig = px.bar(
            plot_df,
            x='項目名',
            y='金額',
            color='シナリオ',
            barmode='group',
            title='主要項目 シナリオ別比較'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # 実績 vs 予測比較
        st.markdown("## 2. 実績 vs 予測比較")
        st.info(
            "実績データ最終月までの累計実績と、"
            "期首時点の予測(現実シナリオ)との乖離を分析します。"
        )
        
        # 期首予測PL(全て予測として計算)
        initial_forecast_pl = processor.calculate_pl(
            st.session_state.actuals_df,
            base_forecasts_df,
            0,
            months
        )
        
        # 現在のPL(実績+予測)
        current_pl = scenario_pls["現実"]
        
        variance_data = []
        
        for item in summary_items:
            # 累計実績
            actual_total = current_pl[current_pl['項目名'] == item]['実績合計'].iloc[0]
            # 期首予測(実績月までの累計)
            initial_forecast_total = initial_forecast_pl[
                initial_forecast_pl['項目名'] == item
            ][months[:split_idx]].sum(axis=1).iloc[0]
            
            variance = actual_total - initial_forecast_total
            variance_rate = (
                variance / initial_forecast_total if initial_forecast_total != 0 else 0
            )
            
            variance_data.append({
                "項目名": item,
                "期首予測(累計)": initial_forecast_total,
                "実績(累計)": actual_total,
                "乖離額": variance,
                "乖離率": f"{variance_rate:.1%}"
            })
        
        variance_df = pd.DataFrame(variance_data).set_index('項目名')
        
        # 表示
        st.dataframe(
            variance_df.style.format({
                "期首予測(累計)": format_currency,
                "実績(累計)": format_currency,
                "乖離額": format_currency
            }),
            use_container_width=True
        )
        
        # 期間比較(月次推移)
        st.markdown("## 3. 期間比較(月次推移)")
        st.info("選択した項目について、月次の推移を詳細に比較します。")
        
        selected_item_monthly = st.selectbox(
            "月次比較する項目を選択",
            processor.all_items
        )
        
        monthly_data = current_pl[current_pl['項目名'] == selected_item_monthly][months].T
        monthly_data.columns = ['金額']
        monthly_data.index.name = '月'
        
        st.dataframe(
            monthly_data.style.format(format_currency),
            use_container_width=True
        )
        
        fig_monthly = px.bar(
            monthly_data.reset_index(),
            x='月',
            y='金額',
            title=f'{selected_item_monthly} 月次推移'
        )
        st.plotly_chart(fig_monthly, use_container_width=True)


elif st.session_state.page == "システム設定":
    st.title("システム設定")
    
    # 会社追加
    st.subheader("会社の追加")
    new_comp = st.text_input("会社名")
    
    if st.button("会社を追加"):
        if new_comp:
            try:
                conn = sqlite3.connect(processor.db_path)
                cursor = conn.cursor()
                cursor.execute("INSERT INTO companies (name) VALUES (?)", (new_comp,))
                conn.commit()
                conn.close()
                st.success(f"会社「{new_comp}」を追加しました。")
                st.rerun()
            except sqlite3.IntegrityError:
                st.error("既に存在する会社名です。")
            except Exception as e:
                st.error(f"エラーが発生しました: {e}")
        else:
            st.warning("会社名を入力してください。")
    
    # 期の追加
    st.subheader("会計期の追加")
    
    if not companies.empty:
        target_comp = st.selectbox("対象会社", comp_names)
        target_comp_id = int(companies[companies['name'] == target_comp]['id'].iloc[0])
        
        c1, c2, c3 = st.columns(3)
        with c1:
            p_num = st.number_input("期数", min_value=1, value=1)
        with c2:
            s_date = st.date_input("開始日")
        with c3:
            e_date = st.date_input("終了日")
        
        if st.button("期を追加"):
            try:
                conn = sqlite3.connect(processor.db_path)
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO fiscal_periods (comp_id, period_num, start_date, end_date) VALUES (?, ?, ?, ?)",
                    (target_comp_id, p_num, s_date.strftime('%Y-%m-%d'), e_date.strftime('%Y-%m-%d'))
                )
                conn.commit()
                conn.close()
                st.success("期を追加しました。")
                st.rerun()
            except sqlite3.IntegrityError:
                st.error("この会社の期数は既に存在します。")
            except Exception as e:
                st.error(f"エラーが発生しました: {e}")
    
    # 登録済みデータ一覧
    st.subheader("登録済みデータ一覧")
    for _, c in companies.iterrows():
        st.write(f"🏢 **{c['name']}**")
        ps = processor.get_company_periods(c['id'])
        if not ps.empty:
            st.table(ps[['period_num', 'start_date', 'end_date']])
        else:
            st.write("　(期データなし)")


elif st.session_state.page == "実績データ入力":
    st.title("実績データ入力")
    st.info("この機能は現在開発中です。データインポート機能をご利用ください。")


else:
    st.title(st.session_state.page)
    st.write("このページは現在開発中です。")
