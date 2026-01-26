import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
import os
import tempfile
import logging

# エラー発生時にアプリを止めないための設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ページ設定
st.set_page_config(page_title="財務予測シミュレーター", layout="wide")

# DataProcessorのインポートをtry-catchで囲む
try:
    from data_processor import DataProcessor
    PROCESSOR_AVAILABLE = True
except Exception as e:
    PROCESSOR_AVAILABLE = False
    st.error(f"⚠️ data_processor.pyの読み込みに失敗しました: {e}")
    st.stop()

# 簡易認証
def check_password():
    """簡易パスワード認証"""
    
    def password_entered():
        """パスワード入力時のコールバック"""
        # secrets.tomlがあればそこから、なければデフォルト
        correct_password = st.secrets.get("password", "admin123")
        
        if st.session_state.get("password") == correct_password:
            st.session_state["password_correct"] = True
            if "password" in st.session_state:
                del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    # 初回またはログアウト後
    if "password_correct" not in st.session_state:
        st.markdown("# 🔐 財務予測シミュレーター")
        st.markdown("### ログイン")
        st.text_input(
            "パスワード", 
            type="password", 
            on_change=password_entered, 
            key="password",
            placeholder="パスワードを入力"
        )
        st.info("💡 デフォルトパスワード: **admin123**")
        st.markdown("---")
        st.caption("初回ログイン後、システム設定から会社と会計期を登録してください")
        return False
    
    # パスワードが間違っている場合
    elif not st.session_state["password_correct"]:
        st.markdown("# 🔐 財務予測シミュレーター")
        st.markdown("### ログイン")
        st.text_input(
            "パスワード", 
            type="password", 
            on_change=password_entered, 
            key="password",
            placeholder="パスワードを入力"
        )
        st.error("❌ パスワードが正しくありません")
        return False
    
    # パスワードが正しい場合
    else:
        return True

# 認証チェック
if not check_password():
    st.stop()

# ログアウトボタン
if st.sidebar.button("🚪 ログアウト", use_container_width=True):
    st.session_state["password_correct"] = False
    st.rerun()

# ========================================================================
# メインアプリケーション
# ========================================================================

# 初期化をtry-catchで囲む
try:
    if 'processor' not in st.session_state:
        st.session_state.processor = DataProcessor()
    processor = st.session_state.processor
except Exception as e:
    st.error(f"❌ DataProcessorの初期化に失敗: {e}")
    st.info("データベースファイルの作成権限がない可能性があります")
    st.stop()

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
        "楽観": 0.1,
        "悲観": -0.1
    }

# サイドバー
st.sidebar.title("📊 財務予測シミュレーター")
st.sidebar.markdown("---")

# 会社選択
try:
    companies = processor.get_companies()
except Exception as e:
    st.error(f"❌ 会社データの取得に失敗: {e}")
    companies = pd.DataFrame()

if companies.empty:
    st.warning("⚠️ 会社データがありません")
    st.info("👉 まずサイドバーから「システム設定」を選択し、会社を追加してください")
    st.session_state.page = "システム設定"
    selected_comp_name = ""
    months = []
else:
    comp_names = companies['name'].tolist()
    selected_comp_name = st.sidebar.selectbox(
        "🏢 会社を選択", 
        comp_names, 
        key="comp_select"
    )
    selected_comp_id = int(companies[companies['name'] == selected_comp_name]['id'].iloc[0])
    st.session_state.selected_comp_id = selected_comp_id
    st.session_state.selected_comp_name = selected_comp_name

    # 期選択
    try:
        periods = processor.get_company_periods(selected_comp_id)
    except Exception as e:
        st.sidebar.error(f"期データ取得エラー: {e}")
        periods = pd.DataFrame()

    if periods.empty:
        st.sidebar.warning("この会社の期データがありません")
        selected_period_num = 0
        months = []
    else:
        period_options = [
            f"第{row['period_num']}期: {row['start_date']} - {row['end_date']}" 
            for _, row in periods.iterrows()
        ]
        selected_period_str = st.sidebar.selectbox(
            "📅 期を選択", 
            period_options, 
            key="period_select"
        )
        selected_period_num = int(selected_period_str.split('第')[1].split('期')[0])
        
        periods.columns = [c.lower() for c in periods.columns]
        period_match = periods[periods['period_num'] == selected_period_num]
        
        if not period_match.empty:
            selected_period_id = int(period_match.iloc[0, 0] if 'id' not in period_match.columns else period_match['id'].iloc[0])
            st.session_state.selected_period_id = selected_period_id
            st.session_state.selected_period_num = selected_period_num
            st.session_state.start_date = period_match['start_date'].iloc[0]
            st.session_state.end_date = period_match['end_date'].iloc[0]
            
            # 月リストを取得
            try:
                months = processor.get_fiscal_months(selected_comp_id, selected_period_id)
            except Exception as e:
                st.sidebar.error(f"月リスト取得エラー: {e}")
                months = []
        else:
            st.error("選択された期が見つかりません")
            months = []

    # 予測シナリオ（期が選択されている場合のみ）
    if months:
        st.sidebar.markdown("---")
        st.sidebar.subheader("🎯 予測シナリオ")
        st.session_state.scenario = st.sidebar.radio(
            "シナリオを選択",
            ["現実", "楽観", "悲観"],
            horizontal=True
        )
        
        if st.session_state.scenario != "現実":
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
        current_month = st.sidebar.selectbox(
            "📊 実績データ最終月", 
            months, 
            key="month_select",
            index=len(months)-1 if months else 0
        )
        st.session_state.current_month = current_month

        # 表示設定
        st.sidebar.markdown("---")
        st.sidebar.subheader("⚙️ 表示設定")
        st.session_state.display_mode = st.sidebar.radio(
            "表示モード",
            ["要約", "詳細"],
            horizontal=True
        )

# メニュー
st.sidebar.markdown("---")
st.sidebar.subheader("📋 メニュー")
menu = [
    "着地予測ダッシュボード",
    "比較分析レポート",
    "全体予測PL & 補助科目入力",
    "データインポート",
    "シナリオ一括設定",
    "システム設定"
]
st.session_state.page = st.sidebar.radio(
    "ページ選択",
    menu,
    index=menu.index(st.session_state.page) if st.session_state.page in menu else 0,
    label_visibility="collapsed"
)

# 通貨フォーマット
def format_currency(val):
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        if pd.isna(val):
            return ""
        return f"¥{int(val):,}"
    return val

# データの読み込みと計算
if 'selected_period_id' in st.session_state and months:
    try:
        actuals_df = processor.load_actual_data(st.session_state.selected_period_id)
        base_forecasts_df = processor.load_forecast_data(st.session_state.selected_period_id, "現実")
        
        split_idx = processor.get_split_index(
            st.session_state.selected_comp_id,
            st.session_state.current_month,
            st.session_state.selected_period_id
        )
        forecast_months = months[split_idx:]
        
        # 補助科目合計の反映
        for item in processor.all_items:
            sub_totals = processor.calculate_sub_account_totals(
                st.session_state.selected_period_id,
                "現実",
                item
            )
            if not sub_totals.empty and sub_totals.sum() != 0:
                for m in forecast_months:
                    if m in base_forecasts_df.columns:
                        base_forecasts_df.loc[base_forecasts_df['項目名'] == item, m] = sub_totals.get(m, 0)
        
        # シナリオ調整
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
        current_pl = processor.calculate_pl(actuals_df, adjusted_forecasts_df, split_idx, months)
        
        st.session_state.actuals_df = actuals_df
        st.session_state.base_forecasts_df = base_forecasts_df
        st.session_state.adjusted_forecasts_df = adjusted_forecasts_df
        st.session_state.current_pl = current_pl
        
    except Exception as e:
        st.error(f"❌ データ処理エラー: {e}")
        logger.error(f"Data processing error: {e}", exc_info=True)

# ========================================================================
# ページコンテンツ
# ========================================================================

if st.session_state.page == "着地予測ダッシュボード":
    st.title("📈 着地予測ダッシュボード")
    
    if 'current_pl' not in st.session_state:
        st.warning("⚠️ データがありません。システム設定から会社と期を追加してください。")
    else:
        st.subheader(f"分析対象: {selected_comp_name} 第{selected_period_num}期 ({st.session_state.scenario}シナリオ)")
        
        current_pl = st.session_state.current_pl
        
        # サマリーカード
        cols = st.columns(5)
        summary_items = ["売上高", "売上総損益金額", "営業損益金額", "経常損益金額", "当期純損益金額"]
        
        for i, item in enumerate(summary_items):
            try:
                val = current_pl[current_pl['項目名'] == item]['合計'].iloc[0]
                cols[i].metric(item, format_currency(val))
            except:
                cols[i].metric(item, "¥0")
        
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
        display_df = current_pl[current_pl['タイプ'] == st.session_state.display_mode] if st.session_state.display_mode == "要約" else current_pl
        
        formatted_df = display_df.copy()
        for m in months + ['合計']:
            if m in formatted_df.columns:
                formatted_df[m] = formatted_df[m].apply(format_currency)
        
        st.dataframe(formatted_df.set_index('項目名')[months + ['合計']], use_container_width=True)

elif st.session_state.page == "システム設定":
    st.title("⚙️ システム設定")
    
    # 会社追加
    st.subheader("🏢 会社の追加")
    with st.form("company_form"):
        new_comp = st.text_input("会社名")
        submitted = st.form_submit_button("会社を追加")
        
        if submitted and new_comp:
            try:
                conn = sqlite3.connect(processor.db_path)
                cursor = conn.cursor()
                cursor.execute("INSERT INTO companies (name) VALUES (?)", (new_comp,))
                conn.commit()
                conn.close()
                st.success(f"✅ 会社「{new_comp}」を追加しました")
                st.rerun()
            except sqlite3.IntegrityError:
                st.error("❌ 既に存在する会社名です")
            except Exception as e:
                st.error(f"❌ エラー: {e}")
    
    # 期の追加
    st.subheader("📅 会計期の追加")
    
    if not companies.empty:
        with st.form("period_form"):
            target_comp = st.selectbox("対象会社", comp_names)
            target_comp_id = int(companies[companies['name'] == target_comp]['id'].iloc[0])
            
            col1, col2, col3 = st.columns(3)
            with col1:
                p_num = st.number_input("期数", min_value=1, value=1)
            with col2:
                s_date = st.date_input("開始日")
            with col3:
                e_date = st.date_input("終了日")
            
            submitted = st.form_submit_button("期を追加")
            
            if submitted:
                try:
                    conn = sqlite3.connect(processor.db_path)
                    cursor = conn.cursor()
                    cursor.execute(
                        "INSERT INTO fiscal_periods (comp_id, period_num, start_date, end_date) VALUES (?, ?, ?, ?)",
                        (target_comp_id, p_num, s_date.strftime('%Y-%m-%d'), e_date.strftime('%Y-%m-%d'))
                    )
                    conn.commit()
                    conn.close()
                    st.success("✅ 期を追加しました")
                    st.rerun()
                except sqlite3.IntegrityError:
                    st.error("❌ この会社の期数は既に存在します")
                except Exception as e:
                    st.error(f"❌ エラー: {e}")
    
    # 登録済みデータ一覧
    st.subheader("📋 登録済みデータ一覧")
    for _, c in companies.iterrows():
        with st.expander(f"🏢 {c['name']}"):
            ps = processor.get_company_periods(c['id'])
            if not ps.empty:
                st.dataframe(ps[['period_num', 'start_date', 'end_date']], use_container_width=True)
            else:
                st.info("期データがありません")

else:
    st.title(f"🚧 {st.session_state.page}")
    st.info("この機能は現在開発中です")

# フッター
st.sidebar.markdown("---")
st.sidebar.caption("💡 財務予測シミュレーター v1.0")
st.sidebar.caption("© 2024 All rights reserved")
