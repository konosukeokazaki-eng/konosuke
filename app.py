import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os
import tempfile
from data_processor import DataProcessor
from datetime import datetime

# ページ設定
st.set_page_config(
    page_title="財務予測シミュレーター",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# カスタムCSS
st.markdown("""
<style>
    /* メインコンテナ */
    .main {
        padding: 0rem 1rem;
    }
    
    /* タイトル */
    h1 {
        color: #1f77b4;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    
    h2 {
        color: #2c3e50;
        font-weight: 600;
        border-bottom: 2px solid #1f77b4;
        padding-bottom: 0.5rem;
        margin-top: 2rem;
    }
    
    h3 {
        color: #34495e;
        font-weight: 600;
    }
    
    /* サマリーカード */
    .summary-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        color: white;
        margin-bottom: 1rem;
    }
    
    .summary-card-green {
        background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        color: white;
        margin-bottom: 1rem;
    }
    
    .summary-card-orange {
        background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        color: white;
        margin-bottom: 1rem;
    }
    
    .summary-card-blue {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        color: white;
        margin-bottom: 1rem;
    }
    
    .summary-card-red {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        color: white;
        margin-bottom: 1rem;
    }
    
    .card-title {
        font-size: 0.9rem;
        font-weight: 500;
        opacity: 0.9;
        margin-bottom: 0.3rem;
    }
    
    .card-value {
        font-size: 2rem;
        font-weight: 700;
        margin: 0;
    }
    
    .card-subtitle {
        font-size: 0.85rem;
        opacity: 0.85;
        margin-top: 0.3rem;
    }
    
    /* サイドバー */
    .css-1d391kg {
        background-color: #f8f9fa;
    }
    
    /* ボタン */
    .stButton>button {
        border-radius: 20px;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    
    /* データフレーム */
    .dataframe {
        font-size: 0.9rem;
    }
    
    /* インフォボックス */
    .info-box {
        background-color: #024270;
        padding: 1rem;
        border-left: 4px solid #1f77b4;
        border-radius: 4px;
        margin: 1rem 0;
        color: white;
    }
    
    .warning-box {
        background-color: #ff8ca1;
        padding: 1rem;
        border-left: 4px solid #ff7f0e;
        border-radius: 4px;
        margin: 1rem 0;
    }
    
    .success-box {
        background-color: #d4edda;
        padding: 1rem;
        border-left: 4px solid #2ca02c;
        border-radius: 4px;
        margin: 1rem 0;
    }
    
    /* タブ */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 4px 4px 0 0;
        padding: 10px 20px;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# 初期化
if 'page' not in st.session_state:
    st.session_state.page = "着地予測ダッシュボード"
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'username' not in st.session_state:
    st.session_state.username = ""

# --------------------------------------------------------------------------------
# シンプルな認証機能
# --------------------------------------------------------------------------------
def check_password():
    """パスワードチェック関数"""
    def password_entered():
        """パスワードが入力されたときの処理"""
        if st.session_state["password"] == st.secrets.get("password", "admin123"):
            st.session_state.authenticated = True
            st.session_state.username = "admin"
            del st.session_state["password"]  # パスワードを削除
        else:
            st.session_state.authenticated = False

    if not st.session_state.authenticated:
        # ログイン画面
        st.markdown("""
        <div style='text-align: center; padding: 2rem;'>
            <h1 style='color: #1f77b4; font-size: 3rem; margin-bottom: 1rem;'>📊</h1>
            <h1 style='color: #2c3e50;'>財務予測シミュレーター</h1>
            <p style='color: #7f8c8d; font-size: 1.1rem;'>ログインして開始してください</p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.text_input(
                "パスワード",
                type="password",
                key="password",
                on_change=password_entered,
                placeholder="パスワードを入力してください"
            )
            
            if "password" in st.session_state:
                st.error("❌ パスワードが正しくありません")
        
        return False
    else:
        return True

# 認証チェック
if not check_password():
    st.stop()

# ログイン成功 - メインアプリケーション

# 初期化
if 'processor' not in st.session_state:
    st.session_state.processor = DataProcessor()
processor = st.session_state.processor

# キャッシュ付きデータ読み込み関数（高速化）
@st.cache_data(ttl=60)  # 60秒間キャッシュ
def load_actual_data_cached(period_id, _processor):
    """実績データをキャッシュ付きで読み込み"""
    return _processor.load_actual_data(period_id)

@st.cache_data(ttl=60)
def load_forecast_data_cached(period_id, scenario, _processor):
    """予測データをキャッシュ付きで読み込み"""
    return _processor.load_forecast_data(period_id, scenario)

@st.cache_data(ttl=60)
def load_sub_accounts_cached(period_id, scenario, _processor):
    """補助科目データをキャッシュ付きで読み込み"""
    return _processor.load_sub_accounts(period_id, scenario)

@st.cache_data(ttl=300)  # 5分間キャッシュ（変更頻度が低い）
def get_companies_cached(_processor):
    """会社一覧をキャッシュ付きで取得"""
    return _processor.get_companies()

@st.cache_data(ttl=300)
def get_company_periods_cached(comp_id, _processor):
    """会計期間一覧をキャッシュ付きで取得"""
    return _processor.get_company_periods(comp_id)

@st.cache_data(ttl=300)
def get_fiscal_months_cached(comp_id, period_id, _processor):
    """会計月一覧をキャッシュ付きで取得"""
    return _processor.get_fiscal_months(comp_id, period_id)

# ヘルパー関数: 安全なint変換
def safe_int(value):
    """NaN/None対応の安全なint変換"""
    try:
        if pd.isna(value) or value is None:
            return 0
        return int(float(value))
    except (ValueError, TypeError):
        return 0

# サイドバー
st.sidebar.markdown("""
<div style='text-align: center; padding: 1rem 0;'>
    <h1 style='color: #1f77b4; margin: 0; font-size: 1.8rem;'>📊</h1>
    <h2 style='color: #2c3e50; margin: 0.5rem 0 0 0; font-size: 1.3rem;'>財務予測<br>シミュレーター</h2>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("---")

# ユーザー情報とログアウト
st.sidebar.markdown(f"**👤 {st.session_state.username}**")
if st.sidebar.button("ログアウト", type="secondary"):
    st.session_state.authenticated = False
    st.session_state.username = ""
    st.rerun()

st.sidebar.markdown("---")

# データベース接続状態の表示
if processor.use_postgres:
    st.sidebar.success("🌐 Supabase接続中")
else:
    st.sidebar.warning("💾 SQLite使用中")
    st.sidebar.caption("⚠️ データは一時的です")

st.sidebar.markdown("---")

# 会社選択
companies = get_companies_cached(processor)
if companies.empty:
    st.sidebar.info("🏢 会社を登録してください")
    st.sidebar.markdown("👉 システム設定から会社を追加")
    # 強制的にシステム設定ページに
    st.session_state.page = "システム設定"
    selected_comp_name = ""
    selected_comp_id = None
    
    # メニューを表示（システム設定のみ使用可能）
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📋 メニュー")
    st.sidebar.markdown("⚙️ システム設定")
    
else:
    comp_names = companies['name'].tolist()
    
    # 前回の選択を保存
    prev_comp_id = st.session_state.get('selected_comp_id', None)
    
    selected_comp_name = st.sidebar.selectbox(
        "🏢 会社を選択",
        comp_names,
        key="comp_select"
    )
    selected_comp_id = int(companies[companies['name'] == selected_comp_name]['id'].iloc[0])
    
    # 会社が変更された場合、データをリフレッシュ
    if prev_comp_id != selected_comp_id:
        # session_stateをクリア（データ再読み込み用）
        for key in ['actuals_df', 'forecasts_df', 'imported_df', 'show_import_button']:
            if key in st.session_state:
                del st.session_state[key]
    
    st.session_state.selected_comp_id = selected_comp_id
    st.session_state.selected_comp_name = selected_comp_name

    # 期選択
    periods = get_company_periods_cached(selected_comp_id, processor)
    if periods.empty:
        st.sidebar.info("📅 会計期間を登録してください")
        st.sidebar.markdown("👉 システム設定から期を追加")
        selected_period_num = 0
        selected_period_id = None
    else:
        # 前回の選択を保存
        prev_period_id = st.session_state.get('selected_period_id', None)
        
        period_options = [
            f"第{row['period_num']}期 ({row['start_date']} 〜 {row['end_date']})"
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
            if 'id' in period_match.columns:
                selected_period_id = int(period_match['id'].iloc[0])
            else:
                selected_period_id = int(period_match.iloc[0, 0])
            
            # 期が変更された場合、データをリフレッシュ
            if prev_period_id != selected_period_id:
                # session_stateをクリア（データ再読み込み用）
                for key in ['actuals_df', 'forecasts_df', 'imported_df', 'show_import_button']:
                    if key in st.session_state:
                        del st.session_state[key]
                
            st.session_state.selected_period_id = selected_period_id
            st.session_state.selected_period_num = selected_period_num
            st.session_state.start_date = period_match['start_date'].iloc[0]
            st.session_state.end_date = period_match['end_date'].iloc[0]
        else:
            st.error("選択された期が見つかりません")
            selected_period_id = None

    # 予測シナリオ
    st.sidebar.markdown("### 🎯 予測シナリオ")
    st.session_state.scenario = st.sidebar.radio(
        "シナリオを選択",
        ["現実", "楽観", "悲観"],
        horizontal=True,
        label_visibility="collapsed"
    )
    
    # シナリオ設定
    if 'scenario_rates' not in st.session_state:
        st.session_state.scenario_rates = {
            "現実": 0.0,
            "楽観": 0.1,
            "悲観": -0.1
        }
    
    # 表示設定
    st.sidebar.markdown("### ⚙️ 表示設定")
    st.session_state.display_mode = st.sidebar.radio(
        "表示モード",
        ["要約", "詳細"],
        horizontal=True
    )
    
    # 月次リスト取得
    if selected_period_id:
        months = get_fiscal_months_cached(selected_comp_id, selected_period_id, processor)
        
        # 実績締月の選択
        if 'current_month' not in st.session_state or st.session_state.current_month not in months:
            st.session_state.current_month = months[0]
            
        st.session_state.current_month = st.sidebar.selectbox(
            "実績締月を選択",
            months,
            index=months.index(st.session_state.current_month) if st.session_state.current_month in months else 0
        )

    # メニュー
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📋 メニュー")
    
    menu_options = [
        "着地予測ダッシュボード",
        "損益計算書 (PL)",
        "実績データ入力",
        "予測データ入力",
        "データインポート",
        "シナリオ一括設定",
        "システム設定"
    ]
    
    st.session_state.page = st.sidebar.radio(
        "ページ移動",
        menu_options,
        label_visibility="collapsed"
    )

# --------------------------------------------------------------------------------
# ヘルパー関数
# --------------------------------------------------------------------------------
def format_currency(val):
    """通貨フォーマット"""
    if pd.isna(val):
        return "¥0"
    return f"¥{safe_int(val):,}"

def format_percent(val):
    """パーセントフォーマット"""
    if pd.isna(val):
        return "0.0%"
    return f"{val:.1f}%"

# --------------------------------------------------------------------------------
# メインコンテンツ
# --------------------------------------------------------------------------------

# システム設定ページ（会社未登録時でも表示）
if st.session_state.page == "システム設定":
    st.title("⚙️ システム設定")
    
    tab1, tab2, tab3 = st.tabs(["🏢 会社設定", "📅 会計期間設定", "🔍 データベース診断"])
    
    with tab1:
        st.subheader("会社情報の管理")
        
        # 新規会社登録
        with st.form("company_form"):
            new_company_name = st.text_input("新規会社名", placeholder="株式会社サンプル")
            if st.form_submit_button("➕ 会社を登録", type="primary"):
                if new_company_name:
                    success, msg = processor.register_company(new_company_name)
                    if success:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
                else:
                    st.error("会社名を入力してください")
        
        st.markdown("---")
        
        # 登録済み会社一覧
        st.subheader("📋 登録済み会社")
        if not companies.empty:
            st.dataframe(companies, width=600)
        else:
            st.info("登録されている会社がありません")
            
    with tab2:
        st.subheader("会計期間の管理")
        
        if companies.empty:
            st.warning("先に会社を登録してください")
        else:
            comp_id_for_period = st.selectbox(
                "対象会社を選択",
                companies['id'].tolist(),
                format_func=lambda x: companies[companies['id'] == x]['name'].iloc[0]
            )
            
            with st.form("period_form"):
                col1, col2 = st.columns(2)
                with col1:
                    period_num = st.number_input("期数 (第n期)", min_value=1, value=1)
                with col2:
                    start_date = st.date_input("開始日")
                    end_date = st.date_input("終了日")
                
                if st.form_submit_button("➕ 期を追加", type="primary"):
                    if start_date and end_date:
                        if start_date < end_date:
                            success, msg = processor.register_fiscal_period(comp_id_for_period, period_num, str(start_date), str(end_date))
                            if success:
                                st.success(msg)
                                st.rerun()
                            else:
                                st.error(msg)
                        else:
                            st.error("❌ 終了日は開始日より後である必要があります")
                    else:
                        st.error("❌ すべてのフィールドを入力してください")
            
            st.markdown("---")
            
            # 登録済み期間一覧
            st.subheader("📋 登録済み会計期間")
            
            if 'selected_comp_id' in st.session_state and st.session_state.selected_comp_id:
                periods_list = processor.get_company_periods(st.session_state.selected_comp_id)
                if not periods_list.empty:
                    st.dataframe(periods_list, width=800)
                else:
                    st.info("登録されている会計期間がありません")
            else:
                st.info("会社を選択すると、その会社の期間が表示されます")
    
    with tab3:
        st.subheader("🔍 データベース診断")
        
        # 接続状態
        st.markdown("### 📡 接続状態")
        if processor.use_postgres:
            st.success("✅ **PostgreSQL (Supabase) 接続中**")
            st.markdown("""
            <div class="success-box">
                <strong>データは永続的に保存されます</strong><br>
                • アプリ再起動後もデータが残ります<br>
                • 複数デバイスから同じデータにアクセス可能<br>
                • データは安全にクラウドに保存されています
            </div>
            """, unsafe_allow_html=True)
            
            # Supabase設定情報
            if hasattr(st, 'secrets') and 'database' in st.secrets:
                st.markdown("### ⚙️ Supabase設定")
                config_info = {
                    "項目": ["ホスト", "データベース", "ユーザー", "ポート"],
                    "値": [
                        st.secrets['database']['host'],
                        st.secrets['database']['database'],
                        st.secrets['database']['user'],
                        str(st.secrets['database']['port'])
                    ]
                }
                st.table(pd.DataFrame(config_info))
        else:
            st.warning("⚠️ **SQLite ローカルデータベース使用中**")
            st.markdown("""
            <div class="warning-box">
                <strong>データは一時的です</strong><br>
                • Streamlit Cloudではアプリ再起動時にデータが消えます<br>
                • ローカル環境では問題なく動作します<br>
                • 永続化するにはSupabaseの設定が必要です
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # データ統計
        st.markdown("### 📊 データ統計")
        
        companies_stat = processor.get_companies()
        total_companies = len(companies_stat)
        
        st.metric("登録会社数", f"{total_companies}社")
        
        if total_companies > 0 and 'selected_comp_id' in st.session_state and st.session_state.selected_comp_id:
            periods_stat = processor.get_company_periods(st.session_state.selected_comp_id)
            st.metric("会計期間数", f"{len(periods_stat)}期")
        
        # 接続テスト
        st.markdown("---")
        st.markdown("### 🧪 接続テスト")
        
        if st.button("🔄 データベース接続をテスト", type="primary"):
            with st.spinner("接続テスト中..."):
                try:
                    # 簡単なクエリで接続テスト
                    test_result = processor.get_companies()
                    st.success(f"✅ 接続成功！会社データを{len(test_result)}件取得しました")
                except Exception as e:
                    st.error(f"❌ 接続失敗: {str(e)}")

# データの読み込み（期が選択されている場合のみ）
if 'selected_period_id' in st.session_state and st.session_state.selected_period_id is not None:
        # キャッシュされたデータを使用
        if 'actuals_df' not in st.session_state:
            st.session_state.actuals_df = load_actual_data_cached(st.session_state.selected_period_id, processor)
        if 'forecasts_df' not in st.session_state:
            st.session_state.forecasts_df = load_forecast_data_cached(st.session_state.selected_period_id, "現実", processor)
            
        actuals_df = st.session_state.actuals_df.copy()
        forecasts_df = st.session_state.forecasts_df.copy()
        
        # シナリオ調整
        if st.session_state.scenario != "現実":
            rate = st.session_state.scenario_rates[st.session_state.scenario]
            split_idx = months.index(st.session_state.current_month) + 1 if st.session_state.current_month in months else 0
            forecast_months = months[split_idx:]
            # DataFrameに存在する月のみを使用
            available_forecast_months = [m for m in forecast_months if m in forecasts_df.columns]
            
            for item in processor.all_items:
                if item == "売上高":
                    forecasts_df.loc[forecasts_df['項目名'] == item, available_forecast_months] *= (1 + rate)
                elif item == "売上原価":
                    forecasts_df.loc[forecasts_df['項目名'] == item, available_forecast_months] *= (1 - rate * 0.5)
                elif item in processor.ga_items:
                    forecasts_df.loc[forecasts_df['項目名'] == item, available_forecast_months] *= (1 - rate * 0.3)
                    
            st.session_state.adjusted_forecasts_df = forecasts_df.copy()
        
        # 補助科目合計の反映
        sub_accounts_df = processor.load_sub_accounts(st.session_state.selected_period_id, st.session_state.scenario)
        if not sub_accounts_df.empty:
            aggregated = sub_accounts_df.groupby(['parent_item', 'month'])['amount'].sum().reset_index()
            for _, row in aggregated.iterrows():
                parent = row['parent_item']
                month = row['month']
                amount = row['amount']
                forecasts_df.loc[forecasts_df['項目名'] == parent, month] = amount
        
        # PL計算
        split_idx = months.index(st.session_state.current_month) + 1 if st.session_state.current_month in months else 0
        pl_df = processor.calculate_pl(
            actuals_df,
            forecasts_df,
            split_idx,
            months
        )
        
        # 表示モードでフィルタ
        if st.session_state.display_mode == "要約":
            pl_display = pl_df[pl_df['タイプ'] == '要約']
        else:
            pl_display = pl_df
        
        # --------------------------------------------------------------------------------
        # ページコンテンツ
        # --------------------------------------------------------------------------------
        
        if st.session_state.page == "着地予測ダッシュボード":
            st.title("📊 着地予測ダッシュボード")
            
            st.markdown(f"""
            <div class="info-box">
                <strong>🏢 {st.session_state.selected_comp_name}</strong> | 
                第{st.session_state.selected_period_num}期 | 
                実績: {st.session_state.start_date} 〜 {st.session_state.current_month} | 
                シナリオ: <strong>{st.session_state.scenario}</strong>
            </div>
            """, unsafe_allow_html=True)
            
            # KPIサマリーカード
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                sales_total = pl_display[pl_display['項目名'] == '売上高']['合計'].iloc[0]
                st.markdown(f"""
                <div class="summary-card-blue">
                    <div class="card-title">売上高</div>
                    <div class="card-value">¥{safe_int(sales_total):,}</div>
                    <div class="card-subtitle">期末着地予測</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                gp_total = pl_display[pl_display['項目名'] == '売上総損益金額']['合計'].iloc[0]
                gp_rate = (gp_total / sales_total * 100) if sales_total != 0 else 0
                st.markdown(f"""
                <div class="summary-card-green">
                    <div class="card-title">売上総利益</div>
                    <div class="card-value">¥{safe_int(gp_total):,}</div>
                    <div class="card-subtitle">粗利率: {gp_rate:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                op_total = pl_display[pl_display['項目名'] == '営業損益金額']['合計'].iloc[0]
                op_rate = (op_total / sales_total * 100) if sales_total != 0 else 0
                st.markdown(f"""
                <div class="summary-card-orange">
                    <div class="card-title">営業利益</div>
                    <div class="card-value">¥{safe_int(op_total):,}</div>
                    <div class="card-subtitle">営業利益率: {op_rate:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                ord_total = pl_display[pl_display['項目名'] == '経常損益金額']['合計'].iloc[0]
                ord_rate = (ord_total / sales_total * 100) if sales_total != 0 else 0
                st.markdown(f"""
                <div class="summary-card">
                    <div class="card-title">経常利益</div>
                    <div class="card-value">¥{safe_int(ord_total):,}</div>
                    <div class="card-subtitle">経常利益率: {ord_rate:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col5:
                net_total = pl_display[pl_display['項目名'] == '当期純損益金額']['合計'].iloc[0]
                net_rate = (net_total / sales_total * 100) if sales_total != 0 else 0
                color_class = "summary-card-green" if net_total >= 0 else "summary-card-red"
                st.markdown(f"""
                <div class="{color_class}">
                    <div class="card-title">当期純利益</div>
                    <div class="card-value">¥{safe_int(net_total):,}</div>
                    <div class="card-subtitle">純利益率: {net_rate:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("---")
            
            # タブで表示切り替え
            tab1, tab2 = st.tabs(["📊 損益計算書", "📈 グラフ分析"])
            
            with tab1:
                st.subheader("期末着地予測 損益計算書")
                
                # スタイル付きデータフレーム
                def highlight_summary(row):
                    if row['タイプ'] == '要約':
                        return ['background-color: #5db5f5; font-weight: bold'] * len(row)
                    return [''] * len(row)
                
                # タイプ列を使ってスタイルを適用してから削除
                styled_df = pl_display.style\
                    .apply(highlight_summary, axis=1)\
                    .format(lambda x: f"¥{safe_int(x):,}" if isinstance(x, (int, float)) else x)
                
                st.dataframe(styled_df, width="stretch", height=500)
                
            with tab2:
                st.subheader("月次推移グラフ")
                
                # グラフ用データの準備
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                # 売上高（棒グラフ）
                fig.add_trace(
                    go.Bar(
                        x=months,
                        y=pl_df[pl_df['項目名'] == '売上高'][months].iloc[0],
                        name="売上高",
                        marker_color='#4facfe'
                    ),
                    secondary_y=False
                )
                
                # 営業利益（折れ線グラフ）
                fig.add_trace(
                    go.Scatter(
                        x=months,
                        y=pl_df[pl_df['項目名'] == '営業損益金額'][months].iloc[0],
                        name="営業利益",
                        line=dict(color='#f5576c', width=3)
                    ),
                    secondary_y=True
                )
                
                # 実績/予測の境界線
                try:
                    # add_vlineの代わりに、より安定したadd_shapeを使用して境界線を描画
                    fig.add_shape(
                        type="line",
                        x0=st.session_state.current_month,
                        x1=st.session_state.current_month,
                        y0=0,
                        y1=1,
                        yref="paper",
                        line=dict(color="gray", width=2, dash="dash")
                    )
                    # 境界線のラベルを追加
                    fig.add_annotation(
                        x=st.session_state.current_month,
                        y=1,
                        yref="paper",
                        text="実績/予測 境界",
                        showarrow=False,
                        xanchor="left",
                        textangle=-90
                    )
                except Exception as e:
                    # 万が一エラーが発生した場合は境界線なしで続行
                    st.sidebar.error(f"グラフ境界線の描画エラー: {e}")
                
                fig.update_layout(
                    title_text="売上高と営業利益の推移",
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                
                fig.update_yaxes(title_text="売上高 (円)", secondary_y=False)
                fig.update_yaxes(title_text="営業利益 (円)", secondary_y=True)
                
                st.plotly_chart(fig, width="stretch")
                
                # 費用構成の円グラフ
                st.subheader("費用構成分析（通期予測）")
                
                ga_items_data = pl_df[pl_df['項目名'].isin(processor.ga_items)]
                fig_pie = px.pie(
                    ga_items_data,
                    values='合計',
                    names='項目名',
                    title="販売管理費の内訳",
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                st.plotly_chart(fig_pie, width="stretch")

        elif st.session_state.page == "損益計算書 (PL)":
            st.title("📄 損益計算書 (PL)")
            
            st.markdown(f"""
            <div class="info-box">
                <strong>🏢 {st.session_state.selected_comp_name}</strong> | 
                第{st.session_state.selected_period_num}期 | 
                実績締月: {st.session_state.current_month} | 
                シナリオ: <strong>{st.session_state.scenario}</strong>
            </div>
            """, unsafe_allow_html=True)
            
            # フィルタリング
            col1, col2 = st.columns([2, 1])
            with col1:
                search_term = st.text_input("🔍 項目名で検索", "")
            
            display_df = pl_display.copy()
            if search_term:
                display_df = display_df[display_df['項目名'].str.contains(search_term)]
            
            # フォーマット
            formatted_df = display_df.style\
                .format(lambda x: f"¥{safe_int(x):,}" if isinstance(x, (int, float)) else x)\
                .apply(lambda row: ['background-color: #f8f9fa; font-weight: bold' if row['タイプ'] == '要約' else '' for _ in row], axis=1)
            
            st.dataframe(formatted_df, width="stretch", height=700)
            
            # CSVダウンロード
            csv = display_df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                "📥 CSVとしてダウンロード",
                csv,
                f"PL_{st.session_state.selected_comp_name}_第{st.session_state.selected_period_num}期.csv",
                "text/csv",
                key='download-csv'
            )

        elif st.session_state.page == "予測データ入力":
            st.title("🔮 予測データ入力")
            
            st.markdown(f"""
            <div class="info-box">
                <strong>シナリオ: {st.session_state.scenario}</strong> | 
                実績締月: {st.session_state.current_month} 以降のデータを編集してください。<br>
                💡 <strong>使い方:</strong> 項目をクリックして展開 → 数値を入力 → 保存
            </div>
            """, unsafe_allow_html=True)
            
            # 予測PLデータ全体を取得
            forecast_pl_df = forecasts_df.copy()
            
            # 展開状態を管理
            if 'expanded_items' not in st.session_state:
                st.session_state.expanded_items = set()
            
            # PLの構造を定義
            pl_structure = {
                "売上": ["売上高"],
                "売上原価": ["売上原価"],
                "売上総利益": ["売上総損益金額"],
                "人件費": ["役員報酬", "給料手当", "賞与", "法定福利費", "福利厚生費"],
                "採用・外注": ["採用教育費", "外注費"],
                "販売費": ["荷造運賃", "広告宣伝費", "販売手数料", "販売促進費"],
                "一般管理費": [
                    "交際費", "会議費", "旅費交通費", "通信費", "消耗品費", 
                    "修繕費", "事務用品費", "水道光熱費", "新聞図書費", "諸会費",
                    "支払手数料", "車両費", "地代家賃", "賃借料", "保険料",
                    "租税公課", "支払報酬料", "研究開発費", "研修費", "減価償却費",
                    "貸倒損失(販)", "雑費", "少額交際費"
                ],
                "営業外・特別損益": [
                    "営業外収益合計", "営業外費用合計", 
                    "特別利益合計", "特別損失合計"
                ],
                "税金": ["法人税、住民税及び事業税"]
            }
            
            # 編集不可の計算項目
            calculated_items_set = set(processor.calculated_items)
            
            # PL表示
            st.markdown("### 📊 損益計算書（予測）")
            
            for category, items in pl_structure.items():
                with st.expander(f"**{category}**", expanded=True):
                    for item in items:
                        if item not in forecast_pl_df['項目名'].values:
                            continue
                        
                        item_data = forecast_pl_df[forecast_pl_df['項目名'] == item]
                        is_calculated = item in calculated_items_set
                        
                        # 項目の展開/折りたたみボタン
                        col1, col2 = st.columns([3, 1])
                        
                        with col1:
                            if is_calculated:
                                st.markdown(f"**{item}** 🔒 (自動計算)")
                            else:
                                if st.button(
                                    f"{'▼' if item in st.session_state.expanded_items else '▶'} {item}",
                                    key=f"expand_{item}",
                                    use_container_width=True
                                ):
                                    if item in st.session_state.expanded_items:
                                        st.session_state.expanded_items.remove(item)
                                    else:
                                        st.session_state.expanded_items.add(item)
                                    st.rerun()
                        
                        with col2:
                            # 合計値を表示
                            if not item_data.empty:
                                month_cols = [m for m in months if m in item_data.columns]
                                total = item_data[month_cols].sum(axis=1).iloc[0] if month_cols else 0
                                st.markdown(f"<div style='text-align: right;'>合計: ¥{safe_int(total):,}</div>", unsafe_allow_html=True)
                        
                        # 展開されている場合、入力フォームを表示
                        if item in st.session_state.expanded_items and not is_calculated:
                            with st.container():
                                st.markdown("---")
                                
                                # 補助科目がある場合は表示
                                if item in processor.parent_items_with_sub_accounts:
                                    sub_accounts = processor.get_sub_accounts_for_parent(
                                        st.session_state.selected_period_id,
                                        st.session_state.scenario,
                                        item
                                    )
                                    
                                    if not sub_accounts.empty:
                                        st.markdown(f"**📋 {item}の内訳（補助科目）**")
                                        for _, sub in sub_accounts.iterrows():
                                            sub_name = sub['sub_account_name']
                                            with st.expander(f"🔹 {sub_name}"):
                                                # 補助科目の編集フォーム
                                                cols_sub = st.columns(4)
                                                sub_values = {}
                                                
                                                for i, month in enumerate(months):
                                                    with cols_sub[i % 4]:
                                                        current_val = 0.0
                                                        if month in sub.index:
                                                            val = sub[month]
                                                            if pd.notna(val):
                                                                current_val = float(val)
                                                        
                                                        new_val = st.number_input(
                                                            month,
                                                            value=current_val,
                                                            step=10000.0,
                                                            format="%.0f",
                                                            key=f"sub_{item}_{sub_name}_{month}"
                                                        )
                                                        sub_values[month] = new_val
                                                
                                                col_save, col_delete = st.columns([1, 1])
                                                with col_save:
                                                    if st.button("💾 保存", key=f"save_sub_{item}_{sub_name}", type="primary"):
                                                        success, msg = processor.save_sub_account(
                                                            st.session_state.selected_period_id,
                                                            st.session_state.scenario,
                                                            item,
                                                            sub_name,
                                                            sub_values
                                                        )
                                                        if success:
                                                            st.success(msg)
                                                            st.rerun()
                                                        else:
                                                            st.error(msg)
                                                
                                                with col_delete:
                                                    if st.button("🗑️ 削除", key=f"del_sub_{item}_{sub_name}"):
                                                        success, msg = processor.delete_sub_account(
                                                            st.session_state.selected_period_id,
                                                            st.session_state.scenario,
                                                            item,
                                                            sub_name
                                                        )
                                                        if success:
                                                            st.success(msg)
                                                            st.rerun()
                                                        else:
                                                            st.error(msg)
                                
                                # 新しい補助科目の追加フォーム
                                if item in processor.parent_items_with_sub_accounts:
                                    with st.expander("➕ 新しい補助科目を追加"):
                                        new_sub_name = st.text_input(
                                            "補助科目名",
                                            key=f"new_sub_{item}",
                                            placeholder="例: 国内売上、海外売上"
                                        )
                                        
                                        if new_sub_name:
                                            cols_new = st.columns(4)
                                            new_sub_values = {}
                                            
                                            for i, month in enumerate(months):
                                                with cols_new[i % 4]:
                                                    val = st.number_input(
                                                        month,
                                                        value=0.0,
                                                        step=10000.0,
                                                        format="%.0f",
                                                        key=f"new_sub_{item}_{new_sub_name}_{month}"
                                                    )
                                                    new_sub_values[month] = val
                                            
                                            if st.button("💾 補助科目を追加", key=f"add_sub_{item}", type="primary"):
                                                success, msg = processor.save_sub_account(
                                                    st.session_state.selected_period_id,
                                                    st.session_state.scenario,
                                                    item,
                                                    new_sub_name,
                                                    new_sub_values
                                                )
                                                if success:
                                                    st.success(msg)
                                                    st.rerun()
                                                else:
                                                    st.error(msg)
                                
                                # 基本項目の入力フォーム
                                st.markdown(f"**💰 {item} の月次予測値**")
                                
                                cols = st.columns(4)
                                item_values = {}
                                
                                for i, month in enumerate(months):
                                    with cols[i % 4]:
                                        current_val = 0.0
                                        if not item_data.empty and month in item_data.columns:
                                            val = item_data[month].iloc[0]
                                            if pd.notna(val):
                                                current_val = float(val)
                                        
                                        new_val = st.number_input(
                                            month,
                                            value=current_val,
                                            step=10000.0,
                                            format="%.0f",
                                            key=f"forecast_{item}_{month}"
                                        )
                                        item_values[month] = new_val
                                
                                if st.button("💾 保存", key=f"save_{item}", type="primary"):
                                    success, msg = processor.save_forecast_item(
                                        st.session_state.selected_period_id,
                                        st.session_state.scenario,
                                        item,
                                        item_values
                                    )
                                    if success:
                                        st.success(msg)
                                        if 'forecasts_df' in st.session_state:
                                            del st.session_state.forecasts_df
                                        st.rerun()
                                    else:
                                        st.error(msg)
                                
                                st.markdown("---")
            
            st.markdown("""
            <div class="info-box">
                <strong>💡 使い方:</strong> 月次の実績データを入力してください。
            </div>
            """, unsafe_allow_html=True)
            
            # 編集可能な項目
            editable_items = [item for item in processor.all_items if item not in processor.calculated_items]
            
            selected_item = st.selectbox("編集する項目", editable_items)
            
            st.markdown(f"### {selected_item} の実績値入力")
            
            cols = st.columns(4)
            new_values = {}
            current_values = actuals_df[actuals_df['項目名'] == selected_item]
            
            for i, month in enumerate(months):
                with cols[i % 4]:
                    current_val = 0.0
                    if not current_values.empty and month in current_values.columns:
                        current_val = current_values[month].iloc[0]
                    
                    new_val = st.number_input(
                        f"{month}",
                        value=float(current_val),
                        step=10000.0,
                        format="%.0f",
                        key=f"actual_{selected_item}_{month}"
                    )
                    new_values[month] = new_val
            
            if st.button("💾 保存", type="primary"):
                success, msg = processor.save_actual_item(
                    st.session_state.selected_period_id,
                    selected_item,
                    new_values
                )
                if success:
                    st.success(msg)
                    # キャッシュクリア
                    if 'actuals_df' in st.session_state:
                        del st.session_state.actuals_df
                    st.rerun()
                else:
                    st.error(msg)
        
        elif st.session_state.page == "データインポート":
            st.title("📥 データインポート")
            
            # タブで実績データと予測データを分ける
            tab1, tab2 = st.tabs(["💰 実績データインポート", "📊 予測データインポート"])
            
            # ===== タブ1: 実績データインポート =====
            with tab1:
                st.markdown("""
                <div class="info-box">
                    <strong>💡 使い方:</strong> 弥生会計からエクスポートしたExcelファイルをアップロードしてください。
                </div>
                """, unsafe_allow_html=True)
                
                uploaded_file = st.file_uploader(
                    "Excelファイルを選択（実績データ）",
                    type=['xlsx', 'xls'],
                    help="弥生会計の月次推移表をアップロードしてください",
                    key="actual_upload"
                )
                
                # ファイルが削除された場合のキャッシュクリア
                if uploaded_file is None:
                    if 'imported_df' in st.session_state:
                        del st.session_state.imported_df
                    if 'show_import_button' in st.session_state:
                        del st.session_state.show_import_button
                
                if uploaded_file:
                    if 'imported_df' not in st.session_state:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                            tmp_file.write(uploaded_file.read())
                            temp_path = tmp_file.name
                            st.session_state.temp_path_to_delete = temp_path
                            
                        st.success(f"✅ ファイル **{uploaded_file.name}** を読み込みました")
                        
                        # fiscal_period_idを渡す
                        st.session_state.imported_df, info = processor.import_yayoi_excel(
                            temp_path, 
                            st.session_state.selected_period_id,
                            preview_only=True
                        )
                        st.session_state.show_import_button = True
                        
                        # 一時ファイルを削除
                        if os.path.exists(temp_path):
                            os.unlink(temp_path)
                        
                    if st.session_state.get('show_import_button'):
                        st.subheader("📋 インポートデータ プレビュー（直接編集可能）")
                        
                        st.markdown("""
                        <div class="info-box">
                            <strong>✏️ 編集:</strong> セルをダブルクリックして値を直接修正できます。
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # 編集可能なデータエディタを使用
                        edited_df = st.data_editor(
                            st.session_state.imported_df,
                            width="stretch",
                            height=400,
                            num_rows="fixed",  # 行の追加・削除は不可
                            disabled=["項目名"],  # 項目名列は編集不可
                            column_config={
                                col: st.column_config.NumberColumn(
                                    format="¥%d",
                                    min_value=-999999999,
                                    max_value=999999999
                                ) for col in st.session_state.imported_df.columns if col != '項目名'
                            }
                        )
                        
                        # 編集後のデータを保存
                        st.session_state.imported_df = edited_df
                        
                        st.markdown("""
                        <div class="warning-box">
                            <strong>⚠️ 注意:</strong> 上記の内容でインポートを実行すると、現在の実績データは上書きされます。
                        </div>
                        """, unsafe_allow_html=True)
                        
                        if st.button("✅ 上記内容でインポートを実行", type="primary", key="import_actual"):
                            success, info = processor.save_extracted_data(
                                st.session_state.selected_period_id,
                                st.session_state.imported_df
                            )
                            if success:
                                st.success("✅ インポートが完了しました！")
                                # キャッシュクリア
                                for key in ['actuals_df', 'imported_df', 'show_import_button']:
                                    if key in st.session_state:
                                        del st.session_state[key]
                                st.rerun()
                            else:
                                st.error(f"❌ インポートに失敗しました: {info}")
            
            # ===== タブ2: 予測データインポート =====
            with tab2:
                st.markdown("""
                <div class="info-box">
                    <strong>💡 使い方:</strong><br>
                    1. テンプレートをダウンロード<br>
                    2. Excelで予測数値を入力<br>
                    3. ファイルをアップロード
                </div>
                """, unsafe_allow_html=True)
                
                # シナリオ選択
                forecast_scenario = st.selectbox(
                    "インポート先シナリオを選択",
                    ["現実", "楽観", "悲観"],
                    key="forecast_import_scenario"
                )
                
                # テンプレートダウンロード
                st.subheader("📥 ステップ1: テンプレートをダウンロード")
                
                template_df = processor.create_forecast_template(
                    st.session_state.selected_period_id,
                    forecast_scenario
                )
                
                if template_df is not None:
                    # Excelファイルとして出力
                    from io import BytesIO
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        template_df.to_excel(writer, index=False, sheet_name='予測データ')
                    excel_data = output.getvalue()
                    
                    st.download_button(
                        label="📥 予測データテンプレートをダウンロード",
                        data=excel_data,
                        file_name=f"予測データテンプレート_{forecast_scenario}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary"
                    )
                    
                    st.info("""
                    💡 **テンプレートの使い方:**
                    - 各項目の予測数値を月ごとに入力してください
                    - 0のままの項目はインポートされません
                    - 項目名の列は変更しないでください
                    """)
                
                st.markdown("---")
                
                # ファイルアップロード
                st.subheader("📤 ステップ2: 入力済みファイルをアップロード")
                
                forecast_file = st.file_uploader(
                    "予測データExcelファイルを選択",
                    type=['xlsx', 'xls'],
                    help="入力済みのテンプレートファイルをアップロードしてください",
                    key="forecast_upload"
                )
                
                # ファイルが削除された場合のキャッシュクリア
                if forecast_file is None:
                    if 'forecast_imported_df' in st.session_state:
                        del st.session_state.forecast_imported_df
                    if 'show_forecast_import_button' in st.session_state:
                        del st.session_state.show_forecast_import_button
                
                if forecast_file:
                    if 'forecast_imported_df' not in st.session_state:
                        try:
                            # Excelファイルを読み込み
                            forecast_df = pd.read_excel(forecast_file)
                            
                            # 基本的なバリデーション
                            if '項目名' not in forecast_df.columns:
                                st.error("❌ テンプレート形式が正しくありません。「項目名」列が見つかりません。")
                            else:
                                st.success(f"✅ ファイル **{forecast_file.name}** を読み込みました")
                                st.session_state.forecast_imported_df = forecast_df
                                st.session_state.show_forecast_import_button = True
                        
                        except Exception as e:
                            st.error(f"❌ ファイルの読み込みに失敗しました: {str(e)}")
                    
                    if st.session_state.get('show_forecast_import_button'):
                        st.subheader("📋 インポートデータ プレビュー（直接編集可能）")
                        
                        st.markdown("""
                        <div class="info-box">
                            <strong>✏️ 編集:</strong> セルをダブルクリックして値を直接修正できます。
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # 編集可能なデータエディタを使用
                        edited_forecast_df = st.data_editor(
                            st.session_state.forecast_imported_df,
                            width="stretch",
                            height=400,
                            num_rows="fixed",
                            disabled=["項目名"],
                            column_config={
                                col: st.column_config.NumberColumn(
                                    format="¥%d",
                                    min_value=-999999999,
                                    max_value=999999999
                                ) for col in st.session_state.forecast_imported_df.columns if col != '項目名'
                            }
                        )
                        
                        # 編集後のデータを保存
                        st.session_state.forecast_imported_df = edited_forecast_df
                        
                        st.markdown(f"""
                        <div class="warning-box">
                            <strong>⚠️ 注意:</strong> 上記の内容でインポートを実行すると、「{forecast_scenario}」シナリオの予測データが上書きされます。
                        </div>
                        """, unsafe_allow_html=True)
                        
                        if st.button("✅ 予測データをインポート", type="primary", key="import_forecast"):
                            success, info = processor.save_forecast_from_excel(
                                st.session_state.selected_period_id,
                                forecast_scenario,
                                st.session_state.forecast_imported_df
                            )
                            if success:
                                st.success(f"✅ {info}")
                                # キャッシュクリア
                                for key in ['forecasts_df', 'forecast_imported_df', 'show_forecast_import_button']:
                                    if key in st.session_state:
                                        del st.session_state[key]
                                st.rerun()
                            else:
                                st.error(f"❌ インポートに失敗しました: {info}")
        
        elif st.session_state.page == "シナリオ一括設定":
            st.title("🎯 シナリオ一括設定")
            
            st.markdown("""
            <div class="info-box">
                <strong>💡 使い方:</strong> 「現実」シナリオをベースに、「楽観」「悲観」シナリオの増減率を設定します。
                設定した増減率は全画面に即座に反映されます。
            </div>
            """, unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 📈 楽観シナリオ")
                st.markdown("""
                <div class="success-box">
                    <strong>想定される効果:</strong><br>
                    • 売上: 増加率そのまま適用<br>
                    • 売上原価: 増加率の50%を逆方向に適用<br>
                    • 販管費: 増加率の30%を逆方向に適用
                </div>
                """, unsafe_allow_html=True)
                
                new_opt_rate = st.number_input(
                    "楽観シナリオ増減率 (%)",
                    value=st.session_state.scenario_rates["楽観"] * 100,
                    min_value=-100.0,
                    max_value=100.0,
                    step=1.0,
                    key="opt_rate_input"
                ) / 100.0
                
                if st.button("💾 楽観シナリオ増減率を保存", type="primary"):
                    st.session_state.scenario_rates["楽観"] = new_opt_rate
                    st.success(f"✅ 楽観シナリオの増減率を **{new_opt_rate * 100:.1f}%** に設定しました")
                    st.rerun()
            
            with col2:
                st.markdown("### 📉 悲観シナリオ")
                st.markdown("""
                <div class="warning-box">
                    <strong>想定される効果:</strong><br>
                    • 売上: 減少率そのまま適用<br>
                    • 売上原価: 減少率の50%を逆方向に適用<br>
                    • 販管費: 減少率の30%を逆方向に適用
                </div>
                """, unsafe_allow_html=True)
                
                new_pes_rate = st.number_input(
                    "悲観シナリオ増減率 (%)",
                    value=st.session_state.scenario_rates["悲観"] * 100,
                    min_value=-100.0,
                    max_value=100.0,
                    step=1.0,
                    key="pes_rate_input"
                ) / 100.0
                
                if st.button("💾 悲観シナリオ増減率を保存", type="primary"):
                    st.session_state.scenario_rates["悲観"] = new_pes_rate
                    st.success(f"✅ 悲観シナリオの増減率を **{new_pes_rate * 100:.1f}%** に設定しました")
                    st.rerun()
            
            st.markdown("---")
            
            # 設定値サマリー
            st.subheader("📋 現在の設定値")
            
            summary_data = {
                "シナリオ": ["現実", "楽観", "悲観"],
                "増減率": [
                    f"{st.session_state.scenario_rates['現実'] * 100:.1f}%",
                    f"{st.session_state.scenario_rates['楽観'] * 100:.1f}%",
                    f"{st.session_state.scenario_rates['悲観'] * 100:.1f}%"
                ],
                "説明": [
                    "ベースとなる予測値",
                    "売上増加・費用削減を想定",
                    "売上減少・費用増加を想定"
                ]
            }
            
            st.table(pd.DataFrame(summary_data))
        

else:
    # 会社または期が未登録の場合
    if companies.empty:
        st.title("👋 ようこそ！財務予測シミュレーターへ")
        
        st.markdown("""
        <div style="background-color: #e3f2fd; padding: 2rem; border-radius: 10px; margin: 2rem 0;">
            <h3 style="color: #1976d2; margin-top: 0;">🚀 はじめての方へ</h3>
            <p style="font-size: 1.1rem; line-height: 1.8;">
                まずは以下の手順でセットアップしてください：
            </p>
            <div style="background-color: white; padding: 1.5rem; border-radius: 8px; margin: 1rem 0;">
                <strong style="font-size: 1.2rem; color: #1976d2;">📍 手順</strong><br><br>
                <strong style="color: #d32f2f;">1️⃣ 左サイドバーの「⚙️ システム設定」をクリック</strong><br>
                <span style="font-size: 0.9rem; color: #666;">← 左側のメニューから選択してください</span><br><br>
                <strong>2️⃣ 会社設定タブで会社名を入力</strong><br><br>
                <strong>3️⃣ 会計期間設定タブで期の情報を入力</strong><br><br>
                <strong>4️⃣ サイドバーで会社と期を選択</strong><br>
                <span style="font-size: 0.9rem; color: #666;">→ すべての機能が使えるようになります！</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # データベース接続状態を表示
        if processor.use_postgres:
            st.success("✅ Supabaseに接続済み - データは永続的に保存されます")
        else:
            st.info("ℹ️ ローカルモードで動作中")
            
    else:
        st.warning("### ⚠️ 会計期間が選択されていません")
        st.markdown("""
        <div class="warning-box">
            <strong>会計期間を登録してください</strong><br><br>
            左サイドバーの「システム設定」→「会計期間設定」タブから<br>
            会計期間を追加してください。
        </div>
        """, unsafe_allow_html=True)
