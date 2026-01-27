import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os
import tempfile
import yaml
from yaml.loader import SafeLoader
import streamlit_authenticator as stauth
from data_processor import DataProcessor

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
        background-color: #e3f2fd;
        padding: 1rem;
        border-left: 4px solid #1f77b4;
        border-radius: 4px;
        margin: 1rem 0;
    }
    
    .warning-box {
        background-color: #fff3cd;
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

# --------------------------------------------------------------------------------
# 認証機能の実装
# --------------------------------------------------------------------------------
try:
    with open('config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)
except FileNotFoundError:
    # パスワードのハッシュ化
    hashed_passwords = stauth.Hasher(['password']).generate()
    default_password_hash = hashed_passwords[0] if isinstance(hashed_passwords, list) else hashed_passwords
    config = {
        'cookie': {
            'expiry_days': 30,
            'key': 'financial_auth_key',
            'name': 'financial_auth'
        },
        'credentials': {
            'usernames': {
                'admin': {
                    'email': 'admin@example.com',
                    'name': '管理者',
                    'password': default_password_hash
                }
            }
        },
        'preauthorized': {
            'emails': ['admin@example.com']
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

# ログインフォームの表示
name, authentication_status, username = authenticator.login('main')

# 認証ステータスに基づいて処理を分岐
if authentication_status:
    # ログイン成功時
    
    # 初期化
    if 'processor' not in st.session_state:
        st.session_state.processor = DataProcessor()
    processor = st.session_state.processor
    
    # サイドバー
    st.sidebar.markdown("""
    <div style='text-align: center; padding: 1rem 0;'>
        <h1 style='color: #1f77b4; margin: 0; font-size: 1.8rem;'>📊</h1>
        <h2 style='color: #2c3e50; margin: 0.5rem 0 0 0; font-size: 1.3rem;'>財務予測<br>シミュレーター</h2>
    </div>
    """, unsafe_allow_html=True)
    
    st.sidebar.markdown("---")
    
    # ユーザー情報とログアウト
    st.sidebar.markdown(f"**👤 {name}**")
    authenticator.logout('ログアウト', 'sidebar')
    
    st.sidebar.markdown("---")
    
    # 会社選択
    companies = processor.get_companies()
    if companies.empty:
        st.sidebar.error("会社データがありません")
        st.session_state.page = "システム設定"
        selected_comp_name = ""
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
        periods = processor.get_company_periods(selected_comp_id)
        if periods.empty:
            st.sidebar.warning("期データがありません")
            selected_period_num = 0
        else:
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
        
        if st.session_state.scenario != "現実":
            st.sidebar.markdown("---")
            rate_key = f"{st.session_state.scenario}_rate"
            initial_rate = st.session_state.scenario_rates[st.session_state.scenario] * 100
            
            new_rate = st.sidebar.number_input(
                f"📈 {st.session_state.scenario}シナリオ増減率 (%)",
                value=initial_rate,
                min_value=-100.0,
                max_value=100.0,
                step=1.0,
                key=rate_key
            ) / 100.0
            
            st.session_state.scenario_rates[st.session_state.scenario] = new_rate
    
        # 実績データ最終月
        months = processor.get_fiscal_months(selected_comp_id, st.session_state.get('selected_period_id'))
        current_month = st.sidebar.selectbox(
            "📆 実績データ最終月",
            months,
            key="month_select"
        )
        st.session_state.current_month = current_month
    
        # 表示設定
        st.sidebar.markdown("### ⚙️ 表示設定")
        st.session_state.display_mode = st.sidebar.radio(
            "表示モード",
            ["要約", "詳細"],
            horizontal=True,
            label_visibility="collapsed"
        )
    
    st.sidebar.markdown("---")
    
    # メニュー
    st.sidebar.markdown("### 📋 メニュー")
    menu = [
        "着地予測ダッシュボード",
        "比較分析レポート",
        "全体予測PL & 補助科目入力",
        "実績データ入力",
        "データインポート",
        "シナリオ一括設定",
        "システム設定"
    ]
    
    # メニューアイコン
    menu_icons = {
        "着地予測ダッシュボード": "📊",
        "比較分析レポート": "📈",
        "全体予測PL & 補助科目入力": "📝",
        "実績データ入力": "⌨️",
        "データインポート": "📥",
        "シナリオ一括設定": "🎯",
        "システム設定": "⚙️"
    }
    
    selected_menu = st.sidebar.radio(
        "移動先を選択",
        menu,
        index=menu.index(st.session_state.page) if st.session_state.page in menu else 0,
        format_func=lambda x: f"{menu_icons.get(x, '•')} {x}",
        label_visibility="collapsed"
    )
    st.session_state.page = selected_menu
    
    # 通貨フォーマット
    def format_currency(val):
        if isinstance(val, (int, float, complex)) and not isinstance(val, bool):
            if pd.isna(val):
                return ""
            return f"¥{int(val):,}"
        return val
    
    # データの読み込み
    if 'selected_period_id' in st.session_state:
        actuals_df = processor.load_actual_data(st.session_state.selected_period_id)
        forecasts_df = processor.load_forecast_data(st.session_state.selected_period_id, "現実")
        
        # シナリオ調整
        if st.session_state.scenario != "現実":
            rate = st.session_state.scenario_rates[st.session_state.scenario]
            split_idx = processor.get_split_index(
                st.session_state.selected_comp_id,
                st.session_state.current_month,
                st.session_state.selected_period_id
            )
            forecast_months = months[split_idx:]
            
            for item in processor.all_items:
                if item == "売上高":
                    forecasts_df.loc[forecasts_df['項目名'] == item, forecast_months] *= (1 + rate)
                elif item == "売上原価":
                    forecasts_df.loc[forecasts_df['項目名'] == item, forecast_months] *= (1 - rate * 0.5)
                elif item in processor.ga_items:
                    forecasts_df.loc[forecasts_df['項目名'] == item, forecast_months] *= (1 - rate * 0.3)
                    
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
        pl_df = processor.calculate_pl(
            actuals_df,
            forecasts_df,
            processor.get_split_index(
                st.session_state.selected_comp_id,
                st.session_state.current_month,
                st.session_state.selected_period_id
            ),
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
                    <div class="card-value">¥{int(sales_total/1000000):,}M</div>
                    <div class="card-subtitle">期末着地予測</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                gp_total = pl_display[pl_display['項目名'] == '売上総損益金額']['合計'].iloc[0]
                gp_rate = (gp_total / sales_total * 100) if sales_total != 0 else 0
                st.markdown(f"""
                <div class="summary-card-green">
                    <div class="card-title">売上総利益</div>
                    <div class="card-value">¥{int(gp_total/1000000):,}M</div>
                    <div class="card-subtitle">粗利率: {gp_rate:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                op_total = pl_display[pl_display['項目名'] == '営業損益金額']['合計'].iloc[0]
                op_rate = (op_total / sales_total * 100) if sales_total != 0 else 0
                st.markdown(f"""
                <div class="summary-card-orange">
                    <div class="card-title">営業利益</div>
                    <div class="card-value">¥{int(op_total/1000000):,}M</div>
                    <div class="card-subtitle">営業利益率: {op_rate:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                ord_total = pl_display[pl_display['項目名'] == '経常損益金額']['合計'].iloc[0]
                ord_rate = (ord_total / sales_total * 100) if sales_total != 0 else 0
                st.markdown(f"""
                <div class="summary-card">
                    <div class="card-title">経常利益</div>
                    <div class="card-value">¥{int(ord_total/1000000):,}M</div>
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
                    <div class="card-value">¥{int(net_total/1000000):,}M</div>
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
                        return ['background-color: #e3f2fd; font-weight: bold'] * len(row)
                    return [''] * len(row)
                
                styled_df = pl_display.drop(columns=['タイプ']).style\
                    .format(format_currency, subset=[c for c in pl_display.columns if c not in ['項目名', 'タイプ']])\
                    .apply(highlight_summary, axis=1)
                
                st.dataframe(styled_df, use_container_width=True, height=600)
            
            with tab2:
                st.subheader("月次推移グラフ")
                
                # グラフ用データ準備
                graph_items = ["売上高", "売上総損益金額", "営業損益金額", "経常損益金額", "当期純損益金額"]
                graph_df = pl_df[pl_df['項目名'].isin(graph_items)]
                
                fig = go.Figure()
                
                split_idx = processor.get_split_index(
                    st.session_state.selected_comp_id,
                    st.session_state.current_month,
                    st.session_state.selected_period_id
                )
                
                for item in graph_items:
                    item_data = graph_df[graph_df['項目名'] == item]
                    
                    # 実績部分
                    actual_months = months[:split_idx]
                    actual_values = [item_data[m].iloc[0] if m in item_data.columns else 0 for m in actual_months]
                    
                    # 予測部分
                    forecast_months_list = months[split_idx:]
                    forecast_values = [item_data[m].iloc[0] if m in item_data.columns else 0 for m in forecast_months_list]
                    
                    # 実績グラフ
                    fig.add_trace(go.Scatter(
                        x=actual_months,
                        y=actual_values,
                        name=f"{item} (実績)",
                        mode='lines+markers',
                        line=dict(width=3),
                        marker=dict(size=8)
                    ))
                    
                    # 予測グラフ
                    if len(forecast_months_list) > 0:
                        fig.add_trace(go.Scatter(
                            x=forecast_months_list,
                            y=forecast_values,
                            name=f"{item} (予測)",
                            mode='lines+markers',
                            line=dict(width=3, dash='dash'),
                            marker=dict(size=8, symbol='diamond')
                        ))
                
                fig.update_layout(
                    title="主要指標の月次推移",
                    xaxis_title="月",
                    yaxis_title="金額 (円)",
                    hovermode='x unified',
                    height=500,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    )
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # 構成比グラフ
                st.subheader("販売管理費 構成比")
                
                ga_items_list = processor.ga_items
                ga_data = pl_df[pl_df['項目名'].isin(ga_items_list)]
                ga_total_values = ga_data['合計'].values
                ga_labels = ga_data['項目名'].values
                
                # 上位10項目のみ表示
                ga_df_for_pie = pd.DataFrame({'項目名': ga_labels, '金額': ga_total_values})
                ga_df_for_pie = ga_df_for_pie.sort_values('金額', ascending=False).head(10)
                
                fig_pie = go.Figure(data=[go.Pie(
                    labels=ga_df_for_pie['項目名'],
                    values=ga_df_for_pie['金額'],
                    hole=.4,
                    textposition='inside',
                    textinfo='label+percent'
                )])
                
                fig_pie.update_layout(
                    title="販売管理費 上位10項目",
                    height=500,
                    showlegend=True
                )
                
                st.plotly_chart(fig_pie, use_container_width=True)
        
        elif st.session_state.page == "比較分析レポート":
            st.title("📈 比較分析レポート")
            
            # シナリオ間比較
            st.subheader("1️⃣ シナリオ間比較 (着地予測)")
            
            split_idx = processor.get_split_index(
                st.session_state.selected_comp_id,
                st.session_state.current_month,
                st.session_state.selected_period_id
            )
            forecast_months = months[split_idx:]
            
            scenario_results = {}
            for scenario, rate in st.session_state.scenario_rates.items():
                temp_forecasts_df = forecasts_df.copy()
                
                for item in processor.all_items:
                    if item == "売上高":
                        temp_forecasts_df.loc[temp_forecasts_df['項目名'] == item, forecast_months] *= (1 + rate)
                    elif item == "売上原価":
                        temp_forecasts_df.loc[temp_forecasts_df['項目名'] == item, forecast_months] *= (1 - rate * 0.5)
                    elif item in processor.ga_items:
                        temp_forecasts_df.loc[temp_forecasts_df['項目名'] == item, forecast_months] *= (1 - rate * 0.3)
                        
                temp_pl_df = processor.calculate_pl(actuals_df, temp_forecasts_df, split_idx, months)
                scenario_results[scenario] = temp_pl_df[['項目名', '合計']].set_index('項目名')['合計']
                
            comparison_df = pd.DataFrame(scenario_results)
            
            # 差異計算
            comparison_df['楽観-現実'] = comparison_df['楽観'] - comparison_df['現実']
            comparison_df['悲観-現実'] = comparison_df['悲観'] - comparison_df['現実']
            
            # 要約行のみ表示
            summary_items = ["売上高", "売上総損益金額", "販売管理費計", "営業損益金額", "経常損益金額", "当期純損益金額"]
            comparison_summary = comparison_df.loc[summary_items]
            
            st.dataframe(
                comparison_summary.style.format(format_currency),
                use_container_width=True
            )
            
            # グラフ
            fig = go.Figure()
            
            for col in ['現実', '楽観', '悲観']:
                fig.add_trace(go.Bar(
                    name=col,
                    x=summary_items,
                    y=comparison_summary[col],
                    text=comparison_summary[col].apply(lambda x: f'¥{int(x/1000000)}M'),
                    textposition='auto'
                ))
            
            fig.update_layout(
                title="シナリオ別 主要指標比較",
                xaxis_title="項目",
                yaxis_title="金額 (円)",
                barmode='group',
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("---")
            
            # 実績 vs 予測比較
            st.subheader("2️⃣ 実績 vs 当初予測比較")
            
            initial_forecast_df = processor.load_forecast_data(st.session_state.selected_period_id, "現実")
            
            actual_months = months[:split_idx]
            actual_sum = actuals_df[actual_months].sum(axis=1)
            actual_sum.index = actuals_df['項目名']
            
            initial_forecast_sum = initial_forecast_df[actual_months].sum(axis=1)
            initial_forecast_sum.index = initial_forecast_df['項目名']
            
            comparison_actual_df = pd.DataFrame({
                '実績合計': actual_sum,
                '当初予測合計': initial_forecast_sum
            }).fillna(0)
            
            comparison_actual_df['差異'] = comparison_actual_df['実績合計'] - comparison_actual_df['当初予測合計']
            comparison_actual_df['差異率'] = comparison_actual_df['差異'] / comparison_actual_df['当初予測合計'].replace(0, np.nan)
            
            comparison_actual_df = comparison_actual_df.loc[summary_items]
            
            st.dataframe(
                comparison_actual_df.style.format({
                    '実績合計': format_currency,
                    '当初予測合計': format_currency,
                    '差異': format_currency,
                    '差異率': "{:.1%}"
                }),
                use_container_width=True
            )
            
            # グラフ
            fig2 = go.Figure()
            
            fig2.add_trace(go.Bar(
                name='実績',
                x=comparison_actual_df.index,
                y=comparison_actual_df['実績合計'],
                marker_color='#1f77b4'
            ))
            
            fig2.add_trace(go.Bar(
                name='当初予測',
                x=comparison_actual_df.index,
                y=comparison_actual_df['当初予測合計'],
                marker_color='#ff7f0e'
            ))
            
            fig2.update_layout(
                title="実績 vs 当初予測比較",
                xaxis_title="項目",
                yaxis_title="金額 (円)",
                barmode='group',
                height=500
            )
            
            st.plotly_chart(fig2, use_container_width=True)
        
        elif st.session_state.page == "全体予測PL & 補助科目入力":
            st.title("📝 全体予測PL & 補助科目入力")
            
            tab1, tab2 = st.tabs(["📊 全体予測PL入力", "📋 補助科目入力"])
            
            with tab1:
                st.subheader("全体予測値入力")
                
                st.markdown("""
                <div class="info-box">
                    <strong>💡 使い方:</strong> 各項目の予測値を入力してください。
                    自動計算項目（売上総損益金額、販売管理費計など）は編集できません。
                </div>
                """, unsafe_allow_html=True)
                
                # 編集可能な項目リスト
                editable_items = [item for item in processor.all_items if item not in processor.calculated_items]
                
                selected_item = st.selectbox("編集する項目を選択", editable_items)
                
                # 月ごとの入力
                st.markdown(f"### {selected_item} の予測値入力")
                
                col_count = 4
                cols = st.columns(col_count)
                
                new_values = {}
                current_values = forecasts_df[forecasts_df['項目名'] == selected_item]
                
                for i, month in enumerate(months):
                    col_idx = i % col_count
                    with cols[col_idx]:
                        current_val = 0
                        if not current_values.empty and month in current_values.columns:
                            current_val = current_values[month].iloc[0]
                        
                        new_val = st.number_input(
                            f"{month}",
                            value=float(current_val),
                            step=10000.0,
                            format="%.0f",
                            key=f"forecast_{selected_item}_{month}"
                        )
                        new_values[month] = new_val
                
                if st.button("💾 保存", key="save_forecast", type="primary"):
                    success = processor.save_forecast_item(
                        st.session_state.selected_period_id,
                        st.session_state.scenario,
                        selected_item,
                        new_values
                    )
                    if success:
                        st.success("✅ 保存しました")
                        st.rerun()
                    else:
                        st.error("❌ 保存に失敗しました")
            
            with tab2:
                st.subheader("補助科目入力")
                
                st.markdown("""
                <div class="info-box">
                    <strong>💡 使い方:</strong> 販売管理費の各項目について、詳細な内訳(補助科目)を入力できます。
                </div>
                """, unsafe_allow_html=True)
                
                parent_item = st.selectbox("親項目を選択", processor.ga_items)
                
                # 既存の補助科目を取得
                existing_subs = processor.get_sub_accounts_for_parent(
                    st.session_state.selected_period_id,
                    st.session_state.scenario,
                    parent_item
                )
                
                # 補助科目追加
                st.markdown("#### 新規補助科目追加")
                new_sub_name = st.text_input("補助科目名", key="new_sub_name")
                
                if new_sub_name:
                    st.markdown(f"**{new_sub_name}** の月次入力")
                    
                    cols = st.columns(4)
                    sub_values = {}
                    
                    for i, month in enumerate(months):
                        with cols[i % 4]:
                            val = st.number_input(
                                f"{month}",
                                value=0.0,
                                step=1000.0,
                                format="%.0f",
                                key=f"sub_{parent_item}_{new_sub_name}_{month}"
                            )
                            sub_values[month] = val
                    
                    if st.button("💾 補助科目を追加", type="primary"):
                        success = processor.save_sub_account(
                            st.session_state.selected_period_id,
                            st.session_state.scenario,
                            parent_item,
                            new_sub_name,
                            sub_values
                        )
                        if success:
                            st.success("✅ 追加しました")
                            st.rerun()
                        else:
                            st.error("❌ 追加に失敗しました")
                
                # 既存補助科目の表示・編集
                if not existing_subs.empty:
                    st.markdown("#### 既存補助科目")
                    
                    for sub_name in existing_subs['sub_account_name'].unique():
                        with st.expander(f"📌 {sub_name}"):
                            sub_data = existing_subs[existing_subs['sub_account_name'] == sub_name]
                            
                            # 月次データ表示
                            display_data = {}
                            for month in months:
                                matching = sub_data[sub_data['month'] == month]
                                if not matching.empty:
                                    display_data[month] = matching['amount'].iloc[0]
                                else:
                                    display_data[month] = 0
                            
                            df_display = pd.DataFrame([display_data])
                            st.dataframe(
                                df_display.style.format(format_currency),
                                use_container_width=True
                            )
                            
                            if st.button(f"🗑️ {sub_name}を削除", key=f"del_{sub_name}"):
                                processor.delete_sub_account(
                                    st.session_state.selected_period_id,
                                    st.session_state.scenario,
                                    parent_item,
                                    sub_name
                                )
                                st.success("削除しました")
                                st.rerun()
        
        elif st.session_state.page == "実績データ入力":
            st.title("⌨️ 実績データ入力")
            
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
                    current_val = 0
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
                success = processor.save_actual_item(
                    st.session_state.selected_period_id,
                    selected_item,
                    new_values
                )
                if success:
                    st.success("✅ 保存しました")
                    st.rerun()
                else:
                    st.error("❌ 保存に失敗しました")
        
        elif st.session_state.page == "データインポート":
            st.title("📥 データインポート")
            
            st.markdown("""
            <div class="info-box">
                <strong>💡 使い方:</strong> 弥生会計からエクスポートしたExcelファイルをアップロードしてください。
            </div>
            """, unsafe_allow_html=True)
            
            uploaded_file = st.file_uploader(
                "Excelファイルを選択",
                type=['xlsx', 'xls'],
                help="弥生会計の月次推移表をアップロードしてください"
            )
            
            if 'show_import_button' not in st.session_state:
                st.session_state.show_import_button = False
            
            if uploaded_file:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                    tmp_file.write(uploaded_file.read())
                    temp_path = tmp_file.name
                    st.session_state.temp_path_to_delete = temp_path
                    
                st.success(f"✅ ファイル **{uploaded_file.name}** を読み込みました")
                
                if 'imported_df' not in st.session_state:
                    st.session_state.imported_df, info = processor.import_yayoi_excel(temp_path, preview_only=True)
                    st.session_state.show_import_button = True
                    
                if st.session_state.show_import_button:
                    st.subheader("📋 インポートデータ プレビュー")
                    
                    imported_df = st.session_state.imported_df
                    st.dataframe(
                        imported_df.style.format(format_currency),
                        use_container_width=True,
                        height=400
                    )
                    
                    st.markdown("""
                    <div class="warning-box">
                        <strong>⚠️ 注意:</strong> 上記の内容でインポートを実行すると、現在の実績データは上書きされます。
                    </div>
                    """, unsafe_allow_html=True)
                    
                    if st.button("✅ 上記内容でインポートを実行", type="primary"):
                        success, info = processor.save_extracted_data(
                            st.session_state.selected_period_id,
                            st.session_state.imported_df
                        )
                        if success:
                            st.success("✅ インポートが完了しました！")
                            del st.session_state.imported_df
                            del st.session_state.show_import_button
                            
                            if 'temp_path_to_delete' in st.session_state:
                                os.unlink(st.session_state.temp_path_to_delete)
                                del st.session_state.temp_path_to_delete
                                
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
        
        elif st.session_state.page == "システム設定":
            st.title("⚙️ システム設定")
            
            tab1, tab2 = st.tabs(["🏢 会社設定", "📅 会計期間設定"])
            
            with tab1:
                st.subheader("会社登録")
                
                with st.form("company_form"):
                    company_name = st.text_input("会社名", placeholder="例: 株式会社サンプル")
                    
                    if st.form_submit_button("➕ 会社を追加", type="primary"):
                        if company_name:
                            success = processor.add_company(company_name)
                            if success:
                                st.success(f"✅ 会社 **{company_name}** を追加しました")
                                st.rerun()
                            else:
                                st.error("❌ 会社の追加に失敗しました")
                        else:
                            st.error("❌ 会社名を入力してください")
                
                st.markdown("---")
                
                # 登録済み会社一覧
                st.subheader("📋 登録済み会社一覧")
                
                companies_list = processor.get_companies()
                if not companies_list.empty:
                    st.dataframe(companies_list, use_container_width=True)
                else:
                    st.info("登録されている会社がありません")
            
            with tab2:
                st.subheader("会計期間登録")
                
                if 'selected_comp_id' not in st.session_state or not st.session_state.selected_comp_id:
                    st.warning("⚠️ まず会社を選択してください")
                else:
                    with st.form("period_form"):
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            period_num = st.number_input("期数", min_value=1, step=1, value=1)
                        with col2:
                            start_date = st.date_input("期首日")
                        with col3:
                            end_date = st.date_input("期末日")
                        
                        if st.form_submit_button("➕ 会計期間を追加", type="primary"):
                            if period_num and start_date and end_date:
                                if start_date < end_date:
                                    success = processor.add_fiscal_period(
                                        st.session_state.selected_comp_id,
                                        period_num,
                                        start_date.strftime('%Y-%m-%d'),
                                        end_date.strftime('%Y-%m-%d')
                                    )
                                    if success:
                                        st.success(f"✅ 第{period_num}期を追加しました")
                                        st.rerun()
                                    else:
                                        st.error("❌ 会計期間の追加に失敗しました")
                                else:
                                    st.error("❌ 期末日は期首日より後に設定してください")
                            else:
                                st.error("❌ すべてのフィールドを入力してください")
                    
                    st.markdown("---")
                    
                    # 登録済み期間一覧
                    st.subheader("📋 登録済み会計期間")
                    
                    periods_list = processor.get_company_periods(st.session_state.selected_comp_id)
                    if not periods_list.empty:
                        st.dataframe(periods_list, use_container_width=True)
                    else:
                        st.info("登録されている会計期間がありません")
    
    else:
        st.warning("⚠️ 会計期間が選択されていません。システム設定から登録してください。")

elif authentication_status == False:
    st.error('❌ ユーザー名/パスワードが間違っています')
elif authentication_status == None:
    st.markdown("""
    <div style='text-align: center; padding: 2rem;'>
        <h1 style='color: #1f77b4; font-size: 3rem; margin-bottom: 1rem;'>📊</h1>
        <h1 style='color: #2c3e50;'>財務予測シミュレーター</h1>
        <p style='color: #7f8c8d; font-size: 1.1rem;'>ログインして開始してください</p>
    </div>
    """, unsafe_allow_html=True)
