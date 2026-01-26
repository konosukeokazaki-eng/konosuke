import sqlite3
import pandas as pd
import numpy as np
import re
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataProcessor:
    """財務データの処理を担当するクラス"""
    
    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.db_path = os.path.join(base_dir, "financial_data.db")
        else:
            self.db_path = db_path
        
        self._init_db()
        
        # 標準的な勘定科目リスト
        self.all_items = [
            "売上高", "売上原価", "売上総損益金額",
            "役員報酬", "給料手当", "賞与", "法定福利費", "福利厚生費", 
            "採用教育費", "外注費", "荷造運賃", "広告宣伝費", "交際費", 
            "会議費", "旅費交通費", "通信費", "販売手数料", "販売促進費", 
            "消耗品費", "修繕費", "事務用品費", "水道光熱費", "新聞図書費", 
            "諸会費", "支払手数料", "車両費", "地代家賃", "賃借料", "保険料", 
            "租税公課", "支払報酬料", "研究開発費", "研修費", "減価償却費", 
            "貸倒損失(販)", "雑費", "少額交際費", "販売管理費計",
            "営業損益金額", "営業外収益合計", "営業外費用合計", "経常損益金額", 
            "特別利益合計", "特別損失合計", "税引前当期純損益金額", 
            "法人税、住民税及び事業税", "当期純損益金額"
        ]
        
        # 収益項目
        self.revenue_items = ["売上高"]
        
        # 原価項目
        self.cogs_items = ["売上原価"]
        
        # 販管費項目
        self.expense_items = [
            "役員報酬", "給料手当", "賞与", "法定福利費", "福利厚生費",
            "採用教育費", "外注費", "荷造運賃", "広告宣伝費", "交際費",
            "会議費", "旅費交通費", "通信費", "販売手数料", "販売促進費",
            "消耗品費", "修繕費", "事務用品費", "水道光熱費", "新聞図書費",
            "諸会費", "支払手数料", "車両費", "地代家賃", "賃借料", "保険料",
            "租税公課", "支払報酬料", "研究開発費", "研修費", "減価償却費",
            "貸倒損失(販)", "雑費", "少額交際費"
        ]
        
        # 計算項目（自動計算される項目）
        self.calculated_items = [
            "売上総損益金額", "販売管理費計", "営業損益金額", 
            "経常損益金額", "税引前当期純損益金額", "当期純損益金額"
        ]
        
        # 弥生会計の項目名マッピング
        self.item_mapping = {
            "売上高": ["売上高", "売上金額", "売上高合計"],
            "売上原価": ["売上原価", "仕入高", "売上原価合計"],
            "役員報酬": ["役員報酬"],
            "給料手当": ["給料手当", "給与手当", "給料"],
            "賞与": ["賞与"],
            "法定福利費": ["法定福利費"],
            "福利厚生費": ["福利厚生費"],
            "広告宣伝費": ["広告宣伝費"],
            "交際費": ["交際費", "接待交際費"],
            "旅費交通費": ["旅費交通費"],
            "通信費": ["通信費"],
            "地代家賃": ["地代家賃", "家賃"],
            "支払手数料": ["支払手数料"],
            "減価償却費": ["減価償却費"],
            "雑費": ["雑費"],
            "営業外収益合計": ["営業外収益", "営業外収益合計"],
            "営業外費用合計": ["営業外費用", "営業外費用合計"],
            "特別利益合計": ["特別利益", "特別利益合計"],
            "特別損失合計": ["特別損失", "特別損失合計"],
            "法人税、住民税及び事業税": ["法人税", "法人税等", "法人税、住民税及び事業税"]
        }

    def _init_db(self) -> None:
        """データベースの初期化"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 会社情報
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            # 会計期情報
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS fiscal_periods (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                comp_id INTEGER NOT NULL,
                period_num INTEGER NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (comp_id) REFERENCES companies (id),
                UNIQUE(comp_id, period_num)
            )
            ''')
            
            # 実績データ
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS actual_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fiscal_period_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                month TEXT NOT NULL,
                amount REAL NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods (id),
                UNIQUE(fiscal_period_id, item_name, month)
            )
            ''')
            
            # 予測データ
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS forecast_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fiscal_period_id INTEGER NOT NULL,
                scenario TEXT NOT NULL,
                item_name TEXT NOT NULL,
                month TEXT NOT NULL,
                amount REAL NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods (id),
                UNIQUE(fiscal_period_id, scenario, item_name, month)
            )
            ''')
            
            # 補助科目（内訳）
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS sub_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fiscal_period_id INTEGER NOT NULL,
                scenario TEXT NOT NULL,
                parent_item TEXT NOT NULL,
                sub_account_name TEXT NOT NULL,
                month TEXT NOT NULL,
                amount REAL NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
                UNIQUE(fiscal_period_id, scenario, parent_item, sub_account_name, month)
            )
            ''')
            
            # 勘定科目属性テーブル（固定費/変動費の属性を保存）
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS item_attributes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fiscal_period_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                is_variable INTEGER DEFAULT 0,
                variable_rate REAL DEFAULT 0.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
                UNIQUE(fiscal_period_id, item_name)
            )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise

    def get_companies(self) -> pd.DataFrame:
        """全会社情報を取得"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query("SELECT * FROM companies ORDER BY name", conn)
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Failed to get companies: {e}")
            return pd.DataFrame()

    def get_company_periods(self, comp_id: int) -> pd.DataFrame:
        """指定会社の会計期情報を取得"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(
                "SELECT * FROM fiscal_periods WHERE comp_id = ? ORDER BY period_num DESC",
                conn,
                params=(comp_id,)
            )
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Failed to get company periods: {e}")
            return pd.DataFrame()

    def get_period_info(self, period_id: int) -> Optional[Dict]:
        """会計期情報を取得"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM fiscal_periods WHERE id = ?", (period_id,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    "id": row[0],
                    "comp_id": row[1],
                    "period_num": row[2],
                    "start_date": row[3],
                    "end_date": row[4]
                }
            return None
        except Exception as e:
            logger.error(f"Failed to get period info: {e}")
            return None

    def get_company_id_from_period_id(self, fiscal_period_id: int) -> Optional[int]:
        """会計期IDから会社IDを取得"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT comp_id FROM fiscal_periods WHERE id = ?",
                (fiscal_period_id,)
            )
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to get company ID: {e}")
            return None

    def get_fiscal_months(self, comp_id: int, fiscal_period_id: int) -> List[str]:
        """会計期の月リストを取得"""
        period = self.get_period_info(fiscal_period_id)
        if not period:
            return []
        
        try:
            start = datetime.strptime(period['start_date'], '%Y-%m-%d')
            end = datetime.strptime(period['end_date'], '%Y-%m-%d')
            
            months = []
            curr = start
            while curr <= end:
                months.append(curr.strftime('%Y-%m'))
                if curr.month == 12:
                    curr = curr.replace(year=curr.year + 1, month=1)
                else:
                    curr = curr.replace(month=curr.month + 1)
            return months
        except Exception as e:
            logger.error(f"Failed to get fiscal months: {e}")
            return []

    def get_split_index(self, comp_id: int, current_month: str, fiscal_period_id: int) -> int:
        """実績と予測の境界インデックスを取得"""
        months = self.get_fiscal_months(comp_id, fiscal_period_id)
        try:
            return months.index(current_month) + 1
        except ValueError:
            logger.warning(f"Current month {current_month} not found in fiscal months")
            return 0

    def load_actual_data(self, fiscal_period_id: int) -> pd.DataFrame:
        """実績データを読み込み"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(
                "SELECT item_name as 項目名, month, amount FROM actual_data WHERE fiscal_period_id = ?",
                conn,
                params=(fiscal_period_id,)
            )
            conn.close()
            
            if df.empty:
                return pd.DataFrame({'項目名': self.all_items}).fillna(0)
            
            # 重複行を削除
            df = df.drop_duplicates(subset=['項目名', 'month'], keep='last')
            
            pivot_df = df.pivot(index='項目名', columns='month', values='amount').reset_index()
            
            # 全項目を網羅
            all_items_df = pd.DataFrame({'項目名': self.all_items})
            pivot_df = pd.merge(all_items_df, pivot_df, on='項目名', how='left').fillna(0)
            
            return pivot_df
        except Exception as e:
            logger.error(f"Failed to load actual data: {e}")
            return pd.DataFrame({'項目名': self.all_items}).fillna(0)

    def load_forecast_data(self, fiscal_period_id: int, scenario: str) -> pd.DataFrame:
        """予測データを読み込み"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(
                "SELECT item_name as 項目名, month, amount FROM forecast_data WHERE fiscal_period_id = ? AND scenario = ?",
                conn,
                params=(fiscal_period_id, scenario)
            )
            conn.close()
            
            if df.empty:
                return pd.DataFrame({'項目名': self.all_items}).fillna(0)
            
            # 重複行を削除
            df = df.drop_duplicates(subset=['項目名', 'month'], keep='last')
            
            pivot_df = df.pivot(index='項目名', columns='month', values='amount').reset_index()
            
            # 全項目を網羅
            all_items_df = pd.DataFrame({'項目名': self.all_items})
            pivot_df = pd.merge(all_items_df, pivot_df, on='項目名', how='left').fillna(0)
            
            return pivot_df
        except Exception as e:
            logger.error(f"Failed to load forecast data: {e}")
            return pd.DataFrame({'項目名': self.all_items}).fillna(0)

    def get_item_attributes(self, fiscal_period_id: int, item_name: str) -> Dict:
        """勘定科目の属性（固定費/変動費、変動費率）を取得"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT is_variable, variable_rate FROM item_attributes WHERE fiscal_period_id = ? AND item_name = ?",
                (fiscal_period_id, item_name)
            )
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {"is_variable": bool(result[0]), "variable_rate": result[1]}
            return {"is_variable": False, "variable_rate": 0.0}
        except Exception as e:
            logger.error(f"Failed to get item attributes: {e}")
            return {"is_variable": False, "variable_rate": 0.0}

    def save_item_attribute(self, fiscal_period_id: int, item_name: str, 
                           is_variable: bool, variable_rate: float) -> None:
        """勘定科目の属性を保存"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO item_attributes (fiscal_period_id, item_name, is_variable, variable_rate, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(fiscal_period_id, item_name) 
                DO UPDATE SET 
                    is_variable = excluded.is_variable,
                    variable_rate = excluded.variable_rate,
                    updated_at = CURRENT_TIMESTAMP
            ''', (fiscal_period_id, item_name, int(is_variable), variable_rate))
            conn.commit()
            conn.close()
            logger.info(f"Saved attributes for item: {item_name}")
        except Exception as e:
            logger.error(f"Failed to save item attributes: {e}")
            raise

    def save_forecast_data(self, fiscal_period_id: int, scenario: str, df: pd.DataFrame) -> None:
        """予測データを保存"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 既存データを削除
            cursor.execute(
                "DELETE FROM forecast_data WHERE fiscal_period_id = ? AND scenario = ?",
                (fiscal_period_id, scenario)
            )
            
            months = [c for c in df.columns if c != '項目名']
            
            for _, row in df.iterrows():
                for m in months:
                    val = row[m]
                    if pd.isna(val) or val is None:
                        val = 0.0
                    cursor.execute('''
                        INSERT INTO forecast_data 
                        (fiscal_period_id, scenario, item_name, month, amount, updated_at)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (fiscal_period_id, scenario, row['項目名'], m, float(val)))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved forecast data for scenario: {scenario}")
        except Exception as e:
            logger.error(f"Failed to save forecast data: {e}")
            raise

    def load_sub_accounts(self, fiscal_period_id: int, scenario: str, parent_item: str) -> pd.DataFrame:
        """補助科目データを読み込み"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(
                "SELECT sub_account_name, month, amount FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ? AND parent_item = ?",
                conn,
                params=(fiscal_period_id, scenario, parent_item)
            )
            conn.close()
            
            if df.empty:
                return pd.DataFrame(columns=['補助科目名'])
            
            pivot_df = df.pivot(index='sub_account_name', columns='month', values='amount').reset_index()
            pivot_df = pivot_df.rename(columns={'sub_account_name': '補助科目名'})
            
            return pivot_df
        except Exception as e:
            logger.error(f"Failed to load sub accounts: {e}")
            return pd.DataFrame(columns=['補助科目名'])

    def save_sub_accounts(self, fiscal_period_id: int, scenario: str, 
                         parent_item: str, df: pd.DataFrame) -> None:
        """補助科目データを保存"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 既存データを削除
            cursor.execute(
                "DELETE FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ? AND parent_item = ?",
                (fiscal_period_id, scenario, parent_item)
            )
            
            months = [c for c in df.columns if c != '補助科目名']
            
            for _, row in df.iterrows():
                if not row['補助科目名'] or pd.isna(row['補助科目名']):
                    continue
                    
                for m in months:
                    val = row[m]
                    if pd.isna(val) or val is None:
                        val = 0.0
                    cursor.execute('''
                        INSERT INTO sub_accounts 
                        (fiscal_period_id, scenario, parent_item, sub_account_name, month, amount, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (fiscal_period_id, scenario, parent_item, row['補助科目名'], m, float(val)))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved sub accounts for: {parent_item}")
        except Exception as e:
            logger.error(f"Failed to save sub accounts: {e}")
            raise

    def calculate_sub_account_totals(self, fiscal_period_id: int, scenario: str, 
                                    parent_item: str) -> pd.Series:
        """補助科目の合計を計算"""
        df = self.load_sub_accounts(fiscal_period_id, scenario, parent_item)
        
        if df.empty:
            return pd.Series(dtype=float)
        
        months = [c for c in df.columns if c != '補助科目名']
        return df[months].sum(axis=0)

    def apply_scenario_adjustment(self, base_df: pd.DataFrame, rate: float, 
                                  forecast_months: List[str]) -> pd.DataFrame:
        """
        シナリオ調整を適用
        
        Args:
            base_df: ベースとなる予測データフレーム
            rate: 調整率（例: 0.1 = +10%, -0.1 = -10%）
            forecast_months: 調整対象の月リスト
            
        Returns:
            調整後のデータフレーム
        """
        adjusted_df = base_df.copy()
        
        # 収益項目: rate倍する（楽観なら増加、悲観なら減少）
        for item in self.revenue_items:
            mask = adjusted_df['項目名'] == item
            for month in forecast_months:
                if month in adjusted_df.columns:
                    adjusted_df.loc[mask, month] *= (1 + rate)
        
        # 原価項目: 売上に連動させる（楽観なら減少、悲観なら増加）
        for item in self.cogs_items:
            mask = adjusted_df['項目名'] == item
            for month in forecast_months:
                if month in adjusted_df.columns:
                    adjusted_df.loc[mask, month] *= (1 - rate * 0.5)  # 原価は売上の半分の影響
        
        # 販管費項目: 逆相関（楽観なら減少、悲観なら増加）
        for item in self.expense_items:
            mask = adjusted_df['項目名'] == item
            for month in forecast_months:
                if month in adjusted_df.columns:
                    adjusted_df.loc[mask, month] *= (1 - rate * 0.3)  # 費用は売上の30%の影響
        
        return adjusted_df

    def apply_scenario_adjustment(self, base_df: pd.DataFrame, rate: float, 
                                  forecast_months: List[str]) -> pd.DataFrame:
        """
        シナリオ調整を適用
        
        Args:
            base_df: ベースとなる予測データフレーム
            rate: 調整率（例: 0.1 = +10%, -0.1 = -10%）
            forecast_months: 調整対象の月リスト
            
        Returns:
            調整後のデータフレーム
        """
        adjusted_df = base_df.copy()
        
        # 収益項目: rate倍する（楽観なら増加、悲観なら減少）
        for item in self.revenue_items:
            mask = adjusted_df['項目名'] == item
            for month in forecast_months:
                if month in adjusted_df.columns:
                    adjusted_df.loc[mask, month] *= (1 + rate)
        
        # 原価項目: 売上に連動させる（楽観なら減少、悲観なら増加）
        for item in self.cogs_items:
            mask = adjusted_df['項目名'] == item
            for month in forecast_months:
                if month in adjusted_df.columns:
                    adjusted_df.loc[mask, month] *= (1 - rate * 0.5)  # 原価は売上の半分の影響
        
        # 販管費項目: 逆相関（楽観なら減少、悲観なら増加）
        for item in self.expense_items:
            mask = adjusted_df['項目名'] == item
            for month in forecast_months:
                if month in adjusted_df.columns:
                    adjusted_df.loc[mask, month] *= (1 - rate * 0.3)  # 費用は売上の30%の影響
        
        return adjusted_df

    def calculate_growth_forecast(self, fiscal_period_id: int, current_month: str,
                                  target_item: str, past_months: int = 3) -> Dict[str, float]:
        """
        過去の成長率を基に予測を自動計算
        
        Args:
            fiscal_period_id: 会計期ID
            current_month: 実績データ最終月
            target_item: 予測対象の項目名
            past_months: 成長率計算に使用する過去月数
            
        Returns:
            {月: 予測値}の辞書
        """
        try:
            comp_id = self.get_company_id_from_period_id(fiscal_period_id)
            if not comp_id:
                return {}
            
            months = self.get_fiscal_months(comp_id, fiscal_period_id)
            split_index = self.get_split_index(comp_id, current_month, fiscal_period_id)
            
            # 実績データを読み込み
            actuals_df = self.load_actual_data(fiscal_period_id)
            
            # 予測対象項目の実績値を取得
            item_row = actuals_df[actuals_df['項目名'] == target_item]
            if item_row.empty:
                return {}
            
            # 成長率計算に使用する過去月を特定
            actual_months = months[:split_index]
            if len(actual_months) < past_months:
                past_months = len(actual_months)
            
            if past_months < 2:
                logger.warning("Not enough historical data for growth forecast")
                return {}
            
            past_months_for_calc = actual_months[-past_months:]
            past_values = item_row[past_months_for_calc].iloc[0].values
            
            # 成長率を計算
            growth_rates = []
            for i in range(1, len(past_values)):
                prev_val = past_values[i-1]
                curr_val = past_values[i]
                if prev_val != 0:
                    growth_rates.append((curr_val - prev_val) / prev_val)
                else:
                    growth_rates.append(0)
            
            if not growth_rates:
                return {}
            
            avg_growth_rate = sum(growth_rates) / len(growth_rates)
            
            # 予測値を計算
            forecast_months = months[split_index:]
            forecast_values = {}
            
            last_actual_value = item_row[actual_months[-1]].iloc[0] if actual_months else 0
            current_forecast_value = last_actual_value
            
            for m in forecast_months:
                current_forecast_value *= (1 + avg_growth_rate)
                forecast_values[m] = max(0, current_forecast_value)  # 負の値を防ぐ
            
            logger.info(f"Generated growth forecast for {target_item}")
            return forecast_values
            
        except Exception as e:
            logger.error(f"Failed to calculate growth forecast: {e}")
            return {}

    def calculate_pl(self, actuals_df: pd.DataFrame, forecasts_df: pd.DataFrame,
                    split_index: int, months: List[str]) -> pd.DataFrame:
        """
        P/L（損益計算書）を計算
        
        Args:
            actuals_df: 実績データフレーム
            forecasts_df: 予測データフレーム
            split_index: 実績と予測の境界インデックス
            months: 月リスト
            
        Returns:
            計算済みP/Lデータフレーム
        """
        df = pd.DataFrame({'項目名': self.all_items})
        
        # 実績と予測を結合
        for i, m in enumerate(months):
            if i < split_index:
                # 実績
                if m in actuals_df.columns:
                    df[m] = df['項目名'].map(actuals_df.set_index('項目名')[m])
                else:
                    df[m] = 0.0
            else:
                # 予測
                if m in forecasts_df.columns:
                    df[m] = df['項目名'].map(forecasts_df.set_index('項目名')[m])
                else:
                    df[m] = 0.0
        
        df = df.fillna(0)
        
        # 計算項目の算出
        def get_val(item_name: str) -> pd.Series:
            """項目値を取得"""
            row = df[df['項目名'] == item_name]
            if not row.empty:
                return row[months].iloc[0]
            return pd.Series(0.0, index=months)
        
        # 売上総損益金額 = 売上高 - 売上原価
        sales = get_val("売上高")
        cogs = get_val("売上原価")
        gross_profit = sales - cogs
        df.loc[df['項目名'] == "売上総損益金額", months] = gross_profit.values
        
        # 販売管理費計
        ga_total = pd.Series(0.0, index=months)
        for item in self.expense_items:
            ga_total += get_val(item)
        df.loc[df['項目名'] == "販売管理費計", months] = ga_total.values
        
        # 営業損益金額 = 売上総損益金額 - 販売管理費計
        operating_profit = gross_profit - ga_total
        df.loc[df['項目名'] == "営業損益金額", months] = operating_profit.values
        
        # 経常損益金額 = 営業損益金額 + 営業外収益 - 営業外費用
        non_op_inc = get_val("営業外収益合計")
        non_op_exp = get_val("営業外費用合計")
        ordinary_profit = operating_profit + non_op_inc - non_op_exp
        df.loc[df['項目名'] == "経常損益金額", months] = ordinary_profit.values
        
        # 税引前当期純損益金額 = 経常損益金額 + 特別利益 - 特別損失
        sp_inc = get_val("特別利益合計")
        sp_exp = get_val("特別損失合計")
        pre_tax_profit = ordinary_profit + sp_inc - sp_exp
        df.loc[df['項目名'] == "税引前当期純損益金額", months] = pre_tax_profit.values
        
        # 当期純損益金額 = 税引前当期純損益金額 - 法人税等
        tax = get_val("法人税、住民税及び事業税")
        net_profit = pre_tax_profit - tax
        df.loc[df['項目名'] == "当期純損益金額", months] = net_profit.values
        
        # 合計列の追加
        df['実績合計'] = df[months[:split_index]].sum(axis=1)
        df['予測合計'] = df[months[split_index:]].sum(axis=1)
        df['合計'] = df['実績合計'] + df['予測合計']
        
        # タイプ（要約/詳細）の付与
        summary_items = [
            "売上高", "売上総損益金額", "販売管理費計", 
            "営業損益金額", "経常損益金額", "当期純損益金額"
        ]
        df['タイプ'] = df['項目名'].apply(lambda x: '要約' if x in summary_items else '詳細')
        
        return df

    def extract_yayoi_excel_data(self, file_path: str, comp_id: int, 
                                 period_num: int) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """
        弥生会計Excelからデータを抽出
        
        Args:
            file_path: Excelファイルパス
            comp_id: 会社ID
            period_num: 期数
            
        Returns:
            (成功フラグ, メッセージ, データフレーム)
        """
        try:
            # 全シートを読み込む
            xls = pd.ExcelFile(file_path)
            
            # 期間情報の取得
            period_df = self.get_company_periods(comp_id)
            period_row = period_df[period_df['period_num'] == period_num]
            
            if period_row.empty:
                return False, "指定された期が見つかりません。", None
            
            period_id = int(period_row.iloc[0]['id'])
            months = self.get_fiscal_months(comp_id, period_id)
            
            # 実績データを格納する辞書
            imported_data = {item: {m: 0.0 for m in months} for item in self.all_items}
            
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                
                # 月の列を特定
                month_cols = {}
                
                for r in range(min(20, len(df))):
                    for c in range(len(df.columns)):
                        val = str(df.iloc[r, c])
                        for m in months:
                            m_num = int(m.split('-')[1])
                            patterns = [
                                f"{m_num}月",
                                f"^{m_num}$",
                                f"{m.replace('-', '/')}",
                                f"{m_num}/"
                            ]
                            if any(re.search(p, val) for p in patterns):
                                month_cols[m] = c
                
                if not month_cols:
                    continue
                
                # 項目の行を特定して数値を抽出
                for r in range(len(df)):
                    # A列からC列までを項目名としてチェック
                    item_val = ""
                    for c in range(min(3, len(df.columns))):
                        v = str(df.iloc[r, c]).strip()
                        if v and v != "nan":
                            item_val = v
                            break
                    
                    if not item_val:
                        continue
                    
                    target_item = None
                    
                    # マッピングから検索
                    for std_name, aliases in self.item_mapping.items():
                        if any(alias in item_val for alias in aliases):
                            target_item = std_name
                            break
                    
                    if not target_item and item_val in self.all_items:
                        target_item = item_val
                    
                    if target_item:
                        for m, col_idx in month_cols.items():
                            raw_val = df.iloc[r, col_idx]
                            try:
                                if isinstance(raw_val, str):
                                    clean_val = raw_val.replace(',', '').replace('¥', '').replace('円', '').strip()
                                    if clean_val.startswith('△') or clean_val.startswith('▲'):
                                        val = -float(clean_val[1:])
                                    elif clean_val.startswith('(') and clean_val.endswith(')'):
                                        val = -float(clean_val[1:-1])
                                    else:
                                        val = float(clean_val)
                                else:
                                    val = float(raw_val)
                                
                                if not np.isnan(val):
                                    imported_data[target_item][m] = val
                            except (ValueError, TypeError):
                                pass
            
            # 辞書をDataFrameに変換
            imported_df = pd.DataFrame.from_dict(imported_data, orient='index').reset_index()
            imported_df = imported_df.rename(columns={'index': '項目名'})
            
            # 項目名でソート
            imported_df['項目名'] = pd.Categorical(
                imported_df['項目名'],
                categories=self.all_items,
                ordered=True
            )
            imported_df = imported_df.sort_values('項目名').reset_index(drop=True)
            
            logger.info("Excel data extraction successful")
            return True, "データ抽出に成功しました。", imported_df

        except Exception as e:
            logger.error(f"Excel extraction failed: {e}")
            return False, str(e), None

    def save_extracted_data(self, fiscal_period_id: int, imported_df: pd.DataFrame) -> Tuple[bool, str]:
        """
        抽出されたデータをデータベースに保存
        
        Args:
            fiscal_period_id: 会計期ID
            imported_df: インポートデータフレーム
            
        Returns:
            (成功フラグ, メッセージ)
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 既存のデータを削除
            cursor.execute(
                "DELETE FROM actual_data WHERE fiscal_period_id = ?",
                (fiscal_period_id,)
            )
            
            months = [c for c in imported_df.columns if c != '項目名']
            
            for _, row in imported_df.iterrows():
                for m in months:
                    val = row[m]
                    if val != 0 and not pd.isna(val):
                        cursor.execute('''
                            INSERT INTO actual_data 
                            (fiscal_period_id, item_name, month, amount, updated_at)
                            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ''', (fiscal_period_id, row['項目名'], m, float(val)))
            
            conn.commit()
            conn.close()
            
            logger.info("Data import successful")
            return True, "インポートが完了しました!"
            
        except Exception as e:
            logger.error(f"Data import failed: {e}")
            return False, str(e)
