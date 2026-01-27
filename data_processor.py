import sqlite3
import pandas as pd
import numpy as np
import re
import os
from datetime import datetime, timedelta

class DataProcessor:
    def __init__(self, db_path=None):
        if db_path is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.db_path = os.path.join(base_dir, "financial_data.db")
        else:
            self.db_path = db_path
        self._init_db()
        
        # 標準的な勘定科目リスト (要件定義書の3.1に準拠)
        self.all_items = [
            # 売上関連
            "売上高",
            "売上原価",
            # 売上総利益
            "売上総損益金額",
            # 販売管理費 (人件費)
            "役員報酬", "給料手当", "賞与", "法定福利費", "福利厚生費",
            # 採用・外注
            "採用教育費", "外注費",
            # 販売費
            "荷造運賃", "広告宣伝費", "販売手数料", "販売促進費",
            # 一般管理費
            "交際費", "会議費", "旅費交通費", "通信費", "消耗品費", "修繕費",
            "事務用品費", "水道光熱費", "新聞図書費", "諸会費", "支払手数料",
            "車両費", "地代家賃", "賃借料", "保険料", "租税公課", "支払報酬料",
            "研究開発費", "研修費", "減価償却費", "貸倒損失(販)", "雑費", "少額交際費",
            # 販売管理費計
            "販売管理費計",
            # 営業損益
            "営業損益金額",
            # 営業外損益
            "営業外収益合計", "営業外費用合計",
            # 経常損益
            "経常損益金額",
            # 特別損益
            "特別利益合計", "特別損失合計",
            # 税引前当期純損益
            "税引前当期純損益金額",
            # 法人税等
            "法人税、住民税及び事業税",
            # 当期純損益
            "当期純損益金額"
        ]
        
        # 販売管理費項目リスト
        self.ga_items = [
            "役員報酬", "給料手当", "賞与", "法定福利費", "福利厚生費",
            "採用教育費", "外注費", "荷造運賃", "広告宣伝費", "交際費",
            "会議費", "旅費交通費", "通信費", "販売手数料", "販売促進費",
            "消耗品費", "修繕費", "事務用品費", "水道光熱費", "新聞図書費",
            "諸会費", "支払手数料", "車両費", "地代家賃", "賃借料",
            "保険料", "租税公課", "支払報酬料", "研究開発費", "研修費",
            "減価償却費", "貸倒損失(販)", "雑費", "少額交際費"
        ]
        
        # 計算項目リスト (ユーザーが編集できない項目)
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
            "採用教育費": ["採用教育費", "採用費", "教育費"],
            "外注費": ["外注費"],
            "荷造運賃": ["荷造運賃"],
            "広告宣伝費": ["広告宣伝費"],
            "販売手数料": ["販売手数料"],
            "販売促進費": ["販売促進費"],
            "交際費": ["交際費", "接待交際費"],
            "会議費": ["会議費"],
            "旅費交通費": ["旅費交通費"],
            "通信費": ["通信費"],
            "消耗品費": ["消耗品費"],
            "修繕費": ["修繕費"],
            "事務用品費": ["事務用品費"],
            "水道光熱費": ["水道光熱費"],
            "新聞図書費": ["新聞図書費"],
            "諸会費": ["諸会費"],
            "支払手数料": ["支払手数料"],
            "車両費": ["車両費"],
            "地代家賃": ["地代家賃", "家賃"],
            "賃借料": ["賃借料"],
            "保険料": ["保険料"],
            "租税公課": ["租税公課"],
            "支払報酬料": ["支払報酬料"],
            "研究開発費": ["研究開発費"],
            "研修費": ["研修費"],
            "減価償却費": ["減価償却費"],
            "貸倒損失(販)": ["貸倒損失", "貸倒損失(販)"],
            "雑費": ["雑費"],
            "少額交際費": ["少額交際費"],
            "営業外収益合計": ["営業外収益", "営業外収益合計"],
            "営業外費用合計": ["営業外費用", "営業外費用合計"],
            "特別利益合計": ["特別利益", "特別利益合計"],
            "特別損失合計": ["特別損失", "特別損失合計"],
            "法人税、住民税及び事業税": ["法人税", "法人税等", "法人税、住民税及び事業税"]
        }

    def _init_db(self):
        """データベーステーブルの初期化 (要件定義書の2.3に準拠)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 2.3.1 会社マスタ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_name ON companies(name)')
        
        # 2.3.2 会計期マスタ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fiscal_periods (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comp_id INTEGER NOT NULL,
            period_num INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (comp_id) REFERENCES companies (id),
            UNIQUE(comp_id, period_num),
            CHECK (start_date < end_date)
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_comp_period ON fiscal_periods(comp_id, period_num)')
        
        # 2.3.3 実績データ
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
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_period_item ON actual_data(fiscal_period_id, item_name)')
        
        # 2.3.4 予測データ
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
            UNIQUE(fiscal_period_id, scenario, item_name, month),
            CHECK (scenario IN ('現実', '楽観', '悲観'))
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_period_scenario ON forecast_data(fiscal_period_id, scenario)')
        
        # 2.3.5 補助科目
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
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_period_parent ON sub_accounts(fiscal_period_id, parent_item)')
        
        # 2.3.6 勘定科目属性
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
            UNIQUE(fiscal_period_id, item_name),
            CHECK (is_variable IN (0, 1)),
            CHECK (variable_rate >= 0 AND variable_rate <= 1)
        )
        ''')
        
        conn.commit()
        conn.close()

    def get_companies(self):
        """会社一覧を取得"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM companies ORDER BY name", conn)
        conn.close()
        return df

    def add_company(self, company_name):
        """会社を追加"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO companies (name) VALUES (?)", (company_name,))
            conn.commit()
            conn.close()
            return True
        except:
            return False

    def get_company_periods(self, comp_id):
        """指定会社の会計期一覧を取得"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(
            "SELECT * FROM fiscal_periods WHERE comp_id = ? ORDER BY period_num DESC",
            conn,
            params=(comp_id,)
        )
        conn.close()
        return df

    def add_fiscal_period(self, comp_id, period_num, start_date, end_date):
        """会計期を追加"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO fiscal_periods (comp_id, period_num, start_date, end_date) VALUES (?, ?, ?, ?)",
                (comp_id, period_num, start_date, end_date)
            )
            conn.commit()
            conn.close()
            return True
        except:
            return False

    def get_period_info(self, period_id):
        """会計期情報を取得"""
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

    def get_company_id_from_period_id(self, fiscal_period_id):
        """会計期IDから会社IDを取得"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT comp_id FROM fiscal_periods WHERE id = ?", (fiscal_period_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def get_fiscal_months(self, comp_id, fiscal_period_id):
        """会計期の月リストを取得"""
        period = self.get_period_info(fiscal_period_id)
        if not period:
            return []
        
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

    def get_split_index(self, comp_id, current_month, fiscal_period_id):
        """実績と予測の境界インデックスを取得"""
        months = self.get_fiscal_months(comp_id, fiscal_period_id)
        try:
            return months.index(current_month) + 1
        except:
            return 0

    def load_actual_data(self, fiscal_period_id):
        """実績データを読み込み"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(
            "SELECT item_name as 項目名, month, amount FROM actual_data WHERE fiscal_period_id = ?",
            conn,
            params=(fiscal_period_id,)
        )
        conn.close()
        
        if df.empty:
            return pd.DataFrame({'項目名': self.all_items}).fillna(0)
        
        df = df.drop_duplicates(subset=['項目名', 'month'], keep='last')
        pivot_df = df.pivot(index='項目名', columns='month', values='amount').reset_index()
        
        all_items_df = pd.DataFrame({'項目名': self.all_items})
        pivot_df = pd.merge(all_items_df, pivot_df, on='項目名', how='left').fillna(0)
        return pivot_df

    def load_forecast_data(self, fiscal_period_id, scenario):
        """予測データを読み込み"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(
            "SELECT item_name as 項目名, month, amount FROM forecast_data WHERE fiscal_period_id = ? AND scenario = ?",
            conn,
            params=(fiscal_period_id, scenario)
        )
        conn.close()
        
        if df.empty:
            return pd.DataFrame({'項目名': self.all_items}).fillna(0)
        
        df = df.drop_duplicates(subset=['項目名', 'month'], keep='last')
        pivot_df = df.pivot(index='項目名', columns='month', values='amount').reset_index()
        
        all_items_df = pd.DataFrame({'項目名': self.all_items})
        pivot_df = pd.merge(all_items_df, pivot_df, on='項目名', how='left').fillna(0)
        return pivot_df

    def save_actual_item(self, fiscal_period_id, item_name, values_dict):
        """実績データを保存"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for month, amount in values_dict.items():
                cursor.execute(
                    "INSERT OR REPLACE INTO actual_data (fiscal_period_id, item_name, month, amount, updated_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
                    (fiscal_period_id, item_name, month, float(amount))
                )
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error saving actual data: {e}")
            return False

    def save_forecast_item(self, fiscal_period_id, scenario, item_name, values_dict):
        """予測データを保存"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for month, amount in values_dict.items():
                cursor.execute(
                    "INSERT OR REPLACE INTO forecast_data (fiscal_period_id, scenario, item_name, month, amount, updated_at) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
                    (fiscal_period_id, scenario, item_name, month, float(amount))
                )
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error saving forecast data: {e}")
            return False

    def load_sub_accounts(self, fiscal_period_id, scenario):
        """補助科目データを読み込み"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(
            "SELECT * FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ?",
            conn,
            params=(fiscal_period_id, scenario)
        )
        conn.close()
        return df

    def get_sub_accounts_for_parent(self, fiscal_period_id, scenario, parent_item):
        """特定親項目の補助科目を取得"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(
            "SELECT * FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ? AND parent_item = ?",
            conn,
            params=(fiscal_period_id, scenario, parent_item)
        )
        conn.close()
        return df

    def save_sub_account(self, fiscal_period_id, scenario, parent_item, sub_account_name, values_dict):
        """補助科目を保存"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for month, amount in values_dict.items():
                cursor.execute(
                    "INSERT OR REPLACE INTO sub_accounts (fiscal_period_id, scenario, parent_item, sub_account_name, month, amount, updated_at) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
                    (fiscal_period_id, scenario, parent_item, sub_account_name, month, float(amount))
                )
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error saving sub account: {e}")
            return False

    def delete_sub_account(self, fiscal_period_id, scenario, parent_item, sub_account_name):
        """補助科目を削除"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ? AND parent_item = ? AND sub_account_name = ?",
                (fiscal_period_id, scenario, parent_item, sub_account_name)
            )
            conn.commit()
            conn.close()
            return True
        except:
            return False

    def calculate_growth_forecast(self, actuals_df, item_name, split_index, months):
        """成長率ベースの予測計算 (要件定義書の5.5.2に準拠)"""
        forecast_values = {}
        
        actual_months = months[:split_index]
        forecast_months = months[split_index:]
        
        if len(actual_months) < 2:
            # 実績が2ヶ月未満の場合は前月踏襲
            if len(actual_months) == 1:
                last_value = actuals_df[actuals_df['項目名'] == item_name][actual_months[0]].iloc[0]
            else:
                last_value = 0
            
            for m in forecast_months:
                forecast_values[m] = last_value
            
            return forecast_values
        
        # 前月比成長率の平均を計算
        item_row = actuals_df[actuals_df['項目名'] == item_name]
        actual_values = [item_row[m].iloc[0] for m in actual_months]
        
        growth_rates = []
        for i in range(1, len(actual_values)):
            if actual_values[i-1] != 0:
                rate = (actual_values[i] - actual_values[i-1]) / abs(actual_values[i-1])
                growth_rates.append(rate)
        
        if len(growth_rates) == 0:
            avg_growth_rate = 0
        else:
            # 異常値を除外 (±100%以上の変動は除外)
            filtered_rates = [r for r in growth_rates if abs(r) < 1.0]
            if len(filtered_rates) > 0:
                avg_growth_rate = np.mean(filtered_rates)
            else:
                avg_growth_rate = 0
        
        # 予測値の生成
        last_actual_value = actual_values[-1]
        current_forecast_value = last_actual_value
        
        for m in forecast_months:
            if last_actual_value != 0:
                current_forecast_value *= (1 + avg_growth_rate)
            else:
                current_forecast_value += avg_growth_rate
                
            forecast_values[m] = current_forecast_value
            
        return forecast_values

    def calculate_pl(self, actuals_df, forecasts_df, split_index, months):
        """
        損益計算書を計算 (要件定義書の3.2に準拠)
        
        計算ロジック:
        - 売上総損益金額 = 売上高 - 売上原価
        - 販売管理費計 = 33項目の合計
        - 営業損益金額 = 売上総損益金額 - 販売管理費計
        - 経常損益金額 = 営業損益金額 + 営業外収益合計 - 営業外費用合計
        - 税引前当期純損益金額 = 経常損益金額 + 特別利益合計 - 特別損失合計
        - 当期純損益金額 = 税引前当期純損益金額 - 法人税、住民税及び事業税
        """
        df = pd.DataFrame({'項目名': self.all_items})
        
        # 実績と予測の結合
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
        def get_val(item_name):
            row = df[df['項目名'] == item_name]
            if not row.empty:
                return row[months].iloc[0]
            return pd.Series(0.0, index=months)

        # 売上総損益金額 = 売上高 - 売上原価
        sales = get_val("売上高")
        cogs = get_val("売上原価")
        gp = sales - cogs
        df.loc[df['項目名'] == "売上総損益金額", months] = gp.values
        
        # 販売管理費計 = 33項目の合計
        ga_total = pd.Series(0.0, index=months)
        for item in self.ga_items:
            ga_total += get_val(item)
        df.loc[df['項目名'] == "販売管理費計", months] = ga_total.values
        
        # 営業損益金額 = 売上総損益金額 - 販売管理費計
        op = gp - ga_total
        df.loc[df['項目名'] == "営業損益金額", months] = op.values
        
        # 経常損益金額 = 営業損益金額 + 営業外収益合計 - 営業外費用合計
        non_op_inc = get_val("営業外収益合計")
        non_op_exp = get_val("営業外費用合計")
        ord_p = op + non_op_inc - non_op_exp
        df.loc[df['項目名'] == "経常損益金額", months] = ord_p.values
        
        # 税引前当期純損益金額 = 経常損益金額 + 特別利益合計 - 特別損失合計
        sp_inc = get_val("特別利益合計")
        sp_exp = get_val("特別損失合計")
        pre_tax = ord_p + sp_inc - sp_exp
        df.loc[df['項目名'] == "税引前当期純損益金額", months] = pre_tax.values
        
        # 当期純損益金額 = 税引前当期純損益金額 - 法人税、住民税及び事業税
        tax = get_val("法人税、住民税及び事業税")
        net_p = pre_tax - tax
        df.loc[df['項目名'] == "当期純損益金額", months] = net_p.values
        
        # 合計列の追加
        df['実績合計'] = df[months[:split_index]].sum(axis=1)
        df['予測合計'] = df[months[split_index:]].sum(axis=1)
        df['合計'] = df['実績合計'] + df['予測合計']
        
        # タイプ（要約/詳細）の付与
        summary_items = ["売上高", "売上総損益金額", "販売管理費計", "営業損益金額", "経常損益金額", "当期純損益金額"]
        df['タイプ'] = df['項目名'].apply(lambda x: '要約' if x in summary_items else '詳細')
        
        return df

    def import_yayoi_excel(self, file_path, preview_only=False):
        """
        弥生会計Excelからデータをインポート
        preview_only=True の場合はプレビュー用のDataFrameを返す
        """
        try:
            xls = pd.ExcelFile(file_path)
            
            # 全シートの最初の会社IDと期数を取得（仮に最初のシート名から推測）
            # 実際の実装では、ユーザーに選択させる必要がある場合もある
            
            # ここでは簡易的に、セッションから会社IDと期数を取得する想定
            # 実際の呼び出し元でこれらを渡すように修正が必要
            
            imported_data = {item: {} for item in self.all_items}
            
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                
                month_cols = {}
                
                # 月の列を特定
                for r in range(min(20, len(df))):
                    for c in range(len(df.columns)):
                        val = str(df.iloc[r, c])
                        # 月のパターンを検出
                        match = re.search(r'(\d{1,2})月', val)
                        if match:
                            month_num = int(match.group(1))
                            # 年度を推定（簡易的に現在年）
                            year = datetime.now().year
                            month_str = f"{year}-{month_num:02d}"
                            month_cols[month_str] = c
                
                if not month_cols:
                    continue
                
                # 項目の行を特定して数値を抽出
                for r in range(len(df)):
                    item_val = ""
                    for c in range(min(3, len(df.columns))):
                        v = str(df.iloc[r, c]).strip()
                        if v and v != "nan":
                            item_val = v
                            break
                    
                    if not item_val:
                        continue
                    
                    target_item = None
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
                            except:
                                pass
            
            # DataFrameに変換
            imported_df = pd.DataFrame.from_dict(imported_data, orient='index').reset_index().rename(columns={'index': '項目名'})
            
            # 項目名でソート
            imported_df['項目名'] = pd.Categorical(imported_df['項目名'], categories=self.all_items, ordered=True)
            imported_df = imported_df.sort_values('項目名').reset_index(drop=True)
            
            return imported_df, "データ抽出に成功しました"

        except Exception as e:
            return pd.DataFrame(), str(e)

    def save_extracted_data(self, fiscal_period_id, imported_df):
        """抽出されたDataFrameをデータベースに保存"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 既存のデータを削除
            cursor.execute("DELETE FROM actual_data WHERE fiscal_period_id = ?", (fiscal_period_id,))
            
            months = [c for c in imported_df.columns if c != '項目名']
            
            for _, row in imported_df.iterrows():
                for m in months:
                    val = row[m]
                    if val != 0 and not pd.isna(val):
                        cursor.execute(
                            "INSERT INTO actual_data (fiscal_period_id, item_name, month, amount) VALUES (?, ?, ?, ?)",
                            (fiscal_period_id, row['項目名'], m, float(val))
                        )
            
            conn.commit()
            conn.close()
            return True, "インポートが完了しました"
        except Exception as e:
            return False, str(e)
