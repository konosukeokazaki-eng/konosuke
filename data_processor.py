import sqlite3
import pandas as pd
import numpy as np
import re
import os
from datetime import datetime, timedelta

class DataProcessor:
    def __init__(self, db_path=None):
        if db_path is None:
            # 実行ファイルのあるディレクトリを基準にする
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.db_path = os.path.join(base_dir, "financial_data.db")
        else:
            self.db_path = db_path
        self._init_db()
        
        # 標準的な勘定科目リスト
        self.all_items = [
            "売上高", "売上原価", "売上総損益金額",
            "役員報酬", "給料手当", "賞与", "法定福利費", "福利厚生費", "採用教育費", "外注費", "荷造運賃", "広告宣伝費", "交際費", "会議費", "旅費交通費", "通信費", "販売手数料", "販売促進費", "消耗品費", "修繕費", "事務用品費", "水道光熱費", "新聞図書費", "諸会費", "支払手数料", "車両費", "地代家賃", "賃借料", "保険料", "租税公課", "支払報酬料", "研究開発費", "研修費", "減価償却費", "貸倒損失(販)", "雑費", "少額交際費", "販売管理費計",
            "営業損益金額", "営業外収益合計", "営業外費用合計", "経常損益金額", "特別利益合計", "特別損失合計", "税引前当期純損益金額", "法人税、住民税及び事業税", "当期純損益金額"
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

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 会社情報
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
        ''')
        
        # 会計期情報
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fiscal_periods (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comp_id INTEGER,
            period_num INTEGER,
            start_date TEXT,
            end_date TEXT,
            FOREIGN KEY (comp_id) REFERENCES companies (id)
        )
        ''')
        
        # 実績データ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS actual_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fiscal_period_id INTEGER,
            item_name TEXT,
            month TEXT,
            amount REAL,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods (id)
        )
        ''')
        
        # 予測データ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS forecast_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fiscal_period_id INTEGER,
            scenario TEXT,
            item_name TEXT,
            month TEXT,
            amount REAL,
            FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods (id)
        )
        ''')
        
        # 補助科目（内訳）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sub_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fiscal_period_id INTEGER,
                scenario TEXT,
                parent_item TEXT,
                sub_account_name TEXT,
                month TEXT,
                amount REAL,
                FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id)
            )
        """)
        
        # 勘定科目属性テーブル（固定費/変動費の属性を保存）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS item_attributes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fiscal_period_id INTEGER,
                item_name TEXT UNIQUE,
                is_variable BOOLEAN DEFAULT 0, -- 0: 固定費, 1: 変動費
                variable_rate REAL DEFAULT 0.0, -- 変動費率（売上に対する割合）
                FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id)
            )
	        """)
        conn.close()

    def get_companies(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM companies", conn)
        conn.close()
        return df

    def get_company_periods(self, comp_id):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(f"SELECT * FROM fiscal_periods WHERE comp_id = {comp_id} ORDER BY period_num DESC", conn)
        conn.close()
        return df

    def get_period_info(self, period_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM fiscal_periods WHERE id = ?", (period_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return {"id": row[0], "comp_id": row[1], "period_num": row[2], "start_date": row[3], "end_date": row[4]}
        return None

    def get_company_id_from_period_id(self, fiscal_period_id):
        """
        会計期IDから会社IDを取得する
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT comp_id FROM fiscal_periods WHERE id = ?", (fiscal_period_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def get_fiscal_months(self, comp_id, fiscal_period_id):
        period = self.get_period_info(fiscal_period_id)
        if not period: return []
        
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
        months = self.get_fiscal_months(comp_id, fiscal_period_id)
        try:
            return months.index(current_month) + 1
        except:
            return 0

    def load_actual_data(self, fiscal_period_id):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(f"SELECT item_name as 項目名, month, amount FROM actual_data WHERE fiscal_period_id = {fiscal_period_id}", conn)
        conn.close()
        
        if df.empty:
            return pd.DataFrame({'項目名': self.all_items}).fillna(0)
        
        # 重複行を削除し、pivot時のValueErrorを防ぐ
        df = df.drop_duplicates(subset=['項目名', 'month'], keep='last')
        
        pivot_df = df.pivot(index='項目名', columns='month', values='amount').reset_index()
        # 全項目を網羅
        all_items_df = pd.DataFrame({'項目名': self.all_items})
        pivot_df = pd.merge(all_items_df, pivot_df, on='項目名', how='left').fillna(0)
        return pivot_df

    def load_forecast_data(self, fiscal_period_id, scenario):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(f"SELECT item_name as 項目名, month, amount FROM forecast_data WHERE fiscal_period_id = {fiscal_period_id} AND scenario = '{scenario}'", conn)
        conn.close()
        
        if df.empty:
            return pd.DataFrame({'項目名': self.all_items}).fillna(0)
        
        # 重複行を削除し、pivot時のValueErrorを防ぐ
        df = df.drop_duplicates(subset=['項目名', 'month'], keep='last')
        
        pivot_df = df.pivot(index='項目名', columns='month', values='amount').reset_index()
        all_items_df = pd.DataFrame({'項目名': self.all_items})
        pivot_df = pd.merge(all_items_df, pivot_df, on='項目名', how='left').fillna(0)
        return pivot_df

    def get_item_attributes(self, fiscal_period_id, item_name):
        """
        勘定科目の属性（固定費/変動費、変動費率）を取得する
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT is_variable, variable_rate FROM item_attributes WHERE fiscal_period_id = ? AND item_name = ?", 
                       (fiscal_period_id, item_name))
        result = cursor.fetchone()
        conn.close()
        if result:
            return {"is_variable": bool(result[0]), "variable_rate": result[1]}
        return {"is_variable": False, "variable_rate": 0.0}

    def save_item_attribute(self, fiscal_period_id, item_name, is_variable, variable_rate):
        """
        勘定科目の属性（固定費/変動費、変動費率）を保存する
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO item_attributes (fiscal_period_id, item_name, is_variable, variable_rate)
            VALUES (?, ?, ?, ?)
        """, (fiscal_period_id, item_name, is_variable, variable_rate))
        conn.commit()
        conn.close()

    def save_forecast_data(self, fiscal_period_id, scenario, df):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM forecast_data WHERE fiscal_period_id = ? AND scenario = ?", (fiscal_period_id, scenario))
        
        months = [c for c in df.columns if c != '項目名']
        for _, row in df.iterrows():
            for m in months:
                val = row[m]
                if pd.isna(val) or val is None:
                    val = 0.0
                cursor.execute(
                    "INSERT INTO forecast_data (fiscal_period_id, scenario, item_name, month, amount) VALUES (?, ?, ?, ?, ?)",
                    (fiscal_period_id, scenario, row['項目名'], m, float(val))
                )
        conn.commit()
        conn.close()

    def load_sub_accounts(self, fiscal_period_id, scenario, parent_item):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(f"SELECT sub_account_name, month, amount FROM sub_accounts WHERE fiscal_period_id = {fiscal_period_id} AND scenario = '{scenario}' AND parent_item = '{parent_item}'", conn)
        conn.close()
        
        if df.empty:
            return pd.DataFrame(columns=['補助科目名'])
        
        pivot_df = df.pivot(index='sub_account_name', columns='month', values='amount').reset_index()
        pivot_df = pivot_df.rename(columns={'sub_account_name': '補助科目名'})
        return pivot_df

    def save_sub_accounts(self, fiscal_period_id, scenario, parent_item, df):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM sub_accounts WHERE fiscal_period_id = ? AND scenario = ? AND parent_item = ?", (fiscal_period_id, scenario, parent_item))
        
        months = [c for c in df.columns if c != '補助科目名']
        for _, row in df.iterrows():
            if not row['補助科目名'] or pd.isna(row['補助科目名']):
                continue
            for m in months:
                val = row[m]
                if pd.isna(val) or val is None:
                    val = 0.0
                cursor.execute(
                    "INSERT INTO sub_accounts (fiscal_period_id, scenario, parent_item, sub_account_name, month, amount) VALUES (?, ?, ?, ?, ?, ?)",
                    (fiscal_period_id, scenario, parent_item, row['補助科目名'], m, float(val))
                )
        conn.commit()
        conn.close()

    def calculate_sub_account_totals(self, fiscal_period_id, scenario, parent_item):
        df = self.load_sub_accounts(fiscal_period_id, scenario, parent_item)
        if df.empty:
            return pd.Series(0.0)
        
        months = [c for c in df.columns if c != '補助科目名']
        return df[months].sum(axis=0)

    def calculate_growth_forecast(self, fiscal_period_id, current_month, target_item, past_months=3):
        """
        過去の成長率を基に、残りの予測月を自動計算する
        
        Args:
            fiscal_period_id: 会計期ID
            current_month: 実績データ最終月
            target_item: 予測対象の項目名
            past_months: 成長率を計算する過去月数
            
        Returns:
            dict: {month: amount} の形式で予測値を返す
        """
        comp_id = self.get_company_id_from_period_id(fiscal_period_id)
        months = self.get_fiscal_months(comp_id, fiscal_period_id)
        split_index = self.get_split_index(comp_id, current_month, fiscal_period_id)
        
        # 1. 実績データ（actuals_df）をロード
        actuals_df = self.load_actual_data(fiscal_period_id)
        
        # 2. 予測対象項目の実績値を取得
        item_row = actuals_df[actuals_df['項目名'] == target_item]
        if item_row.empty:
            return {}
        
        # 3. 成長率の計算に使用する過去月を特定
        actual_months = months[:split_index]
        if len(actual_months) < past_months:
            past_months = len(actual_months)
            
        past_months_for_calc = actual_months[-past_months:]
        
        if len(past_months_for_calc) < 2:
            # 成長率を計算するのに十分な実績がない
            return {}
        
        # 4. 成長率を計算
        past_values = item_row[past_months_for_calc].iloc[0].values
        
        # 成長率のリスト
        growth_rates = []
        for i in range(1, len(past_values)):
            prev_val = past_values[i-1]
            curr_val = past_values[i]
            if prev_val != 0:
                growth_rates.append((curr_val - prev_val) / prev_val)
            else:
                # 前月が0の場合は、当月の値をそのまま成長率とみなす（絶対額の成長）
                growth_rates.append(curr_val)
        
        # 平均成長率（または平均絶対成長額）
        if not growth_rates:
            return {}
            
        avg_growth_rate = sum(growth_rates) / len(growth_rates)
        
        # 5. 予測値を計算
        forecast_months = months[split_index:]
        forecast_values = {}
        
        # 予測の開始値は実績最終月の値
        # 実績データが空の場合を考慮
        if not actual_months:
            last_actual_value = 0
        else:
            last_actual_value = item_row[actual_months[-1]].iloc[0]
        
        current_forecast_value = last_actual_value
        
        for m in forecast_months:
            if last_actual_value != 0:
                # 成長率ベースの予測
                current_forecast_value *= (1 + avg_growth_rate)
            else:
                # 絶対額ベースの予測
                current_forecast_value += avg_growth_rate
                
            forecast_values[m] = current_forecast_value
            
        return forecast_values

    def calculate_pl(self, actuals_df, forecasts_df, split_index, months):
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

        sales = get_val("売上高")
        cogs = get_val("売上原価")
        gp = sales - cogs
        df.loc[df['項目名'] == "売上総損益金額", months] = gp.values
        
        ga_items = [
            "役員報酬", "給料手当", "賞与", "法定福利費", "福利厚生費", "採用教育費", "外注費", "荷造運賃", "広告宣伝費", "交際費", "会議費", "旅費交通費", "通信費", "販売手数料", "販売促進費", "消耗品費", "修繕費", "事務用品費", "水道光熱費", "新聞図書費", "諸会費", "支払手数料", "車両費", "地代家賃", "賃借料", "保険料", "租税公課", "支払報酬料", "研究開発費", "研修費", "減価償却費", "貸倒損失(販)", "雑費", "少額交際費"
        ]
        ga_total = pd.Series(0.0, index=months)
        for item in ga_items:
            ga_total += get_val(item)
        df.loc[df['項目名'] == "販売管理費計", months] = ga_total.values
        
        op = gp - ga_total
        df.loc[df['項目名'] == "営業損益金額", months] = op.values
        
        non_op_inc = get_val("営業外収益合計")
        non_op_exp = get_val("営業外費用合計")
        ord_p = op + non_op_inc - non_op_exp
        df.loc[df['項目名'] == "経常損益金額", months] = ord_p.values
        
        sp_inc = get_val("特別利益合計")
        sp_exp = get_val("特別損失合計")
        pre_tax = ord_p + sp_inc - sp_exp
        df.loc[df['項目名'] == "税引前当期純損益金額", months] = pre_tax.values
        
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



    def extract_yayoi_excel_data(self, file_path, comp_id, period_num):
        """
        弥生会計Excelからデータを抽出し、DataFrame形式で返す（保存はしない）
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
                
                # 全セルをスキャンして「月」と「項目」を探す
                month_cols = {} # {month_str: col_index}
                
                # 1. 月の列を特定
                for r in range(min(20, len(df))):
                    for c in range(len(df.columns)):
                        val = str(df.iloc[r, c])
                        for m in months:
                            m_num = int(m.split('-')[1])
                            patterns = [f"{m_num}月", f"^{m_num}$", f"{m.replace('-', '/')}", f"{m_num}/"]
                            if any(re.search(p, val) for p in patterns):
                                month_cols[m] = c
                
                if not month_cols:
                    continue
                
                # 2. 項目の行を特定して数値を抽出
                for r in range(len(df)):
                    # A列からC列までを項目名としてチェック
                    item_val = ""
                    for c in range(min(3, len(df.columns))):
                        v = str(df.iloc[r, c]).strip()
                        if v and v != "nan":
                            item_val = v
                            break
                    
                    if not item_val: continue
                    
                    target_item = None
                    # マッピングから検索
                    for std_name, aliases in self.item_mapping.items():
                        if any(alias in item_val for alias in aliases):
                            target_item = std_name
                            break
                    
                    if not target_item:
                        # 直接一致も確認
                        if item_val in self.all_items:
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
            
            # 辞書をDataFrameに変換
            imported_df = pd.DataFrame.from_dict(imported_data, orient='index').reset_index().rename(columns={'index': '項目名'})
            
            # 項目名でソート
            imported_df['項目名'] = pd.Categorical(imported_df['項目名'], categories=self.all_items, ordered=True)
            imported_df = imported_df.sort_values('項目名').reset_index(drop=True)
            
            return True, "データ抽出に成功しました。", imported_df

        except Exception as e:
            return False, str(e), None

    def save_extracted_data(self, fiscal_period_id, imported_df):
        """
        抽出されたDataFrameをデータベースに保存する
        """
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
            return True, "インポートが完了しました！"
        except Exception as e:
            return False, str(e)
