import requests
import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta
import time
import logging
from bs4 import BeautifulSoup
import re
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
import threading

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('txo_data_download.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class TXO數據收集器:
    def __init__(self, db_path='txo_data.db'):
        self.db_path = db_path
        self.base_url = "https://www.taifex.com.tw/cht/3/optDailyMarketReport"
        
    def 創建數據庫(self):
        """創建數據庫表結構"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS txo_options (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date DATE NOT NULL,
                contract_type TEXT NOT NULL,
                expiry_date TEXT NOT NULL,
                strike_price INTEGER NOT NULL,
                option_type TEXT NOT NULL,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                last_price REAL,
                settlement_price REAL,
                change_price REAL,
                change_percent REAL,
                after_hours_volume INTEGER,
                regular_volume INTEGER,
                total_volume INTEGER,
                open_interest INTEGER,
                best_bid REAL,
                best_ask REAL,
                historical_high REAL,
                historical_low REAL,
                created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(trade_date, expiry_date, strike_price, option_type)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_date ON txo_options(trade_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_expiry_strike ON txo_options(expiry_date, strike_price, option_type)')
        
        conn.commit()
        conn.close()
        logging.info("數據庫表創建完成")
    
    def 檢查日期數據完整性(self, date):
        """檢查指定日期的數據完整性"""
        conn = sqlite3.connect(self.db_path)
        query = """
        SELECT COUNT(*) as record_count,
               COUNT(DISTINCT expiry_date) as expiry_count,
               MIN(strike_price) as min_strike,
               MAX(strike_price) as max_strike
        FROM txo_options 
        WHERE trade_date = ?
        """
        result = pd.read_sql_query(query, conn, params=[date])
        conn.close()
        
        # 如果記錄數大於1000且有多個到期日，認為數據完整
        record_count = result.iloc[0]['record_count']
        expiry_count = result.iloc[0]['expiry_count']
        
        return record_count > 1000 and expiry_count >= 1
    
    def 下載每日數據(self, query_date):
        """
        從台灣期貨交易所下載指定日期的期權數據
        query_date: 格式為 'YYYY/MM/DD'
        """
        try:
            # 構造POST請求參數
            payload = {
                'queryType': '2',  # 選擇權
                'marketCode': '0',  # 一般
                'dateaddcnt': '',
                'commodity_id': 'TXO',  # 台指選擇權
                'queryDate': query_date,  # 查詢日期
                'MarketCode': '0',
                'commodity_id2': ''
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
                'Origin': 'https://www.taifex.com.tw',
                'Referer': 'https://www.taifex.com.tw/cht/3/optDailyMarketReport'
            }
            
            logging.info(f"正在下載 {query_date} 的數據...")
            
            # 發送POST請求
            response = requests.post(
                self.base_url, 
                data=payload, 
                headers=headers, 
                timeout=30
            )
            response.encoding = 'utf-8'
            response.raise_for_status()
            
            # 解析HTML
            df = self.解析HTML轉數據框(response.text, query_date)
            return df
            
        except Exception as e:
            logging.error(f"下載{query_date}數據失敗: {e}")
            return None
    
    def 解析HTML轉數據框(self, html_content, trade_date):
        """
        解析HTML內容並轉換為DataFrame
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 找到所有表格
            tables = soup.find_all('table')
            
            txo_table = None
            for table in tables:
                if 'TXO' in table.get_text():
                    txo_table = table
                    break
            
            if txo_table is None:
                logging.warning(f"在 {trade_date} 的頁面中未找到TXO表格")
                return None
            
            # 提取表頭
            headers = []
            thead = txo_table.find('thead')
            if thead:
                header_rows = thead.find_all('tr')
                if header_rows:
                    header_row = header_rows[-1]
                    for i, th in enumerate(header_row.find_all(['th', 'td'])):
                        header_text = th.get_text(strip=True)
                        if not header_text:
                            header_text = f'Column_{i}'
                        
                        base_header = header_text
                        counter = 1
                        while header_text in headers:
                            header_text = f"{base_header}_{counter}"
                            counter += 1
                        headers.append(header_text)
            
            # 提取數據行
            data_rows = []
            tbody = txo_table.find('tbody')
            if tbody:
                for tr in tbody.find_all('tr'):
                    row_data = []
                    for td in tr.find_all('td'):
                        cell_text = td.get_text(strip=True)
                        row_data.append(cell_text)
                    
                    if len(row_data) > 3 and row_data[0] == 'TXO':
                        data_rows.append(row_data)
            
            if not data_rows:
                logging.warning(f"在 {trade_date} 的數據中未找到TXO記錄")
                return None
            
            # 確保列數和數據行數匹配
            if len(headers) != len(data_rows[0]):
                logging.warning(f"列數不匹配: 表頭{len(headers)}列, 數據{len(data_rows[0])}列")
                if len(headers) > len(data_rows[0]):
                    headers = headers[:len(data_rows[0])]
                else:
                    headers.extend([f'Column_{i}' for i in range(len(headers), len(data_rows[0]))])
            
            df = pd.DataFrame(data_rows, columns=headers)
            df_cleaned = self.清洗轉換數據(df, trade_date)
            return df_cleaned
            
        except Exception as e:
            logging.error(f"解析HTML失敗: {e}")
            return None
    
    def 清洗轉換數據(self, df, trade_date):
        """清洗和轉換數據"""
        try:
            # 定義列名映射模式
            column_patterns = {
                'contract_type': ['契約'],
                'expiry_date': ['到期月份', '到期月份(週別)'],
                'strike_price': ['履約價'],
                'option_type': ['買賣權'],
                'open_price': ['開盤價'],
                'high_price': ['最高價'],
                'low_price': ['最低價'],
                'last_price': ['最後成交價'],
                'settlement_price': ['結算價'],
                'change_price': ['漲跌價'],
                'change_percent': ['漲跌%'],
                'after_hours_volume': ['盤後交易時段成交量'],
                'regular_volume': ['一般交易時段成交量'],
                'total_volume': ['合計成交量'],
                'open_interest': ['未沖銷契約量'],
                'best_bid': ['最後最佳買價'],
                'best_ask': ['最後最佳賣價'],
                'historical_high': ['歷史最高價'],
                'historical_low': ['歷史最低價']
            }
            
            new_columns = {}
            for new_name, patterns in column_patterns.items():
                for pattern in patterns:
                    for old_col in df.columns:
                        if pattern in old_col:
                            new_columns[old_col] = new_name
                            break
            
            df_renamed = df.rename(columns=new_columns)
            
            required_columns = [
                'contract_type', 'expiry_date', 'strike_price', 'option_type',
                'open_price', 'high_price', 'low_price', 'last_price', 'settlement_price',
                'change_price', 'change_percent', 'after_hours_volume', 'regular_volume',
                'total_volume', 'open_interest', 'best_bid', 'best_ask', 
                'historical_high', 'historical_low'
            ]
            
            available_columns = [col for col in required_columns if col in df_renamed.columns]
            df_final = df_renamed[available_columns].copy()
            
            df_final['trade_date'] = trade_date.replace('/', '-')
            
            numeric_columns = [
                'strike_price', 'open_price', 'high_price', 'low_price',
                'last_price', 'settlement_price', 'change_price', 'change_percent',
                'after_hours_volume', 'regular_volume', 'total_volume', 
                'open_interest', 'best_bid', 'best_ask', 'historical_high', 'historical_low'
            ]
            
            for col in numeric_columns:
                if col in df_final.columns:
                    if df_final[col].dtype == 'object':
                        df_final[col] = df_final[col].astype(str)
                        df_final[col] = df_final[col].str.replace(',', '')
                        df_final[col] = df_final[col].str.replace('▼', '-')
                        df_final[col] = df_final[col].str.replace('▲', '')
                        df_final[col] = df_final[col].str.replace('%', '')
                        df_final[col] = df_final[col].str.replace(' ', '')
                        df_final[col] = df_final[col].str.replace('--', '')
                        df_final[col] = df_final[col].str.replace('-', '')
                    
                    df_final[col] = pd.to_numeric(df_final[col], errors='coerce')
            
            text_columns = ['contract_type', 'expiry_date', 'option_type']
            for col in text_columns:
                if col in df_final.columns:
                    df_final[col] = df_final[col].astype(str).str.strip()
            
            return df_final
            
        except Exception as e:
            logging.error(f"數據清洗失敗: {e}")
            return None
    
    def 保存到數據庫(self, df, date):
        """將數據保存到數據庫"""
        if df is None or df.empty:
            logging.warning(f"{date} 沒有數據需要保存")
            return False
            
        try:
            conn = sqlite3.connect(self.db_path)
            df = df.loc[:, ~df.columns.duplicated()]
            
            delete_sql = "DELETE FROM txo_options WHERE trade_date = ?"
            conn.execute(delete_sql, (date.replace('/', '-'),))
            
            df.to_sql('txo_options', conn, if_exists='append', index=False)
            
            conn.commit()
            conn.close()
            
            logging.info(f"成功保存 {len(df)} 條 {date} 的數據到數據庫")
            return True
            
        except Exception as e:
            logging.error(f"保存到數據庫失敗: {e}")
            return False

class TXO數據分析器:
    def __init__(self, db_path='txo_data.db'):
        self.db_path = db_path
    
    def 獲取所有日期(self):
        """獲取數據庫中所有的交易日期"""
        conn = sqlite3.connect(self.db_path)
        query = "SELECT DISTINCT trade_date FROM txo_options ORDER BY trade_date"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df['trade_date'].tolist()
    
    def 獲取日期數據(self, date):
        """獲取指定日期的所有數據"""
        conn = sqlite3.connect(self.db_path)
        query = "SELECT * FROM txo_options WHERE trade_date = ? ORDER BY strike_price, option_type"
        df = pd.read_sql_query(query, conn, params=[date])
        conn.close()
        return df
    
    def 獲取期權鏈(self, trade_date, expiry_date=None):
        """獲取指定日期的期權鏈"""
        conn = sqlite3.connect(self.db_path)
        
        if expiry_date:
            query = """
            SELECT strike_price, option_type, 
                   last_price, settlement_price, total_volume, open_interest,
                   best_bid, best_ask, change_price, change_percent
            FROM txo_options 
            WHERE trade_date = ? AND expiry_date = ?
            ORDER BY strike_price, option_type
            """
            params = [trade_date, expiry_date]
        else:
            query = """
            SELECT strike_price, option_type, 
                   last_price, settlement_price, total_volume, open_interest,
                   best_bid, best_ask, change_price, change_percent
            FROM txo_options 
            WHERE trade_date = ?
            ORDER BY strike_price, option_type
            """
            params = [trade_date]
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    
    def 獲取成交量分析(self, trade_date):
        """獲取成交量分析"""
        conn = sqlite3.connect(self.db_path)
        query = """
        SELECT 
            option_type,
            SUM(total_volume) as total_volume,
            SUM(open_interest) as total_open_interest,
            COUNT(*) as contract_count
        FROM txo_options 
        WHERE trade_date = ?
        GROUP BY option_type
        """
        df = pd.read_sql_query(query, conn, params=[trade_date])
        conn.close()
        return df

class 日期選擇器(tk.Frame):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.配置界面()
        
    def 配置界面(self):
        """配置日期選擇器界面"""
        self.年變數 = tk.StringVar(value=str(datetime.now().year))
        self.月變數 = tk.StringVar(value=str(datetime.now().month))
        self.日變數 = tk.StringVar(value=str(datetime.now().day))
        
        # 年份選擇
        tk.Label(self, text="年:", bg='black', fg='white', font=('Microsoft JhengHei', 12)).grid(row=0, column=0, padx=2)
        年框架 = tk.Frame(self, bg='black')
        年框架.grid(row=0, column=1, padx=2)
        
        tk.Button(年框架, text="◀", command=self.上一年, 
                 bg='gray', fg='white', font=('Arial', 10), width=3).pack(side=tk.LEFT)
        tk.Label(年框架, textvariable=self.年變數, bg='black', fg='white', 
                font=('Microsoft JhengHei', 12), width=5).pack(side=tk.LEFT, padx=5)
        tk.Button(年框架, text="▶", command=self.下一年,
                 bg='gray', fg='white', font=('Arial', 10), width=3).pack(side=tk.LEFT)
        
        # 月份選擇
        tk.Label(self, text="月:", bg='black', fg='white', font=('Microsoft JhengHei', 12)).grid(row=0, column=2, padx=2)
        月框架 = tk.Frame(self, bg='black')
        月框架.grid(row=0, column=3, padx=2)
        
        tk.Button(月框架, text="◀", command=self.上一月, 
                 bg='gray', fg='white', font=('Arial', 10), width=3).pack(side=tk.LEFT)
        tk.Label(月框架, textvariable=self.月變數, bg='black', fg='white', 
                font=('Microsoft JhengHei', 12), width=5).pack(side=tk.LEFT, padx=5)
        tk.Button(月框架, text="▶", command=self.下一月,
                 bg='gray', fg='white', font=('Arial', 10), width=3).pack(side=tk.LEFT)
        
        # 日期選擇
        tk.Label(self, text="日:", bg='black', fg='white', font=('Microsoft JhengHei', 12)).grid(row=0, column=4, padx=2)
        日下拉 = ttk.Combobox(self, textvariable=self.日變數, values=[str(i) for i in range(1, 32)], 
                            font=('Microsoft JhengHei', 12), width=5, state="readonly")
        日下拉.grid(row=0, column=5, padx=2)
        
        # 更新日期範圍
        self.更新日期範圍()
        
    def 上一年(self):
        年 = int(self.年變數.get())
        self.年變數.set(str(年 - 1))
        self.更新日期範圍()
        
    def 下一年(self):
        年 = int(self.年變數.get())
        self.年變數.set(str(年 + 1))
        self.更新日期範圍()
        
    def 上一月(self):
        月 = int(self.月變數.get())
        if 月 > 1:
            self.月變數.set(str(月 - 1))
        else:
            self.月變數.set("12")
            self.上一年()
        self.更新日期範圍()
        
    def 下一月(self):
        月 = int(self.月變數.get())
        if 月 < 12:
            self.月變數.set(str(月 + 1))
        else:
            self.月變數.set("1")
            self.下一年()
        self.更新日期範圍()
        
    def 更新日期範圍(self):
        """根據年月更新日期範圍"""
        年 = int(self.年變數.get())
        月 = int(self.月變數.get())
        
        # 計算該月的天數
        if 月 in [1, 3, 5, 7, 8, 10, 12]:
            天數 = 31
        elif 月 in [4, 6, 9, 11]:
            天數 = 30
        else:  # 2月
            if (年 % 4 == 0 and 年 % 100 != 0) or (年 % 400 == 0):
                天數 = 29
            else:
                天數 = 28
                
        # 更新日期下拉選項
        日期選項 = [str(i) for i in range(1, 天數 + 1)]
        
        # 找到當前日期選擇框並更新
        for child in self.winfo_children():
            if isinstance(child, ttk.Combobox) and child.grid_info()['column'] == 5:
                child['values'] = 日期選項
                # 如果當前日期超過新範圍，設置為最大值
                當前日 = int(self.日變數.get()) if self.日變數.get().isdigit() else 1
                if 當前日 > 天數:
                    self.日變數.set(str(天數))
                break
    
    def 獲取日期(self):
        """獲取選擇的日期"""
        年 = self.年變數.get().zfill(4)
        月 = self.月變數.get().zfill(2)
        日 = self.日變數.get().zfill(2)
        return f"{年}/{月}/{日}"
    
    def 設置日期(self, 日期字串):
        """設置日期"""
        try:
            年, 月, 日 = 日期字串.split('/')
            self.年變數.set(年)
            self.月變數.set(月.lstrip('0'))
            self.日變數.set(日.lstrip('0'))
            self.更新日期範圍()
        except:
            pass

class TXO數據UI:
    def __init__(self, root):
        self.root = root
        self.root.title("台指期權數據管理系統")
        self.root.configure(bg='black')
        self.root.geometry("1200x800")
        
        # 初始化數據收集器和分析器
        self.收集器 = TXO數據收集器('txo_data.db')
        self.分析器 = TXO數據分析器('txo_data.db')
        
        # 創建數據庫
        self.收集器.創建數據庫()
        
        self.設置界面()
        
    def 設置界面(self):
        """設置用戶界面"""
        # 主框架
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 標題
        title_label = tk.Label(main_frame, text="台指期權數據管理系統", 
                              font=("Microsoft JhengHei", 20, "bold"),
                              fg="white", bg="black")
        title_label.pack(pady=10)
        
        # 創建選項卡
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True)
        
        # 下載數據選項卡
        download_frame = ttk.Frame(notebook)
        notebook.add(download_frame, text="數據下載")
        
        # 數據查詢選項卡
        query_frame = ttk.Frame(notebook)
        notebook.add(query_frame, text="數據查詢")
        
        # 設置樣式
        self.設置樣式()
        
        # 設置各選項卡內容
        self.設置下載選項卡(download_frame)
        self.設置查詢選項卡(query_frame)
    
    def 設置樣式(self):
        """設置UI樣式"""
        style = ttk.Style()
        style.configure('TFrame', background='black')
        style.configure('TLabel', background='black', foreground='white', font=('Microsoft JhengHei', 16))
        style.configure('TButton', font=('Microsoft JhengHei', 16))
        style.configure('TEntry', font=('Microsoft JhengHei', 16))
        style.configure('TCombobox', font=('Microsoft JhengHei', 16))
        
    def 設置下載選項卡(self, parent):
        """設置下載選項卡"""
        # 下載設置框架
        settings_frame = ttk.LabelFrame(parent, text="下載設置")
        settings_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # 開始日期
        tk.Label(settings_frame, text="開始日期:", 
                bg='black', fg='white', font=('Microsoft JhengHei', 16)).grid(row=0, column=0, padx=5, pady=5, sticky='w')
        
        self.開始日期選擇器 = 日期選擇器(settings_frame)
        self.開始日期選擇器.grid(row=0, column=1, padx=5, pady=5)
        self.開始日期選擇器.設置日期((datetime.now() - timedelta(days=30)).strftime('%Y/%m/%d'))
        
        # 結束日期
        tk.Label(settings_frame, text="結束日期:", 
                bg='black', fg='white', font=('Microsoft JhengHei', 16)).grid(row=1, column=0, padx=5, pady=5, sticky='w')
        
        self.結束日期選擇器 = 日期選擇器(settings_frame)
        self.結束日期選擇器.grid(row=1, column=1, padx=5, pady=5)
        self.結束日期選擇器.設置日期(datetime.now().strftime('%Y/%m/%d'))
        
        # 下載按鈕
        download_btn = ttk.Button(settings_frame, text="開始下載數據", command=self.開始下載)
        download_btn.grid(row=2, column=0, columnspan=2, pady=10)
        
        # 進度顯示
        progress_frame = ttk.LabelFrame(parent, text="下載進度")
        progress_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.進度文本 = tk.Text(progress_frame, height=15, bg='black', fg='white', 
                                   font=('Microsoft JhengHei', 16))
        scrollbar = ttk.Scrollbar(progress_frame, orient=tk.VERTICAL, command=self.進度文本.yview)
        self.進度文本.configure(yscrollcommand=scrollbar.set)
        
        self.進度文本.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
    def 設置查詢選項卡(self, parent):
        """設置查詢選項卡"""
        # 查詢設置框架
        query_frame = ttk.LabelFrame(parent, text="數據查詢")
        query_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # 日期選擇
        tk.Label(query_frame, text="選擇日期:", 
                bg='black', fg='white', font=('Microsoft JhengHei', 16)).grid(row=0, column=0, padx=5, pady=5)
        
        # 查詢日期選擇器
        self.查詢日期選擇器 = 日期選擇器(query_frame)
        self.查詢日期選擇器.grid(row=0, column=1, padx=5, pady=5)
        
        # 綁定日期變化事件
        self.查詢日期選擇器.年變數.trace('w', self.日期選擇事件)
        self.查詢日期選擇器.月變數.trace('w', self.日期選擇事件)
        self.查詢日期選擇器.日變數.trace('w', self.日期選擇事件)
        
        # 到期日選擇
        tk.Label(query_frame, text="選擇到期日:", 
                bg='black', fg='white', font=('Microsoft JhengHei', 16)).grid(row=0, column=2, padx=5, pady=5)
        
        self.到期日變數 = tk.StringVar()
        self.到期日下拉框 = ttk.Combobox(query_frame, textvariable=self.到期日變數, font=('Microsoft JhengHei', 16), width=12)
        self.到期日下拉框.grid(row=0, column=3, padx=5, pady=5)
        
        # 查詢按鈕
        query_btn = ttk.Button(query_frame, text="查詢數據", command=self.查詢數據)
        query_btn.grid(row=0, column=4, padx=10, pady=5)
        
        # 導出按鈕
        export_btn = ttk.Button(query_frame, text="導出數據", command=self.導出數據)
        export_btn.grid(row=0, column=5, padx=10, pady=5)
        
        # 數據顯示
        data_frame = ttk.LabelFrame(parent, text="數據預覽")
        data_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 創建Treeview
        columns = ('履約價', '買賣權', '最後成交價', '結算價', '總成交量', '未平倉量', '最佳買價', '最佳賣價')
        
        self.樹狀表格 = ttk.Treeview(data_frame, columns=columns, show='headings', height=15)
        
        # 設置列標題
        column_mapping = {
            '履約價': '履約價',
            '買賣權': '買賣權',
            '最後成交價': '最後成交價', 
            '結算價': '結算價',
            '總成交量': '總成交量',
            '未平倉量': '未平倉量',
            '最佳買價': '最佳買價',
            '最佳賣價': '最佳賣價'
        }
        
        for col in columns:
            self.樹狀表格.heading(col, text=column_mapping[col])
            self.樹狀表格.column(col, width=120)
        
        # 滾動條
        scrollbar = ttk.Scrollbar(data_frame, orient=tk.VERTICAL, command=self.樹狀表格.yview)
        self.樹狀表格.configure(yscrollcommand=scrollbar.set)
        
        self.樹狀表格.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # 更新日期列表
        self.更新日期列表()
    
    def 記錄訊息(self, message):
        """在進度文本框中顯示訊息"""
        self.進度文本.insert(tk.END, f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
        self.進度文本.see(tk.END)
        self.root.update()
    
    def 開始下載(self):
        """開始下載數據（在新線程中）"""
        start_date = self.開始日期選擇器.獲取日期()
        end_date = self.結束日期選擇器.獲取日期()
        
        # 在新線程中運行下載
        thread = threading.Thread(target=self.下載數據, args=(start_date, end_date))
        thread.daemon = True
        thread.start()
    
    def 下載數據(self, start_date, end_date):
        """下載數據的主要邏輯"""
        self.記錄訊息("開始下載數據...")
        
        try:
            start = datetime.strptime(start_date, '%Y/%m/%d')
            end = datetime.strptime(end_date, '%Y/%m/%d')
            
            success_count = 0
            skip_count = 0
            fail_count = 0
            failed_dates = []  # 記錄失敗的日期
            
            # 從結束日期開始往前下載
            current = end
            while current >= start:
                date_str = current.strftime('%Y/%m/%d')
                
                # 跳過週末
                if current.weekday() >= 5:
                    self.記錄訊息(f"{date_str} 是週末，跳過")
                    current -= timedelta(days=1)
                    continue
                
                # 檢查數據是否已存在且完整
                if self.收集器.檢查日期數據完整性(date_str.replace('/', '-')):
                    self.記錄訊息(f"{date_str} 數據已存在且完整，跳過")
                    skip_count += 1
                else:
                    self.記錄訊息(f"下載 {date_str} 的數據...")
                    df = self.收集器.下載每日數據(date_str)
                    
                    if df is not None and not df.empty:
                        if self.收集器.保存到數據庫(df, date_str):
                            self.記錄訊息(f"✓ {date_str} 下載成功 ({len(df)} 條記錄)")
                            success_count += 1
                        else:
                            self.記錄訊息(f"✗ {date_str} 保存失敗")
                            fail_count += 1
                            failed_dates.append(date_str)
                    else:
                        self.記錄訊息(f"✗ {date_str} 下載失敗")
                        fail_count += 1
                        failed_dates.append(date_str)
                
                # 短暫延遲
                time.sleep(1)
                current -= timedelta(days=1)
            
            # 顯示下載結果
            self.記錄訊息(f"\n下載完成!")
            self.記錄訊息(f"成功: {success_count} 天, 跳過: {skip_count} 天, 失敗: {fail_count} 天")
            
            # 如果有失敗的日期，提示用戶可以重新下載
            if failed_dates:
                self.記錄訊息(f"失敗的日期: {', '.join(failed_dates)}")
                self.記錄訊息("提示: 失敗的日期會在下次下載相同日期範圍時自動重新下載")
            
            # 更新日期列表
            self.root.after(0, self.更新日期列表)
            
        except Exception as e:
            self.記錄訊息(f"下載過程中發生錯誤: {str(e)}")
    
    def 更新日期列表(self):
        """更新查詢日期選擇器"""
        try:
            dates = self.分析器.獲取所有日期()
            if dates:
                # 設置查詢日期選擇器為最新日期
                self.查詢日期選擇器.設置日期(dates[-1])
                # 觸發日期選擇事件
                self.日期選擇事件()
        except:
            pass
    
    def 日期選擇事件(self, *args):
        """當選擇日期時更新到期日列表"""
        selected_date = self.查詢日期選擇器.獲取日期()
        if selected_date:
            try:
                # 將日期格式從 YYYY/MM/DD 轉換為 YYYY-MM-DD
                db_date = selected_date.replace('/', '-')
                data = self.分析器.獲取日期數據(db_date)
                if not data.empty:
                    expiry_dates = sorted(data['expiry_date'].unique())
                    self.到期日下拉框['values'] = expiry_dates
                    if expiry_dates:
                        self.到期日下拉框.set(expiry_dates[0])
                else:
                    self.到期日下拉框['values'] = []
                    self.到期日變數.set('')
            except Exception as e:
                print(f"更新到期日列表時出錯: {e}")
                self.到期日下拉框['values'] = []
                self.到期日變數.set('')
    
    def 查詢數據(self):
        """查詢數據"""
        selected_date = self.查詢日期選擇器.獲取日期()
        selected_expiry = self.到期日變數.get()
        
        if not selected_date:
            messagebox.showwarning("警告", "請選擇日期")
            return
        
        try:
            # 將日期格式從 YYYY/MM/DD 轉換為 YYYY-MM-DD
            db_date = selected_date.replace('/', '-')
            
            if selected_expiry:
                data = self.分析器.獲取期權鏈(db_date, selected_expiry)
            else:
                data = self.分析器.獲取期權鏈(db_date)
            
            # 清空Treeview
            for item in self.樹狀表格.get_children():
                self.樹狀表格.delete(item)
            
            # 插入數據
            if not data.empty:
                for _, row in data.iterrows():
                    values = (
                        row['strike_price'],
                        row['option_type'],
                        row['last_price'] if pd.notna(row['last_price']) else '-',
                        row['settlement_price'] if pd.notna(row['settlement_price']) else '-',
                        row['total_volume'] if pd.notna(row['total_volume']) else '-',
                        row['open_interest'] if pd.notna(row['open_interest']) else '-',
                        row['best_bid'] if pd.notna(row['best_bid']) else '-',
                        row['best_ask'] if pd.notna(row['best_ask']) else '-'
                    )
                    self.樹狀表格.insert('', tk.END, values=values)
                
                messagebox.showinfo("成功", f"找到 {len(data)} 條記錄")
            else:
                messagebox.showinfo("提示", "沒有找到相關數據")
                
        except Exception as e:
            messagebox.showerror("錯誤", f"查詢數據時發生錯誤: {str(e)}")
    
    def 導出數據(self):
        """導出數據"""
        selected_date = self.查詢日期選擇器.獲取日期()
        selected_expiry = self.到期日變數.get()
        
        if not selected_date:
            messagebox.showwarning("警告", "請先選擇日期")
            return
        
        # 選擇導出格式
        file_types = [
            ("CSV文件", "*.csv"),
            ("Excel文件", "*.xlsx"),
            ("JSON文件", "*.json")
        ]
        
        filename = filedialog.asksaveasfilename(
            title="導出數據",
            filetypes=file_types,
            defaultextension=".csv"
        )
        
        if not filename:
            return
        
        try:
            # 將日期格式從 YYYY/MM/DD 轉換為 YYYY-MM-DD
            db_date = selected_date.replace('/', '-')
            
            if selected_expiry:
                data = self.分析器.獲取期權鏈(db_date, selected_expiry)
            else:
                data = self.分析器.獲取期權鏈(db_date)
            
            if not data.empty:
                if filename.endswith('.csv'):
                    data.to_csv(filename, index=False, encoding='utf-8-sig')
                elif filename.endswith('.xlsx'):
                    data.to_excel(filename, index=False)
                elif filename.endswith('.json'):
                    data.to_json(filename, orient='records', force_ascii=False, indent=2)
                
                messagebox.showinfo("成功", f"數據已導出到: {filename}")
            else:
                messagebox.showwarning("警告", "沒有數據可以導出")
            
        except Exception as e:
            messagebox.showerror("錯誤", f"導出數據時發生錯誤: {str(e)}")

def 主程序():
    root = tk.Tk()
    app = TXO數據UI(root)
    root.mainloop()

if __name__ == "__main__":
    主程序()