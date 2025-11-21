# 檔名: 台灣期交所專用資料擷取器(可用) 判斷邏輯有修改 但 問題依舊.py
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, simpledialog, filedialog
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import os
import re
import logging
from datetime import datetime, timedelta
import json
import csv
import threading
from urllib.parse import urljoin, urlparse
import yfinance as yf
from typing import Dict, List, Any, Optional

# 創建Data資料夾
if not os.path.exists('Data'):
    os.makedirs('Data')

class FinancialDatabase:
    """金融資料庫管理系統"""
    
    def __init__(self, db_path: str = None):
        # 使用基於Python腳本位置的絕對路徑
        if db_path is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.db_path = os.path.join(base_dir, "Data", "financial.db")
        else:
            self.db_path = db_path
            
        self._ensure_data_directory()
        self._init_database()
        
    def _ensure_data_directory(self):
        """確保Data資料夾存在"""
        data_dir = os.path.dirname(self.db_path)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
    
    def _init_database(self):
        """初始化資料庫表格"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # 建立選擇權表格
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS options_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product TEXT NOT NULL,
                trade_date DATE NOT NULL,
                expiry TEXT NOT NULL,
                strike REAL NOT NULL,
                cp TEXT NOT NULL CHECK (cp IN ('C', 'P')),
                volume INTEGER DEFAULT 0,
                oi INTEGER,
                raw_oi_text TEXT,
                session TEXT DEFAULT 'regular' CHECK (session IN ('regular', 'after_hours')),
                load_file TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(trade_date, product, expiry, strike, cp, session)
            )
        ''')
        
        # 建立期貨表格
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS futures_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product TEXT NOT NULL,
                trade_date DATE NOT NULL,
                expiry TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER DEFAULT 0,
                oi INTEGER DEFAULT 0,
                settlement REAL,
                session TEXT DEFAULT 'regular' CHECK (session IN ('regular', 'after_hours')),
                load_file TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(trade_date, product, expiry, session)
            )
        ''')
        
        # 建立股票表格（新增chinese_name欄位）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stocks_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                chinese_name TEXT,
                trade_date DATE NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER DEFAULT 0,
                value REAL DEFAULT 0,
                load_file TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(trade_date, symbol)
            )
        ''')
        
        # 建立索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_options_trade_date ON options_raw(trade_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_options_product ON options_raw(product)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_options_expiry ON options_raw(expiry)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_options_strike ON options_raw(strike)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_futures_trade_date ON futures_raw(trade_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_futures_product ON futures_raw(product)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_futures_expiry ON futures_raw(expiry)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_stocks_trade_date ON stocks_raw(trade_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_stocks_symbol ON stocks_raw(symbol)')
        
        conn.commit()
        conn.close()
    
    def _get_connection(self):
        """取得資料庫連線"""
        return sqlite3.connect(self.db_path)
    
    def _restore_normal_settings(self):
        """恢復正常資料庫設定"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("PRAGMA synchronous = NORMAL")
            cursor.execute("PRAGMA journal_mode = WAL")
            conn.close()
        except Exception as e:
            logging.error(f"恢復正常設定失敗: {e}")
    
    # === 快速批次插入方法 ===
    def batch_insert_options_fast(self, options_list: List[Dict[str, Any]]) -> int:
        """快速批次插入選擇權資料 - 針對大量資料優化"""
        if not options_list:
            return 0
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 開始事務並優化設定
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("PRAGMA synchronous = OFF")
            cursor.execute("PRAGMA journal_mode = MEMORY")
            cursor.execute("PRAGMA cache_size = 10000")
            cursor.execute("PRAGMA temp_store = MEMORY")
            
            # 準備批次插入資料
            data_tuples = []
            for opt in options_list:
                data_tuples.append((
                    opt['product'],
                    opt['trade_date'],
                    opt['expiry'],
                    opt['strike'],
                    opt['cp'],
                    opt.get('volume', 0),
                    opt.get('oi'),
                    opt.get('raw_oi_text'),
                    opt.get('session', 'regular'),
                    opt.get('load_file')
                ))
            
            # 批次插入
            cursor.executemany('''
                INSERT OR IGNORE INTO options_raw 
                (product, trade_date, expiry, strike, cp, volume, oi, raw_oi_text, session, load_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_tuples)
            
            conn.commit()
            affected_rows = cursor.rowcount
            conn.close()
            
            # 恢復正常設定
            self._restore_normal_settings()
            return affected_rows
            
        except Exception as e:
            logging.error(f"批次插入選擇權資料失敗: {e}")
            self._restore_normal_settings()
            return 0
    
    def batch_insert_futures_fast(self, futures_list: List[Dict[str, Any]]) -> int:
        """快速批次插入期貨資料"""
        if not futures_list:
            return 0
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("PRAGMA synchronous = OFF")
            cursor.execute("PRAGMA journal_mode = MEMORY")
            
            data_tuples = []
            for future in futures_list:
                data_tuples.append((
                    future['product'],
                    future['trade_date'],
                    future['expiry'],
                    future.get('open'),
                    future.get('high'),
                    future.get('low'),
                    future.get('close'),
                    future.get('volume', 0),
                    future.get('oi', 0),
                    future.get('settlement'),
                    future.get('session', 'regular'),
                    future.get('load_file')
                ))
            
            cursor.executemany('''
                INSERT OR IGNORE INTO futures_raw 
                (product, trade_date, expiry, open, high, low, close, volume, oi, settlement, session, load_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_tuples)
            
            conn.commit()
            affected_rows = cursor.rowcount
            conn.close()
            
            self._restore_normal_settings()
            return affected_rows
            
        except Exception as e:
            logging.error(f"批次插入期貨資料失敗: {e}")
            self._restore_normal_settings()
            return 0
    
    def batch_insert_stocks_fast(self, stocks_list: List[Dict[str, Any]]) -> int:
        """快速批次插入股票資料"""
        if not stocks_list:
            return 0
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("PRAGMA synchronous = OFF")
            cursor.execute("PRAGMA journal_mode = MEMORY")
            
            data_tuples = []
            for stock in stocks_list:
                data_tuples.append((
                    stock['symbol'],
                    stock.get('chinese_name'),
                    stock['trade_date'],
                    stock.get('open'),
                    stock.get('high'),
                    stock.get('low'),
                    stock.get('close'),
                    stock.get('volume', 0),
                    stock.get('value', 0),
                    stock.get('load_file')
                ))
            
            cursor.executemany('''
                INSERT OR IGNORE INTO stocks_raw 
                (symbol, chinese_name, trade_date, open, high, low, close, volume, value, load_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_tuples)
            
            conn.commit()
            affected_rows = cursor.rowcount
            conn.close()
            
            self._restore_normal_settings()
            return affected_rows
            
        except Exception as e:
            logging.error(f"批次插入股票資料失敗: {e}")
            self._restore_normal_settings()
            return 0
    
    # === 查詢操作 ===
    def query_options(self, product=None, trade_date=None, expiry=None):
        """查詢選擇權資料"""
        conn = self._get_connection()
        query = "SELECT * FROM options_raw WHERE 1=1"
        params = []
        
        if product:
            query += " AND product = ?"
            params.append(product)
        if trade_date:
            query += " AND trade_date = ?"
            params.append(trade_date)
        if expiry:
            query += " AND expiry = ?"
            params.append(expiry)
        
        query += " ORDER BY trade_date DESC, strike, cp"
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    
    def query_futures(self, product=None, trade_date=None):
        """查詢期貨資料"""
        conn = self._get_connection()
        query = "SELECT * FROM futures_raw WHERE 1=1"
        params = []
        
        if product:
            query += " AND product = ?"
            params.append(product)
        if trade_date:
            query += " AND trade_date = ?"
            params.append(trade_date)
        
        query += " ORDER BY trade_date DESC"
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    
    def query_stocks(self, symbol=None, trade_date=None):
        """查詢股票資料"""
        conn = self._get_connection()
        query = "SELECT * FROM stocks_raw WHERE 1=1"
        params = []
        
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        if trade_date:
            query += " AND trade_date = ?"
            params.append(trade_date)
        
        query += " ORDER BY trade_date DESC"
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    
    # === 資料庫管理 ===
    def get_database_info(self):
        """取得資料庫資訊"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        info = {}
        
        # 取得各表格資料筆數
        cursor.execute("SELECT COUNT(*) FROM options_raw")
        info['options_count'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM futures_raw")
        info['futures_count'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM stocks_raw")
        info['stocks_count'] = cursor.fetchone()[0]
        
        # 取得資料日期範圍
        cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM options_raw")
        info['options_date_range'] = cursor.fetchone()
        
        cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM futures_raw")
        info['futures_date_range'] = cursor.fetchone()
        
        cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM stocks_raw")
        info['stocks_date_range'] = cursor.fetchone()
        
        conn.close()
        return info

class EnhancedTXODataScraper:
    def __init__(self, root):
        self.root = root
        self.root.title("台股市場資料擷取與分析工具 + 金融資料庫")
        self.root.geometry("1400x900")
        self.root.configure(bg='black')
        
        # 設定字體
        self.font_style = ("Microsoft JhengHei", 11)
        self.title_font = ("Microsoft JhengHei", 14, "bold")
        self.mono_font = ("Consolas", 10)
        
        # 儲存結構化資料
        self.structured_data = None
        self.current_url = ""
        self.analysis_results = None
        
        # 初始化金融資料庫
        self.database = FinancialDatabase()
        
        # 台股資料網址清單
        self.taiwan_market_urls = self.load_market_urls()
        
        self.setup_gui()
        
    def load_market_urls(self):
        """載入台股市場資料網址清單（分類整理）"""
        urls = {
            # === 高頻資料 (HF) ===
            "HF-選擇權日報表": "https://www.taifex.com.tw/cht/3/optDailyMarketReport",
            "HF-期貨日報表": "https://www.taifex.com.tw/cht/3/dlFutDailyMarketView",
            
            # === 選擇權資料 ===
            "選擇權-未平倉餘額": "https://www.taifex.com.tw/cht/3/optContractsDate",
            "選擇權-日報表": "https://www.taifex.com.tw/cht/3/optDailyMarketReport", 
            "選擇權-歷史資料": "https://www.taifex.com.tw/cht/3/optPrevious30DaysSalesData",
            "選擇權-買賣權分計": "https://www.taifex.com.tw/cht/3/callsAndPutsDate",
            
            # === 期貨資料 ===
            "期貨-日報表": "https://www.taifex.com.tw/cht/3/futDailyMarketReport",
            "期貨-歷史資料": "https://www.taifex.com.tw/cht/3/futPrevious30DaysSalesData",
            "期貨-未平倉餘額": "https://www.taifex.com.tw/cht/3/futContractsDate",
            
            # === 三大法人 ===
            "法人-期貨未平倉": "https://www.taifex.com.tw/cht/3/futContractsDate",
            "法人-選擇權未平倉": "https://www.taifex.com.tw/cht/3/optContractsDate",
            "法人-外資未平倉": "https://www.taifex.com.tw/cht/3/internationalTreats",
            
            # === 盤後資料下載 ===
            "盤後-期貨資料": "https://www.taifex.com.tw/cht/3/dlFutDataDown",
            "盤後-選擇權資料": "https://www.taifex.com.tw/cht/3/dlOptDataDown",
            "盤後-每筆成交": "https://www.taifex.com.tw/cht/3/dlFutTxfDown",
            
            # === 指數與波動率 ===
            "指數-波動率指數": "https://www.taifex.com.tw/cht/7/vixChart",
            "指數-盤後行情": "https://www.taifex.com.tw/cht/3/futMarketReport",
            
            # === 證交所資料 ===
            "證交所-個股日成交": "https://www.twse.com.tw/zh/page/trading/exchange/STOCK_DAY.html",
            "證交所-三大法人": "https://www.twse.com.tw/zh/page/trading/fund/BFI82U.html",
            "證交所-融資融券": "https://www.twse.com.tw/zh/page/trading/exchange/MI_MARGN.html",
            "證交所-股價指數": "https://www.twse.com.tw/zh/page/trading/indices/MI_5MINS_HIST.html",
            "證交所-個股週轉率": "https://www.twse.com.tw/zh/page/trading/exchange/STOCK_DAY_AVG.html",
            
            # === 櫃買中心 ===
            "櫃買-個股日成交": "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php",
            "櫃買-三大法人": "https://www.tpex.org.tw/web/stock/3insti/3insti_summary/3itrdsum_result.php",
            
            # === 公開資訊觀測站 ===
            "公開資訊-財務報表": "https://mops.twse.com.tw/mops/web/t51sb01",
            
            # === Yahoo Finance ===
            "YF-台股大盤": "https://finance.yahoo.com/quote/%5ETWII/history/",
            "YF-台積電": "https://finance.yahoo.com/quote/2330.TW/history/",
            "YF-聯發科": "https://finance.yahoo.com/quote/2454.TW/history/",
            "YF-鴻海": "https://finance.yahoo.com/quote/2317.TW/history/"
        }
        return urls

    def setup_gui(self):
        """設定圖形化使用者介面"""
        # 主框架
        main_frame = tk.Frame(self.root, bg='black')
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 標題
        title_label = tk.Label(main_frame, text="台股市場資料擷取與分析工具 + 金融資料庫", 
                              font=self.title_font, fg='white', bg='black')
        title_label.pack(pady=10)
        
        # URL選擇框架
        url_select_frame = tk.Frame(main_frame, bg='black')
        url_select_frame.pack(fill=tk.X, pady=5)
        
        tk.Label(url_select_frame, text="選擇資料來源:", font=self.font_style, 
                fg='white', bg='black').pack(side=tk.LEFT)
        
        # 建立下拉選單
        self.url_var = tk.StringVar()
        self.url_combo = ttk.Combobox(url_select_frame, textvariable=self.url_var, 
                                     font=self.font_style, width=80, state="readonly")
        self.url_combo['values'] = list(self.taiwan_market_urls.keys())
        self.url_combo.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        self.url_combo.bind('<<ComboboxSelected>>', self.on_url_selected)
        
        # 自訂URL框架
        custom_url_frame = tk.Frame(main_frame, bg='black')
        custom_url_frame.pack(fill=tk.X, pady=5)
        
        tk.Label(custom_url_frame, text="或輸入自訂網址:", font=self.font_style, 
                fg='white', bg='black').pack(side=tk.LEFT)
        
        self.custom_url_var = tk.StringVar()
        self.custom_url_entry = tk.Entry(custom_url_frame, textvariable=self.custom_url_var, 
                                        font=self.font_style, width=80, bg='white', fg='black')
        self.custom_url_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        self.custom_url_entry.bind('<Return>', self.on_custom_url_entered)
        
        # 功能按鈕框架
        button_frame = tk.Frame(main_frame, bg='black')
        button_frame.pack(fill=tk.X, pady=10)
        
        # 第一排按鈕：網頁分析功能
        buttons_row1 = [
            ("🔍 分析下載連結", self.analyze_download_links),
            ("📊 擷取並解析", self.fetch_and_parse),
            ("📈 顯示結構化資料", self.show_structured_data),
            ("💾 匯出JSON", self.export_structured_json),
            ("📁 匯出CSV", self.export_structured_csv),
        ]
        
        for text, command in buttons_row1:
            tk.Button(button_frame, text=text, font=self.font_style, 
                     command=command, bg='white', fg='black').pack(side=tk.LEFT, padx=2)
        
        # 第二排按鈕：資料庫功能
        button_frame2 = tk.Frame(main_frame, bg='black')
        button_frame2.pack(fill=tk.X, pady=5)
        
        buttons_row2 = [
            ("🗃️ 資料庫資訊", self.show_database_info),
            ("📥 匯入CSV到資料庫", self.import_csv_to_database),
            ("📤 匯出資料庫查詢", self.export_database_query),
            ("🔍 查詢選擇權", self.query_options),
            ("🔍 查詢期貨", self.query_futures),
            ("🔍 查詢股票", self.query_stocks),
        ]
        
        for text, command in buttons_row2:
            tk.Button(button_frame2, text=text, font=self.font_style, 
                     command=command, bg='lightblue', fg='black').pack(side=tk.LEFT, padx=2)
        
        # 結果顯示區域
        result_frame = tk.Frame(main_frame, bg='black')
        result_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # 建立分頁
        self.notebook = ttk.Notebook(result_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # 分析結果分頁
        self.analysis_text = scrolledtext.ScrolledText(
            self.notebook, font=self.mono_font, bg='black', fg='white'
        )
        self.notebook.add(self.analysis_text, text="📊 下載連結分析")
        
        # 原始資料分頁
        self.raw_text = scrolledtext.ScrolledText(
            self.notebook, font=self.mono_font, bg='black', fg='white'
        )
        self.notebook.add(self.raw_text, text="原始資料")
        
        # 結構化資料分頁
        self.structured_text = scrolledtext.ScrolledText(
            self.notebook, font=self.mono_font, bg='black', fg='white'
        )
        self.notebook.add(self.structured_text, text="結構化資料")
        
        # 資料庫分頁
        self.database_text = scrolledtext.ScrolledText(
            self.notebook, font=self.mono_font, bg='black', fg='white'
        )
        self.notebook.add(self.database_text, text="🗃️ 資料庫")
        
        # 狀態欄
        self.status_var = tk.StringVar(value="就緒 - 請選擇資料來源")
        status_bar = tk.Label(main_frame, textvariable=self.status_var, 
                             font=self.font_style, fg='white', bg='black', 
                             anchor=tk.W)
        status_bar.pack(fill=tk.X)
        
        # 設定預設選項
        self.url_combo.set("HF-選擇權日報表")
        self.on_url_selected()

    def on_url_selected(self, event=None):
        """當選擇預設網址時"""
        selected = self.url_var.get()
        if selected in self.taiwan_market_urls:
            url = self.taiwan_market_urls[selected]
            self.custom_url_var.set(url)
            self.update_status(f"已選擇: {selected}")

    def on_custom_url_entered(self, event=None):
        """當輸入自訂網址時"""
        custom_url = self.custom_url_var.get().strip()
        if custom_url:
            self.url_var.set("")  # 清空下拉選單選擇
            self.update_status(f"已輸入自訂網址: {custom_url}")

    def get_current_url(self):
        """取得當前網址"""
        custom_url = self.custom_url_var.get().strip()
        if custom_url:
            return custom_url
        selected = self.url_var.get()
        return self.taiwan_market_urls.get(selected, "")

    def update_status(self, message):
        """更新狀態欄"""
        self.status_var.set(message)
        self.root.update()

    def analyze_download_links(self):
        """分析網頁中的下載連結"""
        url = self.get_current_url()
        if not url:
            messagebox.showwarning("警告", "請選擇或輸入網址")
            return
            
        try:
            self.update_status("正在分析下載連結...")
            self.current_url = url
            
            # 在背景執行分析
            thread = threading.Thread(target=self._analyze_in_thread, args=(url,))
            thread.daemon = True
            thread.start()
            
        except Exception as e:
            error_msg = f"分析失敗: {e}"
            logging.error(error_msg)
            messagebox.showerror("錯誤", error_msg)
            self.update_status("分析失敗")

    def _analyze_in_thread(self, url):
        """在背景執行分析"""
        try:
            results = self.monitor_requests(url)
            self.root.after(0, self._display_analysis_results, results, url)
        except Exception as e:
            self.root.after(0, self._analysis_failed, str(e))

    def _display_analysis_results(self, results, url):
        """顯示分析結果"""
        self.update_status("分析完成")
        
        if not results:
            messagebox.showerror("錯誤", "分析失敗")
            return
        
        self.analysis_results = results
        
        # 顯示分析結果
        self.analysis_text.delete(1.0, tk.END)
        
        # 找出可能包含表格的連結（優先顯示）
        table_potential_links = []
        table_keywords = ['report', 'data', 'market', 'daily', '歷史', '報表', '資料', 'csv', 'excel', 'download', 'export', '下載', '匯出']
        
        overview_content = f"""🌐 網頁分析結果: {url}
分析時間: {results['analysis_time']}
{'='*60}

📊 統計資訊:
• 找到下載連結: {len(results['download_links'])} 個
• 找到表單: {len(results['forms'])} 個
• 找到JavaScript下載功能: {len(results['js_downloads'])} 個

💡 分析摘要:
"""
        
        if results['download_links']:
            overview_content += "✅ 發現直接下載連結\n"
            
            # 找出可能包含表格的連結
            for link in results['download_links']:
                for keyword in table_keywords:
                    if keyword in link['url'].lower() or keyword in link['text'].lower():
                        table_potential_links.append(link)
                        break
        else:
            overview_content += "❌ 未發現直接下載連結\n"
            
        if any(form['likely_download'] for form in results['forms']):
            overview_content += "✅ 發現可能的下載表單\n"
        else:
            overview_content += "❌ 未發現下載表單\n"
            
        if results['js_downloads']:
            overview_content += "✅ 發現JavaScript下載功能\n"
        else:
            overview_content += "❌ 未發現JavaScript下載功能\n"
        
        # 優先顯示可能包含表格的連結
        if table_potential_links:
            overview_content += f"\n🔍 發現 {len(table_potential_links)} 個可能包含表格資料的連結 (優先處理):\n"
            overview_content += "="*60 + "\n"
            for i, link in enumerate(table_potential_links, 1):
                overview_content += f"{i}. {link['text']}\n"
                overview_content += f"   📍 URL: {link['url']}\n"
                overview_content += f"   🏷️ 類型: {link['type']}\n"
                overview_content += "-" * 40 + "\n"
        
        self.analysis_text.insert(tk.END, overview_content)
        
        # 顯示所有下載連結
        if results['download_links']:
            links_content = "\n🔗 所有下載連結:\n" + "="*50 + "\n\n"
            for i, link in enumerate(results['download_links'], 1):
                links_content += f"{i}. {link['text']}\n"
                links_content += f"   📍 URL: {link['url']}\n"
                links_content += f"   🏷️ 類型: {link['type']}\n"
                links_content += f"   🔍 關鍵字: {link['keyword']}\n"
                links_content += "-" * 40 + "\n"
            
            self.analysis_text.insert(tk.END, links_content)
        
        messagebox.showinfo("完成", "網頁分析完成！請查看『下載連結分析』分頁")

    def _analysis_failed(self, error_msg):
        """分析失敗處理"""
        self.update_status("分析失敗")
        messagebox.showerror("錯誤", f"分析失敗:\n{error_msg}")

    def monitor_requests(self, target_url):
        """監控網頁請求"""
        try:
            session = requests.Session()
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
            }
            
            response = session.get(target_url, headers=headers)
            response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            download_links = self._find_download_links(soup, target_url)
            forms = self._analyze_forms(soup, target_url)
            js_downloads = self._find_js_downloads(soup)
            
            results = {
                'analysis_time': datetime.now().isoformat(),
                'target_url': target_url,
                'download_links': download_links,
                'forms': forms,
                'js_downloads': js_downloads
            }
            
            return results
            
        except Exception as e:
            print(f"分析失敗: {e}")
            return None

    def _find_download_links(self, soup, base_url):
        download_keywords = ['download', 'csv', 'excel', 'data', 'export', '下載', '匯出', 'report', '歷史', 'xls', 'xlsx']
        download_links = []
        
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            link_text = link.get_text().lower()
            
            for keyword in download_keywords:
                if keyword in href or keyword in link_text:
                    full_url = urljoin(base_url, link['href'])
                    download_links.append({
                        'url': full_url,
                        'text': link.get_text().strip(),
                        'type': 'direct_link',
                        'keyword': keyword
                    })
                    break
        
        return download_links

    def _analyze_forms(self, soup, base_url):
        forms_info = []
        
        for form in soup.find_all('form'):
            form_info = {
                'action': form.get('action', ''),
                'method': form.get('method', 'get').upper(),
                'inputs': [],
                'full_url': '',
                'likely_download': False
            }
            
            if form_info['action']:
                form_info['full_url'] = urljoin(base_url, form_info['action'])
            else:
                form_info['full_url'] = base_url
            
            for input_tag in form.find_all(['input', 'select', 'textarea']):
                input_info = {
                    'type': input_tag.name,
                    'name': input_tag.get('name', ''),
                    'value': input_tag.get('value', ''),
                    'input_type': input_tag.get('type', '')
                }
                form_info['inputs'].append(input_info)
            
            # 簡單判斷是否為下載表單
            action_lower = form_info['action'].lower()
            download_indicators = ['download', 'export', 'csv', 'excel', 'data', '下載', '匯出']
            for indicator in download_indicators:
                if indicator in action_lower:
                    form_info['likely_download'] = True
                    break
            
            forms_info.append(form_info)
        
        return forms_info

    def _find_js_downloads(self, soup):
        js_downloads = []
        download_keywords = ['download', 'csv', 'export', 'DataDown', 'getData', '下載', '匯出', 'excel', 'xls']
        
        for script in soup.find_all('script'):
            if script.string:
                script_content = script.string.lower()
                for keyword in download_keywords:
                    if keyword in script_content:
                        js_downloads.append({
                            'type': 'javascript',
                            'keyword': keyword,
                            'snippet': script.string[:200] + '...' if len(script.string) > 200 else script.string
                        })
                        break
        
        return js_downloads

    def fetch_and_parse(self):
        """擷取網頁並解析為結構化資料"""
        url = self.get_current_url()
        if not url:
            messagebox.showwarning("警告", "請選擇或輸入網址")
            return
            
        try:
            self.update_status("正在連接網站...")
            self.current_url = url
            
            # 設定請求頭
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            }
            
            # 發送請求
            response = requests.get(url, headers=headers, timeout=30)
            response.encoding = 'utf-8'
            response.raise_for_status()
            
            # 顯示原始資料
            self.raw_text.delete(1.0, tk.END)
            self.raw_text.insert(tk.END, f"URL: {url}\n")
            self.raw_text.insert(tk.END, f"狀態碼: {response.status_code}\n")
            self.raw_text.insert(tk.END, f"資料長度: {len(response.text)} 字元\n\n")
            self.raw_text.insert(tk.END, response.text[:5000] + "\n...")  # 顯示前5000字元
            
            # 解析為結構化資料
            self.structured_data = self.parse_to_structured_data(response.text, url)
            
            self.update_status(f"成功解析為結構化資料，找到 {len(self.structured_data['tables'])} 個表格")
            
            # 顯示結構化資料
            self.show_structured_data()
            
        except Exception as e:
            error_msg = f"擷取解析失敗: {e}"
            logging.error(error_msg)
            messagebox.showerror("錯誤", error_msg)
            self.update_status("擷取失敗")

    def parse_to_structured_data(self, html_content, url):
        """將HTML解析為真正的結構化資料"""
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        
        structured_data = {
            'metadata': {
                'source_url': url,
                'scrape_time': datetime.now().isoformat(),
                'total_tables': len(tables),
                'data_format': 'structured_v1'
            },
            'tables': []
        }
        
        for i, table in enumerate(tables):
            table_data = self.parse_single_table(table, i + 1)
            if table_data:
                structured_data['tables'].append(table_data)
        
        return structured_data

    def parse_single_table(self, table, table_index):
        """解析單一表格為結構化資料"""
        try:
            # 提取表頭
            headers = []
            header_rows = table.find_all(['th', 'td'])
            for th in header_rows:
                header_text = th.get_text(strip=True)
                if header_text:
                    headers.append(header_text)
            
            # 提取資料行
            data_rows = []
            for tr in table.find_all('tr'):
                row_data = []
                for td in tr.find_all(['td', 'th']):
                    cell_text = td.get_text(strip=True)
                    row_data.append(cell_text)
                
                if row_data and len(row_data) > 1:  # 過濾空行和只有一個欄位的行
                    data_rows.append(row_data)
            
            if not data_rows:
                return None
            
            # 建立結構化資料
            table_structure = {
                'table_index': table_index,
                'columns': headers if headers else [f'Column_{j+1}' for j in range(len(data_rows[0]))],
                'row_count': len(data_rows),
                'data': []
            }
            
            # 轉換為字典格式
            for row in data_rows:
                if len(row) == len(table_structure['columns']):
                    row_dict = {}
                    for j, value in enumerate(row):
                        column_name = table_structure['columns'][j] if j < len(table_structure['columns']) else f'Column_{j+1}'
                        row_dict[column_name] = value
                    table_structure['data'].append(row_dict)
                else:
                    # 處理欄位數量不匹配的情況
                    row_dict = {}
                    for j, value in enumerate(row):
                        column_name = f'Column_{j+1}'
                        row_dict[column_name] = value
                    table_structure['data'].append(row_dict)
            
            return table_structure
            
        except Exception as e:
            logging.error(f"解析表格 {table_index} 失敗: {e}")
            return None

    def show_structured_data(self):
        """顯示結構化資料"""
        if not self.structured_data:
            messagebox.showwarning("警告", "沒有可顯示的結構化資料")
            return
            
        self.structured_text.delete(1.0, tk.END)
        
        # 顯示元資料
        metadata = self.structured_data['metadata']
        self.structured_text.insert(tk.END, "=== 元資料 ===\n")
        self.structured_text.insert(tk.END, f"來源網址: {metadata['source_url']}\n")
        self.structured_text.insert(tk.END, f"擷取時間: {metadata['scrape_time']}\n")
        self.structured_text.insert(tk.END, f"表格數量: {metadata['total_tables']}\n\n")
        
        # 顯示每個表格的結構化資料
        for table in self.structured_data['tables']:
            self.structured_text.insert(tk.END, f"=== 表格 {table['table_index']} ===\n")
            self.structured_text.insert(tk.END, f"欄位: {table['columns']}\n")
            self.structured_text.insert(tk.END, f"資料筆數: {table['row_count']}\n")
            self.structured_text.insert(tk.END, "前5筆資料:\n")
            
            # 顯示前5筆資料
            for i, row in enumerate(table['data'][:5]):
                self.structured_text.insert(tk.END, f"第{i+1}筆: {row}\n")
            
            self.structured_text.insert(tk.END, "\n")

    def export_structured_json(self):
        """匯出結構化JSON資料"""
        if not self.structured_data:
            messagebox.showwarning("警告", "沒有可匯出的結構化資料")
            return
            
        try:
            filename = f"Data/structured_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.structured_data, f, ensure_ascii=False, indent=2)
            
            messagebox.showinfo("成功", f"結構化資料已匯出至: {filename}")
            self.update_status(f"JSON匯出完成: {filename}")
            
        except Exception as e:
            error_msg = f"匯出JSON失敗: {e}"
            messagebox.showerror("錯誤", error_msg)

    def export_structured_csv(self):
        """匯出結構化CSV資料"""
        if not self.structured_data:
            messagebox.showwarning("警告", "沒有可匯出的結構化資料")
            return
            
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            for table in self.structured_data['tables']:
                filename = f"Data/table_{table['table_index']}_{timestamp}.csv"
                
                # 轉換為DataFrame
                df = pd.DataFrame(table['data'])
                df.to_csv(filename, index=False, encoding='utf-8-sig')
            
            messagebox.showinfo("成功", f"CSV資料已匯出至Data資料夾")
            self.update_status("CSV匯出完成")
            
        except Exception as e:
            error_msg = f"匯出CSV失敗: {e}"
            messagebox.showerror("錯誤", error_msg)

    # === 資料庫相關方法（改進版本）===
    def show_database_info(self):
        """顯示資料庫資訊"""
        try:
            info = self.database.get_database_info()
            
            self.database_text.delete(1.0, tk.END)
            content = "=== 金融資料庫資訊 ===\n\n"
            content += f"📊 選擇權資料筆數: {info['options_count']:,} 筆\n"
            content += f"📈 期貨資料筆數: {info['futures_count']:,} 筆\n"
            content += f"🏢 股票資料筆數: {info['stocks_count']:,} 筆\n\n"
            
            content += "📅 資料日期範圍:\n"
            if info['options_date_range'][0]:
                content += f"   選擇權: {info['options_date_range'][0]} 至 {info['options_date_range'][1]}\n"
            if info['futures_date_range'][0]:
                content += f"   期貨: {info['futures_date_range'][0]} 至 {info['futures_date_range'][1]}\n"
            if info['stocks_date_range'][0]:
                content += f"   股票: {info['stocks_date_range'][0]} 至 {info['stocks_date_range'][1]}\n"
            
            content += f"\n💾 資料庫位置: {self.database.db_path}"
            
            self.database_text.insert(tk.END, content)
            self.notebook.select(3)  # 切換到資料庫分頁
            self.update_status("資料庫資訊載入完成")
            
        except Exception as e:
            messagebox.showerror("錯誤", f"取得資料庫資訊失敗: {e}")

    def import_csv_to_database(self):
        """快速匯入CSV檔案到資料庫 - 改進的智能分類邏輯"""
        try:
            file_path = filedialog.askopenfilename(
                title="選擇CSV檔案",
                filetypes=[("CSV檔案", "*.csv"), ("所有檔案", "*.*")],
                initialdir="Data"
            )
            
            if not file_path:
                return
            
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
            self.update_status(f"開始匯入 {file_size:.1f}MB 的CSV檔案...")
            
            # 根據檔案大小決定chunk大小
            chunk_size = 50000 if file_size > 10 else 10000
            
            total_imported = 0
            start_time = datetime.now()
            
            # 分批讀取大檔案
            for chunk_num, chunk_df in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
                self.update_status(f"處理第 {chunk_num + 1} 批資料 ({len(chunk_df)} 筆)...")
                
                filename = os.path.basename(file_path)
                import_count = self._process_data_chunk_fast(chunk_df, filename)
                total_imported += import_count
                
                # 顯示進度
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = total_imported / elapsed if elapsed > 0 else 0
                self.update_status(f"已處理: {total_imported:,} 筆, 速度: {rate:.1f} 筆/秒")
                
                # 更新介面
                self.root.update()
            
            elapsed_time = (datetime.now() - start_time).total_seconds()
            messagebox.showinfo("完成", 
                              f"匯入完成！\n"
                              f"總共匯入: {total_imported:,} 筆資料\n"
                              f"花費時間: {elapsed_time:.1f} 秒\n"
                              f"平均速度: {total_imported/elapsed_time:.1f} 筆/秒")
            
            self.update_status(f"快速匯入完成: {total_imported:,} 筆資料")
            
        except Exception as e:
            messagebox.showerror("錯誤", f"匯入CSV失敗: {e}")
            self.update_status("匯入失敗")
    
    def _process_data_chunk_fast(self, chunk_df, filename):
        """改進的自動分類邏輯 - 同時檢查第一列和第二列，保存股票代號資訊"""
        
        # 保存原始的第0行內容（可能包含股票代號）
        original_first_row = None
        if len(chunk_df) > 0:
            original_first_row = chunk_df.iloc[0].tolist()
        
        # 1. 計算分數（先檢查第一列，分數不足才檢查第二列）
        score_first_row = self._calculate_data_score(chunk_df.columns.tolist())
        score_second_row = 0
        if score_first_row < 3 and original_first_row:  # 第一列分數不足才檢查第二列
            score_second_row = self._calculate_data_score(original_first_row)
        
        symbol_info = None
        data_start_index = 0
        
        # 2. 決策邏輯
        if score_first_row >= score_second_row:
            # 使用第一列作為欄位名稱
            column_names = set([str(col).lower() for col in chunk_df.columns.tolist()])
            data_start_index = 0
            self.update_status("使用第一列作為欄位名稱")
        else:
            # 使用第二列作為欄位名稱，但先從被跳過的行提取股票代號
            column_names = set([str(col).lower() for col in original_first_row])
            data_start_index = 1
            
            # 從被跳過的第0行和第1行提取股票代號
            symbol_info = self._extract_symbol_from_skipped_rows(chunk_df.columns.tolist(), original_first_row)
            
            # 重新建立DataFrame（跳過第0行）
            chunk_df = chunk_df.iloc[1:].reset_index(drop=True)
            self.update_status("使用第二列作為欄位名稱，跳過第一行文字說明")
        
        # 3. 計算各類資料分數
        option_indicators = {'cp', 'call/put', '買賣權', 'strike', '履約價', 'expiry', '到期'}
        future_indicators = {'settlement', '結算價', 'oi', '未平倉', '留倉'}
        stock_indicators = {'open', 'high', 'low', 'close', 'volume', 'value', '成交金額', '開盤', '最高', '最低', '收盤', '成交量'}
        
        option_score = len(column_names & option_indicators)
        future_score = len(column_names & future_indicators) 
        stock_score = len(column_names & stock_indicators)
        
        # 4. 根據分數決定資料類型
        if option_score >= 2:
            self.update_status(f"識別為選擇權資料 (分數: {option_score})")
            return self._import_as_options(chunk_df, filename)
            
        elif future_score >= 2 and option_score == 0:
            self.update_status(f"識別為期貨資料 (分數: {future_score})")
            return self._import_as_futures(chunk_df, filename)
            
        elif stock_score >= 2 and option_score == 0 and future_score == 0:
            self.update_status(f"識別為股票資料 (分數: {stock_score})")
            # 如果是股票資料，使用提取的股票代號資訊
            if symbol_info:
                return self._import_as_stocks_with_symbol(chunk_df, filename, symbol_info)
            else:
                # 即使使用第一列作為欄位名稱，也可能包含股票代號
                if data_start_index == 0:
                    symbol_info = self._extract_symbol_from_header(chunk_df.columns.tolist())
                    if symbol_info:
                        return self._import_as_stocks_with_symbol(chunk_df, filename, symbol_info)
                return self._import_as_stocks(chunk_df, filename)
            
        else:
            # 無法明確判斷，嘗試股票代號自動辨識
            self.update_status("第一層無法判斷，進入第二層股票代號辨識")
            symbol_info = self._auto_detect_stock_symbol(chunk_df)
            if symbol_info:
                self.update_status(f"第二層辨識成功: {symbol_info['symbol']} {symbol_info['chinese_name']}")
                return self._import_as_stocks_with_symbol(chunk_df, filename, symbol_info)
            else:
                # 讓使用者選擇
                return self._ask_user_for_data_type(chunk_df, filename)

    def _extract_symbol_from_skipped_rows(self, first_row, second_row):
        """從被跳過的第0行和第1行中提取股票代號"""
        # 同時檢查第0行和第1行
        symbol_info = self._extract_symbol_from_header(first_row)
        if not symbol_info:
            symbol_info = self._extract_symbol_from_header(second_row)
        
        return symbol_info

    def _extract_symbol_from_header(self, header_columns):
        """從表頭辨識股票代號 - 精確版本"""
        import re
        
        for i, col in enumerate(header_columns):
            if isinstance(col, str):
                # 精確正則：在整個字串中尋找「空格或開頭 + 數字代號 + 空格 + 中文名稱」
                pattern = r'(?:\s|^)(\d{3,6}[A-Za-z]*)\s+([\u4e00-\u9fff]+)'
                # 解釋：
                # (?:\s|^) → 空格或字串開頭（非捕獲組）
                # (\d{3,6}[A-Za-z]*) → 3-6位數字，可能包含英文（股票代號）
                # \s+ → 1個或多個空格
                # ([\u4e00-\u9fff]+) → 中文名稱
                
                match = re.search(pattern, col)
                
                if match:
                    symbol = match.group(1).strip()
                    chinese_name = match.group(2).strip()
                    
                    if self._is_valid_tw_stock_symbol(symbol):
                        return {
                            'symbol': symbol,
                            'chinese_name': chinese_name,
                            'found_in': 'header',
                            'column_index': i,
                            'original_text': col
                        }
        
        return None

    def _extract_symbol_from_data(self, data_row):
        """從資料行辨識股票代號"""
        import re
        
        for i, cell in enumerate(data_row):
            if isinstance(cell, str):
                # 同樣的精確正則
                pattern = r'(?:\s|^)(\d{3,6}[A-Za-z]*)\s+([\u4e00-\u9fff]+)'
                match = re.search(pattern, cell)
                
                if match:
                    symbol = match.group(1).strip()
                    chinese_name = match.group(2).strip()
                    
                    if self._is_valid_tw_stock_symbol(symbol):
                        return {
                            'symbol': symbol,
                            'chinese_name': chinese_name,
                            'found_in': 'data',
                            'column_index': i
                        }
        
        return None

    def _is_valid_tw_stock_symbol(self, symbol):
        """驗證是否為有效的台股股票代號"""
        import re
        
        # 長度檢查
        if len(symbol) < 3 or len(symbol) > 6:
            return False
        
        # 格式檢查：必須以數字開頭，可能包含英文
        if not re.match(r'^\d+[A-Za-z]*$', symbol):
            return False
        
        # 常見的台股代號長度
        if len(symbol) in [4, 5, 6]:
            return True
        
        return False

    def _calculate_data_score(self, columns):
        """計算一組文字作為欄位名稱的可信度分數"""
        if not columns:
            return 0
        
        score = 0
        column_texts = [str(col).lower() for col in columns]
        
        # 常見的股票資料欄位關鍵字
        stock_keywords = {
            'open', 'high', 'low', 'close', 'volume', 'value',
            '開盤', '最高', '最低', '收盤', '成交量', '成交金額',
            '日期', 'date', '代號', 'symbol', '名稱', 'name'
        }
        
        # 常見的選擇權/期貨欄位關鍵字
        option_future_keywords = {
            'cp', 'call', 'put', 'strike', '履約價', 'expiry', '到期',
            'settlement', '結算價', 'oi', '未平倉', '留倉'
        }
        
        # 常見的非欄位名稱文字（文字說明）
        non_column_keywords = {
            '報告', '報表', '資料', '統計', '明細', '表', '年度', '月份',
            '公司', '股票', '證券', '交易', '市場', '行情', '投資'
        }
        
        # 計算分數
        for text in column_texts:
            # 如果包含股票欄位關鍵字，加分
            if any(keyword in text for keyword in stock_keywords):
                score += 2
            
            # 如果包含選擇權/期貨關鍵字，加分
            if any(keyword in text for keyword in option_future_keywords):
                score += 2
                
            # 如果看起來像欄位名稱（簡短、英文或簡短中文）
            if len(text) <= 12 and not any(non_word in text for non_word in non_column_keywords):
                score += 1
                
            # 如果看起來像資料內容（長文字、數字等），減分
            if len(text) > 20 or text.replace('.', '').replace(',', '').replace('-', '').isdigit():
                score -= 1
        
        return max(0, score)

    def _auto_detect_stock_symbol(self, chunk_df):
        """自動辨識股票代號和中文名稱"""
        try:
            # 檢查第一列（表頭）
            first_row_symbol = self._extract_symbol_from_header(chunk_df.columns.tolist())
            if first_row_symbol:
                return first_row_symbol
            
            # 如果第一列沒有，檢查第二列（第一筆資料）
            if len(chunk_df) > 0:
                second_row_symbol = self._extract_symbol_from_data(chunk_df.iloc[0])
                if second_row_symbol:
                    return second_row_symbol
                    
            return None
            
        except Exception as e:
            logging.error(f"股票代號自動辨識失敗: {e}")
            return None

    def _ask_user_for_data_type(self, chunk_df, filename):
        """讓使用者選擇資料類型"""
        # 顯示前幾行資料讓使用者確認
        preview = "CSV前3行預覽：\n"
        preview += f"欄位名稱: {chunk_df.columns.tolist()}\n"
        if len(chunk_df) > 0:
            preview += f"第一行資料: {chunk_df.iloc[0].tolist()}\n"
        if len(chunk_df) > 1:
            preview += f"第二行資料: {chunk_df.iloc[1].tolist()}\n"
        
        choice = simpledialog.askstring(
            "選擇資料類型",
            f"{preview}\n"
            "無法自動判斷資料類型，請選擇：\n"
            "1. 選擇權 (options)\n"
            "2. 期貨 (futures)\n" 
            "3. 股票 (stocks)\n\n"
            "請輸入選擇 (1/2/3):"
        )
        
        if choice == '1':
            return self._import_as_options(chunk_df, filename)
        elif choice == '2':
            return self._import_as_futures(chunk_df, filename)
        elif choice == '3':
            return self._import_as_stocks(chunk_df, filename)
        else:
            return 0

    def _import_as_options(self, chunk_df, filename):
        """匯入選擇權資料"""
        options_list = []
        for _, row in chunk_df.iterrows():
            options_list.append({
                'product': row.get('product', 'TXO'),
                'trade_date': row.get('trade_date', datetime.now().strftime('%Y-%m-%d')),
                'expiry': row.get('expiry', ''),
                'strike': row.get('strike', 0),
                'cp': row.get('cp', 'C'),
                'volume': row.get('volume', 0),
                'oi': row.get('oi'),
                'raw_oi_text': row.get('raw_oi_text', ''),
                'load_file': filename
            })
        
        return self.database.batch_insert_options_fast(options_list)

    def _import_as_futures(self, chunk_df, filename):
        """匯入期貨資料"""
        futures_list = []
        for _, row in chunk_df.iterrows():
            futures_list.append({
                'product': row.get('product', 'TXF'),
                'trade_date': row.get('trade_date', datetime.now().strftime('%Y-%m-%d')),
                'expiry': row.get('expiry', ''),
                'open': row.get('open'),
                'high': row.get('high'),
                'low': row.get('low'),
                'close': row.get('close'),
                'volume': row.get('volume', 0),
                'oi': row.get('oi', 0),
                'settlement': row.get('settlement'),
                'load_file': filename
            })
        
        return self.database.batch_insert_futures_fast(futures_list)

    def _import_as_stocks(self, chunk_df, filename):
        """匯入股票資料"""
        stocks_list = []
        for _, row in chunk_df.iterrows():
            stocks_list.append({
                'symbol': row.get('symbol', ''),
                'trade_date': row.get('trade_date', datetime.now().strftime('%Y-%m-%d')),
                'open': row.get('open'),
                'high': row.get('high'),
                'low': row.get('low'),
                'close': row.get('close'),
                'volume': row.get('volume', 0),
                'value': row.get('value', 0),
                'load_file': filename
            })
        
        return self.database.batch_insert_stocks_fast(stocks_list)

    def _import_as_stocks_with_symbol(self, chunk_df, filename, symbol_info):
        """使用自動辨識的股票代號匯入股票資料"""
        stocks_list = []
        for _, row in chunk_df.iterrows():
            stocks_list.append({
                'symbol': symbol_info['symbol'],
                'chinese_name': symbol_info['chinese_name'],
                'trade_date': row.get('trade_date', datetime.now().strftime('%Y-%m-%d')),
                'open': row.get('open'),
                'high': row.get('high'),
                'low': row.get('low'),
                'close': row.get('close'),
                'volume': row.get('volume', 0),
                'value': row.get('value', 0),
                'load_file': filename
            })
        
        return self.database.batch_insert_stocks_fast(stocks_list)

    def export_database_query(self):
        """匯出資料庫查詢結果"""
        try:
            # 選擇匯出類型
            export_type = simpledialog.askstring("匯出查詢", "請輸入查詢類型 (options/futures/stocks):")
            if not export_type:
                return
                
            # 執行查詢
            if export_type.lower() == 'options':
                df = self.database.query_options()
            elif export_type.lower() == 'futures':
                df = self.database.query_futures()
            elif export_type.lower() == 'stocks':
                df = self.database.query_stocks()
            else:
                messagebox.showwarning("警告", "不支援的查詢類型")
                return
            
            # 匯出檔案
            filename = f"Data/{export_type}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            
            messagebox.showinfo("成功", f"查詢結果已匯出至: {filename}")
            self.update_status(f"資料庫查詢結果已匯出: {filename}")
            
        except Exception as e:
            messagebox.showerror("錯誤", f"匯出查詢失敗: {e}")

    def query_options(self):
        """查詢選擇權資料"""
        try:
            product = simpledialog.askstring("查詢選擇權", "商品代碼 (TXO/CAO/CNO，留空查詢所有):")
            trade_date = simpledialog.askstring("查詢選擇權", "交易日期 (YYYY-MM-DD，留空查詢所有):")
            
            df = self.database.query_options(product=product, trade_date=trade_date)
            
            self.database_text.delete(1.0, tk.END)
            self.database_text.insert(tk.END, f"=== 選擇權查詢結果 ===\n\n")
            self.database_text.insert(tk.END, f"找到 {len(df)} 筆資料\n\n")
            self.database_text.insert(tk.END, df.to_string())
            
            self.notebook.select(3)
            self.update_status(f"選擇權查詢完成: {len(df)} 筆資料")
            
        except Exception as e:
            messagebox.showerror("錯誤", f"查詢選擇權失敗: {e}")

    def query_futures(self):
        """查詢期貨資料"""
        try:
            product = simpledialog.askstring("查詢期貨", "商品代碼 (TXF/MXF，留空查詢所有):")
            trade_date = simpledialog.askstring("查詢期貨", "交易日期 (YYYY-MM-DD，留空查詢所有):")
            
            df = self.database.query_futures(product=product, trade_date=trade_date)
            
            self.database_text.delete(1.0, tk.END)
            self.database_text.insert(tk.END, f"=== 期貨查詢結果 ===\n\n")
            self.database_text.insert(tk.END, f"找到 {len(df)} 筆資料\n\n")
            self.database_text.insert(tk.END, df.to_string())
            
            self.notebook.select(3)
            self.update_status(f"期貨查詢完成: {len(df)} 筆資料")
            
        except Exception as e:
            messagebox.showerror("錯誤", f"查詢期貨失敗: {e}")

    def query_stocks(self):
        """查詢股票資料"""
        try:
            symbol = simpledialog.askstring("查詢股票", "股票代碼 (留空查詢所有):")
            trade_date = simpledialog.askstring("查詢股票", "交易日期 (YYYY-MM-DD，留空查詢所有):")
            
            df = self.database.query_stocks(symbol=symbol, trade_date=trade_date)
            
            self.database_text.delete(1.0, tk.END)
            self.database_text.insert(tk.END, f"=== 股票查詢結果 ===\n\n")
            self.database_text.insert(tk.END, f"找到 {len(df)} 筆資料\n\n")
            self.database_text.insert(tk.END, df.to_string())
            
            self.notebook.select(3)
            self.update_status(f"股票查詢完成: {len(df)} 筆資料")
            
        except Exception as e:
            messagebox.showerror("錯誤", f"查詢股票失敗: {e}")

def main():
    """主程式"""
    root = tk.Tk()
    app = EnhancedTXODataScraper(root)
    root.mainloop()

if __name__ == "__main__":
    main()