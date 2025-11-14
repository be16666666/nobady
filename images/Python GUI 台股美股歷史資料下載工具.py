"""
stock_auto_downloader.py
完整自動化股票/期貨/選擇權資料下載系統
包含智慧增量下載、防限流機制、錯誤處理與進度追蹤
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.font_manager as fm
import threading
import os
from datetime import datetime, timedelta
import sqlite3
import requests
import time
import json
import io
import random

# 獲取主程式目錄路徑
MAIN_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(MAIN_DIR, 'taiwan_stocks.db')

# 設定中文字體
def setup_chinese_font():
    """設定中文字體"""
    try:
        # 嘗試使用系統中的中文字體
        font_names = ['Microsoft JhengHei', 'SimHei', 'KaiTi', 'SimSun', 'Arial Unicode MS']
        for font_name in font_names:
            if font_name in [f.name for f in fm.fontManager.ttflist]:
                plt.rcParams['font.sans-serif'] = [font_name, 'Arial Unicode MS', 'DejaVu Sans']
                plt.rcParams['axes.unicode_minus'] = False
                return font_name
        # 如果找不到中文字體，使用預設
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        return 'DejaVu Sans'
    except:
        return 'DejaVu Sans'

CHINESE_FONT = setup_chinese_font()

class DarkMessageBox:
    """深色模式訊息框"""
    
    @staticmethod
    def showinfo(title, message):
        """顯示資訊訊息框"""
        return DarkMessageBox._show_messagebox("info", title, message)
    
    @staticmethod
    def showwarning(title, message):
        """顯示警告訊息框"""
        return DarkMessageBox._show_messagebox("warning", title, message)
    
    @staticmethod
    def showerror(title, message):
        """顯示錯誤訊息框"""
        return DarkMessageBox._show_messagebox("error", title, message)
    
    @staticmethod
    def askyesno(title, message):
        """顯示是/否對話框"""
        return DarkMessageBox._show_messagebox("yesno", title, message)
    
    @staticmethod
    def _show_messagebox(msg_type, title, message):
        """創建深色模式訊息框"""
        # 創建頂層視窗
        dialog = tk.Toplevel()
        dialog.title(title)
        dialog.configure(bg='#121212')
        dialog.transient(dialog.master)
        dialog.grab_set()
        
        # 設定視窗置中
        dialog.update_idletasks()
        width = 400
        height = 200
        x = (dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (dialog.winfo_screenheight() // 2) - (height // 2)
        dialog.geometry(f'{width}x{height}+{x}+{y}')
        dialog.resizable(False, False)
        
        # 創建內容框架
        content_frame = ttk.Frame(dialog, style='Dark.TFrame')
        content_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # 訊息圖標和文字
        icon_label = ttk.Label(content_frame, text="ⓘ", font=('Arial', 24), 
                              foreground='#ffffff', background='#121212')
        icon_label.pack(pady=(10, 5))
        
        message_label = ttk.Label(content_frame, text=message, wraplength=350,
                                 font=('Microsoft JhengHei', 11), 
                                 foreground='#ffffff', background='#121212',
                                 justify=tk.CENTER)
        message_label.pack(pady=10, fill=tk.BOTH, expand=True)
        
        # 按鈕框架
        button_frame = ttk.Frame(content_frame, style='Dark.TFrame')
        button_frame.pack(pady=10)
        
        result = None
        
        def set_result(value):
            nonlocal result
            result = value
            dialog.destroy()
        
        if msg_type == "yesno":
            ttk.Button(button_frame, text="是", command=lambda: set_result(True),
                      style='Dark.TButton').pack(side=tk.LEFT, padx=10)
            ttk.Button(button_frame, text="否", command=lambda: set_result(False),
                      style='Dark.TButton').pack(side=tk.LEFT, padx=10)
        else:
            ttk.Button(button_frame, text="確定", command=dialog.destroy,
                      style='Dark.TButton').pack(padx=10)
        
        # 設定深色樣式
        style = ttk.Style()
        style.configure('Dark.TFrame', background='#121212')
        style.configure('Dark.TButton', background='#1e1e1e', foreground='#ffffff')
        
        dialog.wait_window(dialog)
        return result

class StockListManager:
    """股票清單管理類別 - 增強版"""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.data_sources = [
            {
                'name': 'GitHub_CSV',
                'type': 'csv_url',
                'url': 'https://raw.githubusercontent.com/donny3928/TWSE_Stock_Data/main/Stock_Code.csv',
                'priority': 1
            },
            {
                'name': 'TWSE_CSV',
                'type': 'csv_url', 
                'url': 'https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data',
                'priority': 2
            }
        ]
        self.init_database()
    
    def init_database(self):
        """初始化資料庫結構"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 股票清單資料表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_list (
                stock_id TEXT PRIMARY KEY,
                stock_name TEXT,
                market TEXT,
                industry TEXT,
                listed_date DATE,
                is_active BOOLEAN DEFAULT 1,
                last_updated DATE,
                data_source TEXT
            )
        ''')
        
        # 股票歷史資料表（多時間周期）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_id TEXT,
                date DATETIME,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                interval TEXT,
                created_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_id, date, interval)
            )
        ''')
        
        # 衍生性商品清單
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS derivatives_list (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                type TEXT,
                underlying TEXT,
                expiration DATE,
                last_updated DATE
            )
        ''')
        
        # 衍生性商品歷史資料（多時間周期）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS derivatives_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                date DATETIME,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                open_interest INTEGER,
                interval TEXT,
                created_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, date, interval)
            )
        ''')
        
        # 下載任務記錄
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS download_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_type TEXT,
                symbol TEXT,
                interval TEXT,
                start_date DATE,
                end_date DATE,
                records_downloaded INTEGER,
                status TEXT,
                error_message TEXT,
                created_time DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 預先插入衍生性商品
        self._insert_default_derivatives(cursor)
        
        conn.commit()
        conn.close()
    
    def _insert_default_derivatives(self, cursor):
        """插入預設的衍生性商品"""
        default_derivatives = [
            # 台股期貨
            ('TXF=F', '台股期貨', 'future', '台指', None),
            ('MXF=F', '小型台指期貨', 'future', '台指', None),
            ('EXF=F', '電子期貨', 'future', '電子', None),
            ('FXF=F', '金融期貨', 'future', '金融', None),
            # 國際期貨
            ('ES=F', 'S&P500期貨', 'future', 'SP500', None),
            ('NQ=F', 'NASDAQ期貨', 'future', 'NASDAQ', None),
            ('YM=F', '道瓊期貨', 'future', 'DJIA', None),
            ('GC=F', '黃金期貨', 'future', '黃金', None),
            ('CL=F', '原油期貨', 'future', '原油', None),
        ]
        
        for symbol, name, deriv_type, underlying, expiration in default_derivatives:
            cursor.execute('''
                INSERT OR IGNORE INTO derivatives_list 
                (symbol, name, type, underlying, expiration, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (symbol, name, deriv_type, underlying, expiration, datetime.now().date()))
    
    def auto_detect_csv_format(self, df):
        """自動辨識CSV格式"""
        format_info = {
            'stock_id_col': None,
            'stock_name_col': None,
            'market_col': None,
            'industry_col': None
        }
        
        # 常見的欄位名稱映射
        column_mapping = {
            'stock_id': ['證券代號', 'code', 'symbol', '代號', '股票代號', 'Code', 'Symbol'],
            'stock_name': ['證券名稱', 'name', '股票名稱', '公司名稱', 'Name'],
            'market': ['市場', 'market', 'Market', '市場別'],
            'industry': ['產業', 'industry', 'Industry', '產業別', '類股']
        }
        
        for col in df.columns:
            col_str = str(col).strip()
            
            for field, possible_names in column_mapping.items():
                if col_str in possible_names:
                    format_info[f'{field}_col'] = col
                    break
        
        return format_info
    
    def manual_import_csv(self, csv_file_path):
        """手動匯入CSV股票清單 - 增強自動辨識"""
        try:
            # 嘗試不同編碼
            encodings = ['utf-8', 'big5', 'cp950', 'latin1']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(csv_file_path, encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                return False, "無法讀取CSV檔案，請檢查編碼格式"
            
            # 自動辨識格式
            format_info = self.auto_detect_csv_format(df)
            
            if not format_info['stock_id_col'] or not format_info['stock_name_col']:
                return False, "無法自動識別股票代碼和名稱欄位"
            
            stock_data = []
            for _, row in df.iterrows():
                stock_id = str(row[format_info['stock_id_col']]).strip()
                stock_name = str(row[format_info['stock_name_col']]).strip()
                
                # 跳過空值或標題行
                if not stock_id or stock_id in ['證券代號', 'code', 'Code']:
                    continue
                
                market = '上市'
                if format_info['market_col']:
                    market = str(row[format_info['market_col']]).strip()
                
                industry = ''
                if format_info['industry_col']:
                    industry = str(row[format_info['industry_col']]).strip()
                
                stock_data.append({
                    'stock_id': stock_id,
                    'stock_name': stock_name,
                    'market': market,
                    'industry': industry,
                    'listed_date': datetime.now().date()
                })
            
            new_count = self._update_database(stock_data, 'manual_csv')
            return True, f"成功匯入 {new_count} 筆股票資料"
            
        except Exception as e:
            return False, f"匯入失敗: {str(e)}"
    
    def _update_database(self, stock_data, data_source):
        """更新資料庫"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        new_count = 0
        for stock in stock_data:
            cursor.execute('SELECT stock_id FROM stock_list WHERE stock_id = ?', (stock['stock_id'],))
            existing = cursor.fetchone()
            
            if not existing:
                cursor.execute('''
                    INSERT INTO stock_list 
                    (stock_id, stock_name, market, industry, listed_date, last_updated, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    stock['stock_id'], stock['stock_name'], stock['market'], 
                    stock['industry'], stock['listed_date'], datetime.now().date(), data_source
                ))
                new_count += 1
            else:
                cursor.execute('''
                    UPDATE stock_list 
                    SET stock_name=?, market=?, industry=?, last_updated=?, data_source=?
                    WHERE stock_id=?
                ''', (
                    stock['stock_name'], stock['market'], stock['industry'], 
                    datetime.now().date(), data_source, stock['stock_id']
                ))
        
        conn.commit()
        conn.close()
        return new_count

    def get_all_stocks(self):
        """取得所有股票清單"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT stock_id, stock_name, market FROM stock_list 
            WHERE is_active = 1 ORDER BY stock_id
        ''')
        
        stocks = cursor.fetchall()
        conn.close()
        
        return [f"{stock[0]} {stock[1]} ({stock[2]})" for stock in stocks]
    
    def get_derivatives(self, deriv_type=None):
        """取得衍生性商品清單"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if deriv_type:
            cursor.execute('''
                SELECT symbol, name, type FROM derivatives_list 
                WHERE type = ? ORDER BY symbol
            ''', (deriv_type,))
        else:
            cursor.execute('''
                SELECT symbol, name, type FROM derivatives_list 
                ORDER BY type, symbol
            ''')
        
        derivatives = cursor.fetchall()
        conn.close()
        
        return [f"{deriv[0]} {deriv[1]} ({deriv[2]})" for deriv in derivatives]

class AutoDownloadManager:
    """自動下載管理類別"""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.is_downloading = False
        self.current_progress = 0
        self.total_tasks = 0
        self.completed_tasks = 0
        
        # 定義時間周期和對應的歷史資料範圍
        self.intervals_config = [
            {'interval': '1d', 'period': 'max', 'name': '1日'},
            {'interval': '1h', 'period': '2y', 'name': '1小時'},
            {'interval': '30m', 'period': '60d', 'name': '30分'},
            {'interval': '15m', 'period': '60d', 'name': '15分'},
            {'interval': '5m', 'period': '60d', 'name': '5分'},
            {'interval': '1m', 'period': '7d', 'name': '1分'}
        ]
    
    def smart_download_stock_data(self, stock_id, callback=None, download_all_intervals=True):
        """智慧化增量下載股票資料 - 增強資料完整性檢查"""
        try:
            # 檢查股票代碼是否有效
            if not self.is_valid_stock_id(stock_id):
                if callback:
                    callback(stock_id, 'skip', '無效的股票代碼')
                return True
            
            full_stock_id = f"{stock_id}.TW" if not stock_id.endswith('.TW') else stock_id
            
            if download_all_intervals:
                # 下載所有時間周期
                total_success = 0
                total_records = 0
                
                for interval_config in self.intervals_config:
                    if not self.is_downloading:
                        break
                    
                    interval = interval_config['interval']
                    period = interval_config['period']
                    interval_name = interval_config['name']
                    
                    # 檢查該周期是否已有資料，並檢查資料完整性
                    needs_download, reason = self.check_data_completeness(stock_id, interval, 'stock')
                    
                    if not needs_download:
                        if callback:
                            callback(stock_id, 'progress', f"{interval_name}: {reason}")
                        continue
                    
                    # 防限流：隨機延遲
                    time.sleep(random.uniform(0.5, 1.5))
                    
                    # 下載資料
                    stock_data = self.download_yfinance_data(full_stock_id, period=period, interval=interval)
                    
                    if stock_data is not None and not stock_data.empty:
                        # 檢查資料完整性
                        if self.is_data_complete(stock_data, interval):
                            # 存入資料庫
                            saved_count = self.save_stock_data_to_db(stock_id, stock_data, interval)
                            total_records += saved_count
                            
                            if saved_count > 0:
                                total_success += 1
                                # 記錄下載任務
                                self.log_download_task('stock', stock_id, interval, None, None, saved_count, 'success')
                                
                                if callback:
                                    callback(stock_id, 'progress', f"{interval_name}: {saved_count}筆")
                            else:
                                if callback:
                                    callback(stock_id, 'progress', f"{interval_name}: 無新資料")
                        else:
                            if callback:
                                callback(stock_id, 'progress', f"{interval_name}: 資料不完整")
                    else:
                        if callback:
                            callback(stock_id, 'progress', f"{interval_name}: 無資料")
                
                if total_success > 0:
                    if callback:
                        callback(stock_id, 'success', f"完成{total_success}個周期，共{total_records}筆")
                    return True
                else:
                    if callback:
                        callback(stock_id, 'failed', '所有周期均無資料')
                    return False
            else:
                # 只下載日線資料
                return self.download_single_interval(stock_id, full_stock_id, '1d', callback)
                
        except Exception as e:
            error_msg = str(e)
            # 過濾常見的錯誤訊息
            if "delisted" in error_msg or "not found" in error_msg.lower():
                error_msg = "股票可能已下市或不存在"
            self.log_download_task('stock', stock_id, '1d', None, None, 0, 'error', error_msg)
            if callback:
                callback(stock_id, 'error', error_msg)
            return False
    
    def download_single_interval(self, stock_id, full_stock_id, interval, callback):
        """下載單一時間周期"""
        # 檢查資料完整性
        needs_download, reason = self.check_data_completeness(stock_id, interval, 'stock')
        
        if not needs_download:
            if callback:
                callback(stock_id, 'skip', reason)
            return True
        
        # 下載資料
        stock_data = self.download_yfinance_data(full_stock_id, period='max', interval=interval)
        
        if stock_data is not None and not stock_data.empty:
            # 檢查資料完整性
            if self.is_data_complete(stock_data, interval):
                saved_count = self.save_stock_data_to_db(stock_id, stock_data, interval)
                self.log_download_task('stock', stock_id, interval, None, None, saved_count, 'success')
                
                if callback:
                    callback(stock_id, 'success', f"下載 {saved_count} 筆資料")
                return True
            else:
                if callback:
                    callback(stock_id, 'failed', '資料不完整')
                return False
        else:
            if callback:
                callback(stock_id, 'failed', '無資料')
            return False
    
    def check_data_completeness(self, symbol, interval, data_type='stock'):
        """檢查資料完整性"""
        # 取得最新日期
        if data_type == 'stock':
            latest_date = self.get_latest_stock_date(symbol, interval)
        else:
            latest_date = self.get_latest_derivative_date(symbol, interval)
        
        if not latest_date:
            return True, "無資料，需要下載"
        
        # 檢查是否有缺失的年份
        current_year = datetime.now().year
        missing_years = self.find_missing_years(symbol, interval, data_type, current_year)
        
        if missing_years:
            return True, f"缺失年份: {missing_years}"
        
        # 檢查最新資料是否在近期
        days_since_last = (datetime.now() - latest_date).days
        if days_since_last > 7:  # 如果超過7天沒有新資料
            return True, f"資料已過期 {days_since_last} 天"
        
        return False, "資料完整"
    
    def find_missing_years(self, symbol, interval, data_type, current_year):
        """找出缺失的年份"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if data_type == 'stock':
            cursor.execute('''
                SELECT DISTINCT strftime('%Y', date) as year 
                FROM stock_data 
                WHERE stock_id = ? AND interval = ?
                ORDER BY year
            ''', (symbol, interval))
        else:
            cursor.execute('''
                SELECT DISTINCT strftime('%Y', date) as year 
                FROM derivatives_data 
                WHERE symbol = ? AND interval = ?
                ORDER BY year
            ''', (symbol, interval))
        
        existing_years = [int(row[0]) for row in cursor.fetchall()]
        conn.close()
        
        if not existing_years:
            return list(range(2000, current_year + 1))
        
        # 找出缺失的年份
        all_years = set(range(min(existing_years), current_year + 1))
        existing_years_set = set(existing_years)
        missing_years = sorted(all_years - existing_years_set)
        
        return missing_years
    
    def is_data_complete(self, data, interval):
        """檢查下載的資料是否完整"""
        if data.empty:
            return False
        
        # 檢查是否有足夠的資料點
        if len(data) < 10:  # 至少要有10筆資料
            return False
        
        # 檢查時間跨度
        date_range = data.index.max() - data.index.min()
        if interval == '1d' and date_range.days < 30:  # 日線至少要有30天資料
            return False
        
        # 檢查是否有大量缺失值
        if data['Close'].isna().sum() > len(data) * 0.1:  # 缺失值不超過10%
            return False
        
        return True
    
    def is_valid_stock_id(self, stock_id):
        """檢查股票代碼是否有效"""
        # 過濾明顯無效的代碼
        invalid_patterns = ['', ' ', 'N/A', 'NaN', 'None']
        if not stock_id or stock_id in invalid_patterns:
            return False
        
        # 檢查是否為數字（台股）
        if stock_id.endswith('.TW') or stock_id.endswith('.TWO'):
            base_id = stock_id.replace('.TW', '').replace('.TWO', '')
            if not base_id.isdigit():
                return False
        
        return True
    
    def smart_download_derivative_data(self, symbol, callback=None, download_all_intervals=True):
        """智慧化增量下載衍生性商品資料 - 增強資料完整性檢查"""
        try:
            if download_all_intervals:
                # 下載所有時間周期
                total_success = 0
                total_records = 0
                
                for interval_config in self.intervals_config:
                    if not self.is_downloading:
                        break
                    
                    interval = interval_config['interval']
                    period = interval_config['period']
                    interval_name = interval_config['name']
                    
                    # 檢查資料完整性
                    needs_download, reason = self.check_data_completeness(symbol, interval, 'derivative')
                    
                    if not needs_download:
                        if callback:
                            callback(symbol, 'progress', f"{interval_name}: {reason}")
                        continue
                    
                    # 防限流：隨機延遲
                    time.sleep(random.uniform(0.5, 1.5))
                    
                    # 下載資料
                    derivative_data = self.download_yfinance_data(symbol, period=period, interval=interval)
                    
                    if derivative_data is not None and not derivative_data.empty:
                        # 檢查資料完整性
                        if self.is_data_complete(derivative_data, interval):
                            # 存入資料庫
                            saved_count = self.save_derivative_data_to_db(symbol, derivative_data, interval)
                            total_records += saved_count
                            
                            if saved_count > 0:
                                total_success += 1
                                self.log_download_task('derivative', symbol, interval, None, None, saved_count, 'success')
                                
                                if callback:
                                    callback(symbol, 'progress', f"{interval_name}: {saved_count}筆")
                            else:
                                if callback:
                                    callback(symbol, 'progress', f"{interval_name}: 無新資料")
                        else:
                            if callback:
                                callback(symbol, 'progress', f"{interval_name}: 資料不完整")
                    else:
                        if callback:
                            callback(symbol, 'progress', f"{interval_name}: 無資料")
                
                if total_success > 0:
                    if callback:
                        callback(symbol, 'success', f"完成{total_success}個周期，共{total_records}筆")
                    return True
                else:
                    if callback:
                        callback(symbol, 'failed', '所有周期均無資料')
                    return False
            else:
                # 只下載日線資料
                return self.download_single_interval_derivative(symbol, '1d', callback)
                
        except Exception as e:
            error_msg = str(e)
            self.log_download_task('derivative', symbol, '1d', None, None, 0, 'error', error_msg)
            if callback:
                callback(symbol, 'error', error_msg)
            return False
    
    def download_single_interval_derivative(self, symbol, interval, callback):
        """下載衍生性商品單一時間周期"""
        # 檢查資料完整性
        needs_download, reason = self.check_data_completeness(symbol, interval, 'derivative')
        
        if not needs_download:
            if callback:
                callback(symbol, 'skip', reason)
            return True
        
        # 下載資料
        derivative_data = self.download_yfinance_data(symbol, period='max', interval=interval)
        
        if derivative_data is not None and not derivative_data.empty:
            # 檢查資料完整性
            if self.is_data_complete(derivative_data, interval):
                saved_count = self.save_derivative_data_to_db(symbol, derivative_data, interval)
                self.log_download_task('derivative', symbol, interval, None, None, saved_count, 'success')
                
                if callback:
                    callback(symbol, 'success', f"下載 {saved_count} 筆資料")
                return True
            else:
                if callback:
                    callback(symbol, 'failed', '資料不完整')
                return False
        else:
            if callback:
                callback(symbol, 'failed', '無資料')
            return False
    
    def download_yfinance_data(self, symbol, start_date=None, end_date=None, interval='1d', period=None, max_retries=3):
        """下載yfinance資料，包含重試機制"""
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(symbol)
                
                if period:
                    # 使用period參數下載
                    data = ticker.history(period=period, interval=interval)
                else:
                    # 使用start_date和end_date下載
                    data = ticker.history(start=start_date, end=end_date, interval=interval)
                
                # 檢查是否為空資料或只有NaN值
                if data is None or data.empty or data['Close'].isna().all():
                    return None
                    
                return data
            except Exception as e:
                print(f"下載 {symbol} {interval} 失敗 (嘗試 {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    # 最後一次嘗試，返回None而不是拋出異常
                    return None
                # 重試前等待
                time.sleep(2 ** attempt)  # 指數退避
        
        return None
    
    def get_latest_stock_date(self, stock_id, interval):
        """取得股票最新資料日期"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT MAX(date) FROM stock_data WHERE stock_id = ? AND interval = ?', (stock_id, interval))
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
        return None
    
    def get_latest_derivative_date(self, symbol, interval):
        """取得衍生性商品最新資料日期"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT MAX(date) FROM derivatives_data WHERE symbol = ? AND interval = ?', (symbol, interval))
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
        return None
    
    def save_stock_data_to_db(self, stock_id, data, interval):
        """儲存股票資料到資料庫 - 使用UNIQUE約束避免重複"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        saved_count = 0
        for index, row in data.iterrows():
            try:
                # 檢查資料是否有效
                if pd.isna(row['Close']) or row['Close'] == 0:
                    continue
                
                # 轉換時間格式
                if isinstance(index, pd.Timestamp):
                    date_str = index.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    date_str = str(index)
                    
                cursor.execute('''
                    INSERT OR IGNORE INTO stock_data 
                    (stock_id, date, open, high, low, close, volume, interval)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    stock_id,
                    date_str,
                    float(row['Open']) if pd.notna(row['Open']) else None,
                    float(row['High']) if pd.notna(row['High']) else None,
                    float(row['Low']) if pd.notna(row['Low']) else None,
                    float(row['Close']) if pd.notna(row['Close']) else None,
                    int(row['Volume']) if pd.notna(row['Volume']) else None,
                    interval
                ))
                if cursor.rowcount > 0:
                    saved_count += 1
            except Exception as e:
                print(f"儲存股票資料錯誤 {stock_id} {index}: {e}")
        
        conn.commit()
        conn.close()
        return saved_count
    
    def save_derivative_data_to_db(self, symbol, data, interval):
        """儲存衍生性商品資料到資料庫 - 使用UNIQUE約束避免重複"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        saved_count = 0
        for index, row in data.iterrows():
            try:
                # 轉換時間格式
                if isinstance(index, pd.Timestamp):
                    date_str = index.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    date_str = str(index)
                    
                cursor.execute('''
                    INSERT OR IGNORE INTO derivatives_data 
                    (symbol, date, open, high, low, close, volume, open_interest, interval)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    symbol,
                    date_str,
                    float(row['Open']) if pd.notna(row['Open']) else None,
                    float(row['High']) if pd.notna(row['High']) else None,
                    float(row['Low']) if pd.notna(row['Low']) else None,
                    float(row['Close']) if pd.notna(row['Close']) else None,
                    int(row['Volume']) if pd.notna(row['Volume']) else None,
                    int(row.get('Open Interest', 0)) if pd.notna(row.get('Open Interest')) else None,
                    interval
                ))
                if cursor.rowcount > 0:
                    saved_count += 1
            except Exception as e:
                print(f"儲存衍生性商品資料錯誤 {symbol} {index}: {e}")
        
        conn.commit()
        conn.close()
        return saved_count
    
    def log_download_task(self, task_type, symbol, interval, start_date, end_date, records_count, status, error_message=None):
        """記錄下載任務"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO download_log 
            (task_type, symbol, interval, start_date, end_date, records_downloaded, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (task_type, symbol, interval, start_date, end_date, records_count, status, error_message))
        
        conn.commit()
        conn.close()
    
    def batch_download_stocks(self, stock_list, progress_callback=None, completion_callback=None, download_all_intervals=True):
        """批次下載股票資料"""
        if self.is_downloading:
            return False
        
        self.is_downloading = True
        self.total_tasks = len(stock_list)
        self.completed_tasks = 0
        
        def download_thread():
            success_count = 0
            fail_count = 0
            skip_count = 0
            
            for i, stock_id in enumerate(stock_list):
                if not self.is_downloading:
                    break
                
                # 修正：定義正確的回調函數參數數量
                def task_callback(symbol, status, message):
                    self.completed_tasks += 1
                    progress = (self.completed_tasks / self.total_tasks) * 100
                    
                    nonlocal success_count, fail_count, skip_count
                    if status == 'success':
                        success_count += 1
                    elif status == 'skip':
                        skip_count += 1
                    elif status == 'failed' or status == 'error':
                        fail_count += 1
                    
                    if progress_callback:
                        progress_callback(progress, f"{symbol}: {status} - {message}")
                
                self.smart_download_stock_data(stock_id, task_callback, download_all_intervals)
                
                # 進度更新
                progress = ((i + 1) / len(stock_list)) * 100
                if progress_callback:
                    progress_callback(progress, f"正在下載 {stock_id}...")
            
            self.is_downloading = False
            if completion_callback:
                completion_callback(success_count, fail_count, skip_count)
        
        thread = threading.Thread(target=download_thread)
        thread.daemon = True
        thread.start()
        return True
    
    def batch_download_derivatives(self, derivative_list, progress_callback=None, completion_callback=None, download_all_intervals=True):
        """批次下載衍生性商品資料"""
        if self.is_downloading:
            return False
        
        self.is_downloading = True
        self.total_tasks = len(derivative_list)
        self.completed_tasks = 0
        
        def download_thread():
            success_count = 0
            fail_count = 0
            skip_count = 0
            
            for i, symbol in enumerate(derivative_list):
                if not self.is_downloading:
                    break
                
                # 修正：定義正確的回調函數參數數量
                def task_callback(symb, status, message):
                    self.completed_tasks += 1
                    progress = (self.completed_tasks / self.total_tasks) * 100
                    
                    nonlocal success_count, fail_count, skip_count
                    if status == 'success':
                        success_count += 1
                    elif status == 'skip':
                        skip_count += 1
                    elif status == 'failed' or status == 'error':
                        fail_count += 1
                    
                    if progress_callback:
                        progress_callback(progress, f"{symb}: {status} - {message}")
                
                self.smart_download_derivative_data(symbol, task_callback, download_all_intervals)
                
                progress = ((i + 1) / len(derivative_list)) * 100
                if progress_callback:
                    progress_callback(progress, f"正在下載 {symbol}...")
            
            self.is_downloading = False
            if completion_callback:
                completion_callback(success_count, fail_count, skip_count)
        
        thread = threading.Thread(target=download_thread)
        thread.daemon = True
        thread.start()
        return True
    
    def stop_download(self):
        """停止下載"""
        self.is_downloading = False

class DatabaseManager:
    """資料庫管理類別"""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
    
    def export_to_csv(self, table_name, output_path, selected_ids=None):
        """匯出資料表到CSV - 支援選取記錄"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            if selected_ids:
                # 匯出選取的記錄
                placeholders = ','.join('?' for _ in selected_ids)
                query = f"SELECT * FROM {table_name} WHERE id IN ({placeholders})"
                df = pd.read_sql_query(query, conn, params=selected_ids)
            else:
                # 匯出全部記錄
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                
            conn.close()
            
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            return True, f"成功匯出 {len(df)} 筆資料到 {output_path}"
        except Exception as e:
            return False, f"匯出失敗: {str(e)}"
    
    def export_to_excel(self, table_name, output_path, selected_ids=None):
        """匯出資料表到Excel - 支援選取記錄"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            if selected_ids:
                # 匯出選取的記錄
                placeholders = ','.join('?' for _ in selected_ids)
                query = f"SELECT * FROM {table_name} WHERE id IN ({placeholders})"
                df = pd.read_sql_query(query, conn, params=selected_ids)
            else:
                # 匯出全部記錄
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                
            conn.close()
            
            # 修正：檢查資料量，避免Excel檔案大小限制
            if len(df) > 1000000:  # 如果超過100萬行
                return False, "資料量過大，建議使用CSV格式匯出"
            
            try:
                df.to_excel(output_path, index=False)
                return True, f"成功匯出 {len(df)} 筆資料到 {output_path}"
            except Exception as e:
                if "too large" in str(e).lower():
                    return False, "資料量過大，建議使用CSV格式匯出"
                else:
                    return False, f"匯出失敗: {str(e)}"
                
        except Exception as e:
            return False, f"匯出失敗: {str(e)}"
    
    def get_table_names(self):
        """取得所有資料表名稱"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    
    def get_table_data(self, table_name, limit=1000, search_condition=None, order_by=None):
        """取得資料表資料 - 支援搜尋和排序"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = f"SELECT * FROM {table_name}"
        
        if search_condition:
            query += f" WHERE {search_condition}"
            
        if order_by:
            query += f" ORDER BY {order_by}"
        else:
            query += " ORDER BY id DESC"  # 預設按ID倒序
            
        query += f" LIMIT {limit}"
            
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        conn.close()
        return columns, data
    
    def delete_records(self, table_name, condition):
        """刪除記錄"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {table_name} WHERE {condition}")
            affected_rows = cursor.rowcount
            conn.commit()
            conn.close()
            return True, f"成功刪除 {affected_rows} 筆記錄"
        except Exception as e:
            return False, f"刪除失敗: {str(e)}"
    
    def search_data(self, table_name, search_text, search_column=None, limit=1000, order_by=None):
        """搜尋資料 - 增強版：支援指定欄位搜尋和排序"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 取得所有欄位名稱
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        # 建立搜尋條件
        if search_column and search_column != "全部欄位":
            # 搜尋指定欄位
            conditions = [f"{search_column} LIKE '%{search_text}%'"]
        else:
            # 搜尋所有欄位
            conditions = []
            for col in column_names:
                conditions.append(f"{col} LIKE '%{search_text}%'")
        
        where_clause = " OR ".join(conditions)
        
        # 建立完整查詢
        query = f"SELECT * FROM {table_name} WHERE {where_clause}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
            
        query += f" LIMIT {limit}"
        
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        conn.close()
        
        return columns, data
    
    def get_table_columns(self, table_name):
        """取得資料表的所有欄位名稱"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        conn.close()
        return column_names

class StockDataDownloader:
    def __init__(self, root):
        self.root = root
        self.root.title("完整自動化股票/期貨/選擇權資料下載系統")
        self.root.geometry("1100x800")  # 增加寬度以容納新頁籤
        
        # 設定程式圖標和標題
        self.setup_window()
        
        # 初始化管理員
        self.stock_manager = StockListManager()
        self.download_manager = AutoDownloadManager()
        self.db_manager = DatabaseManager()
        
        # 設定深色模式
        self.setup_dark_theme()
        self.create_widgets()
        
        # 初始化清單
        self.load_stock_list()
        
        # 檢查更新
        self.root.after(1000, self.check_initial_update)
    
    def setup_window(self):
        """設定視窗屬性"""
        # 設定視窗圖標（如果有的話）
        try:
            self.root.iconbitmap(os.path.join(MAIN_DIR, 'icon.ico'))
        except:
            pass
        
        # 設定視窗置中
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')
    
    def setup_dark_theme(self):
        """設定深色模式主題 - 使用黑灰色"""
        # 使用黑灰色背景
        self.root.configure(bg='#121212')  # 更深的黑灰色
        style = ttk.Style()
        style.theme_use('clam')
        
        # 設定深色模式樣式 - 黑灰色系
        style.configure('.', 
                       background='#121212',  # 主背景
                       foreground='#ffffff',
                       fieldbackground='#1e1e1e',  # 輸入框背景
                       selectbackground='#2d2d2d',  # 選擇背景
                       selectforeground='#ffffff',
                       font=('Microsoft JhengHei', 12))
        
        style.configure('TLabel', background='#121212', foreground='#ffffff')
        style.configure('TFrame', background='#121212')
        style.configure('TButton', background='#1e1e1e', foreground='#ffffff')  # 按鈕背景
        style.configure('TEntry', fieldbackground='#1e1e1e', foreground='#ffffff')
        
        # 設定下拉式選單為黑底白字
        style.configure('TCombobox', 
                       fieldbackground='#1e1e1e', 
                       foreground='#ffffff',
                       background='#1e1e1e',
                       selectbackground='#2d2d2d',
                       selectforeground='#ffffff')
        
        # 設定選項卡為黑底白字
        style.configure('TNotebook', background='#121212', foreground='#ffffff')
        style.configure('TNotebook.Tab', 
                       background='#1e1e1e', 
                       foreground='#ffffff',
                       focuscolor='#1e1e1e')
        
        style.configure('Treeview', 
                       background='#1e1e1e',  # 表格背景
                       foreground='#ffffff',
                       fieldbackground='#1e1e1e')
        style.configure('Treeview.Heading',
                       background='#2d2d2d',  # 表頭背景
                       foreground='#ffffff')
        style.configure('Vertical.TScrollbar', 
                       background='#1e1e1e',
                       troughcolor='#121212')
        style.configure('Horizontal.TScrollbar', 
                       background='#1e1e1e',
                       troughcolor='#121212')
        
        # 設定Listbox樣式
        self.root.option_add('*Listbox*Background', '#1e1e1e')
        self.root.option_add('*Listbox*Foreground', '#ffffff')
        self.root.option_add('*Listbox*selectBackground', '#2d2d2d')
        self.root.option_add('*Listbox*selectForeground', '#ffffff')
        
        # 設定下拉式選單清單樣式
        self.root.option_add('*TCombobox*Listbox*Background', '#1e1e1e')
        self.root.option_add('*TCombobox*Listbox*Foreground', '#ffffff')
        self.root.option_add('*TCombobox*Listbox*selectBackground', '#2d2d2d')
        self.root.option_add('*TCombobox*Listbox*selectForeground', '#ffffff')
        
        plt.style.use('dark_background')
    
    def create_widgets(self):
        """創建介面元件"""
        # 主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 配置權重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # 標題
        title_label = ttk.Label(main_frame, text="完整自動化資料下載系統", 
                               font=('Microsoft JhengHei', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=3, pady=10)
        
        # 創建選項卡 - 設定為黑底白字
        notebook = ttk.Notebook(main_frame)
        notebook.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=10)
        
        # 單一商品下載頁籤 - 黑色背景白色文字
        single_tab = ttk.Frame(notebook, style='Dark.TFrame')
        single_tab.configure(style='Dark.TFrame')  # 確保背景為黑色
        notebook.add(single_tab, text="單一商品下載")
        
        # 批次下載頁籤 - 黑色背景白色文字
        batch_tab = ttk.Frame(notebook, style='Dark.TFrame')
        batch_tab.configure(style='Dark.TFrame')  # 確保背景為黑色
        notebook.add(batch_tab, text="批次自動下載")
        
        # 清單管理頁籤 - 黑色背景白色文字
        manage_tab = ttk.Frame(notebook, style='Dark.TFrame')
        manage_tab.configure(style='Dark.TFrame')  # 確保背景為黑色
        notebook.add(manage_tab, text="清單管理")
        
        # 資料庫管理頁籤
        db_tab = ttk.Frame(notebook, style='Dark.TFrame')
        db_tab.configure(style='Dark.TFrame')  # 確保背景為黑色
        notebook.add(db_tab, text="資料庫管理")
        
        # 設定單一商品下載頁籤
        self.setup_single_download_tab(single_tab)
        
        # 設定批次下載頁籤
        self.setup_batch_download_tab(batch_tab)
        
        # 設定清單管理頁籤
        self.setup_manage_tab(manage_tab)
        
        # 設定資料庫管理頁籤
        self.setup_database_tab(db_tab)
        
        # 狀態欄
        self.status_var = tk.StringVar(value="準備就緒")
        status_label = ttk.Label(main_frame, textvariable=self.status_var, relief=tk.SUNKEN)
        status_label.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        main_frame.rowconfigure(1, weight=1)
        main_frame.columnconfigure(1, weight=1)
    
    def setup_single_download_tab(self, parent):
        """設定單一商品下載頁籤 - 黑色背景白色文字"""
        # 設定頁籤背景為黑色
        parent.configure(style='Dark.TFrame')
        
        # 商品類型選擇
        ttk.Label(parent, text="商品類型:", style='Dark.TLabel').grid(row=0, column=0, sticky=tk.W, pady=5)
        self.asset_type_var = tk.StringVar(value="stock")
        asset_frame = ttk.Frame(parent, style='Dark.TFrame')
        asset_frame.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=5)
        
        # 使用黑底白字的單選按鈕
        ttk.Radiobutton(asset_frame, text="股票", variable=self.asset_type_var, value="stock", 
                       command=self.on_asset_type_change, style='Dark.TRadiobutton').pack(side=tk.LEFT)
        ttk.Radiobutton(asset_frame, text="期貨", variable=self.asset_type_var, value="future",
                       command=self.on_asset_type_change, style='Dark.TRadiobutton').pack(side=tk.LEFT)
        ttk.Radiobutton(asset_frame, text="選擇權", variable=self.asset_type_var, value="option",
                       command=self.on_asset_type_change, style='Dark.TRadiobutton').pack(side=tk.LEFT)
        
        # 商品代碼輸入
        ttk.Label(parent, text="商品代碼:", style='Dark.TLabel').grid(row=1, column=0, sticky=tk.W, pady=5)
        code_frame = ttk.Frame(parent, style='Dark.TFrame')
        code_frame.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=5)
        
        self.ticker_var = tk.StringVar()
        self.ticker_entry = ttk.Entry(code_frame, textvariable=self.ticker_var, width=20, 
                                     font=("Microsoft JhengHei", 12), style='Dark.TEntry')
        self.ticker_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        # 下拉式選單設定為黑底白字
        self.stock_combo = ttk.Combobox(code_frame, values=[], state="readonly", width=15, style='Dark.TCombobox')
        self.stock_combo.pack(side=tk.RIGHT, padx=(5, 0))
        self.stock_combo.bind('<<ComboboxSelected>>', self.on_stock_selected)
        
        # 市場選擇
        self.market_frame = ttk.Frame(parent, style='Dark.TFrame')
        self.market_frame.grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=5)
        ttk.Label(self.market_frame, text="市場:", style='Dark.TLabel').pack(side=tk.LEFT)
        self.market_var = tk.StringVar(value="TW")
        ttk.Radiobutton(self.market_frame, text="台股", variable=self.market_var, value="TW", style='Dark.TRadiobutton').pack(side=tk.LEFT)
        ttk.Radiobutton(self.market_frame, text="美股", variable=self.market_var, value="US", style='Dark.TRadiobutton').pack(side=tk.LEFT)
        
        # 下載選項
        self.download_all_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(parent, text="✓ 下載所有時間周期 (1日、1小時、30分、15分、5分、1分)", 
                       variable=self.download_all_var, style='Dark.TCheckbutton').grid(row=3, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        # 下載按鈕
        button_frame = ttk.Frame(parent, style='Dark.TFrame')
        button_frame.grid(row=4, column=0, columnspan=2, pady=10)
        
        ttk.Button(button_frame, text="智慧下載", command=self.smart_download, style='Dark.TButton').pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="顯示資料", command=self.show_data, style='Dark.TButton').pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="繪製圖表", command=self.plot_data, style='Dark.TButton').pack(side=tk.LEFT, padx=5)
        
        # 進度條
        self.progress = ttk.Progressbar(parent, mode='determinate', style='Dark.Horizontal.TProgressbar')
        self.progress.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        
        # 資料顯示
        ttk.Label(parent, text="資料預覽:", style='Dark.TLabel').grid(row=6, column=0, sticky=tk.W, pady=5)
        
        columns = ("Date", "Open", "High", "Low", "Close", "Volume", "Interval")
        self.tree = ttk.Treeview(parent, columns=columns, show="headings", height=10, style='Dark.Treeview')
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=80)
        
        scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=self.tree.yview, style='Dark.Vertical.TScrollbar')
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.grid(row=7, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        scrollbar.grid(row=7, column=2, sticky=(tk.N, tk.S), pady=5)
        
        # 設定右鍵選單
        self.setup_tree_context_menu(self.tree)
        
        parent.rowconfigure(7, weight=1)
        parent.columnconfigure(1, weight=1)
    
    def setup_batch_download_tab(self, parent):
        """設定批次下載頁籤 - 黑色背景白色文字"""
        # 設定頁籤背景為黑色
        parent.configure(style='Dark.TFrame')
        
        # 下載類型選擇
        ttk.Label(parent, text="下載類型:", style='Dark.TLabel').grid(row=0, column=0, sticky=tk.W, pady=5)
        self.batch_type_var = tk.StringVar(value="stock")
        type_frame = ttk.Frame(parent, style='Dark.TFrame')
        type_frame.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Radiobutton(type_frame, text="股票", variable=self.batch_type_var, value="stock", style='Dark.TRadiobutton').pack(side=tk.LEFT)
        ttk.Radiobutton(type_frame, text="期貨", variable=self.batch_type_var, value="future", style='Dark.TRadiobutton').pack(side=tk.LEFT)
        ttk.Radiobutton(type_frame, text="選擇權", variable=self.batch_type_var, value="option", style='Dark.TRadiobutton').pack(side=tk.LEFT)
        
        # 下載選項
        self.batch_download_all_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(parent, text="✓ 下載所有時間周期 (依次下載: 1日 → 1小時 → 30分 → 15分 → 5分 → 1分)", 
                       variable=self.batch_download_all_var, style='Dark.TCheckbutton').grid(row=1, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        # 清單框架（包含Listbox和滾動條）
        list_frame = ttk.Frame(parent, style='Dark.TFrame')
        list_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        ttk.Label(list_frame, text="選擇清單 (支援Shift多選、Ctrl+A全選、Del刪除):", style='Dark.TLabel').grid(row=0, column=0, sticky=tk.W, pady=5)
        
        # 創建Listbox和滾動條
        listbox_frame = ttk.Frame(list_frame, style='Dark.TFrame')
        listbox_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self.batch_listbox = tk.Listbox(listbox_frame, selectmode=tk.EXTENDED, height=12,
                                       bg='#1e1e1e', fg='#ffffff', 
                                       selectbackground='#2d2d2d', selectforeground='#ffffff',
                                       font=('Microsoft JhengHei', 10))
        
        # 垂直滾動條
        v_scrollbar = ttk.Scrollbar(listbox_frame, orient=tk.VERTICAL, command=self.batch_listbox.yview, style='Dark.Vertical.TScrollbar')
        self.batch_listbox.configure(yscrollcommand=v_scrollbar.set)
        
        # 水平滾動條
        h_scrollbar = ttk.Scrollbar(listbox_frame, orient=tk.HORIZONTAL, command=self.batch_listbox.xview, style='Dark.Horizontal.TScrollbar')
        self.batch_listbox.configure(xscrollcommand=h_scrollbar.set)
        
        # 網格佈局
        self.batch_listbox.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        v_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        h_scrollbar.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        # 設定鍵盤快捷鍵
        self.batch_listbox.bind('<Control-a>', lambda e: self.select_all())
        self.batch_listbox.bind('<Delete>', lambda e: self.delete_selected())
        
        # 設定右鍵選單
        self.setup_listbox_context_menu()
        
        # 控制按鈕框架
        control_frame = ttk.Frame(parent, style='Dark.TFrame')
        control_frame.grid(row=3, column=0, columnspan=2, pady=10)
        
        ttk.Button(control_frame, text="全選", command=self.select_all, style='Dark.TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(control_frame, text="全不選", command=self.select_none, style='Dark.TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(control_frame, text="刪除選取", command=self.delete_selected, style='Dark.TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(control_frame, text="重新整理清單", command=self.refresh_batch_list, style='Dark.TButton').pack(side=tk.LEFT, padx=2)
        
        # 批次下載控制
        download_frame = ttk.Frame(parent, style='Dark.TFrame')
        download_frame.grid(row=4, column=0, columnspan=2, pady=10)
        
        self.start_batch_btn = ttk.Button(download_frame, text="開始批次下載", 
                                         command=self.start_batch_download, style='Dark.TButton')
        self.start_batch_btn.pack(side=tk.LEFT, padx=5)
        
        self.stop_batch_btn = ttk.Button(download_frame, text="停止下載", 
                                        command=self.stop_batch_download, state=tk.DISABLED, style='Dark.TButton')
        self.stop_batch_btn.pack(side=tk.LEFT, padx=5)
        
        # 批次進度
        ttk.Label(parent, text="批次進度:", style='Dark.TLabel').grid(row=5, column=0, sticky=tk.W, pady=5)
        self.batch_progress = ttk.Progressbar(parent, mode='determinate', style='Dark.Horizontal.TProgressbar')
        self.batch_progress.grid(row=5, column=1, sticky=(tk.W, tk.E), pady=5)
        
        # 批次狀態
        self.batch_status_var = tk.StringVar(value="等待開始...")
        ttk.Label(parent, textvariable=self.batch_status_var, style='Dark.TLabel').grid(row=6, column=0, columnspan=2, pady=5)
        
        # 設定權重
        list_frame.rowconfigure(1, weight=1)
        list_frame.columnconfigure(0, weight=1)
        listbox_frame.rowconfigure(0, weight=1)
        listbox_frame.columnconfigure(0, weight=1)
        parent.rowconfigure(2, weight=1)
        parent.columnconfigure(1, weight=1)
    
    def setup_listbox_context_menu(self):
        """設定Listbox右鍵選單"""
        self.context_menu = tk.Menu(self.batch_listbox, tearoff=0, bg='#1e1e1e', fg='#ffffff')
        self.context_menu.add_command(label="刪除選取項目", command=self.delete_selected)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="全選", command=self.select_all)
        self.context_menu.add_command(label="全不選", command=self.select_none)
        
        # 綁定右鍵事件
        self.batch_listbox.bind("<Button-3>", self.show_context_menu)
    
    def setup_tree_context_menu(self, tree):
        """設定Treeview右鍵選單"""
        context_menu = tk.Menu(tree, tearoff=0, bg='#1e1e1e', fg='#ffffff')
        context_menu.add_command(label="全選", command=lambda: self.tree_select_all(tree))
        context_menu.add_command(label="複製選取", command=lambda: self.tree_copy_selected(tree))
        context_menu.add_separator()
        context_menu.add_command(label="匯出選取", command=lambda: self.tree_export_selected(tree))
        
        tree.bind("<Button-3>", lambda e: self.show_tree_context_menu(e, context_menu))
    
    def tree_select_all(self, tree):
        """Treeview全選"""
        tree.selection_set(tree.get_children())
    
    def tree_copy_selected(self, tree):
        """複製Treeview選取內容"""
        selected = tree.selection()
        if not selected:
            return
        
        text = ""
        for item in selected:
            values = tree.item(item)['values']
            text += "\t".join(str(v) for v in values) + "\n"
        
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
    
    def tree_export_selected(self, tree):
        """匯出Treeview選取內容"""
        selected = tree.selection()
        if not selected:
            DarkMessageBox.showinfo("資訊", "請先選擇要匯出的資料")
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx"), ("All files", "*.*")],
            title="匯出選取資料"
        )
        
        if filename:
            data = []
            columns = tree['columns']
            for item in selected:
                values = tree.item(item)['values']
                data.append(values)
            
            df = pd.DataFrame(data, columns=columns)
            if filename.endswith('.xlsx'):
                df.to_excel(filename, index=False)
            else:
                df.to_csv(filename, index=False, encoding='utf-8-sig')
            
            DarkMessageBox.showinfo("成功", f"已匯出 {len(data)} 筆資料到 {filename}")
    
    def show_tree_context_menu(self, event, context_menu):
        """顯示Treeview右鍵選單"""
        try:
            context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            context_menu.grab_release()
    
    def show_context_menu(self, event):
        """顯示右鍵選單"""
        try:
            self.context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.context_menu.grab_release()
    
    def setup_manage_tab(self, parent):
        """設定清單管理頁籤 - 黑色背景白色文字"""
        # 設定頁籤背景為黑色
        parent.configure(style='Dark.TFrame')
        
        # 清單管理按鈕
        manage_frame = ttk.Frame(parent, style='Dark.TFrame')
        manage_frame.grid(row=0, column=0, columnspan=2, pady=10)
        
        ttk.Button(manage_frame, text="匯入CSV清單", command=self.import_csv_list, style='Dark.TButton').pack(side=tk.LEFT, padx=5)
        ttk.Button(manage_frame, text="檢視股票清單", command=self.view_stock_list, style='Dark.TButton').pack(side=tk.LEFT, padx=5)
        ttk.Button(manage_frame, text="檢視衍生性商品", command=self.view_derivatives_list, style='Dark.TButton').pack(side=tk.LEFT, padx=5)
        ttk.Button(manage_frame, text="檢視下載記錄", command=self.view_download_log, style='Dark.TButton').pack(side=tk.LEFT, padx=5)
        
        # 清單顯示區域
        ttk.Label(parent, text="清單預覽:", style='Dark.TLabel').grid(row=1, column=0, sticky=tk.W, pady=5)
        
        self.manage_tree = ttk.Treeview(parent, columns=("代碼", "名稱", "類型", "最後更新"), show="headings", height=15, style='Dark.Treeview')
        for col in ["代碼", "名稱", "類型", "最後更新"]:
            self.manage_tree.heading(col, text=col)
            self.manage_tree.column(col, width=120)
        
        manage_scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=self.manage_tree.yview, style='Dark.Vertical.TScrollbar')
        self.manage_tree.configure(yscrollcommand=manage_scrollbar.set)
        
        self.manage_tree.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        manage_scrollbar.grid(row=2, column=2, sticky=(tk.N, tk.S), pady=5)
        
        # 設定右鍵選單
        self.setup_tree_context_menu(self.manage_tree)
        
        parent.rowconfigure(2, weight=1)
        parent.columnconfigure(1, weight=1)
    
    def setup_database_tab(self, parent):
        """設定資料庫管理頁籤 - 增強搜尋和排序功能"""
        # 設定頁籤背景為黑色
        parent.configure(style='Dark.TFrame')
        
        # 資料表選擇
        ttk.Label(parent, text="選擇資料表:", style='Dark.TLabel').grid(row=0, column=0, sticky=tk.W, pady=5)
        
        table_frame = ttk.Frame(parent, style='Dark.TFrame')
        table_frame.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=5)
        
        self.table_var = tk.StringVar()
        self.table_combo = ttk.Combobox(table_frame, textvariable=self.table_var, state="readonly", style='Dark.TCombobox')
        self.table_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.table_combo.bind('<<ComboboxSelected>>', self.on_table_selected)
        
        ttk.Button(table_frame, text="重新整理", command=self.refresh_tables, style='Dark.TButton').pack(side=tk.RIGHT, padx=(5, 0))
        
        # 搜尋功能 - 增強版
        search_frame = ttk.Frame(parent, style='Dark.TFrame')
        search_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Label(search_frame, text="搜尋欄位:", style='Dark.TLabel').pack(side=tk.LEFT)
        self.search_column_var = tk.StringVar(value="全部欄位")
        self.search_column_combo = ttk.Combobox(search_frame, textvariable=self.search_column_var, width=15, state="readonly", style='Dark.TCombobox')
        self.search_column_combo.pack(side=tk.LEFT, padx=(5, 10))
        
        ttk.Label(search_frame, text="搜尋:", style='Dark.TLabel').pack(side=tk.LEFT)
        self.search_var = tk.StringVar()
        self.search_entry = ttk.Entry(search_frame, textvariable=self.search_var, width=20, style='Dark.TEntry')
        self.search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.search_entry.bind('<Return>', lambda e: self.search_data())
        
        ttk.Button(search_frame, text="搜尋", command=self.search_data, style='Dark.TButton').pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(search_frame, text="清除", command=self.clear_search, style='Dark.TButton').pack(side=tk.RIGHT, padx=(2, 0))
        
        # 排序功能
        sort_frame = ttk.Frame(parent, style='Dark.TFrame')
        sort_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Label(sort_frame, text="排序欄位:", style='Dark.TLabel').pack(side=tk.LEFT)
        self.sort_column_var = tk.StringVar()
        self.sort_column_combo = ttk.Combobox(sort_frame, textvariable=self.sort_column_var, width=15, state="readonly", style='Dark.TCombobox')
        self.sort_column_combo.pack(side=tk.LEFT, padx=(5, 10))
        
        ttk.Label(sort_frame, text="排序方式:", style='Dark.TLabel').pack(side=tk.LEFT)
        self.sort_order_var = tk.StringVar(value="ASC")
        ttk.Radiobutton(sort_frame, text="升序", variable=self.sort_order_var, value="ASC", style='Dark.TRadiobutton').pack(side=tk.LEFT)
        ttk.Radiobutton(sort_frame, text="降序", variable=self.sort_order_var, value="DESC", style='Dark.TRadiobutton').pack(side=tk.LEFT)
        
        ttk.Button(sort_frame, text="套用排序", command=self.apply_sort, style='Dark.TButton').pack(side=tk.RIGHT, padx=(5, 0))
        
        # 操作按鈕
        button_frame = ttk.Frame(parent, style='Dark.TFrame')
        button_frame.grid(row=3, column=0, columnspan=2, pady=10)
        
        ttk.Button(button_frame, text="匯出CSV (選取)", command=lambda: self.export_csv(True), style='Dark.TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="匯出Excel (選取)", command=lambda: self.export_excel(True), style='Dark.TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="匯出CSV (全部)", command=lambda: self.export_csv(False), style='Dark.TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="匯出Excel (全部)", command=lambda: self.export_excel(False), style='Dark.TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="顯示資料", command=self.show_table_data, style='Dark.TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="刪除選取", command=self.delete_table_records, style='Dark.TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="清空資料表", command=self.clear_table, style='Dark.TButton').pack(side=tk.LEFT, padx=2)
        
        # 資料顯示區域
        ttk.Label(parent, text="資料預覽:", style='Dark.TLabel').grid(row=4, column=0, sticky=tk.W, pady=5)
        
        self.db_tree = ttk.Treeview(parent, show="headings", height=15, style='Dark.Treeview')
        db_scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=self.db_tree.yview, style='Dark.Vertical.TScrollbar')
        self.db_tree.configure(yscrollcommand=db_scrollbar.set)
        
        self.db_tree.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        db_scrollbar.grid(row=5, column=2, sticky=(tk.N, tk.S), pady=5)
        
        # 設定右鍵選單
        self.setup_tree_context_menu(self.db_tree)
        
        parent.rowconfigure(5, weight=1)
        parent.columnconfigure(1, weight=1)
        
        # 初始化資料表清單
        self.refresh_tables()
    
    def refresh_tables(self):
        """重新整理資料表清單 - 使用中文顯示"""
        tables = self.db_manager.get_table_names()
        # 建立中文對照表
        table_names_cn = {
            'stock_list': '股票清單',
            'stock_data': '股票資料',
            'derivatives_list': '衍生性商品清單', 
            'derivatives_data': '衍生性商品資料',
            'download_log': '下載記錄'
        }
        
        display_names = []
        for table in tables:
            display_names.append(table_names_cn.get(table, table))
        
        self.table_combo['values'] = display_names
        if display_names:
            self.table_combo.set(display_names[0])
            self.update_column_combos()
    
    def update_column_combos(self):
        """更新欄位選擇下拉選單"""
        table_name = self.get_english_table_name(self.table_var.get())
        if not table_name:
            return
        
        columns = self.db_manager.get_table_columns(table_name)
        
        # 更新搜尋欄位下拉選單
        search_columns = ["全部欄位"] + columns
        self.search_column_combo['values'] = search_columns
        self.search_column_combo.set("全部欄位")
        
        # 更新排序欄位下拉選單
        self.sort_column_combo['values'] = columns
        if columns:
            self.sort_column_combo.set(columns[0])
    
    def get_english_table_name(self, display_name):
        """取得英文資料表名稱"""
        table_names_cn = {
            '股票清單': 'stock_list',
            '股票資料': 'stock_data',
            '衍生性商品清單': 'derivatives_list',
            '衍生性商品資料': 'derivatives_data',
            '下載記錄': 'download_log'
        }
        return table_names_cn.get(display_name, display_name)
    
    def on_table_selected(self, event):
        """選擇資料表時顯示資料"""
        self.update_column_combos()
        self.show_table_data()
    
    def show_table_data(self):
        """顯示資料表資料"""
        table_name = self.get_english_table_name(self.table_var.get())
        if not table_name:
            return
        
        # 取得排序條件
        order_by = None
        if self.sort_column_var.get():
            order_by = f"{self.sort_column_var.get()} {self.sort_order_var.get()}"
        
        columns, data = self.db_manager.get_table_data(table_name, order_by=order_by)
        
        # 更新Treeview
        self.db_tree.delete(*self.db_tree.get_children())
        self.db_tree['columns'] = columns
        
        for col in columns:
            self.db_tree.heading(col, text=col)
            self.db_tree.column(col, width=100)
        
        for row in data:
            self.db_tree.insert("", tk.END, values=row)
    
    def apply_sort(self):
        """套用排序"""
        self.show_table_data()
    
    def search_data(self):
        """搜尋資料 - 增強版：支援指定欄位搜尋"""
        table_name = self.get_english_table_name(self.table_var.get())
        search_text = self.search_var.get().strip()
        search_column = self.search_column_var.get()
        
        if not table_name:
            DarkMessageBox.showerror("錯誤", "請選擇資料表")
            return
        
        if not search_text:
            self.show_table_data()
            return
        
        # 取得排序條件
        order_by = None
        if self.sort_column_var.get():
            order_by = f"{self.sort_column_var.get()} {self.sort_order_var.get()}"
        
        columns, data = self.db_manager.search_data(
            table_name, search_text, search_column, order_by=order_by)
        
        # 更新Treeview
        self.db_tree.delete(*self.db_tree.get_children())
        self.db_tree['columns'] = columns
        
        for col in columns:
            self.db_tree.heading(col, text=col)
            self.db_tree.column(col, width=100)
        
        for row in data:
            self.db_tree.insert("", tk.END, values=row)
        
        self.status_var.set(f"找到 {len(data)} 筆符合的記錄")
    
    def clear_search(self):
        """清除搜尋"""
        self.search_var.set("")
        self.search_column_combo.set("全部欄位")
        self.show_table_data()
    
    def export_csv(self, export_selected=False):
        """匯出CSV"""
        table_name = self.get_english_table_name(self.table_var.get())
        if not table_name:
            DarkMessageBox.showerror("錯誤", "請選擇資料表")
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title=f"匯出 {table_name} 到 CSV"
        )
        
        if filename:
            selected_ids = None
            if export_selected:
                selected = self.db_tree.selection()
                if selected:
                    selected_ids = [self.db_tree.item(item)['values'][0] for item in selected]  # 假設第一欄是ID
            
            success, message = self.db_manager.export_to_csv(table_name, filename, selected_ids)
            if success:
                DarkMessageBox.showinfo("成功", message)
            else:
                DarkMessageBox.showerror("錯誤", message)
    
    def export_excel(self, export_selected=False):
        """匯出Excel - 修正檔案大小限制問題"""
        table_name = self.get_english_table_name(self.table_var.get())
        if not table_name:
            DarkMessageBox.showerror("錯誤", "請選擇資料表")
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            title=f"匯出 {table_name} 到 Excel"
        )
        
        if filename:
            selected_ids = None
            if export_selected:
                selected = self.db_tree.selection()
                if selected:
                    selected_ids = [self.db_tree.item(item)['values'][0] for item in selected]
            
            success, message = self.db_manager.export_to_excel(table_name, filename, selected_ids)
            if success:
                DarkMessageBox.showinfo("成功", message)
            else:
                DarkMessageBox.showerror("錯誤", message)
    
    def delete_table_records(self):
        """刪除選取的記錄 - 刪除後會重新下載"""
        selected = self.db_tree.selection()
        if not selected:
            DarkMessageBox.showinfo("資訊", "請先選擇要刪除的記錄")
            return
        
        table_name = self.get_english_table_name(self.table_var.get())
        if not table_name:
            return
        
        # 取得選取的記錄ID
        selected_ids = [self.db_tree.item(item)['values'][0] for item in selected]
        
        if DarkMessageBox.askyesno("確認", f"確定要刪除選取的 {len(selected_ids)} 筆記錄嗎？\n刪除後智慧下載會重新下載這些資料。"):
            placeholders = ','.join('?' for _ in selected_ids)
            success, message = self.db_manager.delete_records(table_name, f"id IN ({placeholders})")
            
            if success:
                DarkMessageBox.showinfo("成功", message)
                self.show_table_data()
            else:
                DarkMessageBox.showerror("錯誤", message)
    
    def clear_table(self):
        """清空資料表 - 清空後會重新下載"""
        table_name = self.get_english_table_name(self.table_var.get())
        if not table_name:
            DarkMessageBox.showerror("錯誤", "請選擇資料表")
            return
        
        if DarkMessageBox.askyesno("確認", f"確定要清空資料表 {table_name} 嗎？\n此操作無法復原！清空後智慧下載會重新下載所有資料。"):
            try:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute(f"DELETE FROM {table_name}")
                conn.commit()
                conn.close()
                DarkMessageBox.showinfo("成功", f"已清空資料表 {table_name}")
                self.show_table_data()
            except Exception as e:
                DarkMessageBox.showerror("錯誤", f"清空失敗: {str(e)}")

    def load_stock_list(self):
        """載入商品清單"""
        asset_type = self.asset_type_var.get()
        
        if asset_type == "stock":
            stocks = self.stock_manager.get_all_stocks()
            self.stock_combo['values'] = stocks
            self.market_frame.grid()
        elif asset_type == "future":
            futures = self.stock_manager.get_derivatives('future')
            self.stock_combo['values'] = futures
            self.market_frame.grid_remove()
        elif asset_type == "option":
            options = self.stock_manager.get_derivatives('option')
            self.stock_combo['values'] = options
            self.market_frame.grid_remove()
        
        if self.stock_combo['values']:
            self.stock_combo.set('')
    
    def on_asset_type_change(self):
        """商品類型變更"""
        self.load_stock_list()
    
    def on_stock_selected(self, event):
        """選擇商品"""
        selected = self.stock_combo.get()
        if selected:
            code = selected.split()[0]
            self.ticker_var.set(code)
    
    def smart_download(self):
        """智慧下載單一商品 - 修正回調函數問題"""
        ticker = self.ticker_var.get().strip()
        if not ticker:
            DarkMessageBox.showerror("錯誤", "請輸入商品代碼")
            return
        
        asset_type = self.asset_type_var.get()
        download_all = self.download_all_var.get()
        
        # 修正：定義正確的回調函數參數數量
        def progress_callback(symbol, status, message):
            progress_value = 0
            if status == 'success':
                progress_value = 100
            elif status == 'progress':
                progress_value = 50
            self.progress['value'] = progress_value
            self.status_var.set(f"{symbol}: {status} - {message}")
        
        # 修正：使用執行緒執行下載，避免介面凍結
        def download_thread():
            try:
                if asset_type == "stock":
                    market = self.market_var.get()
                    if market == "TW" and not ticker.endswith('.TW'):
                        ticker_full = ticker + '.TW'
                    else:
                        ticker_full = ticker
                    
                    success = self.download_manager.smart_download_stock_data(ticker_full, progress_callback, download_all)
                else:
                    success = self.download_manager.smart_download_derivative_data(ticker, progress_callback, download_all)
                
                if success:
                    self.status_var.set("下載完成")
                    DarkMessageBox.showinfo("完成", "智慧下載完成")
                else:
                    self.status_var.set("下載失敗")
                    # 修正錯誤訊息：改為正確的中文錯誤訊息
                    DarkMessageBox.showerror("錯誤", "下載失敗，請檢查商品代碼或網路連線")
                    
            except Exception as e:
                self.status_var.set(f"下載錯誤: {str(e)}")
                DarkMessageBox.showerror("錯誤", f"下載過程中發生錯誤: {str(e)}")
        
        self.status_var.set("開始下載...")
        self.progress['value'] = 0
        
        # 啟動下載執行緒
        thread = threading.Thread(target=download_thread)
        thread.daemon = True
        thread.start()
    
    def show_data(self):
        """顯示資料"""
        ticker = self.ticker_var.get().strip()
        if not ticker:
            DarkMessageBox.showerror("錯誤", "請輸入商品代碼")
            return
        
        # 清除現有資料
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # 從資料庫讀取資料
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        asset_type = self.asset_type_var.get()
        if asset_type == "stock":
            cursor.execute('''
                SELECT date, open, high, low, close, volume, interval
                FROM stock_data 
                WHERE stock_id = ? 
                ORDER BY date DESC, interval
                LIMIT 100
            ''', (ticker,))
        else:
            cursor.execute('''
                SELECT date, open, high, low, close, volume, interval
                FROM derivatives_data 
                WHERE symbol = ? 
                ORDER BY date DESC, interval
                LIMIT 100
            ''', (ticker,))
        
        data = cursor.fetchall()
        conn.close()
        
        if not data:
            DarkMessageBox.showinfo("資訊", "找不到該商品的歷史資料")
            return
        
        for row in data:
            self.tree.insert("", tk.END, values=row)
        
        self.status_var.set(f"顯示 {len(data)} 筆資料")
    
    def plot_data(self):
        """繪製圖表 - 修正中文字體問題"""
        ticker = self.ticker_var.get().strip()
        if not ticker:
            DarkMessageBox.showerror("錯誤", "請輸入商品代碼")
            return
        
        # 從資料庫讀取資料
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        asset_type = self.asset_type_var.get()
        if asset_type == "stock":
            cursor.execute('''
                SELECT date, open, high, low, close, volume 
                FROM stock_data 
                WHERE stock_id = ? AND interval = '1d'
                ORDER BY date
            ''', (ticker,))
        else:
            cursor.execute('''
                SELECT date, open, high, low, close, volume 
                FROM derivatives_data 
                WHERE symbol = ? AND interval = '1d'
                ORDER BY date
            ''', (ticker,))
        
        data = cursor.fetchall()
        conn.close()
        
        if not data:
            DarkMessageBox.showinfo("資訊", "找不到該商品的歷史資料")
            return
        
        # 準備資料
        dates = [datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S') for row in data]
        closes = [row[4] for row in data]
        volumes = [row[5] for row in data]
        
        # 創建圖表視窗
        plot_window = tk.Toplevel(self.root)
        plot_window.title(f"{ticker} 價格走勢圖")
        plot_window.geometry("800x600")
        plot_window.configure(bg='#121212')
        
        # 創建圖表
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
        fig.patch.set_facecolor('#121212')
        
        # 設定中文字體
        plt.rcParams['font.sans-serif'] = [CHINESE_FONT]
        plt.rcParams['axes.unicode_minus'] = False
        
        # 繪製價格走勢
        ax1.plot(dates, closes, label='收盤價', color='#1f77b4', linewidth=2)
        ax1.set_title(f"{ticker} 收盤價走勢", color='white', fontsize=14, fontfamily=CHINESE_FONT)
        ax1.set_ylabel("價格", color='white', fontsize=12, fontfamily=CHINESE_FONT)
        ax1.tick_params(colors='white')
        ax1.legend(prop={'family': CHINESE_FONT})
        ax1.grid(True, color='#555555', alpha=0.3)
        
        # 繪製成交量
        ax2.bar(dates, volumes, color='#ff7f0e', alpha=0.7)
        ax2.set_title("成交量", color='white', fontsize=14, fontfamily=CHINESE_FONT)
        ax2.set_ylabel("成交量", color='white', fontsize=12, fontfamily=CHINESE_FONT)
        ax2.set_xlabel("日期", color='white', fontsize=12, fontfamily=CHINESE_FONT)
        ax2.tick_params(colors='white')
        ax2.grid(True, color='#555555', alpha=0.3)
        
        # 調整佈局
        plt.tight_layout()
        
        # 嵌入Tkinter
        canvas = FigureCanvasTkAgg(fig, master=plot_window)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    
    def select_all(self):
        """全選"""
        self.batch_listbox.select_set(0, tk.END)
    
    def select_none(self):
        """全不選"""
        self.batch_listbox.select_clear(0, tk.END)
    
    def delete_selected(self):
        """刪除選取的項目"""
        selected_indices = self.batch_listbox.curselection()
        if not selected_indices:
            DarkMessageBox.showinfo("資訊", "請先選擇要刪除的項目")
            return
        
        # 從後往前刪除，避免索引變化
        for index in sorted(selected_indices, reverse=True):
            self.batch_listbox.delete(index)
    
    def refresh_batch_list(self):
        """重新整理批次清單"""
        self.batch_listbox.delete(0, tk.END)
        
        batch_type = self.batch_type_var.get()
        if batch_type == "stock":
            items = self.stock_manager.get_all_stocks()
        elif batch_type == "future":
            items = self.stock_manager.get_derivatives('future')
        else:  # option
            items = self.stock_manager.get_derivatives('option')
        
        for item in items:
            self.batch_listbox.insert(tk.END, item)
    
    def start_batch_download(self):
        """開始批次下載"""
        selected_indices = self.batch_listbox.curselection()
        if not selected_indices:
            DarkMessageBox.showerror("錯誤", "請選擇要下載的商品")
            return
        
        # 取得選取的代碼
        selected_items = []
        for index in selected_indices:
            item = self.batch_listbox.get(index)
            code = item.split()[0]  # 提取代碼部分
            selected_items.append(code)
        
        batch_type = self.batch_type_var.get()
        download_all = self.batch_download_all_var.get()
        
        def progress_callback(progress, message):
            self.batch_progress['value'] = progress
            self.batch_status_var.set(message)
        
        def completion_callback(success_count, fail_count, skip_count):
            self.batch_progress['value'] = 100
            self.batch_status_var.set("批次下載完成")
            self.start_batch_btn.config(state=tk.NORMAL)
            self.stop_batch_btn.config(state=tk.DISABLED)
            
            # 修正：批次完成視窗自動關閉，不顯示確定按鈕
            completion_window = tk.Toplevel(self.root)
            completion_window.title("批次下載完成")
            completion_window.configure(bg='#121212')
            completion_window.geometry("400x200")
            completion_window.transient(self.root)
            completion_window.grab_set()
            
            # 設定視窗置中
            completion_window.update_idletasks()
            x = (completion_window.winfo_screenwidth() // 2) - (400 // 2)
            y = (completion_window.winfo_screenheight() // 2) - (200 // 2)
            completion_window.geometry(f'400x200+{x}+{y}')
            
            # 顯示完成訊息
            message_frame = ttk.Frame(completion_window, style='Dark.TFrame')
            message_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
            
            ttk.Label(message_frame, text="批次下載完成", 
                     font=('Microsoft JhengHei', 14, 'bold'),
                     style='Dark.TLabel').pack(pady=10)
            
            ttk.Label(message_frame, 
                     text=f"成功: {success_count} 個\n失敗: {fail_count} 個\n跳過: {skip_count} 個",
                     font=('Microsoft JhengHei', 12),
                     style='Dark.TLabel',
                     justify=tk.CENTER).pack(pady=10)
            
            # 3秒後自動關閉視窗，不顯示確定按鈕
            completion_window.after(3000, completion_window.destroy)
        
        # 更新按鈕狀態
        self.start_batch_btn.config(state=tk.DISABLED)
        self.stop_batch_btn.config(state=tk.NORMAL)
        
        # 開始下載
        if batch_type == "stock":
            success = self.download_manager.batch_download_stocks(
                selected_items, progress_callback, completion_callback, download_all)
        else:
            success = self.download_manager.batch_download_derivatives(
                selected_items, progress_callback, completion_callback, download_all)
        
        if not success:
            DarkMessageBox.showerror("錯誤", "下載啟動失敗")
            self.start_batch_btn.config(state=tk.NORMAL)
            self.stop_batch_btn.config(state=tk.DISABLED)
    
    def stop_batch_download(self):
        """停止批次下載"""
        self.download_manager.stop_download()
        self.batch_status_var.set("下載已停止")
        self.start_batch_btn.config(state=tk.NORMAL)
        self.stop_batch_btn.config(state=tk.DISABLED)
    
    def import_csv_list(self):
        """匯入CSV清單"""
        filename = filedialog.askopenfilename(
            title="選擇CSV檔案",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        
        if filename:
            success, message = self.stock_manager.manual_import_csv(filename)
            if success:
                DarkMessageBox.showinfo("成功", message)
                self.status_var.set("CSV匯入成功")
                self.load_stock_list()
            else:
                DarkMessageBox.showerror("錯誤", message)
                self.status_var.set("CSV匯入失敗")
    
    def view_stock_list(self):
        """檢視股票清單"""
        self.show_manage_list("stock")
    
    def view_derivatives_list(self):
        """檢視衍生性商品清單"""
        self.show_manage_list("derivatives")
    
    def view_download_log(self):
        """檢視下載記錄"""
        self.show_manage_list("download_log")
    
    def show_manage_list(self, list_type):
        """顯示管理清單"""
        # 清除現有資料
        for item in self.manage_tree.get_children():
            self.manage_tree.delete(item)
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        if list_type == "stock":
            cursor.execute('SELECT stock_id, stock_name, market, last_updated FROM stock_list ORDER BY stock_id')
            data = cursor.fetchall()
            for row in data:
                self.manage_tree.insert("", tk.END, values=row)
                
        elif list_type == "derivatives":
            cursor.execute('SELECT symbol, name, type, last_updated FROM derivatives_list ORDER BY type, symbol')
            data = cursor.fetchall()
            for row in data:
                self.manage_tree.insert("", tk.END, values=row)
                
        elif list_type == "download_log":
            # 更新欄位標題
            self.manage_tree['columns'] = ("任務類型", "商品代碼", "時間周期", "開始日期", "結束日期", "記錄數", "狀態", "錯誤訊息", "建立時間")
            for col in self.manage_tree['columns']:
                self.manage_tree.heading(col, text=col)
                self.manage_tree.column(col, width=100)
            
            cursor.execute('''
                SELECT task_type, symbol, interval, start_date, end_date, records_downloaded, status, error_message, created_time 
                FROM download_log 
                ORDER BY created_time DESC 
                LIMIT 100
            ''')
            data = cursor.fetchall()
            for row in data:
                self.manage_tree.insert("", tk.END, values=row)
        
        conn.close()
        self.status_var.set(f"顯示 {len(data)} 筆{list_type}記錄")
    
    def check_initial_update(self):
        """檢查初始更新"""
        # 這裡可以加入自動更新檢查邏輯
        pass

if __name__ == "__main__":
    root = tk.Tk()
    app = StockDataDownloader(root)
    root.mainloop()