# 檔名：backtest_system_enhanced.py
# 程式用途：增強版量化交易回測系統，支持單選和多選策略，顯示策略細節

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.patches import Rectangle
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.dates as mdates
from datetime import datetime
import warnings
import os
warnings.filterwarnings('ignore')

# 設定中文字體解決亂碼問題，字體大小16
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['font.size'] = 16  # 設定圖表字體大小為16

class BacktestEngine:
    def __init__(self, data, slippage=2, commission=0.0002):
        self.data = data.copy()
        self.slippage = slippage
        self.commission = commission
        self.calculate_indicators()
    
    def calculate_indicators(self):
        """計算技術指標"""
        # 計算指數移動平均線
        self.data['EMA20'] = self.data['Close'].ewm(span=20).mean()
        self.data['EMA50'] = self.data['Close'].ewm(span=50).mean()
        self.data['EMA12'] = self.data['Close'].ewm(span=12).mean()
        self.data['EMA26'] = self.data['Close'].ewm(span=26).mean()
        self.data['SMA5'] = self.data['Close'].rolling(window=5).mean()
        self.data['SMA10'] = self.data['Close'].rolling(window=10).mean()
        self.data['SMA20'] = self.data['Close'].rolling(window=20).mean()
        self.data['SMA50'] = self.data['Close'].rolling(window=50).mean()
        
        # 計算MACD
        self.data['MACD'] = self.data['EMA12'] - self.data['EMA26']
        self.data['MACD_Signal'] = self.data['MACD'].ewm(span=9).mean()
        self.data['MACD_Histogram'] = self.data['MACD'] - self.data['MACD_Signal']
        
        # 計算RSI
        delta = self.data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        self.data['RSI'] = 100 - (100 / (1 + rs))
        
        # 計算KD指標
        low_min = self.data['Low'].rolling(window=9).min()
        high_max = self.data['High'].rolling(window=9).max()
        self.data['%K'] = (self.data['Close'] - low_min) / (high_max - low_min) * 100
        self.data['%D'] = self.data['%K'].rolling(window=3).mean()
        
        # 計算布林通道
        self.data['BB_Middle'] = self.data['Close'].rolling(window=20).mean()
        bb_std = self.data['Close'].rolling(window=20).std()
        self.data['BB_Upper'] = self.data['BB_Middle'] + (bb_std * 2)
        self.data['BB_Lower'] = self.data['BB_Middle'] - (bb_std * 2)
        
        # 計算平均真實波幅(ATR)
        high_low = self.data['High'] - self.data['Low']
        high_close = np.abs(self.data['High'] - self.data['Close'].shift())
        low_close = np.abs(self.data['Low'] - self.data['Close'].shift())
        true_range = np.maximum(np.maximum(high_low, high_close), low_close)
        self.data['ATR'] = true_range.ewm(span=14).mean()
        
        # 計算成交量指標
        self.data['Volume_MA20'] = self.data['Volume'].rolling(window=20).mean()
        self.data['Volume_MA50'] = self.data['Volume'].rolling(window=50).mean()
        
        # 計算價格動量
        self.data['Momentum'] = self.data['Close'] - self.data['Close'].shift(5)
        
        # 處理時間資料
        if 'Datetime' in self.data.columns:
            self.data['Time'] = pd.to_datetime(self.data['Datetime']).dt.time
            self.data['Date'] = pd.to_datetime(self.data['Datetime']).dt.date
            self.data['Is_ORB_Period'] = self.data['Time'].between(
                pd.Timestamp('09:00:00').time(), 
                pd.Timestamp('09:15:00').time()
            )
            
            # 計算每個交易日的ORB高低點
            orb_highs = self.data[self.data['Is_ORB_Period']].groupby('Date')['High'].max()
            orb_lows = self.data[self.data['Is_ORB_Period']].groupby('Date')['Low'].min()
            
            self.data['ORB_High'] = self.data['Date'].map(orb_highs)
            self.data['ORB_Low'] = self.data['Date'].map(orb_lows)
    
    # ==================== 基礎策略 ====================
    def kd_crossover_strategy(self):
        """KD金叉策略 - 高勝率策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(2, len(self.data)):
            current = self.data.iloc[i]
            prev = self.data.iloc[i-1]
            prev2 = self.data.iloc[i-2]
            
            # KD金叉（做多信號）
            if (prev['%K'] > prev['%D'] and 
                prev2['%K'] <= prev2['%D'] and 
                prev['%K'] < 30 and 
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['Close'] - current['ATR'] * 1.5
                entry_index = i
                trades.append({
                    'strategy': 'KD金叉策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件
            elif position == 'long':
                profit_target = entry_price + (current['ATR'] * 2)
                
                if (current['Low'] <= stop_loss or 
                    current['High'] >= profit_target or
                    current['%K'] > 80):
                    exit_price = current['Close'] - self.slippage
                    trades.append({
                        'strategy': 'KD金叉策略',
                        'type': 'exit',
                        'datetime': current['Datetime'] if 'Datetime' in current else i,
                        'price': exit_price,
                        'position': position,
                        'reason': 'stop_loss' if current['Low'] <= stop_loss else 'profit_take',
                        'entry_index': entry_index,
                        'exit_index': i
                    })
                    position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': 'KD金叉策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades
    
    def rsi_overbought_oversold_strategy(self):
        """RSI超賣反彈策略 - 高勝率策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(1, len(self.data)):
            current = self.data.iloc[i]
            prev = self.data.iloc[i-1]
            
            # RSI超賣（做多信號）
            if (prev['RSI'] < 30 and current['RSI'] > prev['RSI'] and 
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['Close'] - current['ATR'] * 1.5
                entry_index = i
                trades.append({
                    'strategy': 'RSI超賣反彈策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件
            elif position == 'long':
                profit_target = entry_price + (current['ATR'] * 2)
                
                if (current['Low'] <= stop_loss or 
                    current['High'] >= profit_target or
                    current['RSI'] > 70):
                    exit_price = current['Close'] - self.slippage
                    trades.append({
                        'strategy': 'RSI超賣反彈策略',
                        'type': 'exit',
                        'datetime': current['Datetime'] if 'Datetime' in current else i,
                        'price': exit_price,
                        'position': position,
                        'reason': 'stop_loss' if current['Low'] <= stop_loss else 'profit_take',
                        'entry_index': entry_index,
                        'exit_index': i
                    })
                    position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': 'RSI超賣反彈策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades
    
    def volume_breakout_strategy(self):
        """成交量突破策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(1, len(self.data)):
            current = self.data.iloc[i]
            prev = self.data.iloc[i-1]
            
            # 成交量突破 + 價格上漲（做多信號）
            if (current['Volume'] > current['Volume_MA20'] * 1.5 and 
                current['Close'] > current['Open'] and 
                current['Close'] > prev['Close'] and 
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['Close'] - current['ATR'] * 1.5
                entry_index = i
                trades.append({
                    'strategy': '成交量突破策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件
            elif position == 'long':
                profit_target = entry_price + (current['ATR'] * 2)
                
                if (current['Low'] <= stop_loss or 
                    current['High'] >= profit_target):
                    exit_price = current['Close'] - self.slippage
                    trades.append({
                        'strategy': '成交量突破策略',
                        'type': 'exit',
                        'datetime': current['Datetime'] if 'Datetime' in current else i,
                        'price': exit_price,
                        'position': position,
                        'reason': 'stop_loss' if current['Low'] <= stop_loss else 'profit_take',
                        'entry_index': entry_index,
                        'exit_index': i
                    })
                    position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': '成交量突破策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades
    
    def ema_rebound_strategy(self):
        """EMA反彈策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(1, len(self.data)):
            current = self.data.iloc[i]
            prev = self.data.iloc[i-1]
            
            # 檢查是否觸及50EMA ±1 tick（反彈條件）
            ema_touch = (prev['Low'] <= (prev['EMA50'] + 1) and 
                        prev['High'] >= (prev['EMA50'] - 1))
            
            # 收盤向上條件（確認反彈）
            close_up = current['Close'] > current['Open']
            
            # 進場條件：觸碰50EMA且收盤向上，且無持倉
            if ema_touch and close_up and position is None:
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['EMA20'] - current['ATR'] * 1
                entry_index = i
                trades.append({
                    'strategy': 'EMA反彈策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 持倉狀態下的出場條件
            elif position == 'long':
                stop_level = current['EMA20'] - current['ATR'] * 1
                
                # 停損出場
                if current['Low'] <= stop_level:
                    exit_price = stop_level - self.slippage
                    trades.append({
                        'strategy': 'EMA反彈策略',
                        'type': 'exit',
                        'datetime': current['Datetime'] if 'Datetime' in current else i,
                        'price': exit_price,
                        'position': position,
                        'reason': 'stop_loss',
                        'entry_index': entry_index,
                        'exit_index': i
                    })
                    position = None
                
                # 強制平倉：數據結束時出場
                elif i == len(self.data) - 1:
                    exit_price = current['Close'] - self.slippage
                    trades.append({
                        'strategy': 'EMA反彈策略',
                        'type': 'exit',
                        'datetime': current['Datetime'] if 'Datetime' in current else i,
                        'price': exit_price,
                        'position': position,
                        'reason': 'force_close',
                        'entry_index': entry_index,
                        'exit_index': i
                    })
                    position = None
        
        return trades
    
    def macd_crossover_strategy(self):
        """MACD金叉策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(2, len(self.data)):
            current = self.data.iloc[i]
            prev = self.data.iloc[i-1]
            prev2 = self.data.iloc[i-2]
            
            # MACD金叉（做多信號）
            if (prev['MACD'] > prev['MACD_Signal'] and 
                prev2['MACD'] <= prev2['MACD_Signal'] and 
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['Close'] - current['ATR'] * 2
                entry_index = i
                trades.append({
                    'strategy': 'MACD金叉策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件
            elif position == 'long':
                # 停損或MACD轉弱出場
                if (current['Low'] <= stop_loss or 
                    current['MACD'] < current['MACD_Signal']):
                    exit_price = current['Close'] - self.slippage
                    trades.append({
                        'strategy': 'MACD金叉策略',
                        'type': 'exit',
                        'datetime': current['Datetime'] if 'Datetime' in current else i,
                        'price': exit_price,
                        'position': position,
                        'reason': 'stop_loss' if current['Low'] <= stop_loss else 'signal_exit',
                        'entry_index': entry_index,
                        'exit_index': i
                    })
                    position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': 'MACD金叉策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades

    # ==================== 新增高勝率策略 ====================
    def dual_ma_crossover_strategy(self):
        """雙均線金叉策略 - 高勝率策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(1, len(self.data)):
            current = self.data.iloc[i]
            prev = self.data.iloc[i-1]
            
            # 5MA突破20MA金叉（做多信號）
            if (prev['SMA5'] > prev['SMA20'] and 
                current['SMA5'] > current['SMA20'] and 
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['Close'] - current['ATR'] * 1.5
                entry_index = i
                trades.append({
                    'strategy': '雙均線金叉策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件：死叉出場
            elif position == 'long' and current['SMA5'] < current['SMA20']:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': '雙均線金叉策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'reverse_cross',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': '雙均線金叉策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades
    
    def bollinger_breakout_strategy(self):
        """布林通道突破策略 - 高勝率策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(1, len(self.data)):
            current = self.data.iloc[i]
            prev = self.data.iloc[i-1]
            
            # 突破上軌做多
            if (current['Close'] > current['BB_Upper'] and 
                prev['Close'] <= prev['BB_Upper'] and 
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['BB_Middle'] - current['ATR'] * 0.5
                entry_index = i
                trades.append({
                    'strategy': '布林通道突破策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件：回到中軌
            elif position == 'long' and current['Close'] <= current['BB_Middle']:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': '布林通道突破策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'middle_band_exit',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': '布林通道突破策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades
    
    def rsi_divergence_strategy(self):
        """RSI背離策略 - 高勝率策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(5, len(self.data)):
            current = self.data.iloc[i]
            
            # RSI底背離（價格創新低，RSI未創新低）
            if (current['Low'] < self.data.iloc[i-1]['Low'] and 
                current['RSI'] > self.data.iloc[i-1]['RSI'] and 
                current['RSI'] < 35 and 
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['Close'] - current['ATR'] * 1.5
                entry_index = i
                trades.append({
                    'strategy': 'RSI背離策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件
            elif position == 'long':
                profit_target = entry_price + (current['ATR'] * 2)
                
                if (current['Low'] <= stop_loss or 
                    current['High'] >= profit_target or
                    current['RSI'] > 70):
                    exit_price = current['Close'] - self.slippage
                    trades.append({
                        'strategy': 'RSI背離策略',
                        'type': 'exit',
                        'datetime': current['Datetime'] if 'Datetime' in current else i,
                        'price': exit_price,
                        'position': position,
                        'reason': 'stop_loss' if current['Low'] <= stop_loss else 'profit_take',
                        'entry_index': entry_index,
                        'exit_index': i
                    })
                    position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': 'RSI背離策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades
    
    def volume_price_confirmation_strategy(self):
        """量價確認策略 - 高勝率策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(2, len(self.data)):
            current = self.data.iloc[i]
            prev = self.data.iloc[i-1]
            prev2 = self.data.iloc[i-2]
            
            # 量價齊揚（價格上漲且成交量放大）
            if (current['Close'] > prev['Close'] and 
                current['Volume'] > prev['Volume'] and 
                current['Volume'] > current['Volume_MA20'] and 
                prev['Close'] > prev2['Close'] and 
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['Close'] - current['ATR'] * 1.2
                entry_index = i
                trades.append({
                    'strategy': '量價確認策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件
            elif position == 'long':
                profit_target = entry_price + (current['ATR'] * 2.5)
                
                if (current['Low'] <= stop_loss or 
                    current['High'] >= profit_target or
                    current['Volume'] < current['Volume_MA20']):
                    exit_price = current['Close'] - self.slippage
                    trades.append({
                        'strategy': '量價確認策略',
                        'type': 'exit',
                        'datetime': current['Datetime'] if 'Datetime' in current else i,
                        'price': exit_price,
                        'position': position,
                        'reason': 'stop_loss' if current['Low'] <= stop_loss else 'profit_take',
                        'entry_index': entry_index,
                        'exit_index': i
                    })
                    position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': '量價確認策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades
    
    def ema_trend_following_strategy(self):
        """EMA趨勢跟隨策略 - 高勝率策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(1, len(self.data)):
            current = self.data.iloc[i]
            prev = self.data.iloc[i-1]
            
            # 多頭排列（EMA12 > EMA26 > EMA50）且價格在EMA12之上
            if (current['EMA12'] > current['EMA26'] and 
                current['EMA26'] > current['EMA50'] and 
                current['Close'] > current['EMA12'] and 
                prev['Close'] <= prev['EMA12'] and 
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['EMA26'] - current['ATR'] * 0.8
                entry_index = i
                trades.append({
                    'strategy': 'EMA趨勢跟隨策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件：價格跌破EMA26
            elif position == 'long' and current['Close'] < current['EMA26']:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': 'EMA趨勢跟隨策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'trend_break',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': 'EMA趨勢跟隨策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades
    
    def macd_histogram_strategy(self):
        """MACD柱狀圖策略 - 高勝率策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(2, len(self.data)):
            current = self.data.iloc[i]
            prev = self.data.iloc[i-1]
            prev2 = self.data.iloc[i-2]
            
            # MACD柱狀圖由負轉正
            if (prev['MACD_Histogram'] > 0 and 
                prev2['MACD_Histogram'] <= 0 and 
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['Close'] - current['ATR'] * 1.8
                entry_index = i
                trades.append({
                    'strategy': 'MACD柱狀圖策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件：MACD柱狀圖由正轉負
            elif position == 'long' and current['MACD_Histogram'] < 0:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': 'MACD柱狀圖策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'histogram_turn',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': 'MACD柱狀圖策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades
    
    def kd_momentum_strategy(self):
        """KD動量策略 - 高勝率策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(3, len(self.data)):
            current = self.data.iloc[i]
            prev = self.data.iloc[i-1]
            prev2 = self.data.iloc[i-2]
            prev3 = self.data.iloc[i-3]
            
            # KD在超賣區且連續上升
            if (prev['%K'] < 30 and 
                prev['%K'] > prev2['%K'] and 
                prev2['%K'] > prev3['%K'] and 
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['Close'] - current['ATR'] * 1.3
                entry_index = i
                trades.append({
                    'strategy': 'KD動量策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件
            elif position == 'long':
                profit_target = entry_price + (current['ATR'] * 2.2)
                
                if (current['Low'] <= stop_loss or 
                    current['High'] >= profit_target or
                    current['%K'] > 75):
                    exit_price = current['Close'] - self.slippage
                    trades.append({
                        'strategy': 'KD動量策略',
                        'type': 'exit',
                        'datetime': current['Datetime'] if 'Datetime' in current else i,
                        'price': exit_price,
                        'position': position,
                        'reason': 'stop_loss' if current['Low'] <= stop_loss else 'profit_take',
                        'entry_index': entry_index,
                        'exit_index': i
                    })
                    position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': 'KD動量策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades
    
    def bollinger_squeeze_strategy(self):
        """布林通道擠壓策略 - 高勝率策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(2, len(self.data)):
            current = self.data.iloc[i]
            prev = self.data.iloc[i-1]
            
            # 布林通道擠壓（通道寬度縮小）後突破
            bb_width = (current['BB_Upper'] - current['BB_Lower']) / current['BB_Middle']
            prev_bb_width = (prev['BB_Upper'] - prev['BB_Lower']) / prev['BB_Middle']
            
            if (bb_width < 0.05 and  # 通道擠壓
                prev_bb_width >= 0.05 and 
                current['Close'] > current['BB_Upper'] and 
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['BB_Middle'] - current['ATR'] * 0.6
                entry_index = i
                trades.append({
                    'strategy': '布林通道擠壓策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件：回到中軌
            elif position == 'long' and current['Close'] <= current['BB_Middle']:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': '布林通道擠壓策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'middle_band_exit',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': '布林通道擠壓策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades
    
    def triple_screen_strategy(self):
        """三重濾網策略 - 高勝率策略"""
        trades = []
        position = None
        entry_price = 0
        entry_index = 0
        
        for i in range(5, len(self.data)):
            current = self.data.iloc[i]
            
            # 第一重：趨勢判斷（EMA12 > EMA26）
            # 第二重：動量判斷（MACD > 0）
            # 第三重：進場時機（RSI從超賣區回升）
            if (current['EMA12'] > current['EMA26'] and  # 趨勢向上
                current['MACD'] > 0 and  # 動量向上
                current['RSI'] > 30 and current['RSI'] < 70 and  # RSI在合理區間
                self.data.iloc[i-1]['RSI'] < 30 and  # 前一根RSI在超賣區
                position is None):
                entry_price = current['Close'] + self.slippage
                position = 'long'
                stop_loss = current['Close'] - current['ATR'] * 1.4
                entry_index = i
                trades.append({
                    'strategy': '三重濾網策略',
                    'type': 'entry',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': entry_price,
                    'position': position,
                    'stop_loss': stop_loss,
                    'index': i
                })
            
            # 出場條件
            elif position == 'long':
                profit_target = entry_price + (current['ATR'] * 2.8)
                
                if (current['Low'] <= stop_loss or 
                    current['High'] >= profit_target or
                    current['EMA12'] < current['EMA26']):  # 趨勢反轉
                    exit_price = current['Close'] - self.slippage
                    trades.append({
                        'strategy': '三重濾網策略',
                        'type': 'exit',
                        'datetime': current['Datetime'] if 'Datetime' in current else i,
                        'price': exit_price,
                        'position': position,
                        'reason': 'stop_loss' if current['Low'] <= stop_loss else 'profit_take',
                        'entry_index': entry_index,
                        'exit_index': i
                    })
                    position = None
            
            # 強制平倉
            if position and i == len(self.data) - 1:
                exit_price = current['Close'] - self.slippage
                trades.append({
                    'strategy': '三重濾網策略',
                    'type': 'exit',
                    'datetime': current['Datetime'] if 'Datetime' in current else i,
                    'price': exit_price,
                    'position': position,
                    'reason': 'force_close',
                    'entry_index': entry_index,
                    'exit_index': i
                })
                position = None
        
        return trades

    def get_strategy_details(self, strategy_name):
        """獲取策略詳細參數"""
        strategy_details = {
            'KD金叉策略': {
                'description': 'KD指標金叉交易策略，在超賣區出現金叉時進場',
                'entry_condition': 'K值從下向上突破D值，且K值<30（超賣區）',
                'stop_loss': '進場價 - 1.5 × ATR',
                'take_profit': '進場價 + 2 × ATR',
                'exit_condition': 'K值>80（超買區）或達到停利/停損點',
                'parameters': 'KD週期: 9,3 / ATR週期: 14',
                'timeframe': '適用所有時間框架'
            },
            'RSI超賣反彈策略': {
                'description': 'RSI超賣反彈交易策略，捕捉超賣後的反彈機會',
                'entry_condition': 'RSI < 30且開始回升',
                'stop_loss': '進場價 - 1.5 × ATR',
                'take_profit': '進場價 + 2 × ATR',
                'exit_condition': 'RSI > 70（超買區）或達到停利/停損點',
                'parameters': 'RSI週期: 14 / ATR週期: 14',
                'timeframe': '適用所有時間框架'
            },
            '成交量突破策略': {
                'description': '成交量突破交易策略，跟隨資金流向',
                'entry_condition': '成交量 > 1.5倍20期均量且價格上漲',
                'stop_loss': '進場價 - 1.5 × ATR',
                'take_profit': '進場價 + 2 × ATR',
                'exit_condition': '達到停利/停損點',
                'parameters': '成交量均線週期: 20 / ATR週期: 14',
                'timeframe': '適用所有時間框架'
            },
            'EMA反彈策略': {
                'description': 'EMA均線反彈交易策略，利用均線支撐效果',
                'entry_condition': '價格觸碰50EMA後反彈向上收陽線',
                'stop_loss': '20EMA - 1 × ATR',
                'take_profit': '無固定停利，移動停損出場',
                'exit_condition': '價格跌破20EMA - ATR支撐',
                'parameters': 'EMA週期: 20,50 / ATR週期: 14',
                'timeframe': '5分鐘K線'
            },
            'MACD金叉策略': {
                'description': 'MACD指標金叉交易策略，捕捉趨勢轉折點',
                'entry_condition': 'MACD從下向上突破信號線',
                'stop_loss': '進場價 - 2 × ATR',
                'take_profit': '無固定停利，信號反轉出場',
                'exit_condition': 'MACD跌破信號線或達到停損點',
                'parameters': 'MACD參數: 12,26,9 / ATR週期: 14',
                'timeframe': '適用所有時間框架'
            },
            '雙均線金叉策略': {
                'description': '雙移動平均線金叉策略，確認趨勢方向',
                'entry_condition': '5MA從下向上突破20MA形成金叉',
                'stop_loss': '進場價 - 1.5 × ATR',
                'take_profit': '無固定停利，死叉出場',
                'exit_condition': '5MA跌破20MA形成死叉',
                'parameters': 'MA週期: 5,20 / ATR週期: 14',
                'timeframe': '15分鐘以上K線'
            },
            '布林通道突破策略': {
                'description': '布林通道突破策略，捕捉波動率突破',
                'entry_condition': '收盤價突破布林上軌',
                'stop_loss': '布林中軌 - 0.5 × ATR',
                'take_profit': '無固定停利，回到中軌出場',
                'exit_condition': '收盤價回到布林中軌',
                'parameters': '布林週期: 20 / 標準差: 2 / ATR週期: 14',
                'timeframe': '適用所有時間框架'
            },
            'RSI背離策略': {
                'description': 'RSI指標背離策略，捕捉趨勢反轉信號',
                'entry_condition': '價格創新低但RSI未創新低（底背離）',
                'stop_loss': '進場價 - 1.5 × ATR',
                'take_profit': '進場價 + 2 × ATR',
                'exit_condition': 'RSI > 70或達到停利/停損點',
                'parameters': 'RSI週期: 14 / ATR週期: 14',
                'timeframe': '30分鐘以上K線'
            },
            '量價確認策略': {
                'description': '量價確認策略，確認價格走勢的有效性',
                'entry_condition': '價格連續上漲且成交量同步放大',
                'stop_loss': '進場價 - 1.2 × ATR',
                'take_profit': '進場價 + 2.5 × ATR',
                'exit_condition': '成交量萎縮或達到停利/停損點',
                'parameters': '成交量均線: 20 / ATR週期: 14',
                'timeframe': '日線或更長時間框架'
            },
            'EMA趨勢跟隨策略': {
                'description': 'EMA多頭排列趨勢跟隨策略',
                'entry_condition': 'EMA多頭排列且價格突破EMA12',
                'stop_loss': 'EMA26 - 0.8 × ATR',
                'take_profit': '無固定停利，趨勢破壞出場',
                'exit_condition': '價格跌破EMA26',
                'parameters': 'EMA週期: 12,26,50 / ATR週期: 14',
                'timeframe': '1小時以上K線'
            },
            'MACD柱狀圖策略': {
                'description': 'MACD柱狀圖轉向策略',
                'entry_condition': 'MACD柱狀圖由負轉正',
                'stop_loss': '進場價 - 1.8 × ATR',
                'take_profit': '無固定停利，柱狀圖轉負出場',
                'exit_condition': 'MACD柱狀圖由正轉負',
                'parameters': 'MACD參數: 12,26,9 / ATR週期: 14',
                'timeframe': '適用所有時間框架'
            },
            'KD動量策略': {
                'description': 'KD指標動量策略',
                'entry_condition': 'KD在超賣區連續三日上升',
                'stop_loss': '進場價 - 1.3 × ATR',
                'take_profit': '進場價 + 2.2 × ATR',
                'exit_condition': 'K值>75或達到停利/停損點',
                'parameters': 'KD週期: 9,3 / ATR週期: 14',
                'timeframe': '日線或更長時間框架'
            },
            '布林通道擠壓策略': {
                'description': '布林通道擠壓突破策略',
                'entry_condition': '布林通道擠壓後向上突破',
                'stop_loss': '布林中軌 - 0.6 × ATR',
                'take_profit': '無固定停利，回到中軌出場',
                'exit_condition': '價格回到布林中軌',
                'parameters': '布林週期: 20 / ATR週期: 14',
                'timeframe': '30分鐘以上K線'
            },
            '三重濾網策略': {
                'description': '亞歷山大·埃爾德三重濾網交易系統',
                'entry_condition': '趨勢向上 + 動量向上 + RSI從超賣區回升',
                'stop_loss': '進場價 - 1.4 × ATR',
                'take_profit': '進場價 + 2.8 × ATR',
                'exit_condition': '趨勢反轉或達到停利/停損點',
                'parameters': 'EMA:12,26 / MACD:12,26,9 / RSI:14 / ATR:14',
                'timeframe': '4小時或日線'
            }
        }
        return strategy_details.get(strategy_name, {})
    
    def calculate_performance(self, trades):
        """計算策略績效指標"""
        if len(trades) < 2:
            return None
        
        trade_records = []
        equity_curve = [100000]  # 初始資金10萬元
        current_equity = 100000
        
        # 配對進出場交易
        for i in range(0, len(trades)-1, 2):
            if trades[i]['type'] == 'entry' and trades[i+1]['type'] == 'exit':
                entry = trades[i]
                exit_trade = trades[i+1]
                
                # 計算損益點數
                if entry['position'] == 'long':
                    pnl_points = exit_trade['price'] - entry['price']
                else:
                    pnl_points = entry['price'] - exit_trade['price']
                
                # 計算交易成本（手續費）
                trade_cost = (entry['price'] + exit_trade['price']) * self.commission
                net_pnl = pnl_points - trade_cost
                
                # 更新權益曲線
                current_equity += net_pnl
                equity_curve.append(current_equity)
                
                # 記錄交易明細
                trade_records.append({
                    'strategy': entry['strategy'],
                    'entry_time': entry['datetime'],
                    'exit_time': exit_trade['datetime'],
                    'entry_price': entry['price'],
                    'exit_price': exit_trade['price'],
                    'position': entry['position'],
                    'pnl': net_pnl,
                    'cumulative_equity': current_equity
                })
        
        if not trade_records:
            return None
            
        df_trades = pd.DataFrame(trade_records)
        
        # 計算關鍵績效指標
        total_trades = len(df_trades)
        winning_trades = len(df_trades[df_trades['pnl'] > 0])
        losing_trades = len(df_trades[df_trades['pnl'] < 0])
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        
        total_profit = df_trades[df_trades['pnl'] > 0]['pnl'].sum()
        total_loss = abs(df_trades[df_trades['pnl'] < 0]['pnl'].sum())
        profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
        
        max_drawdown = self.calculate_max_drawdown(equity_curve)
        total_return = (current_equity - 100000) / 100000
        
        # 整合績效結果
        performance = {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'total_profit': total_profit,
            'total_loss': total_loss,
            'profit_factor': profit_factor,
            'max_drawdown': max_drawdown,
            'final_equity': current_equity,
            'total_return': total_return,
            'equity_curve': equity_curve,
            'trade_records': df_trades,
            'trades': trades
        }
        
        return performance
    
    def calculate_max_drawdown(self, equity_curve):
        """計算最大回撤"""
        peak = equity_curve[0]
        max_dd = 0
        
        for equity in equity_curve:
            if equity > peak:
                peak = equity
            dd = (peak - equity) / peak
            if dd > max_dd:
                max_dd = dd
        
        return max_dd

class BacktestApp:
    def __init__(self, root):
        self.root = root
        self.root.title("量化交易回測系統 - 15種策略版")
        
        # 程式啟動時最大化視窗
        self.root.state('zoomed')
        
        self.root.configure(bg='black')
        self.data = None
        self.current_trades = {}
        self.all_strategies_performance = {}
        self.selected_strategies = {}
        
        # 設定字體大小16
        self.font_style = ("Microsoft JhengHei", 16)
        self.title_font = ("Microsoft JhengHei", 18, "bold")
        
        self.setup_ui()
        self.setup_dark_theme()
    
    def setup_dark_theme(self):
        """設定深色主題樣式"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # 設定深色主題顏色
        style.configure('.', 
                       background='black',
                       foreground='white',
                       fieldbackground='black')
        
        style.configure('TLabel', background='black', foreground='white', font=self.font_style)
        style.configure('TButton', background='#333333', foreground='white', font=self.font_style)
        style.configure('TFrame', background='black')
        style.configure('TLabelframe', background='black', foreground='white')
        style.configure('TLabelframe.Label', background='black', foreground='white', font=self.title_font)
        style.configure('TNotebook', background='black')
        style.configure('TNotebook.Tab', background='#333333', foreground='white', font=self.font_style)
        style.configure('Treeview', 
                       background='black', 
                       foreground='white',
                       fieldbackground='black',
                       font=self.font_style)
        style.configure('Treeview.Heading', 
                       background='#333333', 
                       foreground='white',
                       font=self.font_style)
        style.configure('TCheckbutton', background='black', foreground='white', font=self.font_style)
        style.configure('TRadiobutton', background='black', foreground='white', font=self.font_style)
    
    def setup_ui(self):
        """設置使用者介面"""
        # 主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 控制面板框架
        control_frame = ttk.LabelFrame(main_frame, text="控制面板", padding="10")
        control_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=5)
        
        # 文件選擇按鈕
        ttk.Button(control_frame, text="選擇CSV文件", 
                  command=self.load_csv).grid(row=0, column=0, padx=5, pady=5)
        
        # 參數設置區域
        ttk.Label(control_frame, text="滑點:").grid(row=0, column=1, padx=5)
        self.slippage_var = tk.StringVar(value="2")
        ttk.Entry(control_frame, textvariable=self.slippage_var, width=8, font=self.font_style).grid(row=0, column=2, padx=5)
        
        ttk.Label(control_frame, text="手續費(%):").grid(row=0, column=3, padx=5)
        self.commission_var = tk.StringVar(value="0.02")
        ttk.Entry(control_frame, textvariable=self.commission_var, width=8, font=self.font_style).grid(row=0, column=4, padx=5)
        
        # 策略選擇模式
        ttk.Label(control_frame, text="回測模式:").grid(row=0, column=5, padx=5)
        self.mode_var = tk.StringVar(value="multi")
        mode_combo = ttk.Combobox(control_frame, textvariable=self.mode_var, 
                                 values=["single", "multi"], width=10, font=self.font_style)
        mode_combo.grid(row=0, column=6, padx=5)
        mode_combo.bind('<<ComboboxSelected>>', self.on_mode_change)
        
        # 執行回測按鈕
        ttk.Button(control_frame, text="執行回測", 
                  command=self.run_backtest).grid(row=0, column=7, padx=10, pady=5)
        
        # 策略選擇框架
        self.strategy_frame = ttk.LabelFrame(main_frame, text="策略選擇 (15種策略)", padding="10")
        self.strategy_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=5)
        
        self.setup_strategy_selection()
        
        # 結果顯示框架
        result_frame = ttk.LabelFrame(main_frame, text="回測結果", padding="10")
        result_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        # 創建多頁面筆記本
        notebook = ttk.Notebook(result_frame)
        notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 績效報表頁面
        performance_frame = ttk.Frame(notebook, padding="10")
        notebook.add(performance_frame, text="策略績效")
        
        # 績效樹狀視圖
        self.performance_tree = ttk.Treeview(performance_frame, 
                                            columns=('策略名稱', '交易次數', '勝率', '獲利因子', '總盈利', '最大回撤', '總報酬率'), 
                                            show='headings',
                                            height=15)
        self.performance_tree.heading('策略名稱', text='策略名稱')
        self.performance_tree.heading('交易次數', text='交易次數')
        self.performance_tree.heading('勝率', text='勝率')
        self.performance_tree.heading('獲利因子', text='獲利因子')
        self.performance_tree.heading('總盈利', text='總盈利')
        self.performance_tree.heading('最大回撤', text='最大回撤')
        self.performance_tree.heading('總報酬率', text='總報酬率')
        
        # 設定欄位寬度
        columns_config = {
            '策略名稱': 200,
            '交易次數': 100,
            '勝率': 100,
            '獲利因子': 100,
            '總盈利': 120,
            '最大回撤': 100,
            '總報酬率': 100
        }
        
        for col, width in columns_config.items():
            self.performance_tree.column(col, width=width)
        
        self.performance_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 策略細節框架
        self.details_frame = ttk.LabelFrame(performance_frame, text="策略細節")
        self.details_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=5)
        
        # 策略細節文字區域 - 字體改為16
        self.details_text = tk.Text(self.details_frame, height=8, width=100, 
                                   font=("Microsoft JhengHei", 16), bg='black', fg='white')
        scrollbar = ttk.Scrollbar(self.details_frame, orient=tk.VERTICAL, command=self.details_text.yview)
        self.details_text.configure(yscrollcommand=scrollbar.set)
        self.details_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # 權益曲線頁面
        equity_frame = ttk.Frame(notebook, padding="10")
        notebook.add(equity_frame, text="權益曲線")
        
        # 創建圖形
        self.fig_equity, self.ax_equity = plt.subplots(figsize=(12, 6), facecolor='black')
        self.ax_equity.set_facecolor('black')
        self.canvas_equity = FigureCanvasTkAgg(self.fig_equity, equity_frame)
        self.canvas_equity.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # 交易紀錄頁面
        trades_frame = ttk.Frame(notebook, padding="10")
        notebook.add(trades_frame, text="交易紀錄")
        
        # 交易紀錄樹狀視圖
        self.trades_tree = ttk.Treeview(trades_frame, 
                                       columns=('策略', '進場時間', '出場時間', '進場價', '出場價', '方向', '損益'),
                                       show='headings',
                                       height=15)
        
        # 設定欄位標題和寬度
        columns_config = {
            '策略': 120,
            '進場時間': 150,
            '出場時間': 150,
            '進場價': 100,
            '出場價': 100,
            '方向': 80,
            '損益': 100
        }
        
        for col, width in columns_config.items():
            self.trades_tree.heading(col, text=col)
            self.trades_tree.column(col, width=width)
        
        # 滾動條
        scrollbar = ttk.Scrollbar(trades_frame, orient=tk.VERTICAL, command=self.trades_tree.yview)
        self.trades_tree.configure(yscrollcommand=scrollbar.set)
        self.trades_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # 配置版面權重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)
        result_frame.columnconfigure(0, weight=1)
        result_frame.rowconfigure(0, weight=1)
        performance_frame.columnconfigure(0, weight=1)
        performance_frame.rowconfigure(0, weight=1)
        performance_frame.rowconfigure(1, weight=0)
        trades_frame.columnconfigure(0, weight=1)
        trades_frame.rowconfigure(0, weight=1)
        self.details_frame.columnconfigure(0, weight=1)
    
    def setup_strategy_selection(self):
        """設置策略選擇界面 - 修改為2列，超出時往右排列"""
        # 清空現有內容
        for widget in self.strategy_frame.winfo_children():
            widget.destroy()
        
        # 15種策略列表
        strategies = [
            ('KD金叉策略', 'kd_crossover_strategy'),
            ('RSI超賣反彈策略', 'rsi_overbought_oversold_strategy'),
            ('成交量突破策略', 'volume_breakout_strategy'),
            ('EMA反彈策略', 'ema_rebound_strategy'),
            ('MACD金叉策略', 'macd_crossover_strategy'),
            ('雙均線金叉策略', 'dual_ma_crossover_strategy'),
            ('布林通道突破策略', 'bollinger_breakout_strategy'),
            ('RSI背離策略', 'rsi_divergence_strategy'),
            ('量價確認策略', 'volume_price_confirmation_strategy'),
            ('EMA趨勢跟隨策略', 'ema_trend_following_strategy'),
            ('MACD柱狀圖策略', 'macd_histogram_strategy'),
            ('KD動量策略', 'kd_momentum_strategy'),
            ('布林通道擠壓策略', 'bollinger_squeeze_strategy'),
            ('三重濾網策略', 'triple_screen_strategy')
        ]
        
        # 初始化選中狀態
        for name, key in strategies:
            if key not in self.selected_strategies:
                self.selected_strategies[key] = tk.BooleanVar(value=True)
        
        if self.mode_var.get() == "single":
            # 單選模式：使用單選按鈕
            self.strategy_var = tk.StringVar(value=strategies[0][1])
            # 創建多列顯示，每列7個策略，超出時往右排列
            rows_per_column = 7
            num_columns = (len(strategies) + rows_per_column - 1) // rows_per_column
            
            for i, (name, key) in enumerate(strategies):
                column = i // rows_per_column
                row = i % rows_per_column
                rb = ttk.Radiobutton(self.strategy_frame, text=name, 
                                   variable=self.strategy_var, value=key,
                                   command=self.on_strategy_select)
                rb.grid(row=row, column=column, sticky=tk.W, padx=10, pady=5)
        else:
            # 多選模式：使用複選框
            # 創建多列顯示，每列7個策略，超出時往右排列
            rows_per_column = 7
            num_columns = (len(strategies) + rows_per_column - 1) // rows_per_column
            
            for i, (name, key) in enumerate(strategies):
                column = i // rows_per_column
                row = i % rows_per_column
                cb = ttk.Checkbutton(self.strategy_frame, text=name,
                                   variable=self.selected_strategies[key])
                cb.grid(row=row, column=column, sticky=tk.W, padx=10, pady=5)
    
    def on_mode_change(self, event=None):
        """模式改變事件處理"""
        self.setup_strategy_selection()
    
    def on_strategy_select(self):
        """策略選擇事件處理"""
        if self.mode_var.get() == "single" and hasattr(self, 'strategy_var'):
            selected_strategy = self.strategy_var.get()
            self.display_strategy_details(selected_strategy)
    
    def display_strategy_details(self, strategy_key):
        """顯示策略詳細信息 - 字體16"""
        strategy_name_map = {
            'kd_crossover_strategy': 'KD金叉策略',
            'rsi_overbought_oversold_strategy': 'RSI超賣反彈策略',
            'volume_breakout_strategy': '成交量突破策略',
            'ema_rebound_strategy': 'EMA反彈策略',
            'macd_crossover_strategy': 'MACD金叉策略',
            'dual_ma_crossover_strategy': '雙均線金叉策略',
            'bollinger_breakout_strategy': '布林通道突破策略',
            'rsi_divergence_strategy': 'RSI背離策略',
            'volume_price_confirmation_strategy': '量價確認策略',
            'ema_trend_following_strategy': 'EMA趨勢跟隨策略',
            'macd_histogram_strategy': 'MACD柱狀圖策略',
            'kd_momentum_strategy': 'KD動量策略',
            'bollinger_squeeze_strategy': '布林通道擠壓策略',
            'triple_screen_strategy': '三重濾網策略'
        }
        
        strategy_name = strategy_name_map.get(strategy_key, strategy_key)
        
        if self.data is not None:
            try:
                engine = BacktestEngine(self.data)
                details = engine.get_strategy_details(strategy_name)
                
                self.details_text.delete(1.0, tk.END)
                if details:
                    self.details_text.insert(tk.END, f"策略名稱: {strategy_name}\n\n")
                    self.details_text.insert(tk.END, f"策略描述: {details['description']}\n\n")
                    self.details_text.insert(tk.END, f"進場條件: {details['entry_condition']}\n\n")
                    self.details_text.insert(tk.END, f"停損設定: {details['stop_loss']}\n\n")
                    self.details_text.insert(tk.END, f"停利設定: {details['take_profit']}\n\n")
                    self.details_text.insert(tk.END, f"出場條件: {details['exit_condition']}\n\n")
                    self.details_text.insert(tk.END, f"策略參數: {details['parameters']}\n\n")
                    self.details_text.insert(tk.END, f"適用時間框架: {details['timeframe']}\n")
            except Exception as e:
                self.details_text.insert(tk.END, f"加載策略細節時出錯: {str(e)}")
    
    def load_csv(self):
        """載入CSV歷史數據檔案"""
        file_path = filedialog.askopenfilename(
            filetypes=[("CSV files", "*.csv")],
            initialdir=os.path.dirname(os.path.abspath(__file__))
        )
        if file_path:
            try:
                self.data = pd.read_csv(file_path)
                messagebox.showinfo("成功", f"已載入數據，共{len(self.data)}行")
                
                # 如果是在單選模式，顯示第一個策略的細節
                if self.mode_var.get() == "single" and hasattr(self, 'strategy_var'):
                    self.display_strategy_details(self.strategy_var.get())
            except Exception as e:
                self.show_error_message(f"載入文件失敗: {str(e)}")
    
    def show_error_message(self, message):
        """顯示錯誤訊息視窗"""
        messagebox.showerror("錯誤", message)
    
    def run_backtest(self):
        """執行回測"""
        if self.data is None:
            self.show_error_message("請先選擇CSV文件")
            return
        
        try:
            # 取得使用者設定的參數
            slippage = float(self.slippage_var.get())
            commission = float(self.commission_var.get()) / 100
            
            # 初始化回測引擎
            engine = BacktestEngine(self.data, slippage, commission)
            
            # 根據模式選擇策略
            if self.mode_var.get() == "single":
                # 單選模式：只執行選中的策略
                selected_strategy = self.strategy_var.get()
                strategy_method = getattr(engine, selected_strategy)
                strategy_name_map = {
                    'kd_crossover_strategy': 'KD金叉策略',
                    'rsi_overbought_oversold_strategy': 'RSI超賣反彈策略',
                    'volume_breakout_strategy': '成交量突破策略',
                    'ema_rebound_strategy': 'EMA反彈策略',
                    'macd_crossover_strategy': 'MACD金叉策略',
                    'dual_ma_crossover_strategy': '雙均線金叉策略',
                    'bollinger_breakout_strategy': '布林通道突破策略',
                    'rsi_divergence_strategy': 'RSI背離策略',
                    'volume_price_confirmation_strategy': '量價確認策略',
                    'ema_trend_following_strategy': 'EMA趨勢跟隨策略',
                    'macd_histogram_strategy': 'MACD柱狀圖策略',
                    'kd_momentum_strategy': 'KD動量策略',
                    'bollinger_squeeze_strategy': '布林通道擠壓策略',
                    'triple_screen_strategy': '三重濾網策略'
                }
                strategy_name = strategy_name_map.get(selected_strategy, selected_strategy)
                strategies_to_run = [(strategy_name, strategy_method)]
                
                # 顯示策略細節
                self.display_strategy_details(selected_strategy)
            else:
                # 多選模式：執行所有選中的策略
                strategies_to_run = []
                strategy_map = {
                    'kd_crossover_strategy': ('KD金叉策略', engine.kd_crossover_strategy),
                    'rsi_overbought_oversold_strategy': ('RSI超賣反彈策略', engine.rsi_overbought_oversold_strategy),
                    'volume_breakout_strategy': ('成交量突破策略', engine.volume_breakout_strategy),
                    'ema_rebound_strategy': ('EMA反彈策略', engine.ema_rebound_strategy),
                    'macd_crossover_strategy': ('MACD金叉策略', engine.macd_crossover_strategy),
                    'dual_ma_crossover_strategy': ('雙均線金叉策略', engine.dual_ma_crossover_strategy),
                    'bollinger_breakout_strategy': ('布林通道突破策略', engine.bollinger_breakout_strategy),
                    'rsi_divergence_strategy': ('RSI背離策略', engine.rsi_divergence_strategy),
                    'volume_price_confirmation_strategy': ('量價確認策略', engine.volume_price_confirmation_strategy),
                    'ema_trend_following_strategy': ('EMA趨勢跟隨策略', engine.ema_trend_following_strategy),
                    'macd_histogram_strategy': ('MACD柱狀圖策略', engine.macd_histogram_strategy),
                    'kd_momentum_strategy': ('KD動量策略', engine.kd_momentum_strategy),
                    'bollinger_squeeze_strategy': ('布林通道擠壓策略', engine.bollinger_squeeze_strategy),
                    'triple_screen_strategy': ('三重濾網策略', engine.triple_screen_strategy)
                }
                
                for key, var in self.selected_strategies.items():
                    if var.get() and key in strategy_map:
                        strategies_to_run.append(strategy_map[key])
            
            # 執行策略
            all_trades = []
            all_performances = {}
            
            for strategy_name, strategy_func in strategies_to_run:
                print(f"執行策略: {strategy_name}")
                trades = strategy_func()
                performance = engine.calculate_performance(trades)
                
                if performance:
                    all_performances[strategy_name] = performance
                    # 添加策略名稱到交易記錄中
                    for trade in trades:
                        trade['strategy_name'] = strategy_name
                    all_trades.extend(trades)
            
            # 儲存所有策略績效
            self.all_strategies_performance = all_performances
            self.current_trades = {'all': all_trades}
            
            # 顯示策略排名
            self.display_strategies_ranking(all_performances)
            
            # 顯示交易紀錄和權益曲線
            if all_performances:
                best_strategy = self.get_best_strategy(all_performances)
                if best_strategy:
                    self.display_trades(all_performances[best_strategy]['trades'])
                    self.plot_equity_curve(all_performances)
            
            messagebox.showinfo("回測完成", f"已完成 {len(all_performances)} 種策略回測")
            
        except Exception as e:
            self.show_error_message(f"回測執行失敗: {str(e)}")
    
    def get_best_strategy(self, performances):
        """取得最佳策略（基於總報酬率）"""
        if not performances:
            return None
        
        best_strategy = None
        best_total_return = -float('inf')
        
        for strategy_name, performance in performances.items():
            if performance['total_return'] > best_total_return:
                best_total_return = performance['total_return']
                best_strategy = strategy_name
        
        return best_strategy
    
    def display_strategies_ranking(self, performances):
        """顯示策略排名"""
        # 清空現有數據
        for item in self.performance_tree.get_children():
            self.performance_tree.delete(item)
        
        # 創建策略列表並排序（按總報酬率）
        strategies_list = []
        for strategy_name, performance in performances.items():
            strategies_list.append({
                'name': strategy_name,
                'total_trades': performance['total_trades'],
                'win_rate': performance['win_rate'],
                'profit_factor': performance['profit_factor'],
                'total_profit': performance['total_profit'],
                'max_drawdown': performance['max_drawdown'],
                'total_return': performance['total_return']
            })
        
        # 按總報酬率排序
        strategies_list.sort(key=lambda x: x['total_return'], reverse=True)
        
        # 多策略模式只顯示前5名
        if self.mode_var.get() == "multi":
            strategies_list = strategies_list[:5]
        
        # 顯示策略
        for i, strategy in enumerate(strategies_list):
            self.performance_tree.insert('', 'end', values=(
                f"{i+1}. {strategy['name']}",
                strategy['total_trades'],
                f"{strategy['win_rate']:.2%}",
                f"{strategy['profit_factor']:.2f}",
                f"{strategy['total_profit']:,.2f}",
                f"{strategy['max_drawdown']:.2%}",
                f"{strategy['total_return']:.2%}"
            ))
    
    def display_trades(self, trades):
        """顯示交易紀錄"""
        # 清空現有數據
        for item in self.trades_tree.get_children():
            self.trades_tree.delete(item)
        
        all_trades = []
        
        # 處理交易紀錄
        for i in range(0, len(trades)-1, 2):
            if trades[i]['type'] == 'entry' and trades[i+1]['type'] == 'exit':
                entry = trades[i]
                exit_trade = trades[i+1]
                
                # 計算淨損益（含手續費）
                if entry['position'] == 'long':
                    pnl = exit_trade['price'] - entry['price']
                else:
                    pnl = entry['price'] - exit_trade['price']
                
                pnl -= (entry['price'] + exit_trade['price']) * 0.0002
                
                all_trades.append({
                    'strategy': entry['strategy'],
                    'entry_time': entry['datetime'],
                    'exit_time': exit_trade['datetime'],
                    'entry_price': entry['price'],
                    'exit_price': exit_trade['price'],
                    'position': entry['position'],
                    'pnl': pnl
                })
        
        # 按進場時間排序交易紀錄
        all_trades.sort(key=lambda x: x['entry_time'])
        
        # 將交易紀錄添加到樹狀視圖
        for trade in all_trades:
            self.trades_tree.insert('', 'end', values=(
                trade['strategy'],
                str(trade['entry_time']),
                str(trade['exit_time']),
                f"{trade['entry_price']:.2f}",
                f"{trade['exit_price']:.2f}",
                trade['position'],
                f"{trade['pnl']:.2f}"
            ))
    
    def plot_equity_curve(self, performances):
        """繪製權益曲線圖表"""
        self.ax_equity.clear()
        
        # 設定深色主題圖表樣式
        self.ax_equity.set_facecolor('black')
        self.fig_equity.patch.set_facecolor('black')
        self.ax_equity.tick_params(colors='white', labelsize=16)
        self.ax_equity.xaxis.label.set_color('white')
        self.ax_equity.yaxis.label.set_color('white')
        self.ax_equity.title.set_color('white')
        self.ax_equity.grid(True, color='gray', alpha=0.3)
        
        # 顏色列表
        colors = ['cyan', 'yellow', 'lime', 'orange', 'magenta', 'red', 'blue', 'purple']
        
        # 多策略模式只顯示前5名，單策略模式顯示所有
        if self.mode_var.get() == "multi":
            sorted_strategies = sorted(performances.items(), 
                                     key=lambda x: x[1]['total_return'], reverse=True)[:5]
        else:
            sorted_strategies = sorted(performances.items(), 
                                     key=lambda x: x[1]['total_return'], reverse=True)
        
        # 繪製策略的權益曲線
        for i, (strategy_name, performance) in enumerate(sorted_strategies):
            if 'equity_curve' in performance:
                equity_curve = performance['equity_curve']
                color = colors[i % len(colors)]
                self.ax_equity.plot(range(len(equity_curve)), equity_curve, 
                            label=f'{strategy_name} (報酬:{performance["total_return"]:.1%})', 
                            color=color, linewidth=2)
        
        # 設定圖表標題和標籤
        self.ax_equity.set_xlabel('交易次數', color='white', fontsize=16)
        self.ax_equity.set_ylabel('權益曲線', color='white', fontsize=16)
        
        if self.mode_var.get() == "multi":
            self.ax_equity.set_title('前5名策略權益曲線比較 (按總報酬率)', color='white', fontsize=18)
        else:
            self.ax_equity.set_title('策略權益曲線', color='white', fontsize=18)
            
        self.ax_equity.legend(fontsize=12)
        
        # 更新圖表顯示
        self.canvas_equity.draw()

def main():
    """主程式進入點"""
    root = tk.Tk()
    app = BacktestApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()