# txo_db_ui.py
# 完整程式碼：整合 CSV 讀取、SQLAlchemy ORM+SQLite、自動匯入 Data 資料夾、
# Tkinter GUI、日期範圍查詢、動態圖表佈局（1張、左右2張、2x2三張）與原本分析功能
#
# 使用方式
# 1) 將 CSV 放到程式根目錄下的子資料夾 ./Data
# 2) 執行此檔。第一次會建立 financial.db（若不存在）
# 3) 使用 "自動匯入 Data" 按鈕將資料匯入 DB（或用「選擇檔案」匯入單一檔案）
# 4) 設定日期範圍/視窗，點「讀取 / 更新資料」→「畫圖」
#
# 注意：此檔案為單一完整檔案（不中斷），請直接覆蓋原本程式使用。

import os
import sys
import math
import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
from sqlalchemy import (
    create_engine, Column, Integer, String, Date, DateTime, UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import IntegrityError
import sqlite3
import time

# ---- config ----
FONT_FAMILY = "Microsoft JhengHei"
FONT_SIZE = 14
DEFAULT_STRIKE_WINDOW = 10
ANOM_THRESH = {
    "big_order": (400, 800),
    "major": (800, 1200),
    "extreme": 1200
}
DB_FILENAME = "financial.db"
DATA_DIR = os.path.join(os.getcwd(), "Data")   # 強制資料來源資料夾

# ensure Data folder exists
os.makedirs(DATA_DIR, exist_ok=True)

# matplotlib style
plt.rcParams.update({
    "figure.facecolor": "#000000",
    "axes.facecolor": "#000000",
    "savefig.facecolor": "#000000",
    "text.color": "#FFFFFF",
    "axes.labelcolor": "#FFFFFF",
    "xtick.color": "#FFFFFF",
    "ytick.color": "#FFFFFF",
    "font.family": FONT_FAMILY,
    "font.size": FONT_SIZE - 2
})

# ---- ORM / DB setup ----
Base = declarative_base()

class OptionRaw(Base):
    __tablename__ = "options_raw"
    id = Column(Integer, primary_key=True, autoincrement=True)
    product = Column(String, nullable=False)      # TXO / CAO / CNO ...
    trade_date = Column(Date, nullable=False)
    expiry = Column(String, nullable=False)       # 例如 202401W1
    strike = Column(Integer, nullable=False)
    cp = Column(String, nullable=False)           # C / P or 買權/賣權
    volume = Column(Integer)
    oi = Column(Integer)                          # NULL 表示原始為 '-'
    raw_oi_text = Column(String)                  # 原始字串
    session = Column(String, nullable=False)      # 一般/盤後
    load_file = Column(String)
    created_at = Column(DateTime)

    __table_args__ = (
        UniqueConstraint("trade_date", "product", "expiry", "strike", "cp", "session", name="uq_options_unique_row"),
        Index("idx_options_trade_date", "trade_date"),
        Index("idx_options_product", "product"),
        Index("idx_options_expiry", "expiry"),
        Index("idx_options_strike", "strike"),
    )

class FutureRaw(Base):
    __tablename__ = "futures_raw"
    id = Column(Integer, primary_key=True, autoincrement=True)
    product = Column(String, nullable=False)      # TXF / MXF ...
    trade_date = Column(Date, nullable=False)
    expiry = Column(String, nullable=False)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    volume = Column(Integer)
    oi = Column(Integer)
    settlement = Column(Integer)
    session = Column(String)
    load_file = Column(String)
    created_at = Column(DateTime)

    __table_args__ = (
        UniqueConstraint("trade_date", "product", "expiry", "session", name="uq_futures_unique_row"),
        Index("idx_futures_trade_date", "trade_date"),
        Index("idx_futures_product", "product"),
        Index("idx_futures_expiry", "expiry"),
    )

class StockRaw(Base):
    __tablename__ = "stocks_raw"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    trade_date = Column(Date, nullable=False)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    volume = Column(Integer)
    value = Column(Integer)
    load_file = Column(String)
    created_at = Column(DateTime)

    __table_args__ = (
        UniqueConstraint("trade_date", "symbol", name="uq_stocks_unique_row"),
        Index("idx_stocks_trade_date", "trade_date"),
        Index("idx_stocks_symbol", "symbol"),
    )

# create engine + session
engine = create_engine(f"sqlite:///{DB_FILENAME}", connect_args={"check_same_thread": False})
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)

# ---- helper functions for CSV reading & cleaning (borrowed/adapted) ----

def try_read_csv(path):
    """Try multiple encodings to robustly read Taiwanese CSV."""
    encs = ["cp950", "big5", "utf-8-sig", "utf-8"]
    last_ex = None
    for e in encs:
        try:
            df = pd.read_csv(path, encoding=e)
            print(f"成功讀取：{path}（encoding={e}）")
            return df, e
        except Exception as ex:
            last_ex = ex
            continue
    try:
        df = pd.read_csv(path, encoding="cp950", errors="ignore", engine="python")
        print(f"Fallback 讀取（encoding=cp950 errors=ignore）: {path}")
        return df, "cp950-fallback"
    except Exception as ex:
        raise last_ex or ex

def normalize_columns(df):
    df.columns = [str(c).strip() for c in df.columns]
    return df

def find_logical_cols(df):
    """Return mapping for date, strike, cp (買賣權), oi (未沖銷契約數), session & volume."""
    cols = df.columns.tolist()
    def find_any(keylist):
        for k in keylist:
            for c in cols:
                if k in c.replace(" ", ""):
                    return c
        return None
    mapping = {}
    mapping['date'] = find_any(["交易日期", "Date", "tradeDate"])
    mapping['strike'] = find_any(["履約價", "履約", "strike"])
    mapping['cp'] = find_any(["買賣權", "CP", "call", "put", "買權", "賣權"])
    mapping['oi'] = find_any(["未沖銷契約數", "未沖銷", "OI", "OpenInterest"])
    mapping['session'] = find_any(["交易時段", "時段", "Session"])
    mapping['volume'] = find_any(["成交量", "Volume", "VOL"])
    mapping['contract'] = find_any(["契約", "Contract", "product"])
    mapping['expiry'] = find_any(["到期", "到期月份", "到期月份(週別)", "expiry"])
    # for futures/stocks detection
    mapping['symbol'] = find_any(["證券代號", "symbol", "Symbol", "代號"])
    return mapping

def parse_date_val(x):
    if pd.isna(x): return pd.NaT
    s = str(x).strip()
    for fmt in ("%Y/%m/%d","%Y-%m-%d","%Y/%m/%d %H:%M:%S","%Y-%m-%d %H:%M:%S"):
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            pass
    if len(s)==8 and s.isdigit():
        try:
            return pd.to_datetime(s, format="%Y%m%d")
        except Exception:
            pass
    try:
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return pd.NaT

def clean_and_prepare(df, mapping):
    df = df.copy()
    # normalize date
    if mapping.get('date') is None:
        df["交易日期"] = pd.NaT
    else:
        df["交易日期"] = df[mapping['date']].apply(parse_date_val)
    # preserve raw columns
    df_cols = df.columns.tolist()
    # set raw_OI_text if possible
    if mapping.get('oi') is not None:
        df["orig_OI_str"] = df[mapping['oi']].astype(str).fillna("")
        oi_clean = df[mapping['oi']].astype(str).str.replace(",", "").str.strip()
        oi_clean = oi_clean.replace({"-": "", "": ""})
        df["OI_val"] = pd.to_numeric(oi_clean, errors="coerce")
    else:
        df["orig_OI_str"] = ""
        df["OI_val"] = np.nan

    # volume
    if mapping.get('volume') is not None:
        df["volume_val"] = pd.to_numeric(df[mapping['volume']].astype(str).str.replace(",", ""), errors="coerce")
    else:
        df["volume_val"] = np.nan

    # strike
    if mapping.get('strike') is not None:
        df["履約價"] = pd.to_numeric(df[mapping['strike']].astype(str).str.replace(",", ""), errors="coerce")
    else:
        df["履約價"] = np.nan

    # cp
    if mapping.get('cp') is not None:
        df["買賣權"] = df[mapping['cp']].astype(str).str.replace(" ", "")
        df["買賣權"] = df["買賣權"].replace({"C":"買權","P":"賣權","Call":"買權","Put":"賣權","CALL":"買權","PUT":"賣權"})
    else:
        df["買賣權"] = ""

    # session
    if mapping.get('session') is not None:
        df["交易時段_clean"] = df[mapping['session']].astype(str).str.strip()
    else:
        df["交易時段_clean"] = ""

    # contract / product
    if mapping.get('contract') is not None:
        df["契約"] = df[mapping['contract']].astype(str).str.strip()
    else:
        df["契約"] = ""

    # expiry
    if mapping.get('expiry') is not None:
        df["expiry"] = df[mapping['expiry']].astype(str).str.strip()
    else:
        df["expiry"] = ""

    # symbol for stock
    if mapping.get('symbol') is not None:
        df["symbol"] = df[mapping['symbol']].astype(str).str.strip()
    else:
        df["symbol"] = ""

    # generic: fill trade_date NA rows removed
    df["交易日期"] = pd.to_datetime(df["交易日期"], errors="coerce")
    df = df.dropna(subset=["交易日期"])

    # orig_has_number detection used earlier logic
    def orig_has_number(s):
        if not isinstance(s, str): return False
        s2 = s.strip()
        if s2 == "" or s2 == "-" or s2.lower() == "nan":
            return False
        return any(ch.isdigit() for ch in s2)
    df["orig_has_number"] = df["orig_OI_str"].apply(orig_has_number)
    return df

# ---- DB import logic ----

def import_csv_file_to_db(path, session, commit=True):
    """解析單一 CSV，偵測為 options / futures / stocks，然後寫入 DB（避免重複）"""
    try:
        df_raw, enc = try_read_csv(path)
    except Exception as e:
        print("讀 CSV 失敗：", e)
        return 0
    df_raw = normalize_columns(df_raw)
    mapping = find_logical_cols(df_raw)
    dfc = clean_and_prepare(df_raw, mapping)
    # decide type: if has 買賣權 column -> options. else if has symbol -> stocks. else treat as futures (fallback)
    is_option = mapping.get('cp') is not None or any("買權" in str(c) or "賣權" in str(c) for c in dfc.columns)
    is_stock = mapping.get('symbol') is not None or ("symbol" in "".join(dfc.columns).lower())
    inserted = 0
    now = datetime.now()
    # options
    if is_option:
        # ensure required columns exist
        for _, r in dfc.iterrows():
            try:
                obj_exists = session.query(OptionRaw).filter_by(
                    trade_date = r["交易日期"].date(),
                    product = r.get("契約",""),
                    expiry = r.get("expiry",""),
                    strike = int(r["履約價"]) if not pd.isna(r["履約價"]) else None,
                    cp = r.get("買賣權",""),
                    session = r.get("交易時段_clean","")
                ).first()
            except Exception:
                obj_exists = None
            if obj_exists:
                continue
            try:
                oi_val = int(r["OI_val"]) if not pd.isna(r["OI_val"]) else None
            except Exception:
                oi_val = None
            try:
                vol_val = int(r["volume_val"]) if not pd.isna(r["volume_val"]) else None
            except Exception:
                vol_val = None
            row = OptionRaw(
                product = r.get("契約","") or r.get("product","") or "",
                trade_date = r["交易日期"].date(),
                expiry = r.get("expiry",""),
                strike = int(r["履約價"]) if not pd.isna(r["履約價"]) else None,
                cp = r.get("買賣權",""),
                volume = vol_val,
                oi = oi_val,
                raw_oi_text = r.get("orig_OI_str",""),
                session = r.get("交易時段_clean",""),
                load_file = os.path.basename(path),
                created_at = now
            )
            session.add(row)
            inserted += 1
    elif is_stock:
        for _, r in dfc.iterrows():
            sym = r.get("symbol","")
            trade_date = r["交易日期"].date()
            exists = session.query(StockRaw).filter_by(trade_date=trade_date, symbol=sym).first()
            if exists:
                continue
            row = StockRaw(
                symbol = sym,
                trade_date = trade_date,
                open = None,
                high = None,
                low = None,
                close = None,
                volume = int(r["volume_val"]) if not pd.isna(r["volume_val"]) else None,
                value = None,
                load_file = os.path.basename(path),
                created_at = now
            )
            session.add(row)
            inserted += 1
    else:
        # treat as future: attempt to parse close/oi/volume if present
        for _, r in dfc.iterrows():
            trade_date = r["交易日期"].date()
            exists = session.query(FutureRaw).filter_by(trade_date=trade_date, product=r.get("契約",""), expiry=r.get("expiry",""), session=r.get("交易時段_clean","")).first()
            if exists:
                continue
            row = FutureRaw(
                product = r.get("契約",""),
                trade_date = trade_date,
                expiry = r.get("expiry",""),
                open = None,
                high = None,
                low = None,
                close = None,
                volume = int(r["volume_val"]) if not pd.isna(r["volume_val"]) else None,
                oi = int(r["OI_val"]) if not pd.isna(r["OI_val"]) else None,
                settlement = None,
                session = r.get("交易時段_clean",""),
                load_file = os.path.basename(path),
                created_at = now
            )
            session.add(row)
            inserted += 1
    if commit and inserted>0:
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
    return inserted

def auto_import_from_data_dir():
    """自動掃描 ./Data 資料夾內的 csv 檔，匯入 DB（若已匯入則跳過）。"""
    s = Session()
    files = [f for f in os.listdir(DATA_DIR) if f.lower().endswith(".csv")]
    total = 0
    for fn in files:
        path = os.path.join(DATA_DIR, fn)
        try:
            n = import_csv_file_to_db(path, s, commit=False)
            if n>0:
                print(f"匯入 {fn} -> {n} 筆")
            total += n
        except Exception as e:
            print("匯入失敗:", fn, e)
    try:
        s.commit()
    except IntegrityError:
        s.rollback()
    s.close()
    return total

# ---- analysis and plotting functions (reusing earlier logic but loading from DB) ----

def load_agg_from_db(date_from=None, date_to=None):
    """Load aggregated OI per date,strike,cp from DB (options only)."""
    s = Session()
    q = s.query(OptionRaw)
    if date_from:
        q = q.filter(OptionRaw.trade_date >= date_from)
    if date_to:
        q = q.filter(OptionRaw.trade_date <= date_to)
    rows = q.all()
    if not rows:
        s.close()
        return pd.DataFrame()
    recs = []
    for r in rows:
        recs.append({
            "交易日期": pd.to_datetime(r.trade_date),
            "履約價": r.strike,
            "買賣權": r.cp if r.cp else "",
            "未沖銷契約數": r.oi if r.oi is not None else np.nan,
            "orig_OI_str": r.raw_oi_text if r.raw_oi_text else "",
            "交易時段": r.session if r.session else ""
        })
    s.close()
    df = pd.DataFrame(recs)
    # reuse aggregate_daily logic: need to create OI_val and transaction session normalization
    df["未沖銷契約數"] = df["未沖銷契約數"]
    # Convert NaN to suitable handling handled in aggregate_daily below
    return df

def aggregate_daily_from_df(df):
    """Similar to earlier aggregate_daily but accepting cleaned DataFrame with necessary columns."""
    if df is None or df.empty:
        return pd.DataFrame()
    dfc = df.copy()
    # we expect dfc has 交易日期, 履約價, 買賣權, 未沖銷契約數, orig_OI_str, 交易時段
    dfc["交易時段_clean"] = dfc["交易時段"].astype(str).fillna("")
    dfc["orig_OI_str"] = dfc["orig_OI_str"].astype(str).fillna("")
    # OI_val from 未沖銷契約數
    dfc["OI_val"] = pd.to_numeric(dfc["未沖銷契約數"], errors="coerce")
    gen = dfc[dfc["交易時段_clean"] == "一般"].copy()
    if gen.empty:
        gen = dfc[~dfc["交易時段_clean"].isin(["-","盤後","後市",""])].copy()
    if gen.empty:
        gen = dfc.copy()
    gen["OI_for_calc"] = gen["OI_val"].fillna(0).astype(int)
    agg = (gen.groupby(["交易日期","履約價","買賣權"], as_index=False)
           .agg({"OI_for_calc":"sum", "orig_OI_str":"first"}))
    agg = agg.rename(columns={"OI_for_calc":"OI","orig_OI_str":"orig_OI_text"})
    agg = agg.sort_values(["交易日期","履約價","買賣權"]).reset_index(drop=True)
    agg["OI_prev_1"] = agg.groupby(["履約價","買賣權"])["OI"].shift(1).fillna(0).astype(int)
    agg["OI_prev_2"] = agg.groupby(["履約價","買賣權"])["OI"].shift(2).fillna(0).astype(int)
    agg["delta_1"] = agg["OI"] - agg["OI_prev_1"]
    agg["delta_2"] = agg["OI"] - agg["OI_prev_2"]
    return agg

def detect_anomalies_for_date(agg_day, all_agg, atm_strike=None, strike_window=DEFAULT_STRIKE_WINDOW):
    results = {"top_inc":[], "top_dec":[], "top2_inc":[], "top2_dec":[], "max_call":None, "max_put":None, "anomalies":[]}
    if agg_day is None or agg_day.empty:
        return results
    inc = agg_day[agg_day["delta_1"]>0].sort_values("delta_1", ascending=False).head(10)
    dec = agg_day[agg_day["delta_1"]<0].sort_values("delta_1", ascending=True).head(10)
    inc2 = agg_day[agg_day["delta_2"]>0].sort_values("delta_2", ascending=False).head(10)
    dec2 = agg_day[agg_day["delta_2"]<0].sort_values("delta_2", ascending=True).head(10)
    results["top_inc"] = inc.head(3).to_dict("records")
    results["top_dec"] = dec.head(3).to_dict("records")
    results["top2_inc"] = inc2.head(3).to_dict("records")
    results["top2_dec"] = dec2.head(3).to_dict("records")
    date = agg_day["交易日期"].iloc[0]
    flag = (all_agg[all_agg["交易日期"]==date]
            .groupby("買賣權")["OI"].count().reset_index())
    df_calls = agg_day[agg_day["買賣權"].str.contains("買", na=False)]
    df_puts = agg_day[agg_day["買賣權"].str.contains("賣", na=False)]
    if not df_calls.empty:
        row = df_calls.loc[df_calls["OI"].idxmax()]
        results["max_call"] = {"履約價":int(row["履約價"]), "OI":int(row["OI"])}
    if not df_puts.empty:
        row = df_puts.loc[df_puts["OI"].idxmax()]
        results["max_put"] = {"履約價":int(row["履約價"]), "OI":int(row["OI"])}
    anomalies = []
    for _, r in agg_day.iterrows():
        strike = r["履約價"]
        cp = r["買賣權"]
        delta = int(r["delta_1"])
        absd = abs(delta)
        if absd >= ANOM_THRESH["extreme"]:
            level = "極端"
        elif absd >= ANOM_THRESH["major"][0]:
            level = "重大"
        elif absd >= ANOM_THRESH["big_order"][0]:
            level = "大單"
        else:
            level = None
        hist = all_agg[(all_agg["履約價"]==strike) & (all_agg["買賣權"]==cp)]
        hist_recent = hist[hist["交易日期"] < r["交易日期"]].sort_values("交易日期", ascending=False).head(5)
        mean_recent = hist_recent["delta_1"].abs().mean() if not hist_recent.empty else 0.0
        rel_flag = False
        if mean_recent>0 and absd > mean_recent * 3:
            rel_flag = True
        outside_flag = False
        if atm_strike is not None:
            if abs(strike - atm_strike) > strike_window and absd >= 400:
                outside_flag = True
        if level or rel_flag or outside_flag:
            anomalies.append({
                "履約價": int(strike),
                "買賣權": cp,
                "delta": int(delta),
                "abs": int(absd),
                "level": level if level else ("相對異常" if rel_flag else "遠端大單" if outside_flag else "異常"),
                "outside": outside_flag
            })
    anomalies = sorted(anomalies, key=lambda x: x["abs"], reverse=True)
    results["anomalies"] = anomalies[:10]
    return results

# plotting helpers (reused/adapted)
def make_oi_delta_bar(ax, df_day_plot, annotate_top=3):
    ax.clear()
    ax.set_facecolor("#000000")
    if df_day_plot.empty:
        ax.text(0.5,0.5,"No data", ha="center", color="#FFFFFF", transform=ax.transAxes)
        return
    strikes = sorted(df_day_plot["履約價"].unique())
    x = np.arange(len(strikes))
    width = 0.35
    calls = df_day_plot[df_day_plot["買賣權"].str.contains("買", na=False)].set_index("履約價")["OI"].reindex(strikes).fillna(0).values
    puts  = df_day_plot[df_day_plot["買賣權"].str.contains("賣", na=False)].set_index("履約價")["OI"].reindex(strikes).fillna(0).values
    delta_calls = df_day_plot[df_day_plot["買賣權"].str.contains("買", na=False)].set_index("履約價")["delta_1"].reindex(strikes).fillna(0).values
    delta_puts  = df_day_plot[df_day_plot["買賣權"].str.contains("賣", na=False)].set_index("履約價")["delta_1"].reindex(strikes).fillna(0).values
    ax.bar(x-width/2, puts, width, label="賣權 OI")
    ax.bar(x+width/2, calls, width, label="買權 OI")
    ax.bar(x-width/2, delta_puts, width/4)
    ax.bar(x+width/2, delta_calls, width/4)
    ax.set_xticks(x)
    step = max(1, int(len(strikes)/15))
    labels = [str(int(s)) if (i%step==0 or i==0 or i==len(strikes)-1) else "" for i,s in enumerate(strikes)]
    ax.set_xticklabels(labels, rotation=45, fontsize=FONT_SIZE-4)
    ax.set_xlabel("履約價", color="#FFFFFF")
    ax.set_ylabel("OI / ΔOI", color="#FFFFFF")
    ax.legend(facecolor="#111111")
    combined = df_day_plot.copy()
    if "delta_1" in combined.columns:
        combined["abs_delta"] = combined["delta_1"].abs()
        top = combined.sort_values("abs_delta", ascending=False).head(annotate_top)
        for _, r in top.iterrows():
            strike = int(r["履約價"])
            cp = r["買賣權"]
            delta = int(r["delta_1"])
            i = strikes.index(strike)
            xpos = i + (width/2 if "買" in cp else -width/2)
            ax.text(xpos, max(0, r["OI"])+max(50, abs(delta))*0.02, f"{'+' if delta>0 else ''}{delta}", 
                    ha="center", va="bottom", color="#FFFF00", fontsize=FONT_SIZE-2,
                    bbox=dict(facecolor="#111111", alpha=0.7, edgecolor=None, pad=2))

def make_mirror(ax, df_day_plot):
    ax.clear()
    ax.set_facecolor("#000000")
    if df_day_plot.empty:
        ax.text(0.5,0.5,"No data", ha="center", color="#FFFFFF", transform=ax.transAxes)
        return
    strikes = sorted(df_day_plot["履約價"].unique())
    x = np.arange(len(strikes))
    puts = df_day_plot[df_day_plot["買賣權"].str.contains("賣", na=False)].set_index("履約價")["OI"].reindex(strikes).fillna(0).values
    calls = df_day_plot[df_day_plot["買賣權"].str.contains("買", na=False)].set_index("履約價")["OI"].reindex(strikes).fillna(0).values
    ax.barh(x, -puts, height=0.4, label="賣權 OI")
    ax.barh(x, calls, height=0.4, label="買權 OI")
    step = max(1, int(len(strikes)/15))
    ylabels = [str(int(s)) if (i%step==0 or i==0 or i==len(strikes)-1) else "" for i,s in enumerate(strikes)]
    ax.set_yticks(x)
    ax.set_yticklabels(ylabels, fontsize=FONT_SIZE-4)
    ax.set_xlabel("OI", color="#FFFFFF")
    ax.legend(facecolor="#111111")

def make_heatmap(ax, df_all_dates, date_list, strikes_sorted, date_to_plot=None, cmap="magma"):
    ax.clear()
    ax.set_facecolor("#000000")
    if df_all_dates is None or df_all_dates.empty:
        ax.text(0.5,0.5,"No heatmap data", ha="center", color="#FFFFFF", transform=ax.transAxes)
        return
    pivot = df_all_dates.groupby(["交易日期","履約價"])["OI"].sum().unstack(fill_value=0)
    pivot = pivot.reindex(columns=strikes_sorted, fill_value=0)
    pivot = pivot.loc[pd.DatetimeIndex(date_list).intersection(pivot.index)]
    if pivot.empty:
        ax.text(0.5,0.5,"No heatmap data", ha="center", color="#FFFFFF", transform=ax.transAxes)
        return
    im = ax.imshow(pivot.values, aspect='auto', origin='lower', cmap=cmap)
    ncols = pivot.shape[1]
    stepx = max(1, int(ncols/20))
    xticks = list(range(0,ncols,stepx))
    xticklabels = [str(int(pivot.columns[i])) for i in xticks]
    ax.set_xticks(xticks)
    ax.set_xticklabels(xticklabels, rotation=90, fontsize=FONT_SIZE-6)
    ny = pivot.shape[0]
    stepy = max(1, int(ny/12))
    yticks = list(range(0,ny,stepy))
    ylabels = [pd.to_datetime(pivot.index[i]).strftime("%Y-%m-%d") for i in yticks]
    ax.set_yticks(yticks)
    ax.set_yticklabels(ylabels, fontsize=FONT_SIZE-6)
    ax.set_xlabel("履約價", color="#FFFFFF", fontsize=FONT_SIZE)
    ax.set_ylabel("日期", color="#FFFFFF", fontsize=FONT_SIZE)
    plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)

# ---- GUI / main app ----

class TXOApp:
    def __init__(self, root):
        self.root = root
        root.title("TXO OI Analyzer + DB")
        root.configure(bg="#000000")
        # top frame: Data folder info, file operations
        top = tk.Frame(root, bg="#000000")
        top.pack(side="top", fill="x", padx=6, pady=6)
        tk.Label(top, text=f"Data 資料夾：{DATA_DIR}", fg="#FFFFFF", bg="#000000", font=(FONT_FAMILY, FONT_SIZE-2)).pack(side="left")
        tk.Button(top, text="自動匯入 Data", command=self.on_auto_import, bg="#111111", fg="#FFFFFF", font=(FONT_FAMILY, FONT_SIZE-2)).pack(side="right", padx=6)
        tk.Button(top, text="匯入單一檔案", command=self.on_import_single_file, bg="#111111", fg="#FFFFFF", font=(FONT_FAMILY, FONT_SIZE-2)).pack(side="right", padx=6)

        # options
        opts = tk.Frame(root, bg="#000000")
        opts.pack(side="top", fill="x", padx=6, pady=6)
        tk.Label(opts, text="日期範圍 (YYYY-MM-DD，可留空)：", fg="#FFFFFF", bg="#000000", font=(FONT_FAMILY, FONT_SIZE)).pack(side="left")
        self.date_from_var = tk.StringVar()
        self.date_to_var = tk.StringVar()
        self.date_from_entry = tk.Entry(opts, textvariable=self.date_from_var, width=12, fg="#FFFFFF", bg="#111111", insertbackground="#FFFFFF", font=(FONT_FAMILY, FONT_SIZE))
        self.date_from_entry.pack(side="left", padx=6)
        self.date_to_entry = tk.Entry(opts, textvariable=self.date_to_var, width=12, fg="#FFFFFF", bg="#111111", insertbackground="#FFFFFF", font=(FONT_FAMILY, FONT_SIZE))
        self.date_to_entry.pack(side="left", padx=6)

        tk.Label(opts, text="視窗 ±X 履約價：", fg="#FFFFFF", bg="#000000", font=(FONT_FAMILY, FONT_SIZE)).pack(side="left", padx=(10,0))
        self.win_var = tk.IntVar(value=DEFAULT_STRIKE_WINDOW)
        tk.Spinbox(opts, from_=1, to=100, textvariable=self.win_var, width=4, bg="#111111", fg="#FFFFFF", font=(FONT_FAMILY, FONT_SIZE)).pack(side="left", padx=6)

        # plot choices
        chkf = tk.Frame(root, bg="#000000")
        chkf.pack(side="top", fill="x", padx=6, pady=6)
        self.show_bar = tk.IntVar(value=1)
        self.show_mirror = tk.IntVar(value=1)
        self.show_heat = tk.IntVar(value=1)
        tk.Checkbutton(chkf, text="OI+Δ 並列柱狀", variable=self.show_bar, bg="#000000", fg="#FFFFFF", selectcolor="#111111", font=(FONT_FAMILY, FONT_SIZE)).pack(side="left", padx=6)
        tk.Checkbutton(chkf, text="買權/賣權 鏡像圖", variable=self.show_mirror, bg="#000000", fg="#FFFFFF", selectcolor="#111111", font=(FONT_FAMILY, FONT_SIZE)).pack(side="left", padx=6)
        tk.Checkbutton(chkf, text="Heatmap", variable=self.show_heat, bg="#000000", fg="#FFFFFF", selectcolor="#111111", font=(FONT_FAMILY, FONT_SIZE)).pack(side="left", padx=6)

        tk.Button(root, text="讀取 / 更新資料", command=self.load_and_prepare, bg="#111111", fg="#FFFFFF", font=(FONT_FAMILY, FONT_SIZE)).pack(side="top", pady=(4,4))
        tk.Button(root, text="畫圖", command=self.draw_plots, bg="#222222", fg="#FFFFFF", font=(FONT_FAMILY, FONT_SIZE)).pack(side="top", pady=(0,8))

        # main content: left canvas, right summary
        content = tk.Frame(root, bg="#000000")
        content.pack(side="top", fill="both", expand=True, padx=6, pady=6)

        self.fig = plt.Figure(figsize=(12,8), dpi=100)
        self.canvas = FigureCanvasTkAgg(self.fig, master=content)
        self.canvas.get_tk_widget().pack(side="left", fill="both", expand=True)

        right = tk.Frame(content, bg="#000000")
        right.pack(side="right", fill="y", padx=6)
        tk.Label(right, text="分析結果（永久標示）", fg="#FFFFFF", bg="#000000", font=(FONT_FAMILY, FONT_SIZE)).pack(anchor="nw", pady=(0,6))
        self.text = tk.Text(right, width=48, height=30, bg="#111111", fg="#FFFFFF", font=(FONT_FAMILY, FONT_SIZE), wrap="word")
        self.text.pack(fill="both", expand=True)
        self.text.configure(state="disabled")

        # internal data
        self.df_raw = None
        self.agg = None

    # file import handlers
    def on_auto_import(self):
        cnt = auto_import_from_data_dir()
        messagebox.showinfo("自動匯入完成", f"共匯入 {cnt} 筆新資料（若有）\n來源：{DATA_DIR}")

    def on_import_single_file(self):
        p = filedialog.askopenfilename(title="選擇 CSV 檔案（可在任意位置）", filetypes=[("CSV 檔","*.csv"),("All files","*.*")])
        if not p:
            return
        # copy selected file into Data folder to follow rule that program reads from Data folder
        try:
            import shutil
            dst = os.path.join(DATA_DIR, os.path.basename(p))
            shutil.copy2(p, dst)
            s = Session()
            n = import_csv_file_to_db(dst, s, commit=True)
            s.close()
            messagebox.showinfo("匯入完成", f"檔案已複製至 Data，並匯入 DB，共新增 {n} 筆")
        except Exception as e:
            messagebox.showerror("匯入失敗", str(e))

    # load and prepare data (from DB)
    def load_and_prepare(self):
        # parse date range
        dfrom = self.date_from_var.get().strip()
        dto = self.date_to_var.get().strip()
        try:
            if dfrom == "":
                date_from = None
            else:
                date_from = pd.to_datetime(dfrom, errors="coerce")
                if pd.isna(date_from):
                    raise ValueError("日期格式錯誤（from）")
                date_from = date_from.date()
            if dto == "":
                date_to = None
            else:
                date_to = pd.to_datetime(dto, errors="coerce")
                if pd.isna(date_to):
                    raise ValueError("日期格式錯誤（to）")
                date_to = date_to.date()
            # if one is None and other isn't, it's fine. If both None, default to last 30 days in DB
            if date_from is None and date_to is None:
                # find max date in DB
                s = Session()
                mx = s.query(OptionRaw).order_by(OptionRaw.trade_date.desc()).first()
                s.close()
                if mx:
                    maxd = mx.trade_date
                    date_to = maxd
                    date_from = maxd - timedelta(days=29)
                else:
                    messagebox.showwarning("警告", "資料庫沒有任何選擇權資料，請先匯入 CSV")
                    return
            # ensure date_from <= date_to
            if date_from is not None and date_to is not None and date_from > date_to:
                raise ValueError("日期範圍錯誤：起始日大於結束日")
        except Exception as e:
            messagebox.showerror("日期錯誤", str(e))
            return

        # load aggregated data from DB
        df_from_db = load_agg_from_db(date_from=date_from, date_to=date_to)
        if df_from_db is None or df_from_db.empty:
            messagebox.showwarning("無資料", "在指定日期範圍內未找到選擇權資料（options_raw）")
            return
        self.df_raw = df_from_db
        self.agg = aggregate_daily_from_df(self.df_raw)
        messagebox.showinfo("完成", f"已載入資料：{len(self.df_raw)} 筆（聚合後 {len(self.agg)} 筆）")

    def draw_plots(self):
        if self.agg is None or self.agg.empty:
            messagebox.showwarning("警告", "尚未載入資料，請先按「讀取 / 更新資料」")
            return
        # parse selected date: choose last date in range or a single date input? We'll prompt user for specific date via simple dialog using last date as default
        dates = sorted(self.agg["交易日期"].unique())
        if not dates:
            messagebox.showwarning("無資料日期", "沒有可畫的日期")
            return
        # default to last date
        selected_date = dates[-1]
        # determine window (strike window)
        win = int(self.win_var.get())
        # compute atm
        agg = self.agg.copy()
        agg["交易日期"] = pd.to_datetime(agg["交易日期"]).dt.normalize()
        date = pd.to_datetime(selected_date).date()
        agg_day = agg[agg["交易日期"]==np.datetime64(date)]
        if agg_day.empty:
            messagebox.showwarning("無該日資料", f"資料中沒有 {date} 的記錄")
            return
        atm_candidate = agg_day.groupby("履約價")["OI"].sum().idxmax() if not agg_day.empty else None
        atm = int(atm_candidate) if atm_candidate is not None else None
        # determine visualize strikes
        all_strikes = sorted(self.agg["履約價"].unique())
        if atm is not None and len(all_strikes)>0:
            try:
                idx = all_strikes.index(atm)
            except ValueError:
                idx = int(np.argmin([abs(x-atm) for x in all_strikes]))
            left = max(0, idx-win)
            right = min(len(all_strikes)-1, idx+win)
            vis_strikes = all_strikes[left:right+1]
        else:
            vis_strikes = sorted([s for s in all_strikes if s in agg_day["履約價"].unique()])[:win*2+1]
        df_day_plot = agg_day[agg_day["履約價"].isin(vis_strikes)].copy()

        anomalies_results = detect_anomalies_for_date(agg_day, self.agg, atm_strike=atm, strike_window=win)

        # write summary to right panel
        self.text.configure(state="normal")
        self.text.delete("1.0", tk.END)
        self.text.insert(tk.END, f"日期: {date.strftime('%Y-%m-%d')}\n\n")
        if atm is not None:
            self.text.insert(tk.END, f"ATM (推定): 履約價 {atm}\n視窗 ±{win}\n\n")
        def insert_top_list(title, arr):
            self.text.insert(tk.END, title + "\n")
            if not arr:
                self.text.insert(tk.END, "  （無）\n")
            else:
                for i,r in enumerate(arr,1):
                    self.text.insert(tk.END, f"  {i}. 履約價 {int(r['履約價'])} {r['買賣權']}  今 {int(r['OI'])} 前1 {int(r['OI_prev_1'])}  變化 {int(r['delta_1'])}\n")
            self.text.insert(tk.END, "\n")
        insert_top_list("單日增加 Top3", anomalies_results["top_inc"])
        insert_top_list("單日減少 Top3", anomalies_results["top_dec"])
        insert_top_list("兩日累積增加 Top3", anomalies_results["top2_inc"])
        insert_top_list("兩日累積減少 Top3", anomalies_results["top2_dec"])
        mc = anomalies_results.get("max_call")
        mp = anomalies_results.get("max_put")
        self.text.insert(tk.END, "每日最大 OI（買/賣）：\n")
        if mc:
            self.text.insert(tk.END, f"  最大買權：履約價 {mc['履約價']}  OI {mc['OI']}\n")
        else:
            self.text.insert(tk.END, f"  最大買權：無\n")
        if mp:
            self.text.insert(tk.END, f"  最大賣權：履約價 {mp['履約價']}  OI {mp['OI']}\n")
        else:
            self.text.insert(tk.END, f"  最大賣權：無\n")
        self.text.insert(tk.END, "\n異常提示：\n")
        if anomalies_results["anomalies"]:
            for a in anomalies_results["anomalies"][:10]:
                self.text.insert(tk.END, f"  履約價 {a['履約價']} {a['買賣權']}  變化 {a['delta']}  等級: {a['level']}\n")
        else:
            self.text.insert(tk.END, "  （無偵測到重大異常）\n")
        self.text.configure(state="disabled")

        # --------- plot layout logic ----------
        # determine which charts are selected
        charts = []
        if self.show_bar.get(): charts.append("bar")
        if self.show_mirror.get(): charts.append("mirror")
        if self.show_heat.get(): charts.append("heat")
        n = len(charts)
        # clear previous figure
        self.fig.clf()
        ax_map = {}
        if n == 1:
            ax = self.fig.add_subplot(1,1,1)
            ax_map[charts[0]] = ax
        elif n == 2:
            # LEFT-RIGHT layout (1 row, 2 cols)
            for i,name in enumerate(charts, start=1):
                ax = self.fig.add_subplot(1,2,i)
                ax_map[name] = ax
        elif n >= 3:
            # 2x2 grid, use first three positions (1,1),(1,2),(2,1)
            pos = [(1,1),(1,2),(2,1)]
            for i,name in enumerate(charts[:3]):
                row,col = pos[i]
                ax = self.fig.add_subplot(2,2,i+1)  # subplot index works as i+1
                ax_map[name] = ax
        else:
            # no chart selected: show message
            ax = self.fig.add_subplot(1,1,1)
            ax.text(0.5,0.5,"No chart selected", ha="center", color="#FFFFFF", transform=ax.transAxes)
            self.canvas.draw()
            return

        # draw charts
        if "bar" in ax_map:
            make_oi_delta_bar(ax_map["bar"], df_day_plot, annotate_top=3)
        if "mirror" in ax_map:
            make_mirror(ax_map["mirror"], df_day_plot)
        if "heat" in ax_map:
            all_dates = sorted(self.agg["交易日期"].unique())
            idx_date = all_dates.index(np.datetime64(date))
            start_idx = max(0, idx_date - 19)
            date_list = pd.DatetimeIndex(all_dates[start_idx: idx_date+1])
            strikes_sorted = sorted(self.agg["履約價"].unique())
            make_heatmap(ax_map["heat"], self.agg, date_list, strikes_sorted, date_to_plot=date)

        self.fig.tight_layout()
        self.canvas.draw()

def main():
    root = tk.Tk()
    app = TXOApp(root)
    root.geometry("1400x900")
    root.mainloop()

if __name__ == "__main__":
    main()
