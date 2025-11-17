# data_loader.py
# Robust TXO data loader with diagnostic output and multiple fallbacks.
# 覆蓋此檔案並執行。若仍報「讀入後資料為空」，請把 console 全部貼上來。

import pandas as pd
import chardet
import csv
from typing import Tuple, List
import io
import sys
import os

# ---------------- utilities ----------------
def detect_encoding_guess(path: str) -> List[str]:
    with open(path, "rb") as f:
        raw = f.read()
    res = chardet.detect(raw)
    enc = res.get("encoding") or "cp950"
    # 常見嘗試清單（優先順序）
    encs = [enc, "cp950", "big5", "utf-8-sig", "utf-8", "latin1"]
    # remove duplicates preserving order
    seen = set()
    out = []
    for e in encs:
        if e and e.lower() not in seen:
            out.append(e)
            seen.add(e.lower())
    return out

def sniff_delimiters(sample: str) -> List[str]:
    # candidates ordered
    cand = [",", "\t", ";", "|"]
    found = []
    for d in cand:
        if d in sample:
            found.append(d)
    # always include comma as last fallback
    if "," not in found:
        found.append(",")
    return found

def read_sample_lines(path: str, encoding: str, n=200) -> List[str]:
    with open(path, "r", encoding=encoding, errors="replace") as f:
        lines = []
        for i in range(n):
            ln = f.readline()
            if not ln:
                break
            lines.append(ln.rstrip("\n\r"))
    return lines

def show_tokenization(lines: List[str], delimiter: str, limit=10):
    print(f"\n--- tokenization preview (delimiter='{delimiter}') ---")
    for i, ln in enumerate(lines[:limit]):
        toks = [t for t in ln.split(delimiter)]
        print(f"{i:03d} | {len(toks):02d} tokens | {toks}")
    print("--- end preview ---\n")

# ---------------- sniff header ----------------
def find_header_row(lines: List[str], delimiter: str) -> int:
    # try find a line among first 10 that contains required keywords
    keywords = ["交易日期", "履約", "買賣權"]
    for i, ln in enumerate(lines[:10]):
        toks = [t.strip() for t in ln.split(delimiter)]
        if all(any(k in t for t in toks) for k in keywords):
            return i
    return 0

# ---------------- core read with attempts ----------------
def attempt_read(path: str):
    """
    Try multiple (encoding, delimiter, header_row) combos.
    Return DataFrame when a plausible one found, else None and diagnostics.
    """
    enc_guesses = detect_encoding_guess(path)
    diagnostics = []
    for enc in enc_guesses:
        try:
            sample = read_sample_lines(path, enc, n=200)
        except Exception as e:
            diagnostics.append((enc, None, None, f"read_sample_error: {e}"))
            continue

        sample_text = "\n".join(sample)
        delims = sniff_delimiters(sample_text)
        for delim in delims:
            header_row = find_header_row(sample, delim)
            # show tokenization for debugging
            diagnostics.append((enc, delim, header_row, f"sample_lines={len(sample)}"))
            # attempt to read
            try:
                df = pd.read_csv(
                    path,
                    encoding=enc,
                    delimiter=delim,
                    header=header_row,
                    engine="python",
                    dtype=str,
                    keep_default_na=False
                )
                # drop completely-empty columns (all empty or whitespace)
                empty_cols = [c for c in df.columns if df[c].astype(str).str.strip().eq("").all()]
                if empty_cols:
                    # drop tail empty columns first
                    for c in empty_cols:
                        df.drop(columns=c, inplace=True)
                # normalize column names
                df.columns = [str(c).strip() if str(c).strip() != "" else "空欄位" for c in df.columns]
                # quick plausibility check: must contain at least one of the keywords and >0 rows
                cols_join = ",".join(df.columns)
                plausible = any(k in cols_join for k in ["交易日期", "履約", "買賣權", "未沖銷契約"])
                if not df.empty and plausible:
                    return df, {"encoding": enc, "delimiter": delim, "header_row": header_row, "sample_lines": sample[:20]}
                # else continue trying (maybe header row wrong)
            except Exception as e:
                diagnostics.append((enc, delim, header_row, f"pandas_read_error: {e}"))
                continue
    # if reached here, nothing plausible
    return None, {"diagnostics": diagnostics, "sample": sample[:50] if 'sample' in locals() else []}

# ---------------- column matching ----------------
def match_column(df, candidates):
    for cand in candidates:
        for c in df.columns:
            if cand in c.replace(" ", ""):
                return c
    return None

# ---------------- load minimal raw and print diagnostics ----------------
def diagnostic_report(path: str, info: dict):
    print("\n===== DIAGNOSTIC REPORT =====")
    if "encoding" in info:
        print(f"Successful parse using encoding={info['encoding']}, delimiter={info['delimiter']}, header_row={info['header_row']}")
        print("Sample of parsed header columns:", list(info['sample'][0:1]))
    else:
        print("No successful parse. Tried combos and failures below:")
        for item in info.get("diagnostics", []):
            enc, delim, header_row, note = item
            print(f"  enc={enc}, delim={delim}, header_row={header_row} -> {note}")
        print("\nRaw sample (first lines):")
        for i, ln in enumerate(info.get("sample", [])):
            print(f"{i:03d}: {ln!r}")
        print("\n-- tokenization hints --")
        # try tokenization hints with common delimiters on first 20 lines
        sample_lines = info.get("sample", [])
        if sample_lines:
            for d in [",", "\t", ";", "|"]:
                print(f"\nDelimiter guessing: '{d}'")
                show_tokenization(sample_lines, d, limit=10)
    print("===== END DIAGNOSTIC =====\n")

# ---------------- main load_and_process (robust) ----------------
def load_and_process(path: str):
    # 1) quick checks
    if not os.path.exists(path):
        raise FileNotFoundError(f"檔案不存在: {path}")
    # 2) attempt read
    df, info = attempt_read(path)
    if df is None:
        diagnostic_report(path, info)
        raise ValueError("讀取或解析失敗：所有嘗試皆不成功。請檢查 CSV（我已列出 sample 與嘗試組合）。")
    # print success meta
    print(f"成功讀入（encoding={info['encoding']}, delimiter={info['delimiter']}, header_row={info['header_row']})")
    print("讀到欄位：", df.columns.tolist())
    # show first 5 rows
    print("\n前 5 筆（raw）：")
    print(df.head(5).to_string(index=False))

    # 3) match columns
    col_date = match_column(df, ["交易日期", "日期"])
    col_cp = match_column(df, ["買賣權", "權別"])
    col_strike = match_column(df, ["履約價", "履約"])
    col_oi = match_column(df, ["未沖銷契約", "未平倉", "OI", "未沖銷"])

    print("\n欄位自動對應結果：")
    print("  date ->", col_date)
    print("  cp   ->", col_cp)
    print("  strike ->", col_strike)
    print("  oi   ->", col_oi)

    if None in (col_date, col_cp, col_strike, col_oi):
        print("\n警告：自動匹配缺少必要欄位，會輸出欄位診斷並中止。")
        diagnostic_report(path, {"diagnostics": info.get("diagnostics", []), "sample": info.get("sample", [])})
        raise ValueError("讀取或解析失敗: 必要欄位 (交易日期/買賣權/履約價/未沖銷契約) 未能自動匹配。")

    # 4) extract minimal and clean
    df2 = df[[col_date, col_cp, col_strike, col_oi]].copy()
    df2.columns = ["Date", "CP", "Strike", "OI_raw"]
    # keep OI original string
    df2["orig_OI_str"] = df2["OI_raw"].astype(str)

    # clean OI
    df2["OI_clean"] = df2["OI_raw"].astype(str).str.replace(",", "").str.strip()
    df2["OI_clean"] = df2["OI_clean"].replace({"-": "0", "": "0", "nan": "0", "NaN": "0"})
    df2["OI"] = pd.to_numeric(df2["OI_clean"], errors="coerce").fillna(0).astype(int)

    # date / strike conversions
    df2["Date"] = pd.to_datetime(df2["Date"], errors="coerce")
    df2["Strike"] = pd.to_numeric(df2["Strike"], errors="coerce")

    # drop rows where Date or Strike cannot parse
    before = len(df2)
    df2 = df2.dropna(subset=["Date", "Strike"]).reset_index(drop=True)
    after = len(df2)
    print(f"\n清洗：總筆數 {before} -> drop 無效 Date/Strike -> {after}")

    if df2.empty:
        # print diagnostics and raise
        print("讀入後資料為空（drop 後）。我會列出前 200 raw lines 幫你檢查：")
        enc = info.get("encoding", "unknown")
        raw = read_sample_lines(path, enc, n=200)
        for i, ln in enumerate(raw):
            print(f"{i:03d}: {ln!r}")
        raise ValueError("讀入後資料為空。請檢查 CSV 是否含有可解析的交易日期與履約價欄位。")

    # 5) aggregate by Date, Strike, CP (sum OI; concat orig strings)
    agg = (df2.groupby([df2["Date"].dt.date, "Strike", "CP"], as_index=False)
           .agg({"OI": "sum", "orig_OI_str": lambda s: "|".join([str(x) for x in s.tolist()])}))
    agg["Date"] = pd.to_datetime(agg["Date"])

    # sort and compute prev1/prev2 and deltas
    agg = agg.sort_values(["Strike", "CP", "Date"]).reset_index(drop=True)
    agg["OI_prev1"] = agg.groupby(["Strike", "CP"])["OI"].shift(1).fillna(0).astype(int)
    agg["OI_prev2"] = agg.groupby(["Strike", "CP"])["OI"].shift(2).fillna(0).astype(int)
    agg["delta_1"] = agg["OI"] - agg["OI_prev1"]
    agg["delta_2"] = agg["OI"] - agg["OI_prev2"]

    # prepare meta
    date_list = sorted(agg["Date"].dt.date.unique())
    strike_min = int(agg["Strike"].min())
    strike_max = int(agg["Strike"].max())

    print(f"\n處理完成：日期數 {len(date_list)}，履約價範圍 {strike_min} ~ {strike_max}")
    return agg, date_list, strike_min, strike_max

# If run as script for direct debugging:
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python data_loader.py <path/to/txo.csv>")
        sys.exit(1)
    p = sys.argv[1]
    try:
        agg, dates, smin, smax = load_and_process(p)
        print("\n=== AGG head ===")
        print(agg.head(20).to_string(index=False))
    except Exception as e:
        print("讀取或解析失敗:", e)
        sys.exit(2)
