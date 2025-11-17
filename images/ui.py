# ui.py
# -*- coding: utf-8 -*-
"""
主程式 UI（Tkinter）
- 黑底白字，字型 16
- 讀取 / 更新資料、選日期、選圖、畫圖
- 提示視窗（黑底白字）
"""

import os
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
import numpy as np

from data_loader import load_and_process
from charting import plot_oi_delta, plot_cp_mirror, plot_heatmap
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

FONT = ("Microsoft JhengHei", 16)


class TXOUI:
    def __init__(self, root):
        self.root = root
        root.title("TXO OI Viewer")
        root.configure(bg="black")

        self.data = None
        self.df_general = None
        self.agg = None

        # Top frame
        frm_top = tk.Frame(root, bg="black")
        frm_top.pack(padx=8, pady=8, fill="x")

        btn_load = tk.Button(frm_top, text="讀取 / 更新資料", command=self.on_load,
                             bg="black", fg="white", font=FONT, relief="raised", bd=2)
        btn_load.pack(side="left", padx=6)

        self.combo_date = ttk.Combobox(frm_top, font=FONT, width=14)
        self.combo_date.pack(side="left", padx=6)

        self.combo_chart = ttk.Combobox(frm_top, font=FONT, width=18,
                                        values=["OI + ΔOI", "買權/賣權鏡像", "OI Heatmap"])
        self.combo_chart.current(0)
        self.combo_chart.pack(side="left", padx=6)

        self.cb_range = ttk.Combobox(frm_top, font=FONT, width=12,
                                     values=["ATM±10", "ATM±5", "OI>0", "全部"])
        self.cb_range.set("ATM±10")
        self.cb_range.pack(side="left", padx=6)

        btn_plot = tk.Button(frm_top, text="畫圖", command=self.on_plot,
                             bg="black", fg="white", font=FONT, relief="raised", bd=2)
        btn_plot.pack(side="left", padx=6)

        # Canvas frame
        self.canvas_frame = tk.Frame(root, bg="black")
        self.canvas_frame.pack(fill="both", expand=True)

        self.fig_canvas = None

        # message box
        self.msg_box = ScrolledText(root, height=6, font=("Microsoft JhengHei", 12), bg="black", fg="white")
        self.msg_box.pack(fill="x", padx=8, pady=6)
        self.msg("啟動：請先按「讀取 / 更新資料」。")

    def msg(self, text):
        self.msg_box.configure(state="normal")
        self.msg_box.insert("end", text + "\n")
        self.msg_box.see("end")
        self.msg_box.configure(state="disabled")

    def on_load(self):
        path = filedialog.askopenfilename(title="選擇 TXO CSV", filetypes=[("CSV 檔案", "*.csv"), ("All files", "*.*")])
        if not path:
            return
        try:
            res = load_and_process(path)
        except Exception as e:
            messagebox.showerror("讀取錯誤", f"讀取或解析失敗：{e}")
            return

        self.data = res
        self.df_general = res["df_general"]
        self.agg = res["agg"]
        enc = res.get("encoding", "?")
        delim = res.get("delimiter", "?")

        # show session distribution (black popup)
        self.show_session_distribution()

        # fill dates
        dates = sorted(self.agg["交易日期"].dt.strftime("%Y-%m-%d").unique())
        self.combo_date["values"] = dates
        if dates:
            self.combo_date.set(dates[-1])

        self.msg(f"成功讀取：{os.path.basename(path)}（encoding={enc} delimiter='{delim}'）")
        self.msg(f"一般時段資料 rows: {len(self.df_general)}，agg rows: {len(self.agg)}")

        # save outputs
        out_dir = os.path.dirname(path)
        try:
            self.agg.to_csv(os.path.join(out_dir, "TXO_daily_all_with_deltas.csv"), index=False, encoding="utf-8-sig")
            self.msg("已輸出：TXO_daily_all_with_deltas.csv")
        except Exception as e:
            self.msg(f"輸出失敗：{e}")

    def show_session_distribution(self):
        raw = self.data["raw"]
        # find session-like column
        cand = [c for c in raw.columns if "時" in c or "段" in c or "session" in c.lower()]
        session_col = cand[0] if cand else None
        counts = {}
        if session_col:
            counts = raw[session_col].astype(str).str.strip().value_counts().to_dict()
        top = tk.Toplevel(self.root)
        top.title("交易時段分布")
        top.configure(bg="black")
        top.geometry("420x300")
        lbl = tk.Label(top, text="交易時段分布（CSV 原始）", bg="black", fg="white", font=FONT)
        lbl.pack(pady=6)
        txt = ScrolledText(top, bg="black", fg="white", font=("Microsoft JhengHei", 12))
        txt.pack(fill="both", expand=True, padx=8, pady=6)
        if counts:
            for k, v in counts.items():
                txt.insert("end", f"{k}: {v} 筆\n")
        else:
            txt.insert("end", "未偵測到時段欄位或該欄位皆空白。\n")
        txt.configure(state="disabled")
        btn = tk.Button(top, text="關閉", command=top.destroy, bg="black", fg="white", font=FONT)
        btn.pack(pady=6)

    def get_strike_window(self, date_str):
        mode = self.cb_range.get()
        df_date = self.agg[self.agg["交易日期"].dt.strftime("%Y-%m-%d") == date_str]
        if df_date.empty:
            return None
        if mode.startswith("ATM"):
            # ATM: 中位數的履約價
            atm = int(np.median(df_date["履約價"].values))
            try:
                n = int(mode.split("±")[1])
            except Exception:
                n = 10
            lo = atm - n * 100
            hi = atm + n * 100
            return lo, hi
        if mode == "OI>0":
            s = df_date[df_date["OI"] > 0]["履約價"]
            if s.empty:
                return None
            return int(s.min()), int(s.max())
        return None

    def on_plot(self):
        if self.agg is None or self.agg.empty:
            messagebox.showwarning("尚未匯入或無資料", "尚未匯入或無資料，請先按「讀取 / 更新資料」")
            return
        date_str = self.combo_date.get()
        if not date_str:
            messagebox.showwarning("未選日期", "請先選擇日期")
            return

        strike_window = self.get_strike_window(date_str)
        df_day = self.agg[self.agg["交易日期"].dt.strftime("%Y-%m-%d") == date_str]
        if df_day.empty:
            messagebox.showwarning("無資料", "該日期無資料可畫圖")
            return

        # 若當日買權全部沒有原始數字，則跳過買權
        flags = df_day.groupby("買賣權")["has_orig_number"].any().to_dict()
        if not flags.get("買權", False):
            df_day = df_day[df_day["買賣權"] != "買權"]
            self.msg(f"注意：{date_str} 買權全部為 '-' 或空白，已跳過買權計算。")

        chart_type = self.combo_chart.get()
        if chart_type == "OI + ΔOI":
            fig = plot_oi_delta(df_day, date_str, strike_limit=strike_window)
        elif chart_type == "買權/賣權鏡像":
            fig = plot_cp_mirror(df_day, date_str, strike_limit=strike_window)
        else:
            fig = plot_heatmap(self.agg, strike_range=strike_window, days_limit=30)

        if self.fig_canvas:
            self.fig_canvas.get_tk_widget().destroy()
        self.fig_canvas = FigureCanvasTkAgg(fig, master=self.canvas_frame)
        self.fig_canvas.draw()
        self.fig_canvas.get_tk_widget().pack(fill="both", expand=True)
        self.msg(f"已畫圖：{chart_type}（{date_str}）")


if __name__ == "__main__":
    root = tk.Tk()
    app = TXOUI(root)
    root.geometry("1200x800")
    root.mainloop()
