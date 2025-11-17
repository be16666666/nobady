# charting.py
# -*- coding: utf-8 -*-
"""
繪圖模組（黑底白字）
- plot_oi_delta
- plot_cp_mirror
- plot_heatmap
"""

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

plt.rcParams["font.family"] = ["Microsoft JhengHei", "sans-serif"]
plt.rcParams["figure.facecolor"] = "black"
plt.rcParams["axes.facecolor"] = "black"
plt.rcParams["savefig.facecolor"] = "black"


def apply_dark(ax):
    ax.set_facecolor("black")
    ax.tick_params(colors="white", which="both")
    ax.xaxis.label.set_color("white")
    ax.yaxis.label.set_color("white")
    ax.title.set_color("white")
    for spine in ax.spines.values():
        spine.set_color("white")


def detect_alert_level(value):
    a = abs(int(value))
    if a >= 1200:
        return "極端"
    if a >= 800:
        return "重大布局"
    if a >= 400:
        return "大單"
    return None


def plot_oi_delta(df_day, title_date, strike_limit=None):
    d = df_day.copy()
    if strike_limit is not None:
        lo, hi = strike_limit
        d = d[(d["履約價"] >= lo) & (d["履約價"] <= hi)]
    if d.empty:
        fig, ax = plt.subplots(figsize=(8,4))
        apply_dark(ax)
        ax.text(0.5, 0.5, "無資料（篩選後）", ha="center", va="center", color="white")
        return fig

    d = d.sort_values("履約價")
    strikes = d["履約價"].astype(int).values
    oi = d["OI"].values
    delta = d["delta_1"].values

    x = np.arange(len(strikes))
    fig, ax = plt.subplots(figsize=(12, 5))
    apply_dark(ax)
    width = 0.35
    ax.bar(x - width/2, oi, width=width, label="OI")
    ax.bar(x + width/2, delta, width=width, label="ΔOI")
    ax.set_xticks(x)
    ax.set_xticklabels([str(s) for s in strikes], rotation=45)
    ax.set_xlabel("履約價")
    ax.set_ylabel("數量")
    ax.set_title(f"{title_date}  — OI 與 ΔOI")
    leg = ax.legend(framealpha=0.0)
    for text in leg.get_texts():
        text.set_color("white")

    # 右側警示
    idxs = np.argsort(-np.abs(delta))[:3]
    alerts = []
    for idx in idxs:
        if idx < len(delta):
            lvl = detect_alert_level(delta[idx])
            if lvl:
                alerts.append(f"{lvl}: {strikes[idx]} Δ={delta[idx]}")
    if alerts:
        ax.text(1.02, 0.5, "\n".join(alerts), transform=ax.transAxes, color="yellow", fontsize=12, va="center")
    return fig


def plot_cp_mirror(df_day, title_date, strike_limit=None):
    d = df_day.copy()
    if strike_limit is not None:
        lo, hi = strike_limit
        d = d[(d["履約價"] >= lo) & (d["履約價"] <= hi)]
    if d.empty:
        fig, ax = plt.subplots(figsize=(8,4))
        apply_dark(ax)
        ax.text(0.5, 0.5, "無資料（篩選後）", ha="center", va="center", color="white")
        return fig

    calls = d[d["買賣權"] == "買權"].sort_values("履約價")
    puts = d[d["買賣權"] == "賣權"].sort_values("履約價")

    fig, ax = plt.subplots(figsize=(12, 5))
    apply_dark(ax)

    # bar width relative
    width = max(20, (calls["履約價"].max() - calls["履約價"].min()) / max(1, len(calls)) * 0.6) if not calls.empty else 10
    if not calls.empty:
        ax.bar(calls["履約價"] - width/2, calls["OI"], width=width, label="買權")
    if not puts.empty:
        ax.bar(puts["履約價"] + width/2, -puts["OI"], width=width, label="賣權")
    ax.axhline(0, color="white", linewidth=0.8)
    ax.set_xlabel("履約價")
    ax.set_title(f"{title_date} — 買權 / 賣權 鏡像圖")
    leg = ax.legend(framealpha=0.0)
    for text in leg.get_texts():
        text.set_color("white")
    return fig


def plot_heatmap(agg_df, strike_range=None, days_limit=30):
    df = agg_df.copy()
    all_dates = sorted(df["交易日期"].dt.date.unique())
    recent = all_dates[-days_limit:] if len(all_dates) > 0 else []
    df = df[df["交易日期"].dt.date.isin(recent)]
    if strike_range is not None:
        lo, hi = strike_range
        df = df[(df["履約價"] >= lo) & (df["履約價"] <= hi)]
    if df.empty:
        fig, ax = plt.subplots(figsize=(8,4))
        apply_dark(ax)
        ax.text(0.5, 0.5, "無資料（篩選後）", ha="center", va="center", color="white")
        return fig

    pivot = df.pivot_table(index="交易日期", columns="履約價", values="OI", aggfunc="sum").fillna(0)
    fig, ax = plt.subplots(figsize=(12, max(4, 0.2*len(pivot.index))))
    apply_dark(ax)
    im = ax.imshow(pivot.values, aspect="auto", cmap="viridis")
    fig.colorbar(im, ax=ax)
    ax.set_yticks(np.arange(len(pivot.index)))
    ax.set_yticklabels([d.strftime("%Y-%m-%d") for d in pivot.index], color="white")
    ax.set_xticks(np.arange(len(pivot.columns)))
    ax.set_xticklabels([str(int(x)) for x in pivot.columns], rotation=90, color="white")
    ax.set_title("OI Heatmap（日期 × 履約價）")
    ax.set_xlabel("履約價")
    return fig
