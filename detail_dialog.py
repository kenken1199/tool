"""
==========================================================
 品種別詳細画面ダイアログ
==========================================================

 タブ構成:
   1. サマリー: 統計値テーブル + 統合ヒストグラム + 推奨規格
   2. 経時変化: 日別の平均/σ/不良率推移グラフ
   3. 日別一覧: 日付フォルダ単位の統計値テーブル
   4. データエクスポート: 全部入りExcel出力ボタン

 元データはCSV再読込方式（バックグラウンドスレッド）
==========================================================
"""

import os
import threading
import queue
import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from detail_loader import (
    load_hinshoku_data,
    aggregate_by_date_folder,
    detect_abnormal_dates,
    compute_overall_stats,
)
from detail_export import export_hinshoku_detail


class HinshokuDetailDialog(tk.Toplevel):
    def __init__(self, parent, record_dir, aggregate_info):
        super().__init__(parent)
        self.record_dir = record_dir
        self.aggregate_info = aggregate_info
        self.hinshoku_num = aggregate_info["品種番号"]

        self.combined_df = None
        self.daily_df = None
        self.overall_stats = None

        self.title(f"品種詳細 - 品種番号 {self.hinshoku_num}")
        self.geometry("1100x720")
        self.resizable(True, True)

        self._build_ui()

        self.update_idletasks()
        x = parent.winfo_rootx() + 30
        y = parent.winfo_rooty() + 30
        self.geometry(f"+{x}+{y}")

        # 起動と同時にデータ読込開始
        self.after(100, self._start_load_data)

    # ------------------------------
    # UI構築
    # ------------------------------
    def _build_ui(self):
        # トップ情報
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        agg = self.aggregate_info
        info_text = (
            f"品種番号: {self.hinshoku_num}   "
            f"製造期間: {agg.get('初回製造日', '-')} 〜 {agg.get('最終製造日', '-')}   "
            f"製造日数: {agg.get('製造日数', 0)}日   "
            f"ファイル数: {agg.get('ファイル数', 0)}   "
            f"総件数: {agg.get('総件数', 0):,}件"
        )
        ttk.Label(top, text=info_text, font=("", 10, "bold")).pack(side="left")

        self.status_var = tk.StringVar(value="データ読込中...")
        ttk.Label(top, textvariable=self.status_var,
                  foreground="navy").pack(side="right")

        # タブ
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.tab_summary = ttk.Frame(self.notebook)
        self.tab_trend = ttk.Frame(self.notebook)
        self.tab_daily = ttk.Frame(self.notebook)
        self.tab_export = ttk.Frame(self.notebook)

        self.notebook.add(self.tab_summary, text="📊 サマリー")
        self.notebook.add(self.tab_trend, text="📈 経時変化")
        self.notebook.add(self.tab_daily, text="📅 日別一覧")
        self.notebook.add(self.tab_export, text="💾 エクスポート")

        # 各タブの初期表示はローディング
        for tab in (self.tab_summary, self.tab_trend, self.tab_daily):
            ttk.Label(tab, text="データ読込中...",
                      font=("", 12), foreground="gray").pack(expand=True)

        # エクスポートタブだけは早めに作る
        self._build_export_tab()

    # ------------------------------
    # データ読込（バックグラウンド）
    # ------------------------------
    def _start_load_data(self):
        self.load_queue = queue.Queue()

        def worker():
            try:
                file_list = self.aggregate_info.get("_file_list", [])

                def progress_cb(current, total, fname):
                    self.load_queue.put(("progress", current, total, fname))

                combined_df, errors = load_hinshoku_data(
                    self.record_dir, self.hinshoku_num, file_list,
                    progress_callback=progress_cb,
                )
                daily_df = aggregate_by_date_folder(file_list)
                daily_df = detect_abnormal_dates(daily_df)

                if len(combined_df) > 0:
                    overall = compute_overall_stats(combined_df)
                else:
                    overall = None

                self.load_queue.put(("done", {
                    "combined_df": combined_df,
                    "daily_df": daily_df,
                    "overall": overall,
                    "errors": errors,
                }))
            except Exception as e:
                import traceback
                self.load_queue.put(("error", f"{e}\n\n{traceback.format_exc()}"))

        threading.Thread(target=worker, daemon=True).start()
        self._poll_load()

    def _poll_load(self):
        try:
            while True:
                msg = self.load_queue.get_nowait()
                kind = msg[0]
                if kind == "progress":
                    _, cur, total, fname = msg
                    short = fname if len(fname) < 60 else "..." + fname[-57:]
                    self.status_var.set(f"読込中: {cur}/{total}  {short}")
                elif kind == "done":
                    self._on_load_done(msg[1])
                    return
                elif kind == "error":
                    messagebox.showerror("読込エラー", msg[1], parent=self)
                    self.status_var.set("読込失敗")
                    return
        except queue.Empty:
            pass
        self.after(80, self._poll_load)

    def _on_load_done(self, result):
        self.combined_df = result["combined_df"]
        self.daily_df = result["daily_df"]
        self.overall_stats = result["overall"]
        errors = result["errors"]

        if len(self.combined_df) == 0:
            self.status_var.set("有効なデータがありません")
            messagebox.showwarning(
                "データなし",
                "この品種のデータが読み込めませんでした。",
                parent=self,
            )
            return

        n = len(self.combined_df)
        msg = f"読込完了: {n:,}レコード"
        if errors:
            msg += f" / 一部ファイル読込失敗 {len(errors)}件"
        self.status_var.set(msg)

        # 各タブの中身を構築
        self._build_summary_tab()
        self._build_trend_tab()
        self._build_daily_tab()
        self._enable_export_button()

    # ------------------------------
    # タブ1: サマリー
    # ------------------------------
    def _build_summary_tab(self):
        for w in self.tab_summary.winfo_children():
            w.destroy()

        # 左：統計値テーブル
        left = ttk.Frame(self.tab_summary, padding=10)
        left.pack(side="left", fill="y")

        ttk.Label(left, text="■ 統計値（全期間OKデータ）",
                  font=("", 10, "bold")).pack(anchor="w", pady=(0, 5))

        tree = ttk.Treeview(left, columns=("値",), show="tree headings", height=14)
        tree.column("#0", width=160, anchor="w")
        tree.column("値", width=120, anchor="e")
        tree.heading("#0", text="項目")
        tree.heading("値", text="値")

        s = self.overall_stats
        a = self.aggregate_info
        rows = [
            ("件数", f"{s['件数']:,}"),
            ("平均(g)", f"{s['平均']:.4f}"),
            ("標準偏差(g)", f"{s['σ']:.5f}"),
            ("Min(g)", f"{s['Min']:.3f}"),
            ("Max(g)", f"{s['Max']:.3f}"),
            ("", ""),
            ("推奨下限 -3σ(g)", f"{s['推奨下限']:.4f}"),
            ("推奨上限 +3σ(g)", f"{s['推奨上限']:.4f}"),
            ("95%CI 下限(g)", f"{s['CI下限']:.4f}"),
            ("95%CI 上限(g)", f"{s['CI上限']:.4f}"),
            ("外れ値件数", f"{s['外れ値件数']:,}"),
            ("", ""),
            ("総件数", f"{a.get('総件数', 0):,}"),
            ("不良率(%)", f"{a.get('不良率(%)', 0):.3f}"),
        ]
        for label, val in rows:
            tree.insert("", "end", text=label, values=(val,))
        tree.pack(fill="y")

        # 推奨規格値のメッセージ
        spec_frame = ttk.LabelFrame(left, text="推奨規格値（実績ベース）", padding=8)
        spec_frame.pack(fill="x", pady=(10, 0))
        spec_text = (
            f"平均 {s['平均']:.3f} g\n"
            f"範囲 {s['推奨下限']:.3f} 〜 {s['推奨上限']:.3f} g\n"
            f"幅 ±{3*s['σ']:.3f} g (3σ)"
        )
        ttk.Label(spec_frame, text=spec_text,
                  foreground="navy", font=("", 9)).pack(anchor="w")

        # 右：ヒストグラム
        right = ttk.Frame(self.tab_summary, padding=10)
        right.pack(side="right", fill="both", expand=True)

        fig = Figure(figsize=(7, 5), dpi=90)
        ax = fig.add_subplot(111)

        ok_data = pd.to_numeric(
            self.combined_df.loc[self.combined_df["ランクコード"] == "2", "測定値(g)"],
            errors="coerce"
        ).dropna()

        ax.hist(ok_data, bins=40, edgecolor="black", alpha=0.7, color="steelblue")
        ax.axvline(s["平均"], color="red", linestyle="-", linewidth=2,
                   label=f"平均: {s['平均']:.3f}")
        ax.axvline(s["推奨下限"], color="orange", linestyle="--", linewidth=2,
                   label=f"-3σ: {s['推奨下限']:.3f}")
        ax.axvline(s["推奨上限"], color="orange", linestyle="--", linewidth=2,
                   label=f"+3σ: {s['推奨上限']:.3f}")
        ax.set_title(f"品種番号{self.hinshoku_num} 全期間ヒストグラム(n={s['件数']:,})",
                     fontsize=11, fontweight="bold")
        ax.set_xlabel("測定値(g)")
        ax.set_ylabel("頻度")
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=right)
        canvas.get_tk_widget().pack(fill="both", expand=True)
        canvas.draw()

    # ------------------------------
    # タブ2: 経時変化
    # ------------------------------
    def _build_trend_tab(self):
        for w in self.tab_trend.winfo_children():
            w.destroy()

        if self.daily_df is None or len(self.daily_df) == 0:
            ttk.Label(self.tab_trend, text="日別データがありません").pack(expand=True)
            return

        valid = self.daily_df.dropna(subset=["平均(g)"]).copy()
        if len(valid) == 0:
            ttk.Label(self.tab_trend, text="有効な日別データがありません").pack(expand=True)
            return

        valid["日付dt"] = pd.to_datetime(valid["日付"], format="%Y%m%d", errors="coerce")

        fig = Figure(figsize=(11, 7), dpi=90)
        gs = fig.add_gridspec(3, 1, hspace=0.15)
        ax1 = fig.add_subplot(gs[0])
        ax2 = fig.add_subplot(gs[1], sharex=ax1)
        ax3 = fig.add_subplot(gs[2], sharex=ax1)

        # 平均値
        ax1.plot(valid["日付dt"], valid["平均(g)"],
                 marker="o", color="steelblue", linewidth=1.5, markersize=5)
        overall_mean = valid["平均(g)"].mean()
        ax1.axhline(overall_mean, color="red", linestyle="--", alpha=0.5,
                    label=f"全期間平均: {overall_mean:.3f}")

        # 異常日をハイライト
        abnormal = valid[valid["異常フラグ"] == True]
        if len(abnormal) > 0:
            ax1.scatter(abnormal["日付dt"], abnormal["平均(g)"],
                        color="red", s=80, zorder=5,
                        marker="o", facecolors="none", edgecolors="red", linewidth=2,
                        label=f"異常日({len(abnormal)}日)")

        ax1.set_ylabel("平均(g)", fontsize=10)
        ax1.set_title(f"品種番号{self.hinshoku_num} 日別推移",
                      fontsize=12, fontweight="bold")
        ax1.legend(fontsize=8)
        ax1.grid(True, alpha=0.3)
        plt.setp(ax1.xaxis.get_majorticklabels(), visible=False)

        # σ
        valid_std = valid.dropna(subset=["σ(g)"])
        ax2.plot(valid_std["日付dt"], valid_std["σ(g)"],
                 marker="s", color="darkorange", linewidth=1.5, markersize=5)
        ax2.set_ylabel("σ(g)", fontsize=10)
        ax2.grid(True, alpha=0.3)
        plt.setp(ax2.xaxis.get_majorticklabels(), visible=False)

        # 不良率
        ax3.bar(valid["日付dt"], valid["不良率(%)"],
                color="firebrick", alpha=0.7, width=0.8)
        ax3.set_ylabel("不良率(%)", fontsize=10)
        ax3.set_xlabel("製造日", fontsize=10)
        ax3.grid(True, alpha=0.3)
        ax3.xaxis.set_major_formatter(mdates.DateFormatter("%Y/%m/%d"))
        plt.setp(ax3.xaxis.get_majorticklabels(), rotation=30, ha="right")

        canvas = FigureCanvasTkAgg(fig, master=self.tab_trend)
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
        canvas.draw()

        # ナビゲーションツールバー(ズーム等)
        toolbar_frame = ttk.Frame(self.tab_trend)
        toolbar_frame.pack(fill="x", padx=10)
        NavigationToolbar2Tk(canvas, toolbar_frame)

    # ------------------------------
    # タブ3: 日別一覧
    # ------------------------------
    def _build_daily_tab(self):
        for w in self.tab_daily.winfo_children():
            w.destroy()

        if self.daily_df is None or len(self.daily_df) == 0:
            ttk.Label(self.tab_daily, text="日別データがありません").pack(expand=True)
            return

        frame = ttk.Frame(self.tab_daily, padding=10)
        frame.pack(fill="both", expand=True)

        cols = ("日付", "ファイル数", "総件数", "OK件数", "NG件数",
                "不良率(%)", "平均(g)", "σ(g)", "Min(g)", "Max(g)", "備考")
        tree = ttk.Treeview(frame, columns=cols, show="headings", height=18)

        widths = {
            "日付": 90, "ファイル数": 70, "総件数": 75, "OK件数": 75, "NG件数": 70,
            "不良率(%)": 75, "平均(g)": 80, "σ(g)": 80, "Min(g)": 75, "Max(g)": 75,
            "備考": 250,
        }
        for col in cols:
            tree.heading(col, text=col)
            anchor = "center" if col in ("日付", "ファイル数") else "e"
            if col == "備考":
                anchor = "w"
            tree.column(col, anchor=anchor, width=widths[col])

        tree.tag_configure("abnormal", background="#FFE4E1")

        for _, row in self.daily_df.iterrows():
            tags = ("abnormal",) if row.get("異常フラグ") else ()
            mean_v = row["平均(g)"]
            std_v = row["σ(g)"]
            min_v = row["Min(g)"]
            max_v = row["Max(g)"]
            tree.insert("", "end", tags=tags, values=(
                row["日付"],
                row["ファイル数"],
                f"{row['総件数']:,}",
                f"{row['OK件数']:,}",
                f"{row['NG件数']:,}",
                f"{row['不良率(%)']:.3f}",
                f"{mean_v:.4f}" if pd.notna(mean_v) else "-",
                f"{std_v:.5f}"  if pd.notna(std_v)  else "-",
                f"{min_v:.3f}"  if pd.notna(min_v)  else "-",
                f"{max_v:.3f}"  if pd.notna(max_v)  else "-",
                row.get("異常理由", ""),
            ))

        sb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=sb.set)
        tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

    # ------------------------------
    # タブ4: エクスポート
    # ------------------------------
    def _build_export_tab(self):
        frame = ttk.Frame(self.tab_export, padding=20)
        frame.pack(fill="both", expand=True)

        ttk.Label(
            frame,
            text=f"品種番号 {self.hinshoku_num} の詳細レポートをExcelに出力します。",
            font=("", 11),
        ).pack(anchor="w", pady=(0, 10))

        ttk.Label(
            frame,
            text=(
                "出力内容:\n"
                "  ・統計結果（全期間サマリー）\n"
                "  ・日別集計\n"
                "  ・ヒストグラム\n"
                "  ・時系列チャート\n"
                "  ・日別推移グラフ\n"
                "  ・全OKデータ（生データ）\n"
                "  ・外れ値"
            ),
            justify="left",
        ).pack(anchor="w", pady=(0, 20))

        self.export_btn = ttk.Button(
            frame,
            text="📤 Excelに出力する",
            command=self._on_export,
            state="disabled",
        )
        self.export_btn.pack(anchor="w")

        self.export_status_var = tk.StringVar(value="")
        ttk.Label(frame, textvariable=self.export_status_var,
                  foreground="navy").pack(anchor="w", pady=(10, 0))

    def _enable_export_button(self):
        if hasattr(self, "export_btn"):
            self.export_btn.config(state="normal")

    def _on_export(self):
        if self.combined_df is None or self.overall_stats is None:
            messagebox.showwarning("データなし", "出力するデータがありません",
                                   parent=self)
            return

        default_name = (
            f"品種詳細_品種番号{self.hinshoku_num}_"
            f"{datetime.datetime.now():%Y%m%d_%H%M}.xlsx"
        )
        filepath = filedialog.asksaveasfilename(
            parent=self,
            defaultextension=".xlsx",
            initialfile=default_name,
            initialdir=self.record_dir,
            filetypes=[("Excel files", "*.xlsx")],
        )
        if not filepath:
            return

        try:
            self.export_status_var.set("出力中...")
            self.update_idletasks()

            export_hinshoku_detail(
                filepath=filepath,
                hinshoku_num=self.hinshoku_num,
                aggregate_info=self.aggregate_info,
                combined_df=self.combined_df,
                daily_df=self.daily_df,
                overall_stats=self.overall_stats,
            )

            self.export_status_var.set(f"✓ 出力完了: {os.path.basename(filepath)}")
            messagebox.showinfo(
                "完了",
                f"Excelファイルを出力しました:\n{filepath}",
                parent=self,
            )
        except Exception as e:
            import traceback
            self.export_status_var.set("出力失敗")
            messagebox.showerror(
                "エラー",
                f"出力に失敗しました:\n{e}\n\n{traceback.format_exc()}",
                parent=self,
            )
