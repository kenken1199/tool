import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import tkinter as tk
from tkinter import filedialog, messagebox
import datetime
from io import BytesIO

plt.rcParams["font.family"] = "MS Gothic"

def analyze(data):
    mean = data.mean()
    std = data.std()
    n = len(data)

    ci = stats.t.interval(0.95, df=n-1, loc=mean, scale=std/np.sqrt(n))

    max1 = data.max()
    min1 = data.min()

    # ±3σ
    lower = mean - 3 * std
    upper = mean + 3 * std

    return mean, std, ci, max1, min1, lower, upper

def save_to_excel(df_ok, mean, std, ci, max1, min1, lower, upper,
                  outliers_df, img_buffer, rank_counts, filename):

    result_df = pd.DataFrame({
        "項目": ["平均", "標準偏差", "データ数", "Max", "Min", "下限(-3σ)", "上限(+3σ)"],
        "値": [mean, std, len(df_ok), max1, min1, lower, upper]
    })

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name="統計結果", index=False)

        df_ok[["測定値出力No.", "日付時刻", "測定値(g)"]] \
            .to_excel(writer, sheet_name="OKデータ", index=False)

        outliers_df[["測定値出力No.", "日付時刻", "測定値(g)"]] \
            .to_excel(writer, sheet_name="外れ値", index=False)

        rank_counts.to_excel(writer, sheet_name="ランクコード集計", index=False)

        from openpyxl.drawing.image import Image
        sheet = writer.book.create_sheet("グラフ")
        sheet.add_image(Image(img_buffer), "A1")

def run():
    try:
        # CSV選択
        file = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if not file:
            return

        # 保存先選択
        filename = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            initialfile=f"分析結果_{datetime.datetime.now():%Y%m%d_%H%M}.xlsx"
        )
        if not filename:
            return

        df = pd.read_csv(file, encoding="cp932")

        # 前処理
        df.columns = df.columns.str.replace("　", "").str.strip()
        df["ランクコード"] = df["ランクコード"].astype(str).str.strip()
        df["測定値(g)"] = pd.to_numeric(df["測定値(g)"], errors="coerce")

        # ランクコード
        rank_map = {
            "2": "OK",
            "1": "軽量",
            "E": "過量",
            "0": "２個乗り"
        }

        rank_counts = df["ランクコード"].value_counts().reset_index()
        rank_counts.columns = ["ランクコード", "件数"]
        rank_counts["内容"] = rank_counts["ランクコード"].map(rank_map)
        rank_counts = rank_counts[["ランクコード", "内容", "件数"]]

        # OKデータ
        df_ok = df[df["ランクコード"] == "2"].copy()
        df_ok["測定値(g)"] = pd.to_numeric(df_ok["測定値(g)"], errors="coerce")
        df_ok = df_ok.dropna(subset=["測定値(g)"])

        if len(df_ok) < 2:
            return

        data = df_ok["測定値(g)"]

        # 分析
        mean, std, ci, max1, min1, lower, upper = analyze(data)

        # 外れ値
        outliers_df = df_ok[(data < lower) | (data > upper)]

        # グラフ
        plt.figure()
        plt.hist(data, bins=30, alpha=0.7)
        plt.axvline(mean, label="平均")
        plt.axvline(lower, linestyle="--", label="-3σ")
        plt.axvline(upper, linestyle="--", label="+3σ")
        plt.title("重量分布（OKデータ）")
        plt.legend()
        plt.grid(True)

        img_buffer = BytesIO()
        plt.savefig(img_buffer, format="png")
        img_buffer.seek(0)
        plt.close()

        # 保存
        save_to_excel(df_ok, mean, std, ci, max1, min1,
                      lower, upper, outliers_df,
                      img_buffer, rank_counts, filename)

        # 最後だけ表示
        messagebox.showinfo("完了", "Excel保存が完了しました")

    except Exception as e:
        messagebox.showerror("エラー", str(e))

# GUI
root = tk.Tk()
root.title("重量分析ツール（最終版）")

btn = tk.Button(root, text="CSV選択して解析", command=run, height=2, width=30)
btn.pack(pady=20)

root.mainloop()
