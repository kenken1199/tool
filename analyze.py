import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import tkinter as tk
from tkinter import filedialog, messagebox
import datetime
from io import BytesIO
import os

plt.rcParams["font.family"] = "MS Gothic"

def analyze(data):
    mean = data.mean()
    std = data.std()
    n = len(data)

    ci = stats.t.interval(0.95, df=n-1, loc=mean, scale=std/np.sqrt(n))

    max1 = data.max()
    min1 = data.min()

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

        # === 書き込み ===
        result_df.to_excel(writer, sheet_name="統計結果", index=False)

        df_ok[["測定値出力No.", "日付時刻", "測定値(g)"]] \
            .to_excel(writer, sheet_name="OKデータ", index=False)

        outliers_df[["測定値出力No.", "日付時刻", "測定値(g)"]] \
            .to_excel(writer, sheet_name="外れ値", index=False)

        rank_counts.to_excel(writer, sheet_name="ランクコード集計", index=False)

        # === フィルター追加 ===
        workbook = writer.book

        ws_ok = workbook["OKデータ"]
        ws_out = workbook["外れ値"]

        ok_rows = len(df_ok) + 1
        out_rows = len(outliers_df) + 1

        ws_ok.auto_filter.ref = f"A1:C{ok_rows}"
        ws_out.auto_filter.ref = f"A1:C{out_rows}"

        # === グラフ ===
        from openpyxl.drawing.image import Image
        sheet = workbook.create_sheet("グラフ")
        sheet.add_image(Image(img_buffer), "A1")

def process_lot(group, lot, save_dir, rank_counts):

    df_ok = group[group["ランクコード"] == "2"].copy()
    df_ok = df_ok.dropna(subset=["測定値(g)"])

    if len(df_ok) < 2:
        return

    data = df_ok["測定値(g)"]

    mean, std, ci, max1, min1, lower, upper = analyze(data)

    outliers_df = df_ok[(data < lower) | (data > upper)]

    # グラフ
    plt.figure()
    plt.hist(data, bins=30, alpha=0.7)
    plt.axvline(mean, label="平均")
    plt.axvline(lower, linestyle="--", label="-3σ")
    plt.axvline(upper, linestyle="--", label="+3σ")
    plt.title(f"重量分布（ロット{lot}）")
    plt.legend()
    plt.grid(True)

    img_buffer = BytesIO()
    plt.savefig(img_buffer, format="png")
    img_buffer.seek(0)
    plt.close()

    filename = os.path.join(
        save_dir,
        f"分析結果_ロット{lot}_{datetime.datetime.now():%Y%m%d_%H%M}.xlsx"
    )

    save_to_excel(df_ok, mean, std, ci, max1, min1,
                  lower, upper, outliers_df,
                  img_buffer, rank_counts, filename)

def run():
    try:
        file = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if not file:
            return

        save_dir = os.path.dirname(file)

        # CSV読み込み
        try:
            df = pd.read_csv(file, encoding="cp932")
        except:
            df = pd.read_csv(file, encoding="utf-8-sig")

        # 前処理
        df.columns = df.columns.str.replace("　", "").str.strip()
        df["ランクコード"] = df["ランクコード"].astype(str).str.strip()
        df["測定値(g)"] = pd.to_numeric(df["測定値(g)"], errors="coerce")
        df["日付時刻"] = pd.to_datetime(df["日付時刻"], errors="coerce")

        # ===== ロット選択 =====
        use_split = messagebox.askyesno(
            "ロット判定",
            "時間差30分以上でロット分割しますか？\n（いいえ→1ロット）"
        )

        if use_split:
            df = df.sort_values("日付時刻")
            df["時間差(分)"] = df["日付時刻"].diff().dt.total_seconds() / 60
            threshold = 30
            df["ロット"] = (df["時間差(分)"] > threshold).cumsum() + 1
        else:
            df["ロット"] = 1

        lot_count = df["ロット"].nunique()

        # ランクコード集計
        rank_map = {"2": "OK", "1": "軽量", "E": "過量", "0": "２個乗り"}
        rank_counts = df["ランクコード"].value_counts().reset_index()
        rank_counts.columns = ["ランクコード", "件数"]
        rank_counts["内容"] = rank_counts["ランクコード"].map(rank_map)

        # ===== 分岐 =====
        if lot_count == 1:
            process_lot(df, 1, save_dir, rank_counts)

        else:
            ok = messagebox.askyesno(
                "確認",
                f"{lot_count}ロット検出されました。\nこのままでよろしいですか？"
            )

            if not ok:
                messagebox.showwarning("中止", "CSVをロットごとに分割してください")
                return

            for lot, group in df.groupby("ロット"):
                process_lot(group, lot, save_dir, rank_counts)

        messagebox.showinfo("完了", "Excelファイル作成完了")

    except Exception as e:
        messagebox.showerror("エラー", str(e))

# GUI
root = tk.Tk()
root.title("重量分析ツール（完成版）")

btn = tk.Button(root, text="CSV選択して解析", command=run, height=2, width=30)
btn.pack(pady=20)

root.mainloop()
