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
                  outliers_df, img_buffer, rank_counts, filename, lot):

    from openpyxl.styles import PatternFill, Font, Border, Side, Alignment
    from openpyxl.drawing.image import Image

    red_fill = PatternFill(start_color="FFFF0000", end_color="FFFF0000", fill_type="solid")
    bold = Font(bold=True, size=12)
    title_font = Font(bold=True, size=16)

    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    result_df = pd.DataFrame({
        "項目": ["平均", "標準偏差", "データ数", "Max", "Min", "下限(-3σ)", "上限(+3σ)"],
        "値": [mean, std, len(df_ok), max1, min1, lower, upper]
    })

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:

        result_df.to_excel(writer, sheet_name="統計結果", startrow=2, index=False)
        df_ok[["測定値出力No.", "日付時刻", "測定値(g)"]].to_excel(writer, sheet_name="OKデータ", index=False)
        outliers_df[["測定値出力No.", "日付時刻", "測定値(g)"]].to_excel(writer, sheet_name="外れ値", index=False)
        rank_counts.to_excel(writer, sheet_name="ランクコード集計", index=False)

        wb = writer.book

        # ===== 統計結果 =====
        ws = wb["統計結果"]

        ws["A1"] = f"重量分析結果（ロット{lot}）"
        ws["A1"].font = title_font

        ws["D1"] = "作成日時"
        ws["D2"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

        for cell in ws[3]:
            cell.font = bold

        ws.column_dimensions["A"].width = 20
        ws.column_dimensions["B"].width = 20

        for i, row in enumerate(ws.iter_rows(min_row=4, max_row=10, min_col=2, max_col=2), start=0):
            for cell in row:
                if i == 1:
                    cell.number_format = "0.000"
                elif i == 2:
                    cell.number_format = "0"
                else:
                    cell.number_format = "0.0"

        for row in ws.iter_rows(min_row=3, max_row=10, min_col=1, max_col=2):
            for cell in row:
                cell.border = border

        # ===== OK / 外れ値 =====
        ws_ok = wb["OKデータ"]
        ws_out = wb["外れ値"]

        for sheet in [ws_ok, ws_out]:
            sheet.column_dimensions["A"].width = 18
            sheet.column_dimensions["B"].width = 22
            sheet.column_dimensions["C"].width = 15

            for row in sheet.iter_rows(min_row=2, min_col=2, max_col=2):
                for cell in row:
                    cell.number_format = "yyyy-mm-dd hh:mm:ss"

            sheet.auto_filter.ref = f"A1:C{sheet.max_row}"

        # 外れ値をOKシートで赤
        outlier_ids = set(outliers_df["測定値出力No."].values)

        for row in ws_ok.iter_rows(min_row=2, max_row=ws_ok.max_row):
            if row[0].value in outlier_ids:
                for cell in row:
                    cell.fill = red_fill

        # ===== ランクコード =====
        ws_rank = wb["ランクコード集計"]

        ws_rank.column_dimensions["A"].width = 15
        ws_rank.column_dimensions["B"].width = 20
        ws_rank.column_dimensions["C"].width = 10

        for cell in ws_rank[1]:
            cell.font = bold
            cell.alignment = Alignment(horizontal="center")

        for row in ws_rank.iter_rows(min_row=1, max_row=ws_rank.max_row, min_col=1, max_col=3):
            for cell in row:
                cell.border = border

        # ===== グラフ =====
        ws_graph = wb.create_sheet("グラフ")
        ws_graph.add_image(Image(img_buffer), "A1")


def process_lot(group, lot, save_dir):

    rank_map = {"2": "OK", "1": "軽量", "E": "過量", "0": "２個乗り"}

    rank_counts = group["ランクコード"].value_counts().reset_index()
    rank_counts.columns = ["ランクコード", "件数"]
    rank_counts["内容"] = rank_counts["ランクコード"].map(rank_map)
    rank_counts = rank_counts[["ランクコード", "内容", "件数"]]

    df_ok = group[group["ランクコード"] == "2"].copy()
    df_ok = df_ok.dropna(subset=["測定値(g)"])

    if len(df_ok) < 2:
        return

    data = df_ok["測定値(g)"]

    mean, std, ci, max1, min1, lower, upper = analyze(data)
    outliers_df = df_ok[(data < lower) | (data > upper)]

    plt.figure()
    plt.hist(data, bins=30)
    plt.axvline(mean)
    plt.axvline(lower, linestyle="--")
    plt.axvline(upper, linestyle="--")
    plt.title(f"重量分布（ロット{lot}）")

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
                  img_buffer, rank_counts, filename, lot)


def run():
    try:
        file = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if not file:
            return

        save_dir = os.path.dirname(file)

        try:
            df = pd.read_csv(file, encoding="cp932")
        except:
            df = pd.read_csv(file, encoding="utf-8-sig")

        df.columns = df.columns.str.replace("　", "").str.strip()
        df["ランクコード"] = df["ランクコード"].astype(str).str.strip()
        df["測定値(g)"] = pd.to_numeric(df["測定値(g)"], errors="coerce")
        df["日付時刻"] = pd.to_datetime(df["日付時刻"], errors="coerce")

        # ===== ロット判定 =====
        use_split = messagebox.askyesno("ロット判定", "時間差30分以上でロット分割しますか？")

        if use_split:
            df = df.sort_values("日付時刻")
            df["時間差(分)"] = df["日付時刻"].diff().dt.total_seconds() / 60
            df["ロット"] = (df["時間差(分)"] > 30).cumsum() + 1
        else:
            df["ロット"] = 1

        # ===== ロット確認（復活）=====
        lot_count = df["ロット"].nunique()

        if lot_count > 1:
            ok = messagebox.askyesno(
                "確認",
                f"{lot_count}ロット検出されました。\nこの分割でよろしいですか？"
            )

            if not ok:
                messagebox.showwarning(
                    "中止",
                    "CSVを手動で分割してください。処理を終了します。"
                )
                return

        # ===== 実行 =====
        for lot, group in df.groupby("ロット"):
            process_lot(group, lot, save_dir)

        messagebox.showinfo("完了", "Excelファイル作成完了")

    except Exception as e:
        messagebox.showerror("エラー", str(e))


root = tk.Tk()
root.title("重量分析ツール（最終完成版）")

btn = tk.Button(root, text="CSV選択して解析", command=run, height=2, width=30)
btn.pack(pady=20)

root.mainloop()
