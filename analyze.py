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

# =========================
# ■ CSV正規化【最終修正版】
# =========================
def normalize_columns(file):

    try:
        df = pd.read_csv(file, encoding="cp932")
    except:
        df = pd.read_csv(file, encoding="utf-8-sig")

    # ★【修正】全角スペース＋半角スペースの両方を削除
    df.columns = df.columns.str.replace("　", "").str.replace(" ", "").str.strip()
    df = df.loc[:, ~df.columns.duplicated()]

    cols = df.columns.tolist()

    # ===== アンリツ =====
    if any("測定値" in col for col in cols) and any("ランクコード" in col for col in cols):

        rename_map = {}
        for col in cols:
            # ★【重要修正】「測定値出力No.」と「測定値(g)」を区別する
            if col == "測定値出力No.":
                rename_map[col] = "測定値出力No."  # そのままにする
            elif col == "測定値(g)":
                rename_map[col] = "測定値(g)"  # そのままにする
            elif "ランクコード" in col:
                rename_map[col] = "ランクコード"
            elif "日付時刻" in col:
                rename_map[col] = "日付時刻"

        df = df.rename(columns=rename_map)

        if "日付時刻" not in df.columns:
            if "日付" in df.columns and "時刻" in df.columns:
                df["日付時刻"] = pd.to_datetime(
                    df["日付"].astype(str) + " " + df["時刻"].astype(str),
                    errors="coerce"
                )

        df["メーカー"] = "アンリツ"

        # ★必要な列だけを抽出
        df = df[["測定値出力No.", "日付時刻", "測定値(g)", "ランクコード", "メーカー"]].copy()

        # ★【重要】データ型を正しく変換
        df["測定値出力No."] = pd.to_numeric(df["測定値出力No."], errors="coerce")
        df["測定値(g)"] = pd.to_numeric(df["測定値(g)"], errors="coerce")
        df["ランクコード"] = df["ランクコード"].astype(str).str.strip()
        df["日付時刻"] = pd.to_datetime(df["日付時刻"], errors="coerce")

        return df

    # ===== イシダ =====
    try:
        df = pd.read_csv(file, encoding="cp932", skiprows=10)
    except:
        df = pd.read_csv(file, encoding="utf-8-sig", skiprows=10)

    df.columns = df.columns.str.replace("　", "").str.replace(" ", "").str.strip()

    df = df.iloc[:, [0, 1, 4, 5]].copy()
    df.columns = ["日付", "時刻", "測定値(g)", "判定"]

    df["日付時刻"] = pd.to_datetime(
        df["日付"].astype(str) + " " + df["時刻"].astype(str),
        errors="coerce"
    )

    df["測定値出力No."] = range(1, len(df) + 1)

    rank_map = {"正量": "2", "軽量": "1", "過量": "E"}
    df["ランクコード"] = df["判定"].map(rank_map)
    df["メーカー"] = "イシダ"

    return df[["測定値出力No.", "日付時刻", "測定値(g)", "ランクコード", "メーカー"]]


# =========================
# ■ 分析
# =========================
def analyze(data):

    # ===== 完全1次元化 =====
    data = np.asarray(data).astype(float).ravel()

    # NaN除去
    data = data[~np.isnan(data)]

    n = len(data)
    if n < 2:
        return None, None, (None, None), None, None, None, None

    mean = float(np.mean(data))
    std = float(np.std(data, ddof=1))

    # ===== ここが修正ポイント =====
    # t値を使ってCI計算（安全）
    t_value = stats.t.ppf(0.975, df=n-1)
    margin = t_value * std / np.sqrt(n)

    ci_lower = mean - margin
    ci_upper = mean + margin

    max1 = float(np.max(data))
    min1 = float(np.min(data))

    lower = mean - 3 * std
    upper = mean + 3 * std

    return mean, std, (ci_lower, ci_upper), max1, min1, lower, upper


# =========================
# ■ Excel出力
# =========================
def save_to_excel(df_ok, mean, std, ci, max1, min1, lower, upper,
                  outliers_df, img_buffer, rank_counts, filename, lot):

    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
    from openpyxl.drawing.image import Image

    red_fill = PatternFill(start_color="FFFF0000", fill_type="solid")
    header_fill = PatternFill(start_color="FF4472C4", fill_type="solid")  # 青
    header_font = Font(bold=True, color="FFFFFFFF")  # 白
    result_fill = PatternFill(start_color="FFE7E6E6", fill_type="solid")  # 薄灰色
    center_align = Alignment(horizontal="center", vertical="center")
    border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin")
    )

    result_df = pd.DataFrame({
        "項目": ["平均", "標準偏差", "データ数", "Max", "Min", "下限(-3σ)", "上限(+3σ)"],
        "値": [mean, std, len(df_ok), max1, min1, lower, upper]
    })

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:

        result_df.to_excel(writer, sheet_name="統計結果", index=False)
        df_ok[["測定値出力No.", "日付時刻", "測定値(g)"]].to_excel(writer, sheet_name="OKデータ", index=False)
        outliers_df[["測定値出力No.", "日付時刻", "測定値(g)"]].to_excel(writer, sheet_name="外れ値", index=False)
        rank_counts.to_excel(writer, sheet_name="ランクコード集計", index=False)

        wb = writer.book

        # ===== 統計結果シートの装飾 =====
        ws_result = wb["統計結果"]
        ws_result.column_dimensions["A"].width = 20
        ws_result.column_dimensions["B"].width = 20

        # ヘッダー行を装飾
        for cell in ws_result[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center_align
            cell.border = border

        # データ行を装飾
        for row in ws_result.iter_rows(min_row=2, max_row=ws_result.max_row):
            for cell in row:
                cell.fill = result_fill
                cell.border = border
                cell.alignment = center_align
                # 値の列は項目に応じて異なるフォーマットを適用
                if cell.column == 2:
                    item = ws_result[cell.row][0].value
                    if item == "標準偏差":
                        cell.number_format = '0.000'  # 小数第3位
                    elif item == "データ数":
                        cell.number_format = '0'  # 整数
                    else:
                        cell.number_format = '0.0'  # 小数第1位

        # ===== ランクコード集計シートの装飾 =====
        ws_rank = wb["ランクコード集計"]
        ws_rank.column_dimensions["A"].width = 15
        ws_rank.column_dimensions["B"].width = 15
        ws_rank.column_dimensions["C"].width = 20

        # ヘッダー行を装飾
        for cell in ws_rank[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center_align
            cell.border = border

        # データ行を装飾
        for row in ws_rank.iter_rows(min_row=2, max_row=ws_rank.max_row):
            for cell in row:
                cell.border = border
                cell.alignment = center_align
                # 交互に背景色を変更
                if row[0].row % 2 == 0:
                    cell.fill = PatternFill(start_color="FFF2F2F2", fill_type="solid")

        # ===== OKデータシートの装飾 =====
        ws_ok = wb["OKデータ"]
        ws_ok.column_dimensions["A"].width = 18
        ws_ok.column_dimensions["B"].width = 22
        ws_ok.column_dimensions["C"].width = 15

        # ヘッダー行を装飾
        for cell in ws_ok[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center_align
            cell.border = border

        for row in ws_ok.iter_rows(min_row=2, min_col=1, max_col=3):
            for cell in row:
                cell.border = border

        for row in ws_ok.iter_rows(min_row=2, min_col=2, max_col=2):
            for cell in row:
                cell.number_format = "yyyy-mm-dd hh:mm:ss"

        ws_ok.auto_filter.ref = f"A1:C{ws_ok.max_row}"

        # 外れ値を赤表示
        outlier_ids = set(outliers_df["測定値出力No."].values)
        for row in ws_ok.iter_rows(min_row=2, max_row=ws_ok.max_row):
            if row[0].value in outlier_ids:
                for cell in row:
                    cell.fill = red_fill

        # ===== 外れ値シートの装飾 =====
        ws_out = wb["外れ値"]
        ws_out.column_dimensions["A"].width = 18
        ws_out.column_dimensions["B"].width = 22
        ws_out.column_dimensions["C"].width = 15

        # ヘッダー行を装飾
        for cell in ws_out[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center_align
            cell.border = border

        for row in ws_out.iter_rows(min_row=2, min_col=1, max_col=3):
            for cell in row:
                cell.border = border

        for row in ws_out.iter_rows(min_row=2, min_col=2, max_col=2):
            for cell in row:
                cell.number_format = "yyyy-mm-dd hh:mm:ss"

        ws_out.auto_filter.ref = f"A1:C{ws_out.max_row}"

        ws_graph = wb.create_sheet("グラフ")
        ws_graph.add_image(Image(img_buffer), "A1")


# =========================
# ■ ロット処理
# =========================
def process_lot(group, lot, save_dir):

    rank_map = {"2": "OK", "1": "軽量", "E": "過量", "0": "２個乗り"}

    rank_counts = group["ランクコード"].value_counts().reset_index()
    rank_counts.columns = ["ランクコード", "件数"]
    rank_counts["内容"] = rank_counts["ランクコード"].map(rank_map)
    rank_counts = rank_counts[["ランクコード", "内容", "件数"]]

    df_ok = group[group["ランクコード"] == "2"].copy()

    # ★完全データ整形（ここが最重要）
    data = pd.to_numeric(df_ok["測定値(g)"], errors="coerce")
    df_ok = df_ok.loc[data.notna()].copy()
    data = data.loc[data.notna()]

    # ★1次元強制
    data = np.asarray(data).ravel()

    if len(data) < 2:
        return

    mean, std, ci, max1, min1, lower, upper = analyze(data)

    outliers_df = df_ok[(df_ok["測定値(g)"] < lower) | (df_ok["測定値(g)"] > upper)]

    plt.figure(figsize=(10, 6))
    plt.hist(data, bins=30, edgecolor='black', alpha=0.7)
    plt.axvline(mean, color='red', linestyle='-', linewidth=2, label=f'平均: {mean:.2f}')
    plt.axvline(lower, color='orange', linestyle='--', linewidth=2, label=f'下限(-3σ): {lower:.2f}')
    plt.axvline(upper, color='orange', linestyle='--', linewidth=2, label=f'上限(+3σ): {upper:.2f}')

    plt.title(f'ロット{lot} 測定値の分布（n={len(data)}）', fontsize=14, fontweight='bold')
    plt.xlabel('測定値(g)', fontsize=12)
    plt.ylabel('頻度', fontsize=12)
    plt.legend(fontsize=10)
    plt.grid(True, alpha=0.3)

    img_buffer = BytesIO()
    plt.savefig(img_buffer, format="png", dpi=100, bbox_inches='tight')
    img_buffer.seek(0)
    plt.close()

    filename = os.path.join(
        save_dir,
        f"分析結果_ロット{lot}_{datetime.datetime.now():%Y%m%d_%H%M}.xlsx"
    )

    save_to_excel(df_ok, mean, std, ci, max1, min1,
                  lower, upper, outliers_df,
                  img_buffer, rank_counts, filename, lot)


# =========================
# ■ メイン
# =========================
def run():
    try:
        file = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if not file:
            return

        save_dir = os.path.dirname(file)

        df = normalize_columns(file)

        use_split = messagebox.askyesno("ロット判定", "時間差30分以上でロット分割しますか？")

        if use_split:
            df = df.sort_values("日付時刻")
            df["時間差(分)"] = df["日付時刻"].diff().dt.total_seconds() / 60
            df["ロット"] = (df["時間差(分)"] > 30).cumsum() + 1
        else:
            df["ロット"] = 1

        lot_count = df["ロット"].nunique()

        if lot_count > 1:
            ok = messagebox.askyesno("確認", f"{lot_count}ロット検出。この分割でOK？")
            if not ok:
                messagebox.showwarning("中止", "CSVを手動分割してください")
                return

        for lot, group in df.groupby("ロット"):
            process_lot(group, lot, save_dir)

        messagebox.showinfo("完了", "Excel作成完了")

    except Exception as e:
        messagebox.showerror("エラー", str(e))


# =========================
# ■ GUI
# =========================
root = tk.Tk()
root.title("重量分析ツール（完全最終版）")

btn = tk.Button(root, text="CSV選択して解析", command=run, height=2, width=30)
btn.pack(pady=20)

root.mainloop()
