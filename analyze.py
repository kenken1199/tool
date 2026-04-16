import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import tkinter as tk
from tkinter import filedialog, messagebox
import datetime
from io import BytesIO

# 日本語フォント
plt.rcParams["font.family"] = "MS Gothic"

def analyze(data):
    mean = data.mean()
    std = data.std()
    n = len(data)

    ci = stats.t.interval(0.95, df=n-1, loc=mean, scale=std/np.sqrt(n))

    lower = mean - 3*std
    upper = mean + 3*std
    outliers = data[(data < lower) | (data > upper)]

    return mean, std, ci, outliers

def save_to_excel(data, clean, mean, std, ci, outliers,
                  mean2, std2, ci2,
                  img1_buffer, img2_buffer, filename):

    result_df = pd.DataFrame({
        "項目": ["平均", "標準偏差", "データ数", "CI下限", "CI上限", "外れ値数"],
        "全データ": [mean, std, len(data), ci[0], ci[1], len(outliers)],
        "外れ値除外後": [mean2, std2, len(clean), ci2[0], ci2[1], 0]
    })

    if len(outliers) > 0:
        outlier_df = outliers.to_frame(name="外れ値")
    else:
        outlier_df = pd.DataFrame({"外れ値": []})

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name="統計結果", index=False)
        outlier_df.to_excel(writer, sheet_name="外れ値(±３σ以上)", index=False)
        data.to_frame(name="全データ").to_excel(writer, sheet_name="元データ", index=False)
        clean.to_frame(name="外れ値除外後").to_excel(writer, sheet_name="外れ値除外後データ", index=False)

        # グラフ貼り付け（メモリ画像）
        from openpyxl.drawing.image import Image
        workbook = writer.book
        sheet = workbook.create_sheet("グラフ")

        img_obj1 = Image(img1_buffer)
        img_obj2 = Image(img2_buffer)

        sheet.add_image(img_obj1, "A1")
        sheet.add_image(img_obj2, "A25")

def run():
    try:
        file = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        df = pd.read_csv(file, encoding="cp932")

        # 前処理
        df.columns = df.columns.str.replace("　", "").str.strip()
        df = df.replace(r"^\s*$", np.nan, regex=True)
        df = df.dropna(axis=1, how="all")
        df["測定値(g)"] = pd.to_numeric(df["測定値(g)"], errors="coerce")

        data = df["測定値(g)"].dropna()

        # 全データ分析
        mean, std, ci, outliers = analyze(data)

        result = f"""
【全データ】
平均: {mean:.3f}
σ: {std:.3f}
データ数: {len(data)}

95%信頼区間:
{ci[0]:.3f} ～ {ci[1]:.3f}

外れ値数: {len(outliers)}
"""
        messagebox.showinfo("結果", result)

        # グラフ①（全データ）
        plt.figure()
        plt.hist(data, bins=30)
        plt.axvline(mean, label="平均")
        plt.title("重量分布（全データ）")
        plt.legend()
        plt.grid(True)

        img1_buffer = BytesIO()
        plt.savefig(img1_buffer, format="png")
        img1_buffer.seek(0)
        plt.close()

        # 外れ値表示
        if len(outliers) > 0:
            messagebox.showinfo("外れ値一覧", str(outliers.values))

        # 外れ値除外
        clean = data[~data.isin(outliers)]

        mean2, std2, ci2, _ = analyze(clean)

        result2 = f"""
【外れ値除外後】
平均: {mean2:.3f}
σ: {std2:.3f}
データ数: {len(clean)}

95%信頼区間:
{ci2[0]:.3f} ～ {ci2[1]:.3f}
"""
        messagebox.showinfo("結果（外れ値除外後）", result2)

        # グラフ②（除外後）
        plt.figure()
        plt.hist(clean, bins=30)
        plt.axvline(mean2, label="平均")
        plt.title("重量分布（外れ値除外後）")
        plt.legend()
        plt.grid(True)

        img2_buffer = BytesIO()
        plt.savefig(img2_buffer, format="png")
        img2_buffer.seek(0)
        plt.close()

        # Excel保存
        if messagebox.askyesno("確認", "Excel保存しますか？"):
            filename = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                initialfile=f"分析結果_{datetime.datetime.now():%Y%m%d_%H%M}.xlsx"
            )

            save_to_excel(data, clean, mean, std, ci, outliers,
                          mean2, std2, ci2,
                          img1_buffer, img2_buffer, filename)

            messagebox.showinfo("完了", "Excel保存完了！")

    except Exception as e:
        messagebox.showerror("エラー", str(e))

# GUI
root = tk.Tk()
root.title("重量分析ツール（完成版）")

btn = tk.Button(root, text="CSV選択して解析", command=run, height=2, width=30)
btn.pack(pady=20)

root.mainloop()
