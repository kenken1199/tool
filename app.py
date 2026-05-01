"""
RECORD フォルダ集計ツール - Streamlit Web版
"""

import os
import io
import json
import datetime
import traceback

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from record_analyzer import (
    find_indiv_csvs,
    analyze_csv_file,
    aggregate_by_hinshoku,
    load_product_names,
    CACHE_FILENAME,
    CACHE_VERSION,
)
from detail_loader import (
    load_hinshoku_data,
    aggregate_by_date_folder,
    detect_abnormal_dates,
    compute_overall_stats,
)
from detail_export import export_hinshoku_detail
from analyze import analyze as _analyze_data, MIN_OK_COUNT, save_to_excel


# ─────────────────────────────────────────────────────────────
# ページ設定
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="RECORD フォルダ集計ツール",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ─────────────────────────────────────────────────────────────
# セッション状態初期化
# ─────────────────────────────────────────────────────────────
def _init_state():
    defaults = {
        "record_dir": "",
        "file_index": {},
        "aggregates": [],
        "product_names": {},
        "view": "list",
        "selected_hinshoku": None,
        "combined_df": None,
        "daily_df": None,
        "overall_stats": None,
        "detail_errors": [],
        # フォルダブラウザ
        "show_browser": False,
        "browse_path": "",
        "browse_filter": "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()


# ─────────────────────────────────────────────────────────────
# スキャン処理（同期・進捗表示付き）
# ─────────────────────────────────────────────────────────────
def run_scan(record_dir: str):
    cache_path = os.path.join(record_dir, CACHE_FILENAME)
    old_cache = {}
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                cached = json.load(f)
                if cached.get("version") == CACHE_VERSION:
                    old_cache = cached.get("files", {})
        except (json.JSONDecodeError, OSError):
            pass

    files = find_indiv_csvs(record_dir)
    total = len(files)
    if total == 0:
        st.error("INDIV.csv が見つかりません。RECORDフォルダの構造を確認してください。")
        return

    new_cache = {}
    errors = []
    reused = 0
    rescanned = 0

    prog = st.progress(0, text="スキャン開始...")
    status_txt = st.empty()

    for i, file_info in enumerate(files):
        relpath = file_info["relpath"]
        mtime = file_info["mtime"]
        size = file_info["size"]

        prog.progress((i + 1) / total, text=f"処理中 {i+1}/{total}: {relpath}")
        status_txt.caption(relpath)

        cached_entries = {
            k: v for k, v in old_cache.items()
            if v.get("relpath") == relpath
            and v.get("mtime") == mtime
            and v.get("size") == size
        }
        if cached_entries:
            new_cache.update(cached_entries)
            reused += 1
            continue

        try:
            entries = analyze_csv_file(file_info["abspath"])
            if not entries:
                errors.append((relpath, "有効なデータがありません"))
                continue
            for entry in entries:
                h = entry["品種番号"]
                new_cache[f"{relpath}#{h}"] = {
                    "relpath": relpath,
                    "mtime": mtime,
                    "size": size,
                    "date_folder": file_info["date_folder"],
                    "filename": file_info["filename"],
                    **entry,
                }
            rescanned += 1
        except Exception as e:
            errors.append((relpath, str(e)))

    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(
                {"version": CACHE_VERSION,
                 "scanned_at": datetime.datetime.now().isoformat(),
                 "record_dir": record_dir,
                 "files": new_cache},
                f, ensure_ascii=False, indent=2,
            )
    except OSError as e:
        errors.append(("(キャッシュ保存)", str(e)))

    prog.empty()
    status_txt.empty()

    st.session_state.file_index = new_cache
    st.session_state.aggregates = aggregate_by_hinshoku(new_cache)
    st.session_state.product_names = load_product_names(record_dir)

    n = len(st.session_state.aggregates)
    st.success(
        f"スキャン完了: ファイル {len(new_cache)} / 品種数 {n} "
        f"/ キャッシュ再利用 {reused} / 新規 {rescanned}"
        + (f" / エラー {len(errors)} 件" if errors else "")
    )
    if errors:
        with st.expander(f"⚠ エラー詳細 ({len(errors)} 件)"):
            for r, e in errors[:20]:
                st.text(f"{r}: {e}")


# ─────────────────────────────────────────────────────────────
# 品種詳細データ読込
# ─────────────────────────────────────────────────────────────
def load_detail(agg: dict):
    with st.spinner("データ読込中..."):
        combined_df, errors = load_hinshoku_data(
            st.session_state.record_dir,
            agg["品種番号"],
            agg.get("_file_list", []),
        )
        daily_df = detect_abnormal_dates(aggregate_by_date_folder(agg.get("_file_list", [])))
        overall_stats = compute_overall_stats(combined_df) if len(combined_df) > 0 else None

    st.session_state.combined_df = combined_df
    st.session_state.daily_df = daily_df
    st.session_state.overall_stats = overall_stats
    st.session_state.detail_errors = errors


# ─────────────────────────────────────────────────────────────
# ロット Excel 生成（メモリ上）
# ─────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def _lot_excel_bytes(
    lot_group: pd.DataFrame,
    lot_num: int,
    hinshoku_num,
    product_name: str,
) -> bytes | None:
    import platform
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import matplotlib.font_manager as fm

    candidates = {
        "Windows": ["MS Gothic", "Yu Gothic", "Meiryo"],
        "Darwin": ["Hiragino Sans", "Hiragino Maru Gothic Pro"],
        "Linux": ["Noto Sans CJK JP", "IPAGothic"],
    }.get(platform.system(), [])
    available = {f.name for f in fm.fontManager.ttflist}
    for font in candidates:
        if font in available:
            plt.rcParams["font.family"] = font
            break

    rank_map = {"2": "OK", "1": "軽量", "E": "過量", "0": "２個乗り"}
    rank_counts = lot_group["ランクコード"].value_counts().reset_index()
    rank_counts.columns = ["ランクコード", "件数"]
    rank_counts["内容"] = rank_counts["ランクコード"].map(rank_map)
    rank_counts = rank_counts[["ランクコード", "内容", "件数"]]

    total_count = len(lot_group)
    original_ok_count = int((lot_group["ランクコード"] == "2").sum())

    df_ok = lot_group[lot_group["ランクコード"] == "2"].copy()
    data = pd.to_numeric(df_ok["測定値(g)"], errors="coerce")
    df_ok = df_ok.loc[data.notna()].copy()
    data = data.loc[data.notna()]
    data_arr = np.asarray(data).ravel()

    if len(data_arr) < MIN_OK_COUNT:
        return None

    mean, std, ci, max1, min1, lower, upper = _analyze_data(data_arr)
    outliers_df = df_ok[(df_ok["測定値(g)"] < lower) | (df_ok["測定値(g)"] > upper)]

    lot_date = lot_group["日付時刻"].dropna().min()
    display_label = product_name if product_name else (
        f"品種番号{hinshoku_num}" if hinshoku_num is not None else None
    )
    date_str = (
        f"{lot_date.year}/{lot_date.month}/{lot_date.day}"
        if pd.notna(lot_date) and display_label else None
    )
    chart_prefix = f"{date_str}製造 {display_label} " if date_str else ""

    # ヒストグラム
    fig1, ax1 = plt.subplots(figsize=(10, 6))
    ax1.hist(data_arr, bins=30, edgecolor="black", alpha=0.7)
    ax1.axvline(mean, color="red", linestyle="-", linewidth=2, label=f"平均: {mean:.2f}")
    ax1.axvline(lower, color="orange", linestyle="--", linewidth=2, label=f"下限(-3σ): {lower:.2f}")
    ax1.axvline(upper, color="orange", linestyle="--", linewidth=2, label=f"上限(+3σ): {upper:.2f}")
    ax1.set_title(f"{chart_prefix}測定値の分布（n={len(data_arr)}）", fontsize=14, fontweight="bold")
    ax1.set_xlabel("測定値(g)", fontsize=12)
    ax1.set_ylabel("頻度", fontsize=12)
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3)
    fig1.tight_layout()
    img_hist = io.BytesIO()
    fig1.savefig(img_hist, format="png", dpi=100, bbox_inches="tight")
    img_hist.seek(0)
    plt.close(fig1)

    # 時系列チャート
    fig2, ax2 = plt.subplots(figsize=(12, 5))
    y_vals = df_ok["測定値(g)"].values
    outlier_mask = (df_ok["測定値(g)"] < lower) | (df_ok["測定値(g)"] > upper)
    has_dt = df_ok["日付時刻"].notna().any()
    if has_dt:
        x_all = df_ok["日付時刻"]
        x_ok = df_ok.loc[~outlier_mask, "日付時刻"]
        x_out = df_ok.loc[outlier_mask, "日付時刻"]
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax2.xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=30, ha="right")
        ax2.set_xlabel("時刻", fontsize=12)
    else:
        x_all = np.arange(1, len(df_ok) + 1)
        x_ok = x_all[~outlier_mask.values]
        x_out = x_all[outlier_mask.values]
        ax2.set_xlabel("測定順序", fontsize=12)
    y_ok = y_vals[~outlier_mask.values]
    y_out = y_vals[outlier_mask.values]
    ax2.plot(x_all, y_vals, color="steelblue", linewidth=0.6, alpha=0.4, zorder=1)
    ax2.scatter(x_ok, y_ok, color="steelblue", s=18, alpha=0.8, zorder=2, label="OK")
    if len(x_out) > 0:
        ax2.scatter(x_out, y_out, color="red", s=50, marker="x", linewidths=2, zorder=3,
                    label=f"外れ値 ({len(x_out)}件)")
    ax2.axhline(mean, color="red", linewidth=1.5, linestyle="-", label=f"平均: {mean:.2f}")
    ax2.axhline(upper, color="orange", linewidth=1.5, linestyle="--", label=f"+3σ: {upper:.2f}")
    ax2.axhline(lower, color="orange", linewidth=1.5, linestyle="--", label=f"-3σ: {lower:.2f}")
    ax2.set_title(f"{chart_prefix}時系列チャート（n={len(df_ok)}）", fontsize=14, fontweight="bold")
    ax2.set_ylabel("測定値(g)", fontsize=12)
    ax2.legend(fontsize=10)
    ax2.grid(True, alpha=0.3)
    fig2.tight_layout()
    img_series = io.BytesIO()
    fig2.savefig(img_series, format="png", dpi=100, bbox_inches="tight")
    img_series.seek(0)
    plt.close(fig2)

    buf = io.BytesIO()
    save_to_excel(
        df_ok, mean, std, ci, max1, min1, lower, upper,
        outliers_df, img_hist, img_series, rank_counts, buf, lot_num,
        total_count=total_count, original_ok_count=original_ok_count,
        hinshoku_num=hinshoku_num, date_str=date_str, product_name=product_name,
    )
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────
# フォルダブラウザ（サイドバー用）
# ─────────────────────────────────────────────────────────────
def _list_dirs(path: str, filter_str: str) -> list[str]:
    """path 直下のディレクトリ一覧を返す（アクセス不可は空リスト）"""
    try:
        entries = [
            e.path for e in os.scandir(path)
            if e.is_dir() and not e.name.startswith(".")
        ]
        entries.sort(key=lambda p: os.path.basename(p).lower())
        if filter_str:
            entries = [p for p in entries if filter_str.lower() in os.path.basename(p).lower()]
        return entries
    except (PermissionError, OSError):
        return []


def _windows_drives() -> list[str]:
    import string
    return [f"{d}:\\" for d in string.ascii_uppercase if os.path.exists(f"{d}:\\")]


def render_folder_browser():
    """サイドバー内フォルダブラウザを描画する"""
    current = st.session_state.browse_path

    # ── 現在地の表示 ──
    st.caption("現在地:")
    st.code(current or "(ドライブ一覧)", language=None)

    # ── ナビゲーション ──
    col_up, col_home = st.columns(2)
    with col_up:
        parent = os.path.dirname(current) if current else ""
        at_root = (not current) or (parent == current)
        if st.button("↑ 上へ", disabled=at_root, use_container_width=True, key="fb_up"):
            # Windows: ドライブルート（例 C:\）の上はドライブ一覧へ
            if os.path.splitdrive(current)[1] in ("\\", "/", ""):
                st.session_state.browse_path = ""
            else:
                st.session_state.browse_path = parent
            st.session_state.browse_filter = ""
            st.rerun()
    with col_home:
        if st.button("🏠 ホーム", use_container_width=True, key="fb_home"):
            st.session_state.browse_path = os.path.expanduser("~")
            st.session_state.browse_filter = ""
            st.rerun()

    # ── フィルタ ──
    filter_str = st.text_input(
        "絞込", value=st.session_state.browse_filter,
        placeholder="フォルダ名で絞込...", key="fb_filter_input", label_visibility="collapsed",
    )
    if filter_str != st.session_state.browse_filter:
        st.session_state.browse_filter = filter_str
        st.rerun()

    # ── ディレクトリ一覧 ──
    if not current:
        # Windows: ドライブ一覧
        entries = _windows_drives()
    else:
        entries = _list_dirs(current, filter_str)

    if not entries:
        st.caption("サブフォルダなし")
    else:
        for path in entries[:60]:
            name = os.path.basename(path) or path  # ドライブは basename が空
            if st.button(f"📁 {name}", key=f"fb_{path}", use_container_width=True):
                st.session_state.browse_path = path
                st.session_state.browse_filter = ""
                st.rerun()
        if len(entries) > 60:
            st.caption(f"…他 {len(entries)-60} 件（絞込で絞ってください）")

    st.divider()

    # ── 選択ボタン ──
    if current and os.path.isdir(current):
        if st.button(
            f"✓ このフォルダを選択",
            type="primary", use_container_width=True, key="fb_select",
        ):
            st.session_state.record_dir = current
            st.session_state.show_browser = False
            st.session_state.browse_filter = ""
            st.rerun()
    else:
        st.button("✓ このフォルダを選択", disabled=True, use_container_width=True, key="fb_select_dis")


# ─────────────────────────────────────────────────────────────
# サイドバー
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚖️ RECORD 集計ツール")
    st.divider()

    # 選択中フォルダの表示
    selected = st.session_state.record_dir
    if selected:
        st.success(f"**選択中:**\n{selected}")
    else:
        st.info("フォルダが未選択です")

    # フォルダ選択ボタン
    browser_label = "📂 フォルダを閉じる" if st.session_state.show_browser else "📁 フォルダを選択..."
    if st.button(browser_label, use_container_width=True):
        st.session_state.show_browser = not st.session_state.show_browser
        if st.session_state.show_browser and not st.session_state.browse_path:
            # 初回: 現在選択中フォルダ or Cドライブ or ホーム
            st.session_state.browse_path = (
                selected or
                (r"C:\\" if os.path.exists(r"C:\\") else os.path.expanduser("~"))
            )
        st.rerun()

    # フォルダブラウザ（展開時）
    if st.session_state.show_browser:
        st.divider()
        render_folder_browser()
        st.divider()

    # スキャンボタン
    col1, col2 = st.columns(2)
    with col1:
        scan_btn = st.button(
            "🔍 スキャン", use_container_width=True, type="primary",
            disabled=not st.session_state.record_dir,
        )
    with col2:
        rescan_btn = st.button(
            "🔄 再スキャン", use_container_width=True,
            disabled=not st.session_state.record_dir,
        )

    if scan_btn and st.session_state.record_dir:
        st.session_state.view = "list"
        st.session_state.selected_hinshoku = None
        st.session_state.show_browser = False
        run_scan(st.session_state.record_dir)
        st.rerun()

    if rescan_btn and st.session_state.record_dir:
        run_scan(st.session_state.record_dir)
        st.rerun()

    st.divider()

    if st.session_state.aggregates:
        st.metric("品種数", len(st.session_state.aggregates))
        st.metric("ファイル数", len(st.session_state.file_index))

    if st.session_state.view == "detail":
        st.divider()
        if st.button("← 品種一覧に戻る", use_container_width=True):
            st.session_state.view = "list"
            st.session_state.selected_hinshoku = None
            st.rerun()


# ─────────────────────────────────────────────────────────────
# メインエリア
# ─────────────────────────────────────────────────────────────

# ══════════════ 品種一覧ビュー ══════════════
if st.session_state.view == "list":
    st.header("品種別集計一覧")

    if not st.session_state.aggregates:
        st.info("左のサイドバーで RECORD フォルダパスを入力して「スキャン」してください。")
        st.stop()

    search = st.text_input("🔍 品種番号・製品名で絞込", placeholder="例: 12 またはチョコ")

    product_names = st.session_state.product_names
    rows = []
    for agg in st.session_state.aggregates:
        h = agg["品種番号"]
        pname = product_names.get(h, "")
        if search and search not in str(h) and search not in pname:
            continue
        rows.append({
            "品種番号": str(h) if h >= 0 else "(不明)",
            "製品名": pname,
            "製造日数": agg["製造日数"],
            "総件数": agg["総件数"],
            "OK件数": agg["OK件数"],
            "不良率(%)": round(agg["不良率(%)"], 2),
            "平均(g)": round(agg["平均(g)"], 3) if agg["平均(g)"] is not None else None,
            "σ(g)": round(agg["σ(g)"], 4) if agg["σ(g)"] is not None else None,
            "Min(g)": round(agg["Min(g)"], 2) if agg["Min(g)"] is not None else None,
            "Max(g)": round(agg["Max(g)"], 2) if agg["Max(g)"] is not None else None,
            "推奨下限(g)": round(agg["推奨下限(g)"], 2) if agg["推奨下限(g)"] is not None else None,
            "推奨上限(g)": round(agg["推奨上限(g)"], 2) if agg["推奨上限(g)"] is not None else None,
            "最終製造日": agg["最終製造日"] or "-",
        })

    if not rows:
        st.warning("該当する品種がありません")
        st.stop()

    df_display = pd.DataFrame(rows)
    st.caption(f"表示: {len(rows)} 品種　｜　行を選択して「詳細を見る」ボタンを押してください")

    event = st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "不良率(%)": st.column_config.NumberColumn(format="%.2f"),
            "平均(g)": st.column_config.NumberColumn(format="%.3f"),
            "σ(g)": st.column_config.NumberColumn(format="%.4f"),
        },
    )

    selected_rows = event.selection.rows if (event and hasattr(event, "selection")) else []
    if selected_rows:
        row_idx = selected_rows[0]
        hinshoku_val = df_display.iloc[row_idx]["品種番号"]
        if hinshoku_val != "(不明)":
            h_num = int(hinshoku_val)
            pname = product_names.get(h_num, "")
            label = pname if pname else f"品種番号 {h_num}"
            c1, c2 = st.columns([1, 4])
            with c1:
                if st.button("📊 詳細を見る", type="primary"):
                    agg = next(
                        (a for a in st.session_state.aggregates if a["品種番号"] == h_num),
                        None,
                    )
                    if agg:
                        st.session_state.selected_hinshoku = h_num
                        st.session_state.view = "detail"
                        load_detail(agg)
                        st.rerun()
            with c2:
                st.info(f"選択中: **{label}**")


# ══════════════ 品種詳細ビュー ══════════════
else:
    h_num = st.session_state.selected_hinshoku
    if h_num is None:
        st.session_state.view = "list"
        st.rerun()

    agg = next(
        (a for a in st.session_state.aggregates if a["品種番号"] == h_num), None
    )
    if agg is None:
        st.error("品種データが見つかりません")
        st.stop()

    product_names = st.session_state.product_names
    product_name = product_names.get(h_num, "")
    display_name = product_name if product_name else f"品種番号 {h_num}"

    combined_df: pd.DataFrame = st.session_state.combined_df
    daily_df: pd.DataFrame = st.session_state.daily_df
    overall_stats: dict = st.session_state.overall_stats

    # ヘッダ情報
    st.header(f"📊 {display_name}")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("製造日数", agg["製造日数"])
    c2.metric("総件数", f"{agg['総件数']:,}")
    c3.metric("OK件数", f"{agg['OK件数']:,}")
    c4.metric("NG件数", f"{agg['NG件数']:,}")
    c5.metric("不良率", f"{agg['不良率(%)']:.2f}%")
    c6.metric("最終製造日", agg.get("最終製造日") or "-")

    if combined_df is None or len(combined_df) == 0:
        st.warning("有効なデータが読み込めませんでした")
        st.stop()

    if st.session_state.detail_errors:
        with st.expander(f"⚠ 読込エラー {len(st.session_state.detail_errors)} 件"):
            for r, e in st.session_state.detail_errors:
                st.text(f"{r}: {e}")

    tab_sum, tab_trend, tab_daily, tab_export = st.tabs([
        "📊 サマリー", "📈 経時変化", "📅 日別一覧・ロット分析", "💾 エクスポート",
    ])

    # ─── タブ1: サマリー ───
    with tab_sum:
        if overall_stats is None:
            st.warning("統計計算できるデータがありません（OKデータが不足しています）")
        else:
            s = overall_stats
            col_left, col_right = st.columns([1, 2])

            with col_left:
                st.subheader("統計値（全期間 OKデータ）")
                stats_rows = [
                    ("件数", f"{s['件数']:,}"),
                    ("平均(g)", f"{s['平均']:.4f}"),
                    ("標準偏差(g)", f"{s['σ']:.5f}"),
                    ("Min(g)", f"{s['Min']:.3f}"),
                    ("Max(g)", f"{s['Max']:.3f}"),
                    ("", ""),
                    ("下限 −3σ(g)", f"{s['推奨下限']:.4f}"),
                    ("上限 +3σ(g)", f"{s['推奨上限']:.4f}"),
                    ("95%CI 下限(g)", f"{s['CI下限']:.4f}"),
                    ("95%CI 上限(g)", f"{s['CI上限']:.4f}"),
                    ("外れ値件数", f"{s['外れ値件数']:,}"),
                    ("", ""),
                    ("総件数（全ランク）", f"{agg.get('総件数', 0):,}"),
                    ("不良率(%)", f"{agg.get('不良率(%)', 0):.3f}"),
                ]
                st.dataframe(
                    pd.DataFrame(stats_rows, columns=["項目", "値"]),
                    hide_index=True,
                    use_container_width=True,
                    height=460,
                )

                st.info(
                    f"**推奨規格値（実績ベース）**  \n"
                    f"平均: {s['平均']:.3f} g  \n"
                    f"範囲: {s['推奨下限']:.3f} 〜 {s['推奨上限']:.3f} g  \n"
                    f"幅: ±{3*s['σ']:.3f} g（3σ）"
                )

            with col_right:
                ok_data = pd.to_numeric(
                    combined_df.loc[combined_df["ランクコード"] == "2", "測定値(g)"],
                    errors="coerce",
                ).dropna()

                fig = go.Figure()
                fig.add_trace(go.Histogram(
                    x=ok_data, nbinsx=40, name="測定値",
                    marker_color="steelblue", opacity=0.7,
                ))
                fig.add_vline(x=s["平均"], line_color="red", line_width=2,
                              annotation_text=f"平均: {s['平均']:.3f}",
                              annotation_position="top right")
                fig.add_vline(x=s["推奨下限"], line_color="orange", line_dash="dash", line_width=2,
                              annotation_text=f"−3σ: {s['推奨下限']:.3f}",
                              annotation_position="bottom left")
                fig.add_vline(x=s["推奨上限"], line_color="orange", line_dash="dash", line_width=2,
                              annotation_text=f"+3σ: {s['推奨上限']:.3f}",
                              annotation_position="bottom right")
                fig.update_layout(
                    title=f"{display_name} 全期間ヒストグラム（n={s['件数']:,}）",
                    xaxis_title="測定値(g)",
                    yaxis_title="頻度",
                    showlegend=False,
                    height=480,
                )
                st.plotly_chart(fig, use_container_width=True)

    # ─── タブ2: 経時変化 ───
    with tab_trend:
        if daily_df is None or len(daily_df) == 0:
            st.warning("日別データがありません")
        else:
            valid = daily_df.dropna(subset=["平均(g)"]).copy()
            if len(valid) == 0:
                st.warning("有効な日別データがありません")
            else:
                valid["日付dt"] = pd.to_datetime(valid["日付"], format="%Y%m%d", errors="coerce")

                fig = make_subplots(
                    rows=3, cols=1,
                    shared_xaxes=True,
                    subplot_titles=("平均(g)", "σ(g)", "不良率(%)"),
                    vertical_spacing=0.08,
                    row_heights=[0.4, 0.3, 0.3],
                )

                # 平均値
                fig.add_trace(go.Scatter(
                    x=valid["日付dt"], y=valid["平均(g)"],
                    mode="lines+markers", name="平均",
                    line=dict(color="steelblue", width=2),
                    marker=dict(size=6),
                ), row=1, col=1)
                overall_mean = valid["平均(g)"].mean()
                fig.add_hline(
                    y=overall_mean, line_dash="dash", line_color="red",
                    annotation_text=f"全期間平均: {overall_mean:.3f}",
                    annotation_position="bottom right", row=1, col=1,
                )
                if "異常フラグ" in valid.columns:
                    abn = valid[valid["異常フラグ"] & valid["異常理由"].str.contains("平均", na=False)]
                    if len(abn) > 0:
                        fig.add_trace(go.Scatter(
                            x=abn["日付dt"], y=abn["平均(g)"],
                            mode="markers", name="平均異常",
                            marker=dict(size=12, color="red", symbol="circle-open",
                                        line=dict(width=2, color="red")),
                        ), row=1, col=1)

                # σ
                valid_std = valid.dropna(subset=["σ(g)"])
                fig.add_trace(go.Scatter(
                    x=valid_std["日付dt"], y=valid_std["σ(g)"],
                    mode="lines+markers", name="σ",
                    line=dict(color="darkorange", width=2),
                    marker=dict(size=6, symbol="square"),
                ), row=2, col=1)

                # 不良率
                fig.add_trace(go.Bar(
                    x=valid["日付dt"], y=valid["不良率(%)"],
                    name="不良率", marker_color="firebrick", opacity=0.7,
                ), row=3, col=1)

                fig.update_layout(
                    title=f"{display_name} 日別推移",
                    height=620,
                    showlegend=False,
                )
                fig.update_xaxes(tickformat="%Y/%m/%d", tickangle=30, row=3, col=1)
                st.plotly_chart(fig, use_container_width=True)

    # ─── タブ3: 日別一覧・ロット分析 ───
    with tab_daily:
        if daily_df is None or len(daily_df) == 0:
            st.warning("日別データがありません")
        else:
            st.subheader("日別一覧")
            st.caption("行を選択するとその日のロット分析ができます。赤背景相当の日は「異常理由」欄に詳細が表示されます。")

            disp_cols = ["日付", "ファイル数", "総件数", "OK件数", "NG件数",
                         "不良率(%)", "平均(g)", "σ(g)", "Min(g)", "Max(g)", "異常理由"]
            daily_disp = daily_df[[c for c in disp_cols if c in daily_df.columns]].copy()
            for col in ["平均(g)", "σ(g)", "Min(g)", "Max(g)"]:
                if col in daily_disp.columns:
                    daily_disp[col] = daily_disp[col].round(4)
            if "不良率(%)" in daily_disp.columns:
                daily_disp["不良率(%)"] = daily_disp["不良率(%)"].round(3)

            daily_event = st.dataframe(
                daily_disp,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
            )

            selected_daily = (
                daily_event.selection.rows
                if (daily_event and hasattr(daily_event, "selection"))
                else []
            )

            st.divider()
            st.subheader("ロット分析")

            if not selected_daily:
                st.info("上の日別一覧から行を選択してください")
            else:
                row_idx = selected_daily[0]
                date_folder = str(daily_df.iloc[row_idx]["日付"])
                st.info(f"対象日: **{date_folder}**")

                try:
                    target_date = pd.to_datetime(date_folder, format="%Y%m%d").date()
                    day_df = combined_df[
                        combined_df["日付時刻"].dt.date == target_date
                    ].copy()
                except Exception:
                    day_df = pd.DataFrame()

                if len(day_df) == 0:
                    st.warning("この日のデータが見つかりません（データ読込エラーの可能性があります）")
                else:
                    threshold = st.slider(
                        "ロット分割しきい値（分）",
                        min_value=1, max_value=480, value=30,
                        help="この時間間隔（分）を超えた場合に別ロットとして分割します",
                    )

                    df_sorted = day_df.sort_values("日付時刻").copy()
                    df_sorted["時間差(分)"] = (
                        df_sorted["日付時刻"].diff().dt.total_seconds() / 60
                    )
                    df_sorted["ロット"] = (
                        (df_sorted["時間差(分)"] > threshold).cumsum() + 1
                    )

                    lot_rows = []
                    for lot, grp in df_sorted.groupby("ロット"):
                        start = grp["日付時刻"].min()
                        end = grp["日付時刻"].max()
                        ok_c = int((grp["ランクコード"] == "2").sum())
                        lot_rows.append({
                            "ロット": f"ロット{lot}",
                            "開始時刻": start.strftime("%H:%M") if pd.notna(start) else "-",
                            "終了時刻": end.strftime("%H:%M") if pd.notna(end) else "-",
                            "総件数": len(grp),
                            "OKデータ件数": ok_c,
                            "状態": "✓ 分析可能" if ok_c >= MIN_OK_COUNT else "⚠ OKデータ不足",
                        })
                    st.dataframe(pd.DataFrame(lot_rows), hide_index=True, use_container_width=True)

                    lot_options = [f"ロット{lot}" for lot in sorted(df_sorted["ロット"].unique())]
                    sel_lot = st.selectbox("詳細・ダウンロードするロットを選択", lot_options)

                    if sel_lot:
                        lot_num = int(sel_lot.replace("ロット", ""))
                        lot_group = df_sorted[df_sorted["ロット"] == lot_num].copy()
                        ok_count = int((lot_group["ランクコード"] == "2").sum())

                        if ok_count < MIN_OK_COUNT:
                            st.warning(
                                f"OKデータが {ok_count} 件（最低 {MIN_OK_COUNT} 件必要）のため分析できません。"
                                "しきい値を変更してロットを統合することを検討してください。"
                            )
                        else:
                            ok_data_lot = pd.to_numeric(
                                lot_group.loc[lot_group["ランクコード"] == "2", "測定値(g)"],
                                errors="coerce",
                            ).dropna()
                            mean_l, std_l, ci_l, max_l, min_l, lower_l, upper_l = (
                                _analyze_data(np.asarray(ok_data_lot))
                            )
                            ng_l = len(lot_group) - ok_count

                            m1, m2, m3, m4 = st.columns(4)
                            m1.metric("件数", f"{len(lot_group):,}")
                            m2.metric("平均(g)", f"{mean_l:.3f}")
                            m3.metric("σ(g)", f"{std_l:.4f}")
                            m4.metric("不良率", f"{ng_l/len(lot_group)*100:.2f}%")

                            ch1, ch2 = st.columns(2)

                            with ch1:
                                fig_h = go.Figure()
                                fig_h.add_trace(go.Histogram(
                                    x=ok_data_lot, nbinsx=20,
                                    marker_color="steelblue", opacity=0.7,
                                ))
                                fig_h.add_vline(x=mean_l, line_color="red", line_width=2,
                                                annotation_text=f"平均: {mean_l:.2f}")
                                fig_h.add_vline(x=lower_l, line_color="orange", line_dash="dash",
                                                annotation_text=f"−3σ: {lower_l:.2f}")
                                fig_h.add_vline(x=upper_l, line_color="orange", line_dash="dash",
                                                annotation_text=f"+3σ: {upper_l:.2f}")
                                fig_h.update_layout(
                                    title=f"ロット{lot_num} ヒストグラム",
                                    xaxis_title="測定値(g)", yaxis_title="頻度",
                                    height=350, showlegend=False,
                                )
                                st.plotly_chart(fig_h, use_container_width=True)

                            with ch2:
                                df_ok_lot = lot_group[lot_group["ランクコード"] == "2"].copy()
                                omask = (
                                    (df_ok_lot["測定値(g)"] < lower_l) |
                                    (df_ok_lot["測定値(g)"] > upper_l)
                                )
                                has_dt = df_ok_lot["日付時刻"].notna().any()
                                x_axis = df_ok_lot["日付時刻"] if has_dt else np.arange(len(df_ok_lot))
                                fig_ts = go.Figure()
                                fig_ts.add_trace(go.Scatter(
                                    x=x_axis[~omask.values],
                                    y=df_ok_lot.loc[~omask, "測定値(g)"].values,
                                    mode="markers",
                                    marker=dict(color="steelblue", size=5),
                                    name="OK",
                                ))
                                if omask.any():
                                    fig_ts.add_trace(go.Scatter(
                                        x=x_axis[omask.values],
                                        y=df_ok_lot.loc[omask, "測定値(g)"].values,
                                        mode="markers",
                                        marker=dict(color="red", size=8, symbol="x"),
                                        name=f"外れ値 ({omask.sum()}件)",
                                    ))
                                fig_ts.add_hline(y=mean_l, line_color="red", line_width=1.5)
                                fig_ts.add_hline(y=upper_l, line_color="orange", line_dash="dash")
                                fig_ts.add_hline(y=lower_l, line_color="orange", line_dash="dash")
                                fig_ts.update_layout(
                                    title=f"ロット{lot_num} 時系列",
                                    xaxis_title="時刻" if has_dt else "測定順序",
                                    yaxis_title="測定値(g)",
                                    height=350,
                                )
                                st.plotly_chart(fig_ts, use_container_width=True)

                            # Excel ダウンロード
                            lot_date_min = lot_group["日付時刻"].dropna().min()
                            safe_label = (product_name or f"品種番号{h_num}").replace("/", "-").replace("\\", "-")
                            if pd.notna(lot_date_min):
                                ds = f"{lot_date_min.year}-{lot_date_min.month:02d}-{lot_date_min.day:02d}"
                                excel_fname = f"分析結果_{ds}製造_{safe_label}_ロット{lot_num}.xlsx"
                            else:
                                excel_fname = f"分析結果_{safe_label}_ロット{lot_num}.xlsx"

                            if st.button("📊 Excel を生成する", key=f"gen_{date_folder}_{lot_num}"):
                                with st.spinner("Excel 生成中（グラフ描画に数秒かかります）..."):
                                    excel_bytes = _lot_excel_bytes(
                                        lot_group, lot_num, h_num, product_name
                                    )
                                if excel_bytes:
                                    st.session_state["_lot_excel_bytes"] = excel_bytes
                                    st.session_state["_lot_excel_fname"] = excel_fname

                            if st.session_state.get("_lot_excel_fname") == excel_fname and \
                               st.session_state.get("_lot_excel_bytes"):
                                st.download_button(
                                    label="📥 Excel ダウンロード",
                                    data=st.session_state["_lot_excel_bytes"],
                                    file_name=excel_fname,
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    type="primary",
                                )

    # ─── タブ4: エクスポート ───
    with tab_export:
        st.subheader(f"{display_name} 詳細レポート Excel 出力")
        st.markdown(
            "**出力内容**\n"
            "- 統計結果（全期間サマリー）\n"
            "- 日別集計\n"
            "- ヒストグラム・時系列チャート・日別推移グラフ\n"
            "- 全 OKデータ（生データ）\n"
            "- 外れ値一覧"
        )

        if overall_stats is None:
            st.warning("統計計算できるデータがありません（OKデータが不足しています）")
        else:
            safe_name = display_name.replace("/", "-").replace("\\", "-")
            detail_fname = f"品種詳細_{safe_name}_{datetime.datetime.now():%Y%m%d_%H%M}.xlsx"

            if st.button("📊 Excel を生成する", key="gen_detail"):
                try:
                    with st.spinner("Excel 生成中（グラフ描画に数秒かかります）..."):
                        buf = io.BytesIO()
                        export_hinshoku_detail(
                            filepath=buf,
                            hinshoku_num=h_num,
                            aggregate_info=agg,
                            combined_df=combined_df,
                            daily_df=daily_df,
                            overall_stats=overall_stats,
                            product_name=product_name,
                        )
                        st.session_state["_detail_excel_bytes"] = buf.getvalue()
                        st.session_state["_detail_excel_fname"] = detail_fname
                    st.success("✓ Excel 生成完了")
                except Exception as e:
                    st.error(f"Excel 生成に失敗しました:\n\n```\n{traceback.format_exc()}\n```")

            if st.session_state.get("_detail_excel_bytes") and \
               st.session_state.get("_detail_excel_fname") == detail_fname:
                st.download_button(
                    label="📥 Excel ダウンロード",
                    data=st.session_state["_detail_excel_bytes"],
                    file_name=detail_fname,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                )
