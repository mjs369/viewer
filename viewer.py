import streamlit as st
import polars as pl
import io

st.set_page_config(page_title="Parquet ビューア", layout="wide")
st.title("Parquet ビューア")

# 表示状態を保持するためのセッションステート
if "show_data" not in st.session_state:
    st.session_state.show_data = False

# ファイルアップロード
uploaded_file = st.file_uploader("Parquetファイルをアップロード", type=["parquet"])

# OKボタンで表示状態をTrueに
if uploaded_file and st.button("表示する"):
    st.session_state.show_data = True

# 表示処理（セッションステートがTrueのときだけ）
if st.session_state.show_data and uploaded_file:
    with st.spinner("ファイルを読み込み中..."):
        df = pl.read_parquet(io.BytesIO(uploaded_file.read()))

    st.success("読み込み完了！")

    # カラム選択（絞り込み対象）
    st.subheader("🔍 絞り込み条件")
    col_to_filter = st.selectbox("カラムを選択して検索・フィルタ", df.columns)

    if df[col_to_filter].dtype == pl.Utf8:
        keyword = st.text_input("文字列で部分一致検索")
        if keyword:
            df = df.filter(pl.col(col_to_filter).str.contains(keyword, literal=True))
    elif df[col_to_filter].dtype.is_numeric():
        min_val = float(df[col_to_filter].min())
        max_val = float(df[col_to_filter].max())
        selected_range = st.slider("数値範囲で絞り込み", min_val, max_val, (min_val, max_val))
        df = df.filter(pl.col(col_to_filter).is_between(*selected_range))

    # 並び替え
    st.subheader("↕ 並び替え")
    sort_col = st.selectbox("並び替えカラム", df.columns, key="sort")
    ascending = st.radio("順序", ["昇順", "降順"]) == "昇順"
    df = df.sort(by=sort_col, descending=not ascending)

    # 表示
    st.subheader("📄 データプレビュー（最大1000行）")
    st.dataframe(df.head(1000).to_pandas())
