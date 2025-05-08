import streamlit as st
import polars as pl
import io

st.title("Parquet ビューア")

# アップロード用ウィジェット
uploaded_file = st.file_uploader("Parquetファイルをアップロード", type=["parquet"])

# 表示トリガーボタン
if uploaded_file and st.button("表示する"):
    # 読み込み
    with st.spinner("読み込み中..."):
        bytes_data = uploaded_file.read()
        df = pl.read_parquet(io.BytesIO(bytes_data))

    st.success("読み込み完了！")

    # カラム選択（絞り込み対象）
    st.subheader("絞り込み条件")
    col_to_filter = st.selectbox("絞り込みたいカラムを選択", df.columns)

    if df[col_to_filter].dtype == pl.Utf8:
        keyword = st.text_input("部分一致で検索")
        if keyword:
            df = df.filter(pl.col(col_to_filter).str.contains(keyword, literal=True))
    elif df[col_to_filter].dtype.is_numeric():
        min_val = df[col_to_filter].min()
        max_val = df[col_to_filter].max()
        selected_range = st.slider("数値範囲でフィルタ", float(min_val), float(max_val), (float(min_val), float(max_val)))
        df = df.filter(pl.col(col_to_filter).is_between(*selected_range))

    # 並び替え機能
    st.subheader("並び替え")
    sort_col = st.selectbox("並び替え対象カラム", df.columns, key="sort")
    ascending = st.radio("昇順 / 降順", ["昇順", "降順"]) == "昇順"
    df = df.sort(by=sort_col, descending=not ascending)

    # 表示
    st.subheader("データ表示（最大1000件）")
    st.dataframe(df.head(1000).to_pandas())
