import streamlit as st
import duckdb
import polars as pl
import numpy as np
import time
import os
st.markdown('<p style="text-align: center; color: gray; margin-bottom: 50px;">Generate table</p>', unsafe_allow_html=True)
rows = st.slider("rows",min_value=1,max_value=1_000_000,value=1_000_000)
columns = st.slider("columns",min_value=1,max_value=500,value=300)

if st.button("Generate", type="primary"):
    for file in os.listdir("."):
        if "example" in file:
            os.remove(file)

    start_time = time.time()

    num_rows = rows
    columns = {"col_{}".format(i): np.random.rand(num_rows) for i in range(columns)}
    df = pl.DataFrame(columns)

    conn = duckdb.connect('example.duckdb')
    table_name = "data_from_polars"
    conn.register('temp_df', df)
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
    conn.close()
    
    st.write(f"DataFrame written to table '{table_name}' in DuckDB database. ({time.time() - start_time})")
    written = False


st.markdown('---')
st.markdown('<p style="text-align: center; color: gray; margin-bottom: 50px;">Preview table</p>', unsafe_allow_html=True)


if st.button("Preview table", type="primary"):
    found = False
    for file in os.listdir("."):
        if "example" in file:
            found = True
    if found:
        start_time = time.time()

        con = duckdb.connect('example.duckdb')
        query_result = con.execute("SELECT * FROM data_from_polars").fetchdf()
        polars_df = pl.DataFrame(query_result)
        # sliced_df = polars_df.slice(0, 10).select(pl.all().head(10))
        # st.write("little sample")
        # st.table(sliced_df)
        st.write(f"Time taken to fetch from database and read ({time.time() - start_time})")
    else:
        st.write("No database found bro")
