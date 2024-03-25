import streamlit as st
import duckdb
import polars as pl
import numpy as np
import time
import os

rows = st.slider("rows",min_value=1,max_value=1_000_000,value=1_000_000)
columns = st.slider("columns",min_value=1,max_value=3000,value=300)

if st.button("Generate", type="primary"):
    for file in os.listdir("."):
        if "example" in file:
            os.remove(file)

    start_time = time.time()

    num_rows = rows
    columns = {"col_{}".format(i): np.random.rand(num_rows) for i in range(columns)}
    df = pl.DataFrame(columns)
    # st.table(df)
    conn = duckdb.connect('example.duckdb')

    table_name = "data_from_polars"
    conn.register('temp_df', df)
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
    conn.close()
    
    st.write(f"DataFrame written to table '{table_name}' in DuckDB database. ({time.time() - start_time})")
