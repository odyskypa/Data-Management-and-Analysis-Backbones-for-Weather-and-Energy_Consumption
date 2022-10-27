import duckdb

def getDataframeFrom_trusted(DataBasesDir, data_source_name):
    try:
        con = duckdb.connect(database=f'{DataBasesDir}{data_source_name}_trusted.duckdb', read_only=False)
        df = con.execute(f'SELECT * FROM {data_source_name}').fetchdf()
        con.close()
        return df
    except Exception as e:
        print(e)
        con.close()