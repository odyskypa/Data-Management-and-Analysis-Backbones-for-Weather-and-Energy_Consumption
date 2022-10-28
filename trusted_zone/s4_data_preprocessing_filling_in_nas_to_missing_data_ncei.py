# Recognising missing values - NCEI
## Replacing extreme values (e.g.: 9999.9) which correspond to missing data with NAs
### Saving table with NAs in the trusted database 
#### Actions took place following conventions stated at the data sources web pages

import duckdb
import pandas as pd
import numpy as np
from paths import trustedDataBasesDir

def changingExtremeValuesOfNCEIwithNAs():
    # Filling in extreme values like 9999.99 with NAs, base on information from data source's metadata
    # This script refers only to NCEI data, WEB data do not need any NA filling
    data_source_name = "NCEI"     
    print(data_source_name)
    try:
        con = duckdb.connect(database=f'{trustedDataBasesDir}{data_source_name}_trusted_with_extreme_values.duckdb', read_only=False)
        
        
        df = con.execute(f'SELECT * FROM {data_source_name}').fetchdf()
        print(f"\n{data_source_name} Dataframe description before filling in NAs in extreme values\n")
        print(df.describe())
        con.close()
        
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        numeric_df = df.select_dtypes(include=numerics)
        string_df = df.select_dtypes(include='object')

        con1 = duckdb.connect(database=f'{trustedDataBasesDir}{data_source_name}_trusted.duckdb', read_only=False)
        
        numeric_NA_values = [99.99, 9999.9, 999.9, 999., 999.9]
        numeric_df[numeric_df.columns[1:-1]] = numeric_df[numeric_df.columns[1:-1]].applymap(lambda x: np.nan if x > 900.0 else x)
        
        for value in numeric_NA_values:
            numeric_df = numeric_df.replace({value: np.nan})
        for col in string_df.columns:
            if col in ["MAX_ATTRIBUTES", "MIN_ATTRIBUTES"]:
                string_df[col].values[string_df[col].values == " "] = "explicit_temp_report"
            if col in ["PRCP_ATTRIBUTES"]:
                string_df[col].values[string_df[col].values == " "] = np.nan
        
        NCEI_df = pd.concat([string_df, numeric_df], axis=1)
        NCEI_df['STATION'] = NCEI_df.STATION.astype('category')
        NCEI_df['FRSHTT'] = NCEI_df.FRSHTT.astype('category')
        print(f"\n{data_source_name} Dataframe description after filling in NAs in extreme values\n")
        print(NCEI_df.describe())
        
        table = data_source_name
        con1.execute(f'DROP TABLE IF EXISTS {table}')
        con1.execute(f'CREATE TABLE {table} AS SELECT * FROM NCEI_df')
        con1.close()

    except Exception as e:
        print(e)
        con.close()
        con1.close()


def main():
    changingExtremeValuesOfNCEIwithNAs()
    
if __name__ == "__main__":
    main()