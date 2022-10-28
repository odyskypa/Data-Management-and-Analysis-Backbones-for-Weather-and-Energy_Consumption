from paths import dataBasesDir
import glob
import duckdb

def diagnosis():       
    for database_path in glob.iglob(dataBasesDir + '**/*.duckdb', recursive=True):
        print(f"\n Printing information for the {database_path} database: \n")
        con = duckdb.connect(database=database_path, read_only=False)
        x = con.execute("DESCRIBE;")
        print(x.description)

        