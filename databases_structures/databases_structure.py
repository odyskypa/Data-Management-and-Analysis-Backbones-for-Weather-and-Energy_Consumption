from paths import dataBasesDir
from utilities.db_utilities import getListOfTables
import glob
import duckdb

def diagnosis():       
    for database_path in glob.iglob(dataBasesDir + '**/*.duckdb', recursive=True):
        print(f"\n Printing information for the {database_path} database: \n")
        con = duckdb.connect(database=database_path, read_only=False)
        tables = getListOfTables(con)
        print(tables)
        for table in tables:
            x = con.execute(f"DESCRIBE {table};")
            print(x.description)

        