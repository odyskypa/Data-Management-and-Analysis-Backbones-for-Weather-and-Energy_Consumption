import duckdb
from paths import trustedDataBasesDir, exploitationDatabasesDir

def getDataframeFrom_trusted(data_source_name, trustedDataBasesDir = trustedDataBasesDir):
    """
      Getting the dataframe of a data source from the trusted database.

      @param
        -   data_source_name: the name of the data source
        -   trustedDataBasesDir: the absolute path of the directory where the databases are saved
      @Output:
        -   df: the dataframe of the data source from the trusted database
    """
    try:
        con = duckdb.connect(database=f'{trustedDataBasesDir}{data_source_name}_trusted.duckdb', read_only=False)
        df = con.execute(f'SELECT * FROM {data_source_name}').fetchdf()
        con.close()
        return df
    except Exception as e:
        print(e)
        con.close()

def getDataframeFrom_trusted_noNAs(data_source_name, trustedDataBasesDir = trustedDataBasesDir):
    """
      Getting the dataframe of a data source from the trusted_noNAs database.

      @param
        -   data_source_name: the name of the data source
        -   trustedDataBasesDir: the absolute path of the directory where the databases are saved
      @Output:
        -   df: the dataframe of the data source from the trusted_noNAs database
    """
    try:
        con = duckdb.connect(database=f'{trustedDataBasesDir}{data_source_name}_trusted_noNAs.duckdb', read_only=False)
        df = con.execute(f'SELECT * FROM {data_source_name}').fetchdf()
        con.close()
        return df
    except Exception as e:
        print(e)
        con.close()

def getListOfTables(con):
    """
      Return a list containing all the tables of a database from its established connection.

      @param
        -   con: the established connection with a database
      @Output:
        -   list_of_tables: a list containing the names of all tables of a database
    """

    # Initialization of the list which will contain the names of the tables
    list_of_tables =[]

    # Loading a list with the names of all the tables from the formatted zone database
    con.execute("SHOW TABLES;")
    list_of_tuples_of_tables = con.fetchall() # The result of this command is a list of tuples

    # Looping through the list of tuples and extracting table names into list_of_tables variable
    for tuple_of_table in list_of_tuples_of_tables:
            for table in tuple_of_table:
                    list_of_tables.append(table)
    return list_of_tables

def saveDataframeTo_trusted_noNAs(df, data_source_name, trustedDataBasesDir = trustedDataBasesDir):
    """
      Creates a table in the trusted_noNAs database from the input dataframe (df) with name = data_source_name.

      @param
        -   df: the dataframe to be saved as a table in the database
        -   data_source_name: the name of the data source
        -   trustedDataBasesDir: the absolute path of the directory where the databases are saved
      @Output:
    """
    try:
        con = duckdb.connect(database=f'{trustedDataBasesDir}{data_source_name}_trusted_noNAs.duckdb', read_only=False)
        con.execute(f'DROP TABLE IF EXISTS {data_source_name}')
        df = df
        con.execute(f'CREATE TABLE {data_source_name} AS SELECT * FROM df')
        con.close()
    except Exception as e:
        print(e)
        con.close()

def saveDataframeTo_trusted_outliers(df, data_source_name, trustedDataBasesDir = trustedDataBasesDir):
    """
      Creates a table in the trusted_outliers database from the input dataframe (df) with name = data_source_name.

      @param
        -   df: the outliers dataframe to be saved as a table in the database
        -   data_source_name: the name of the data source
        -   trustedDataBasesDir: the absolute path of the directory where the databases are saved
      @Output:
    """
    try:
        con = duckdb.connect(database=f'{trustedDataBasesDir}{data_source_name}_trusted_outliers.duckdb', read_only=False)
        con.execute(f'DROP TABLE IF EXISTS {data_source_name}')
        df = df
        con.execute(f'CREATE TABLE {data_source_name} AS SELECT * FROM df')
        con.close()
    except Exception as e:
        print(e)
        con.close()

def saveDataframeTo_exploitation_year_and_country(df, data_source_name, exploitationDatabasesDir = exploitationDatabasesDir):
    """
      Creates a table in the _exploitation_year_and_country database from the input dataframe (df) with name = data_source_name.

      @param
        -   df: the dataframe to be saved as a table in the database
        -   data_source_name: the name of the data source
        -   exploitationDatabasesDir: the absolute path of the directory where the databases are saved
      @Output:
    """
    try:
        con = duckdb.connect(database=f'{exploitationDatabasesDir}{data_source_name}_exploitation_year_and_country.duckdb', read_only=False)
        con.execute(f'DROP TABLE IF EXISTS {data_source_name}')
        df = df
        con.execute(f'CREATE TABLE {data_source_name} AS SELECT * FROM df')
        con.close()
    except Exception as e:
        print(e)
        con.close()

def saveDataframeTo_exploitation(df, exploitationDatabasesDir = exploitationDatabasesDir):
    """
      Creates a table in the _exploitation database from the input dataframe (df) with name = VIEW.

      @param
        -   df: the dataframe to be saved as a table in the database
        -   exploitationDatabasesDir: the absolute path of the directory where the databases are saved
      @Output:
    """
    try:
        con = duckdb.connect(database=f'{exploitationDatabasesDir}exploitation.duckdb', read_only=False)
        table = "VIEW"
        con.execute(f'DROP TABLE IF EXISTS {table}')
        df = df
        con.execute(f'CREATE TABLE {table} AS SELECT * FROM df')
        con.close()

    except Exception as e:
        print(e)
        con.close()