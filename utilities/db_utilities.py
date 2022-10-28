import duckdb

def getDataframeFrom_trusted(DataBasesDir, data_source_name):
    """
      Getting the dataframe of a data source from the trusted database.

      @param
        -   DataBasesDir: the absolute path of the directory where the databases are saved
        -   data_source_name: the name of the data source
      @Output:
        -   df: the dataframe of the data source from the trusted database
    """
    try:
        con = duckdb.connect(database=f'{DataBasesDir}{data_source_name}_trusted.duckdb', read_only=False)
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