#  Trusted zone creation
## Processing data tables from the formatted zone and moving them to the trusted zone
### This script automatically creates the trusted zone databases
import os
import duckdb
import pandas as pd
import numpy as np
from functools import reduce
from utilities.db_utilities import getListOfTables
from utilities.os_utilities import createDirectory
from paths import formattedDataBasesDir, trustedDataBasesDir

def loadDataFromFormattedToTrustedDatabase():
    """
    Loading data of different data sources into the trusted zone database. 
    Handling of versions of data tables and homogenizing their schema and data for every data source.
    In order to load more data for other data sources, this function must be updated by adding a case for each data source.
    
    @param:
    @Output:
    
    """

def first_step_version_handling_WEB():
    # WEB merge by year
    """# Version handling for WEB
    ## 1st step of version handling
    ### Merging tables based on their year
    #### In the formatted database table names are as follows:
    #### WEB_2021_ts1, WEB_2021_ts2, WEB_2022_ts1, WEB_20222_ts2
    """
    try:
        # Establishing connection to the formatted database of the data source and the merged_by_year database, which is the result database
        con = duckdb.connect(database=f'{formattedDataBasesDir}WEB_formatted.duckdb', read_only=False)
        con1 = duckdb.connect(database=f'{formattedDataBasesDir}WEB_merged_by_year.duckdb', read_only=False)

        list_of_tables = getListOfTables(con)

        # First step for WEB data is to merge all tables from the same year
        
        tables_year_list = [item.split('_')[1] for item in list_of_tables] # For every table we save their year

        unique_year_values = set(tables_year_list) # Getting the unique year values

        # Creating a dictionary that maps the tables (based on their index inside the list_of_tables) to year
        map_of_tables_and_years = {} # Initialization of the dictionary
        for year in unique_year_values:
            index_list = [] # Initializing the index list, which will save the indexes of each year
            for c,table in enumerate(list_of_tables):
                if table.split('_')[1] == year:
                    index_list.append(c)
            map_of_tables_and_years[year] = index_list # In this dictionary now, the indexes of the tables for each year are grouped
        print("\nThe following dictionary maps table indexes from list_of_tables and years: \n")
        print(map_of_tables_and_years)

        # Values of the dictionary, contain the indexes of tables from the list_of_tables that have the same year in their title (key)
        for key, value in map_of_tables_and_years.items(): 
            list_of_dataframes_from_the_same_year =[]
            for index in value:
                table_name = list_of_tables[index] # Getting the table name from the list_of_tables base on the index we have
                df = con.execute(f'SELECT * FROM {table_name}').fetchdf() # Read the specific table from the formatted database
                list_of_dataframes_from_the_same_year.append(df) # Add the df of the table in the list_of_dataframes_from_the_same_year
            if len(list_of_dataframes_from_the_same_year) > 1:
                # Merge all dataframes from the same year, they will always have the same column names
                df = reduce(lambda df1,df2: pd.merge(df1,df2), list_of_dataframes_from_the_same_year)
            else:
                df = list_of_dataframes_from_the_same_year[0]
            
            # Drop duplicates if any
            df.drop_duplicates()
            merged_by_year_table = "WEB_" + key
            
            # Save table per year in the merged_by_year_database
            con1.execute(f'DROP TABLE IF EXISTS {merged_by_year_table}')
            con1.execute(f'CREATE TABLE {merged_by_year_table} AS SELECT * FROM df')

        con.close()
        con1.close()
    except:
        con.close()
        con1.close()

def second_step_version_handling_WEB():
    """## 2nd step of version handling
    ### Mergining all the tables from different years
    #### In the merged_by_year database table names are as follows:
    #### WEB_2021, WEB_2022
    """

    # Web merge all years into one single table (trusted table)
    try:
        # Establishing connection to the formatted database of the data source
        con = duckdb.connect(database=f'{formattedDataBasesDir}WEB_merged_by_year.duckdb', read_only=False)
        con1 = duckdb.connect(database=f'{formattedDataBasesDir}WEB_merged_final.duckdb', read_only=False)

        list_of_tables = getListOfTables(con)

        list_of_dfs =[]
        for table in list_of_tables:
            df = con.execute(f'SELECT * FROM {table}').fetchdf()
            list_of_dfs.append(df)
        df = reduce(lambda df1,df2: pd.merge(df1,df2, on=["Country", "Product", "Flow"], suffixes=(f"_{str(int(float(df1.columns[-2])) + 2)}",f"_{str(int(float(df2.columns[-2])) + 2)}")), list_of_dfs) 
        df.drop_duplicates()
        merged_final_table = "WEB"
        con1.execute(f'DROP TABLE IF EXISTS {merged_final_table}')
        con1.execute(f'CREATE TABLE {merged_final_table} AS SELECT * FROM df')

        con.close()
        con1.close()
    except Exception as e:
        print(e)
        con.close()
        con1.close()

def third_step_version_handling_WEB():
    """## 3rd step of version handling
    ### Removing redundant columns from the WEB merged_final database
    ### Calculating the mean of some measures coming from different versions of the data
    #### In the merged_final database table name is as follows:
    #### WEB

    """

    try:
        con = duckdb.connect(database=f'{formattedDataBasesDir}WEB_merged_final.duckdb', read_only=False)
        con1 = duckdb.connect(database=f'{trustedDataBasesDir}WEB_trusted.duckdb', read_only=False)

        table = "WEB"
        df = con.execute(f'SELECT * FROM {table}').fetchdf() # Read the data source table from the merged_final database
        cols = df.columns # Get the names of the columns
        
        double_columns =[] # List containing columns that exist more than one time, from different versions of data source
        for col in cols:
            if "No" in col: # Droping NoCountry, NoProduct, NoFlow columns, redundant information, creating a lot of problems
                df.drop(columns=col,inplace=True)
                continue
            if "_" in col: # Checking which column names contain symbol "_", this mean that the columns exist two times from different versions of data source
                name_of_double_col_without_year = col.split("_")[0] # Keeping the name of the duplicate columns
                if name_of_double_col_without_year not in double_columns:
                    double_columns.append(name_of_double_col_without_year)

        double_cols_idx = {} # Dictionary containing the indexes where dublicate columns are located
        for double_col in double_columns:
            double_col_list =[] # The key of the dictionary is the general column name e.g.: 1971, and the values are the column names that need to be merged e.g. 1971_2020, 1971_2021
            for col in cols:
                if double_col in col.split("_")[0]:
                    double_col_list.append(col)
            double_cols_idx[double_col] = double_col_list

        print("\nThe following dictionary maps the index of columns that contain data from the same year but different data versions: \n")
        print(double_cols_idx)
        
        for key,values in double_cols_idx.items(): # For every duplicate column calculate its mean, save it to the df in a new column and drop the old ones.
            df[key] = df[values].mean(axis=1)
            for value in values:
                df.drop(columns=value, inplace=True)
        
        con1.execute(f'DROP TABLE IF EXISTS {table}')
        con1.execute(f'CREATE TABLE {table} AS SELECT * FROM df') # Save the table in the trusted database
        
        con.close()
        con1.close()

    except Exception as e:
        print(e)
        con.close()
        con1.close()

def first_step_version_handling_NCEI():
    """# Version handling for NCEI
    ## 1st step of version handling - NCEI
    ### Merging tables based on same year and stationID
    #### In the formatted database table names are as follows:
    #### NCEI_year_weatherStationID_ts1, NCEI_year_weatherStationID_ts2, NCEI_year_weatherStationID_ts3, etc..
    #### Formatted database contain tables with several years, weather stations and timestamps
    """

    # NCEI merge on same year and station ID - 1st Step
    try:
        # Establishing connection to the formatted database of the data source
        con = duckdb.connect(database=f'{formattedDataBasesDir}NCEI_formatted.duckdb', read_only=False)
        con1 = duckdb.connect(database=f'{formattedDataBasesDir}NCEI_merged_by_year_and_station_ID.duckdb', read_only=False)

        list_of_tables = getListOfTables(con)

        # FOR NCEI
        # First step for WEB data is to merge all tables from the same year
        # Second step will be to merge all tables from different years and end up with a single table for the WEB data source

        tables_year_list = [item.split('_')[1] for item in list_of_tables] # For every table we save their year
        station_ID_list = [item.split('_')[2] for item in list_of_tables] # For every table we save the stationID

        unique_year_values = set(tables_year_list) # Getting the unique year values
        unique_station_ID_values = set(station_ID_list) # Getting the unique stationID values

        # Creating a dictionary that maps the tables (based on their index inside the list_of_tables) to year
        map_of_tables_and_years_station_ID = {} # Initialization of the dictionary
        for year in unique_year_values:
            for station_ID in unique_station_ID_values:
                index_list = [] # Initializing the index list, which will save the indexes of each year
                for c,table in enumerate(list_of_tables):
                    table_year = table.split('_')[1]
                    table_station_ID = table.split('_')[2]
                    if table_year == year and table_station_ID == station_ID:
                        index_list.append(c)
                map_of_tables_and_years_station_ID[year + '_' + station_ID] = index_list # In this dictionary now, the indexes of the tables for each year are grouped
        print("\nThe following dictionary maps table indexes from list_of_tables and years: \n")
        print(map_of_tables_and_years_station_ID)


        for key, value in map_of_tables_and_years_station_ID.items(): 
            list_of_dataframes_from_the_same_year_and_station_ID =[]
            for index in value:
                table_name = list_of_tables[index] # Getting the table name from the list_of_tables base on the index we have
                df = con.execute(f'SELECT * FROM {table_name}').fetchdf() # Read the specific table from the formatted database
                list_of_dataframes_from_the_same_year_and_station_ID.append(df) # Add the df of the table in the list_of_dataframes_from_the_same_year
                # Merge all dataframes from the same year, they will always have the same column names
                if len(list_of_dataframes_from_the_same_year_and_station_ID) > 1:
                    df = reduce(lambda df1,df2: pd.merge(df1,df2), list_of_dataframes_from_the_same_year_and_station_ID) # , on=["STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME"]
                    # Drop duplicates if any
                else:
                    df = list_of_dataframes_from_the_same_year_and_station_ID[0]

                df.drop_duplicates()
                merged_by_year_and_station_ID_table = "NCEI_" + key
                # Save table per year in the merged_by_year_database
                con1.execute(f'DROP TABLE IF EXISTS {merged_by_year_and_station_ID_table}')
                con1.execute(f'CREATE TABLE {merged_by_year_and_station_ID_table} AS SELECT * FROM df')


        con.close()
        con1.close()
    except:
        con.close()
        con1.close()

def second_step_version_handling_NCEI():
    """## 2nd step of version handling - NCEI
    ### Merging tables based on differen years but same station_ID
    #### In the merged_by_year_and_station_ID database table names are as follows:
    #### NCEI_year_weatherStationID
    """

    # NCEI merge on same station ID - 2nd Step
    try: 
        # Establishing connection to the formatted database of the data source
        con = duckdb.connect(database=f'{formattedDataBasesDir}NCEI_merged_by_year_and_station_ID.duckdb', read_only=False)
        con1 = duckdb.connect(database=f'{formattedDataBasesDir}NCEI_merged_final.duckdb', read_only=False)

        list_of_tables = getListOfTables(con)

        # FOR NCEI
        # First step for WEB data is to merge all tables from the same year
        # Second step will be to merge all tables from different years and end up with a single table for the WEB data source

        tables_year_list = [item.split('_')[1] for item in list_of_tables] # For every table we save their year
        station_ID_list = [item.split('_')[2] for item in list_of_tables] # For every table we save the stationID

        unique_year_values = set(tables_year_list) # Getting the unique year values
        unique_station_ID_values = set(station_ID_list) # Getting the unique stationID values

        # Creating a dictionary that maps the tables (based on their index inside the list_of_tables) to station IDs
        map_of_tables_and_station_IDs = {} # Initialization of the dictionary
        for station_ID in unique_station_ID_values:
            index_list = [] # Initializing the index list, which will save the indexes of each year
            for c,table in enumerate(list_of_tables):
                if table.split('_')[2] == station_ID:
                    index_list.append(c)
            map_of_tables_and_station_IDs[station_ID] = index_list # In this dictionary now, the indexes of the tables for each station ID are grouped
        print("\nThe following dictionary maps table indexes from list_of_tables and station IDs: \n")
        print(map_of_tables_and_station_IDs)

        # Values of the dictionary, contain the indexes of tables from the list_of_tables that have the same station ID in their title (key)
        for key, value in map_of_tables_and_station_IDs.items(): 
            list_of_dataframes_from_the_same_station_ID =[]
            for index in value:
                table_name = list_of_tables[index] # Getting the table name from the list_of_tables base on the index we have
                df = con.execute(f'SELECT * FROM {table_name}').fetchdf() # Read the specific table from the formatted database
                list_of_dataframes_from_the_same_station_ID.append(df) # Add the df of the table in the list_of_dataframes_from_the_same_weatherID
            # Merge all dataframes by rows from the same weather station
            df = reduce(lambda df1,df2: pd.concat([df1, df2], ignore_index=True, sort=False), list_of_dataframes_from_the_same_station_ID)
            # Drop duplicates if any
            df.drop_duplicates()
            merged_by_station_ID_table = "NCEI_" + key
            # Save table per year in the merged_by_year_database
            con1.execute(f'DROP TABLE IF EXISTS {merged_by_station_ID_table}')
            con1.execute(f'CREATE TABLE {merged_by_station_ID_table} AS SELECT * FROM df')

        con.close()
        con1.close()
    except:
        con.close()
        con1.close()

def third_step_version_handling_NCEI():
    """## 3rd step of version handling - NCEI
    ### Merging all tables into a single table
    #### In the NCEI_merged_final database table names are as follows:
    #### NCEI_weatherStationID
    """

    # NCEI merge all stations into one single table by rows (trusted table)
    try:
        # Establishing connection to the formatted database of the data source
        con = duckdb.connect(database=f'{formattedDataBasesDir}NCEI_merged_final.duckdb', read_only=False)
        con1 = duckdb.connect(database=f'{trustedDataBasesDir}NCEI_trusted_with_extreme_values.duckdb', read_only=False)

        list_of_tables = getListOfTables(con)
        
        # Getting all tables of all stations as dataframes and appending them to a list
        list_of_dfs =[]
        for table in list_of_tables:
            df = con.execute(f'SELECT * FROM {table}').fetchdf()
            list_of_dfs.append(df)
        
        # Merging by row all the data from all stations
        df = reduce(lambda df1,df2: pd.concat([df1, df2], ignore_index=True, sort=False), list_of_dfs)
        df.drop_duplicates() # drop duplicates if any
        trusted_table = "NCEI"
        con1.execute(f'DROP TABLE IF EXISTS {trusted_table}')
        con1.execute(f'CREATE TABLE {trusted_table} AS SELECT * FROM df')

        con.close()
        con1.close()
    except:
        con.close()
        con1.close()

def main():
    
    # Creating trusted database directory
    createDirectory(trustedDataBasesDir)
    first_step_version_handling_WEB() # Generate WEB_merged_by_year.duckdb in the formatted zone
    second_step_version_handling_WEB() # Generate WEB_merged_final.duckdb in the formatted zone
    third_step_version_handling_WEB() # Generate WEB_trusted.duckdb in the trusted zone
    first_step_version_handling_NCEI() # Generate NCEI_merged_by_year_and_station_ID.duckdb in the formatted zone
    second_step_version_handling_NCEI() # Generate NCEI_merged_final.duckdb in the formatted zone
    third_step_version_handling_NCEI() # Generate NCEI_trusted_with_extreme_values.duckdb in the trusted zone

if __name__ == "__main__":
    main()
