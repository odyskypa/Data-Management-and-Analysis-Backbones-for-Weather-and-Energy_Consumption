# Moving data sources from the persistent zone to the formated zone
### This script automatically creates the formatted zone database containing one table per data source version
import duckdb
import pandas as pd
import sqlalchemy
import os
import tarfile
import numpy as np
from paths import temporalPath, persistentPath, dataBasesDir, formattedDataBasesDir

def getDataSourcesNames(temporalPath):
  """
      Getting a list with all the names of the data sources saved in the landing zone inside the temporal folder.
      Inside temporal folder there is one folder for landing each data source and their versions.

      @param:
        -  temporalPath: the absolute path of the temporal folder
      @Output:
        - data_sources_names: a list with all the names of the different datasources
  """
  for _, dirs, _ in os.walk(temporalPath):
    if len(dirs) > 0:
      data_sources_names = dirs
  return data_sources_names

def loadDataFromPersistentToFormattedDatabase (persistentPath, data_source_name, countries_abbreviations):
    """
    Loading data of different data sources into the formatted zone database. 
    Creating one table per data source version from the persistent zone.
    In order to load more data for other data sources, this function must be updated by adding a case for each data source.

    @TODO: THIS FUNCTION NEEDS TO BE MORE SIMPLE FOR REPRODUCABILITY
  
    @param:
        -  persistentPath: the absolute path of the persistent zone folder
        -  data_source_name: the name of the specific data source
        -  countries_abbreviations: a list of abbreviations of the countries names for which we want to mine data from NCEI datasource. For WEB datasource we will always get the data for all countries
    @Output:
  
    """
    try:
        # Establishing connection to the formatted database of the data source
        con = duckdb.connect(database=f'{formattedDataBasesDir}{data_source_name}_formatted.duckdb', read_only=False)

        # Loop through the folders of variable persistentPath, in this case persistent zone
        for root, dirs, files in os.walk(persistentPath):
            for dir in dirs: # For every dir in persistent zone
                if dir.startswith(data_source_name): # Check if the dir contain NCEI or WEB data
                    for root1,_,files1 in os.walk(persistentPath + dir): # Walking inside the dir of NCEI or WEB
                        for file in files1: # For every file inside dir of NCEI or WEB
                            if not file.endswith('.ini'): # Dont take into account .ini files
                                if data_source_name == "WEB": # Handling cases of WEB (xlsx)
                                    file_path = root1 + '/' + file # Absolute path of the xlsx file for WEB data
                                    year_of_file = file.split('_')[0][-4:] # Getting the version year of the file, they change the sheet name containing the information based on it.
                                    year_of_timeseries = str(int(year_of_file) - 1) # Example: If file is from 2021, then timeseries title is TimeSeries 1971-2020
                                    timestamp_of_file = file.split('_')[-1].split('.xlsx')[0]
                                    sheet = "TimeSeries_1971-" + year_of_timeseries
                                    df = pd.read_excel(file_path, sheet_name= sheet, header = 1) # WEB reading data from xlsx to df
                                    cols = df.columns[6:]
                                    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
                                    table_name = data_source_name + '_' + year_of_file + '_' + timestamp_of_file # table name for database, format: WEB_year_of_file_timestampOfFile
                                    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df") # Saving the datasource's dataframe as a table in DuckDB
                                    print(f"{table_name} created in the {data_source_name}_formatted.duckdb in the formatted zone")
                                elif data_source_name == "NCEI": # Handlind cases of NCEI (tar.gzip contaning csv files)
                                    tar_file_path = os.path.join(os.path.abspath(root1), file) # Absolute path of the tar.gz file
                                    tar = tarfile.open(tar_file_path) # Opening tar.gz file
                                    for member in tar.getmembers(): # For every file inside tar.gz, which is a csv
                                        f = tar.extractfile(member) # Extract the csv file
                                        df = pd.read_csv(f) # Read the csv file
                                        for country_abbreviation in countries_abbreviations: # For every country we want to get data
                                            country_abb = str(df["NAME"].iloc[0])[-2:] # Find the abbreviation of the countries that this tar contain information about, it can be found from the last two string elements of the "NAME" column
                                            if country_abb == country_abbreviation:
                                                weather_station_ID = str(df["STATION"].iloc[0])
                                                year_of_file = file.split('_')[0]
                                                timestamp_of_file = file.split('_')[-1].split('.')[0]
                                                table_name = data_source_name + '_' + year_of_file + '_' + weather_station_ID + '_' + timestamp_of_file # table name for database, format: NCEI_StationID_timestampOfDatasourceVersion
                                                con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df") # Saving the datasource's dataframe as a table in DuckDB
                                                print(f"{table_name} created in the {data_source_name}_formatted.duckdb")
        con.close() # Closing connection to the formatted database

    except Exception as e:
        print(e)
        con.close()

def main ():
    """# Main code of the script
    ## Getting the names of all the data sources
    ### Creating a table in the appropriate formatted database for every data source version
    """

    """# Creating the folder for saving the databases
    ## And the folder for formatted database as well
    """

    # Checking if database directory exists, if not it is being created
    if not os.path.exists(dataBasesDir):
        os.mkdir(dataBasesDir)
    if not os.path.exists(formattedDataBasesDir):
        os.mkdir(formattedDataBasesDir)

     # Getting the names of the different data sources
    data_sources_names = getDataSourcesNames(temporalPath)

    # Creating a list with the abbreviations of countries for which we want to mine data from the NCEI dataset
    countries_abbreviations = ["BE"] # We will start only with Belgium (BE), by adding new countries abbreviations here data for those countries will be saved in the formatted database as well

    # Creating one table in the appropriate formatted database for each data source version 
    for data_source_name in data_sources_names:    
        loadDataFromPersistentToFormattedDatabase(persistentPath, data_source_name, countries_abbreviations)

if __name__ == "__main__":
    main()
