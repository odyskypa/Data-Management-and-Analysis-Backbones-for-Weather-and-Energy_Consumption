import os
import duckdb
import pandas as pd
import numpy as np
import seaborn as sns
from pylab import savefig
from matplotlib import pyplot as plt
from paths import temporalPath, trustedDataBasesDir, profilingDir, profilingPlotsDir

def getDataSourcesNames(temporalPath):
    """
            Getting a list with all the names of the data sources saved in the landing zone inside the temporal folder.
            Inside temporal folder there is one folder for landing each data source and their versions.

            @param:
                -    temporalPath: the absolute path of the temporal folder
            @Output:
                - data_sources_names: a list with all the names of the different datasources
    """
    for _, dirs, _ in os.walk(temporalPath):
        if len(dirs) > 0:
            data_sources_names = dirs
    return data_sources_names

def main():
    """# Data Profiling
    ## For every data source a profile of each variable is generated 
    ### In this way one can gain insights on the quality of the data 
    """

    # Getting the names of the different data sources
    data_sources_names = getDataSourcesNames(temporalPath)


    # Creating profile for each data source in reports folder 
    for data_source_name in data_sources_names:
        try:
            print(f"Profile generation for {data_source_name} data source")
            con = duckdb.connect(database=f'{trustedDataBasesDir}{data_source_name}_trusted.duckdb', read_only=False)
            df = con.execute(f'SELECT * FROM {data_source_name}').fetchdf()

            ##### YOU ARE HERE
            con.close()
        except Exception as e:
            print(e)
            con.close()

if __name__ == "__main__":
    main() #TODO NEEDS FIXING, TOO SLOW, NOT PLOTING EVENTUALLY