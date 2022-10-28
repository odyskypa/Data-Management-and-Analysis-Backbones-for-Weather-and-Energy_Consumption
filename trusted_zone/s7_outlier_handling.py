import duckdb
import pandas as pd
import numpy as np
import seaborn as sns
from pylab import savefig
from matplotlib import pyplot as plt
from utilities.os_utilities import getDataSourcesNames, createDirectory
from utilities.db_utilities import getDataframeFrom_trusted
from paths import temporalPath, outliersDir, outliersPlotsDir

def outlier_detection(df):
    ############ FIX THATTTTTTTTTTTTT
    print('yoyo')

def main():
    """
    # Outlier handling
    ## For every data source apply outlier detection techniques. 
    ### In this way one can gain insights on the quality of the data.
    """

    createDirectory(outliersDir)
    createDirectory(outliersPlotsDir)

    # Getting the names of the different data sources
    data_sources_names = getDataSourcesNames(temporalPath)

    # Creating profile for each data source, results saved in reports folder 
    for data_source_name in data_sources_names:
        
        plotDir = outliersPlotsDir + data_source_name + '/'
        createDirectory(plotDir)

        print(f"\n Detecting outliers for {data_source_name} data source")

        df = getDataframeFrom_trusted(data_source_name)
        outlier_detection(df)


if __name__ == "__main__":
    main()