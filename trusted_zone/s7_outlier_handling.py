import numpy as np
from utilities.graphs_utilities import generateBoxplot
from utilities.os_utilities import getDataSourcesNames, createDirectory
from utilities.db_utilities import getDataframeFrom_trusted_noNAs, saveDataframeTo_trusted_outliers
from paths import temporalPath, outliersDir, outliersPlotsDir

def outlier_detection(df, data_source_name, plotDir):
    """
      Detecting outlier values of columns of a dataframe based on boxplots and IQR.
      Saving boxplots to the plotDir.
      Saving outliers dataframe to the _outliers database in the trusted zone.

      @param:
        -   df: the dataframe for which we want to detect outliers
        -   data_source_name: the name of the data source
        -   plotDir: the absolute path where any plot generated in this function is saved
      @Output:
        -   IQR_outliers: a dataframe containing the outliers
    """

    print(f"\n Outlier detection for each column in the {data_source_name} dataframe:\n")
    
    if data_source_name == "NCEI": # NEEDS to move from here
        df['STATION'] = df.STATION.astype('category')
        df['FRSHTT'] = df.FRSHTT.astype('category')
    
    # Keeping only numerical variables of the dataframe
    # Generate boxplots for each column of the numerical dataframe
    numeric_df = df.select_dtypes(include=[np.number])
    generateBoxplot(numeric_df, data_source_name, plotDir)
    
    # Calculate Q1 and Q3
    Q1 = numeric_df.quantile(0.25)
    Q3 = numeric_df.quantile(0.75)

    # Calculate the IQR
    IQR = Q3 - Q1

    # Filter the dataset with the IQR
    IQR_outliers = df[((numeric_df < (Q1 - 1.5 * IQR)) |(numeric_df > (Q3 + 1.5 * IQR))).any(axis=1)]
    print(IQR_outliers.head)
    return IQR_outliers

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

        df = getDataframeFrom_trusted_noNAs(data_source_name)
        outliers_df = outlier_detection(df, data_source_name, plotDir)
        saveDataframeTo_trusted_outliers(outliers_df, data_source_name)


if __name__ == "__main__":
    main()