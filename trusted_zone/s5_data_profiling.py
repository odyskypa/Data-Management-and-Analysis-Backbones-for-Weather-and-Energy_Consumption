#  Trusted zone
## Data profiling for all data sources
### This script automatically creates a data profile for each data source from the tables of the trusted database

from utilities.graphs_utilities import *
from utilities.os_utilities import *
from utilities.db_utilities import *
from paths import temporalPath, trustedDataBasesDir, profilingDir, profilingPlotsDir

def main():
    """# Data Profiling
    ## For every data source a profile of each variable is generated 
    ### In this way one can gain insights on the quality of the data 
    """

        
    createDirectory(profilingDir)
    createDirectory(profilingPlotsDir)

    #Getting the names of the different data sources
    data_sources_names = getDataSourcesNames(temporalPath)

    # Creating profile for each data source, results saved in reports folder 
    for data_source_name in data_sources_names:
        
        plotDir = profilingPlotsDir + data_source_name + '/'
        createDirectory(plotDir)

        print(f"Profile generation for {data_source_name} data source")

        df = getDataframeFrom_trusted(trustedDataBasesDir, data_source_name)

        if data_source_name == "NCEI": # NEEDS to move from here
            df['STATION'] = df.STATION.astype('category')
            df['FRSHTT'] = df.FRSHTT.astype('category')
        
        exportDataProfileReportToHTML(df, profilingDir, data_source_name)

        # Multivariate Analysis resulting in heatmap, pairplot and lineplot
        generateCorrelationHeatmap(df, data_source_name, plotDir)
        generatePairplot(df, data_source_name, plotDir)
        generateLineplot(df, data_source_name, plotDir)

if __name__ == "__main__":
    main()