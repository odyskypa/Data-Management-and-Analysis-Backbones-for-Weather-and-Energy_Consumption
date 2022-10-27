#  Trusted zone
## Data profiling for all data sources
### This script automatically creates a data profile for each data source from the tables of the trusted database

import os
import duckdb
import pandas as pd
import numpy as np
import seaborn as sns
from pylab import savefig
from matplotlib import pyplot as plt
from pandas_profiling import ProfileReport
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

    # Checking if reports directory exists, if not it is being created
    if not os.path.exists(profilingDir):
        os.mkdir(profilingDir)
    if not os.path.exists(profilingPlotsDir):
        os.mkdir(profilingPlotsDir)

    # Getting the names of the different data sources
    data_sources_names = getDataSourcesNames(temporalPath)

    # Creating plot folders for each data source in profiling folder 
    for data_source_name in data_sources_names:
        if not os.path.exists(profilingPlotsDir + data_source_name):
            os.mkdir(profilingPlotsDir + data_source_name)


    # Creating profile for each data source in reports folder 
    for data_source_name in data_sources_names:
        plotDir = profilingPlotsDir + data_source_name + '/'
        try:
            print(f"Profile generation for {data_source_name} data source")
            con = duckdb.connect(database=f'{trustedDataBasesDir}{data_source_name}_trusted.duckdb', read_only=False)
            df = con.execute(f'SELECT * FROM {data_source_name}').fetchdf()
            con.close()
            

            if data_source_name == "NCEI": # TODO: THIS NEEDS TO BE HANDLED BEFORE !!! 
                df['STATION'] = df.STATION.astype('category')
                df['FRSHTT'] = df.FRSHTT.astype('category')
            
            # Univariate Analysis resulting in htlm files
            profile = ProfileReport(df, title = f"{data_source_name}_Profiling_Report", minimal=True)
            profile.to_file(f"{profilingDir}{data_source_name}_Report.html")

            # Multivariate Analysis resulting in heatmap, pairplot and lineplot
            cor = df.corr(method="spearman")
            heatmap = sns.heatmap(cor, vmin=-1, vmax=1, annot=False, cmap='coolwarm', linecolor='black', linewidths=1)
            heatmap.set_title(f'{data_source_name} Correlation Heatmap', fontdict={'fontsize':12}, pad=12);
            plt.savefig(f"{plotDir}{data_source_name}_corr_heatmap.png", dpi=400)
            plt.clf()

            pairplot = sns.pairplot(df, corner= True)
            pairplot.fig.suptitle(f"{data_source_name} Pairplot")
            plt.savefig(f"{plotDir}{data_source_name}_pairplot.png", dpi=400)
            plt.clf()

            lineplot = sns.lineplot(data=df)
            lineplot.fig.suptitle(f"{data_source_name} Lineplot")
            plt.savefig(f"{plotDir}{data_source_name}_lineplot.png", dpi=400)
            plt.clf()

        except Exception as e:
            print(e)
            con.close()


if __name__ == "__main__":
    main() #TODO NEEDS FIXING, TOO SLOW, NOT PLOTING EVENTUALLY