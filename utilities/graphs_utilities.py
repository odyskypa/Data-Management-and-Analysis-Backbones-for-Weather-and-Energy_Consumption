import seaborn as sns
from pylab import savefig
import pandas as pd
from matplotlib import pyplot as plt
from pandas_profiling import ProfileReport

def exportDataProfileReportToHTML (df, profilingDir, data_source_name):
    """
        This function generates a report containing information about data profiling of the dataframe.
        For each variable a univariate analysis is completed and the results are saved as HTML files in the
        profiling directory.

        @param:
            -   df: the dataframe for which we want to accomplish data profiling
            -   profilingDir: the absolute path of the profiling directory
            -   data_source_name: the name of the data source (e.g. WEB, NCEI, etc..)
        @Output:
    """
    # Univariate Analysis resulting in htlm files
    profile = ProfileReport(df, title = f"{data_source_name}_Profiling_Report", minimal=True)
    profile.to_file(f"{profilingDir}{data_source_name}_Report.html")

def generateCorrelationHeatmap(df, data_source_name, plotDir):
    """
        This function generates the spearman correlation heatmap for the input dataframe.

        @param:
            -   df: the dataframe for which we want to generate Correlation heatmap
            -   data_source_name: the name of the data source (e.g. WEB, NCEI, etc..)
            -   plotDir: the absolute path of the directory where plots are saved
        @Output:
    """
    cor = df.corr(method="spearman")
    heatmap = sns.heatmap(cor, vmin=-1, vmax=1, annot=False, cmap='coolwarm', linecolor='black', linewidths=1)
    heatmap.set_title(f'{data_source_name} Correlation Heatmap', fontdict={'fontsize':12}, pad=12)
    plt.savefig(f"{plotDir}{data_source_name}_corr_heatmap.png", dpi=400)
    plt.clf()

def generatePairplot(df, data_source_name, plotDir):
    """
        This function generates a pairplot between all the variables of the input dataframe.

        @param:
            -   df: the dataframe for which we want to generate the pairplot
            -   data_source_name: the name of the data source (e.g. WEB, NCEI, etc..)
            -   plotDir: the absolute path of the directory where plots are saved
        @Output:
    """
    pairplot = sns.pairplot(df, corner= True)
    pairplot.fig.suptitle(f"{data_source_name} Pairplot")
    plt.savefig(f"{plotDir}{data_source_name}_pairplot.png", dpi=400)
    plt.clf()

def generateLineplot(df, data_source_name, plotDir):
    """
        This function generates a lineplot between all the variables of the input dataframe.

        @param:
            -   df: the dataframe for which we want to generate the lineplot
            -   data_source_name: the name of the data source (e.g. WEB, NCEI, etc..)
            -   plotDir: the absolute path of the directory where plots are saved
        @Output:
    """
    lineplot = sns.lineplot(data=df)
    #lineplot.fig.suptitle(f"{data_source_name} Lineplot")
    plt.savefig(f"{plotDir}{data_source_name}_lineplot.png", dpi=400)
    plt.clf()        

def generateBoxplot(df, data_source_name, plotDir):
    """
        This function generates a boxplots for all the variables of the input dataframe.

        @param:
            -   df: the dataframe for which we want to generate the pairplot
            -   data_source_name: the name of the data source (e.g. WEB, NCEI, etc..)
            -   plotDir: the absolute path of the directory where plots are saved
        @Output:
    """
    for col in df.columns:

        boxplots = sns.boxplot(data=df[col])
        plt.savefig(f"{plotDir}{data_source_name}{col}_boxplots.png", dpi=400)
        plt.clf()