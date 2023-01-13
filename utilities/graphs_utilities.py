import seaborn as sns
from pylab import savefig
import pandas as pd
from matplotlib import pyplot as plt
from pandas_profiling import ProfileReport
import numpy as np
import statsmodels.api as sm
from paths import profilingTrainTestPlotsDir

def exportDataProfileReportToHTML (df, profilingDir, data_source_name, minim=True):
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
    profile = ProfileReport(df, title = f"{data_source_name}_Profiling_Report", minimal=minim)
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
        
def tidy_corr_matrix(corr_mat):
    """
        Function to convert a pandas correlation matrix to tidy format.
        
        @param:
            -   corr_mat: a correlation matrix
        @Output:
    """
    corr_mat = corr_mat.stack().reset_index()
    corr_mat.columns = ['variable_1','variable_2','r']
    corr_mat = corr_mat.loc[corr_mat['variable_1'] != corr_mat['variable_2'], :]
    corr_mat['abs_r'] = np.abs(corr_mat['r'])
    corr_mat = corr_mat.sort_values('abs_r', ascending=False)
    
    return(corr_mat)

def generateCorrHeatMap(corr_matrix):
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(4, 4))

    sns.heatmap(
        corr_matrix,
        annot     = True,
        cbar      = False,
        annot_kws = {"size": 8},
        vmin      = -1,
        vmax      = 1,
        center    = 0,
        cmap      = sns.diverging_palette(20, 220, n=200),
        square    = True,
        ax        = ax
    )

    ax.set_xticklabels(
        ax.get_xticklabels(),
        rotation = 45,
        horizontalalignment = 'right',
    )

    ax.tick_params(labelsize = 10)
    
def generateResidualPlots(residuos_train, y_train, prediccion_train, ext, profilingTrainTestPlotsDir = profilingTrainTestPlotsDir):
    # Plots
    # ==============================================================================
    fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(9, 8))

    axes[0, 0].scatter(y_train, prediccion_train, edgecolors=(0, 0, 0), alpha = 0.4)
    axes[0, 0].plot([y_train.min(), y_train.max()], [y_train.min(), y_train.max()],
                    'k--', color = 'black', lw=2)
    axes[0, 0].set_title('Valor predicho vs valor real', fontsize = 10, fontweight = "bold")
    axes[0, 0].set_xlabel('Real')
    axes[0, 0].set_ylabel('Predicción')
    axes[0, 0].tick_params(labelsize = 7)

    axes[0, 1].scatter(list(range(len(y_train))), residuos_train,
                    edgecolors=(0, 0, 0), alpha = 0.4)
    axes[0, 1].axhline(y = 0, linestyle = '--', color = 'black', lw=2)
    axes[0, 1].set_title('Residuos del modelo', fontsize = 10, fontweight = "bold")
    axes[0, 1].set_xlabel('id')
    axes[0, 1].set_ylabel('Residuo')
    axes[0, 1].tick_params(labelsize = 7)

    sns.histplot(
        data    = residuos_train,
        stat    = "density",
        kde     = True,
        line_kws= {'linewidth': 1},
        color   = "firebrick",
        alpha   = 0.3,
        ax      = axes[1, 0]
    )

    axes[1, 0].set_title('Distribución residuos del modelo', fontsize = 10,
                        fontweight = "bold")
    axes[1, 0].set_xlabel("Residuo")
    axes[1, 0].tick_params(labelsize = 7)


    sm.qqplot(
        residuos_train,
        fit   = True,
        line  = 'q',
        ax    = axes[1, 1], 
        color = 'firebrick',
        alpha = 0.4,
        lw    = 2
    )
    axes[1, 1].set_title('Q-Q residuos del modelo', fontsize = 10, fontweight = "bold")
    axes[1, 1].tick_params(labelsize = 7)

    axes[2, 0].scatter(prediccion_train, residuos_train,
                    edgecolors=(0, 0, 0), alpha = 0.4)
    axes[2, 0].axhline(y = 0, linestyle = '--', color = 'black', lw=2)
    axes[2, 0].set_title('Residuos del modelo vs predicción', fontsize = 10, fontweight = "bold")
    axes[2, 0].set_xlabel('Predicción')
    axes[2, 0].set_ylabel('Residuo')
    axes[2, 0].tick_params(labelsize = 7)

    # Se eliminan los axes vacíos
    fig.delaxes(axes[2,1])

    fig.tight_layout()
    plt.subplots_adjust(top=0.9)
    fig.suptitle('Diagnóstico residuos', fontsize = 12, fontweight = "bold")
    plt.savefig(f'{profilingTrainTestPlotsDir}model_{ext}_res.png')