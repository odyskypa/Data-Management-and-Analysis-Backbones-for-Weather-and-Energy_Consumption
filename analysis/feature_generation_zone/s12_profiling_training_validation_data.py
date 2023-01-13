from utilities.os_utilities import createDirectory
from utilities.db_utilities import getDataframeFrom_exploitation
from utilities.analysis_utilities import train_test_profiling
from utilities.graphs_utilities import tidy_corr_matrix, generateCorrelationHeatmap
from paths import profilingTrainTestDir, profilingTrainTestPlotsDir, plotDirTrain, plotDirTest

def main():
    
    # Creating necessary folders for saving plots
    createDirectory(profilingTrainTestDir)
    createDirectory(profilingTrainTestPlotsDir)
    createDirectory(plotDirTrain)
    createDirectory(plotDirTest)
    
    df_train = getDataframeFrom_exploitation("training_set")
    df_test = getDataframeFrom_exploitation("validation_set")
    
    # corr_matrix = df_train.select_dtypes(include=['float64', 'int']).corr(method='pearson')
    # tidy_corr_matrix(corr_matrix).head(10)
    # generateCorrelationHeatmap(df_train, "training_set", plotDirTrain)
    
    #corr_matrix = df_test.select_dtypes(include=['float64', 'int']).corr(method='pearson')
    #tidy_corr_matrix(corr_matrix).head(10)
    # generateCorrelationHeatmap(df_train, "validation_set", plotDirTest)
    
    train_test_profiling(df_train, plotDirTrain, "training_set")
    train_test_profiling(df_test, plotDirTest, "validation_set")
    

    

if __name__ == "__main__":
    main()