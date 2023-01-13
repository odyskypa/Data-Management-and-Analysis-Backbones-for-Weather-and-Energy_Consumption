from utilities.db_utilities import saveDataframeTo_exploitation, getDataForTrainingModels
from utilities.analysis_utilities import featureSelectionWithDevTest
import pandas as pd


def main():
    
    # Getting Data from Exploitation Database
    df_train, df_test, X_train, y_train, X_test, y_test, _, _ = getDataForTrainingModels()
    
    # Checking The p-values of Deviance Test MLR Summary
    X_train, X_test = featureSelectionWithDevTest(X_train, X_test, y_train)
    
    # Saving last version of training and validation sets to exploitation zone
    df_train = pd.concat([X_train, y_train], axis=1)
    df_test = pd.concat([X_test, y_test], axis=1)
    
    saveDataframeTo_exploitation(df_train, "training_set_dev_test")
    saveDataframeTo_exploitation(df_test, "validation_set_dev_test")
        

if __name__ == "__main__":
    main()