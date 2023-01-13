from utilities.db_utilities import saveDataframeTo_exploitation, getDataForTrainingModels
from utilities.analysis_utilities import featureSelectionWithVIF
import pandas as pd

def main():
    # Getting Data from Exploitation Database
    df_train, df_test, X_train, y_train, X_test, y_test, _, _ = getDataForTrainingModels()
    
    X_train, X_test = featureSelectionWithVIF(X_train, X_test)
    
    # Saving last version of training and validation sets to exploitation zone
    df_train = pd.concat([X_train, y_train], axis=1)
    df_test = pd.concat([X_test, y_test], axis=1)
    
    saveDataframeTo_exploitation(df_train, "training_set_vif")
    saveDataframeTo_exploitation(df_test, "validation_set_vif")

if __name__ == "__main__":
    main()