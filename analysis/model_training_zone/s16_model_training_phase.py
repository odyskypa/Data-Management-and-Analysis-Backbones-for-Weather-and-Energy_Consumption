from utilities.os_utilities import createDirectory
from utilities.db_utilities import getDataForTrainingModels
from utilities.analysis_utilities import modelTraining
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
import statsmodels.api as sm
import numpy as np
from paths import modelsDir

def main():
    
    # Creating Directory for Saving Trained Models
    createDirectory(modelsDir)
    
    # The creation of this list must be automated in future work
    extensions = ["", "_fisher", "_dev_test", "_vif"]
    
    for ext in extensions:
        _, _, X_train, y_train, _, _, _, _ = getDataForTrainingModels(ext)
        model = modelTraining(X_train, y_train)
        model.save(f'{modelsDir}\model{ext}.pickle')
        
        # Predicting the accuracy score for the TRAIN to check for overfitting
        # ==============================================================================
        X_train = sm.add_constant(X_train, prepend=True)
        prediccion_train = model.predict(exog = X_train)
        print("\n")
        print('Results for TRAIN')
        score=r2_score(y_train,prediccion_train)
        print("r2 score for TRAIN is" ,score) #accuracy
        print("mean_sqrd_error for TRAIN is==" ,mean_squared_error(y_train,prediccion_train))
        print("root_mean_squared error for TRAIN is==" ,np.sqrt(mean_squared_error(y_train,prediccion_train)))

if __name__ == "__main__":
    main()