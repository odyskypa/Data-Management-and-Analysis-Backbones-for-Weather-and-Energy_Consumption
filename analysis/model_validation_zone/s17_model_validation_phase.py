from scipy import stats
from utilities.db_utilities import getDataForTrainingModels
from utilities.analysis_utilities import ciModelCoeff
from utilities.graphs_utilities import generateResidualPlots
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
import statsmodels.api as sm
import numpy as np
from paths import modelsDir


def main():
    # The creation of this list must be automated in future work
    extensions = ["", "_fisher", "_dev_test", "_vif"]
    
    for ext in extensions:
        _, _, _, y_train, X_test, y_test, X_train_np, _ = getDataForTrainingModels(ext, True)
        modelo = sm.load(f'{modelsDir}\model{ext}.pickle')
        print(modelo.summary())
        
        intervalos_ci = ciModelCoeff(modelo)
        print(intervalos_ci)
        
        # Diagnostic errors (residuals) of training predictions
        # ==============================================================================
        prediccion_train = modelo.predict(exog = X_train_np)
        residuos_train   = prediccion_train - y_train
        
        generateResidualPlots(residuos_train, y_train, prediccion_train, ext)
        
        # Normality of residuals Shapiro-Wilk test
        # ==============================================================================
        shapiro_test = stats.shapiro(residuos_train)
        print(shapiro_test)
        
        # Normality of the residuals D'Agostino's K-squared test
        # ==============================================================================
        k2, p_value = stats.normaltest(residuos_train)
        print(f"Estadítico= {k2}, p-value = {p_value}")
        
        # Model test error
        # ==============================================================================
        X_test = sm.add_constant(X_test, prepend=True)
        prediccion_test = modelo.predict(exog = X_test)
        
        # Predicting the accuracy score for the TEST
        # ==============================================================================
        print("\n")
        print('Results for TEST')
        score=r2_score(y_test, prediccion_test)
        print("r2 score for TEST is", score)
        print("mean_sqrd_error for TEST is==", mean_squared_error(y_test,prediccion_test))
        print("root_mean_squared error for TEST is==", np.sqrt(mean_squared_error(y_test,prediccion_test))) #for new data
        
        
        

if __name__ == "__main__":
    main()