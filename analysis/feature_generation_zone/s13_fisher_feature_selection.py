import pandas as pd
from utilities.db_utilities import getDataForTrainingModels, saveDataframeTo_exploitation
from utilities.analysis_utilities import fisher_score, feature_ranking
import warnings
warnings.filterwarnings('ignore')

def main():
    # Getting Data from Exploitation Database
    df_train, df_test, X_train, y_train, X_test, y_test, X_train_np, y_train_np = getDataForTrainingModels()
    
    # Calculating Fisher Score
    score = fisher_score(X_train_np, y_train_np)
    # print(score)
    
    # Ranking Features Based on Fisher Scoer
    idx = feature_ranking(score)
    # print (idx)
    num_fea = 13 #after making several trials, the best results (concering AIC) are found with this number of features
    
    # Selecting the Best 13 Features -- This solution is too specific, needs generalization
    X_train_fisher = X_train.iloc[:, idx[0:num_fea]]
    X_test_fisher = X_test.iloc[:, idx[0:num_fea]]
    df_train = pd.concat([X_train_fisher, y_train], axis=1)
    df_test = pd.concat([X_test_fisher, y_test], axis=1)
    
    # Saving Fisher Training and Validation Sets to Exploitation Database
    saveDataframeTo_exploitation(df_train, "training_set_fisher")
    saveDataframeTo_exploitation(df_test, "validation_set_fisher")

if __name__ == "__main__":
    main()