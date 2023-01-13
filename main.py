from databases_structures import databases_structure
from landing_zone import s1_temporal_to_persistent_zone
from formatted_zone import s2_persistent_to_formatted_zone
from trusted_zone import (s3_formatted_to_trusted_zone_version_handling_both_data_sources, 
                            s4_data_preprocessing_filling_in_nas_to_missing_data_ncei,
                            s5_data_profiling, s6_NAs_imputation, s7_outlier_handling,
                            s8_data_quality_tasks)
from exploitation_zone import s9_trusted_zone_to_explotation_zone
from analysis.analytical_sandbox_zone import s10_exploitation_zone_to_analytical_sandboxes_zone
from analysis.feature_generation_zone import (s11_split_training_validation_data, 
                                              s12_profiling_training_validation_data,
                                              s13_fisher_feature_selection,
                                              s14_deviance_test_feature_selection,
                                              s15_vif_feature_selection)
from analysis.model_training_zone import s16_model_training_phase
from analysis.model_validation_zone import s17_model_validation_phase


def  main():

    # DATA MANAGEMENT BACKBONE
    
    #s1_temporal_to_persistent_zone.main()
    #s2_persistent_to_formatted_zone.main()
    #s3_formatted_to_trusted_zone_version_handling_both_data_sources.main()
    #s4_data_preprocessing_filling_in_nas_to_missing_data_ncei.main()
    #s5_data_profiling.main()
    #s6_NAs_imputation.main()
    #s7_outlier_handling.main()
    #s8_data_quality_tasks.main()
    #s9_trusted_zone_to_explotation_zone.main()
    #databases_structure.diagnosis()
    
    # DATA MANAGEMENT BACKBONE is commented out in case supervisor want to run new generated code artefacts.
    
    # DATA ANALYSIS BACKBONE
    s10_exploitation_zone_to_analytical_sandboxes_zone.main()
    s11_split_training_validation_data.main()
    s12_profiling_training_validation_data.main()
    s13_fisher_feature_selection.main()
    s14_deviance_test_feature_selection.main()
    s15_vif_feature_selection.main()
    s16_model_training_phase.main()
    s17_model_validation_phase.main()
    

if __name__ == "__main__":
    main()