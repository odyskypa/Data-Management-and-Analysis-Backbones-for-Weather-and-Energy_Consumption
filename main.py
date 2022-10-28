from databases_structures import databases_structure
from landing_zone import s1_temporal_to_persistent_zone
from formatted_zone import s2_persistent_to_formatted_zone
from trusted_zone import (s3_formatted_to_trusted_zone_version_handling_both_data_sources, 
                            s4_data_preprocessing_filling_in_nas_to_missing_data_ncei,
                            s5_data_profiling, s6_NAs_imputation, s7_outlier_handling,
                            s8_data_quality_tasks)
from exploitation_zone import s9_trusted_zone_to_explotation_zone


def  main():

    s1_temporal_to_persistent_zone.main()
    s2_persistent_to_formatted_zone.main()
    s3_formatted_to_trusted_zone_version_handling_both_data_sources.main()
    s4_data_preprocessing_filling_in_nas_to_missing_data_ncei.main()
    s5_data_profiling.main()
    s6_NAs_imputation.main()
    s7_outlier_handling.main()
    #s8_data_quality_tasks.main()
    s9_trusted_zone_to_explotation_zone.main()
    #databases_structure.diagnosis()

if __name__ == "__main__":
    main()