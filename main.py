from landing_zone import s1_temporal_to_persistent_zone
from formatted_zone import s2_persistent_to_formatted_zone
from trusted_zone import s3_formatted_to_trusted_zone_version_handling_both_data_sources, s4_data_preprocessing_filling_in_nas_to_missing_data_ncei



def  main():

    #s1_temporal_to_persistent_zone.main() # WORKS FINE - PERFECT !
    #s2_persistent_to_formatted_zone.main() # WORKS FINE - loadDataFromPersistentToFormattedDatabase NEED TO BE MORE GENERIC
    #s3_formatted_to_trusted_zone_version_handling_both_data_sources.main() # BAD --> loadDataFromFormattedToTrustedDatabase NEED TO BE CREATED
    s4_data_preprocessing_filling_in_nas_to_missing_data_ncei.main()



if __name__ == "__main__":
    main()