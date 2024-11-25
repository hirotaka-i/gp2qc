import numpy as np
import pandas as pd



##### Sub-functions for the "base_check" function #####
def check_columns_exist(df, required_columns):
    """Check if all required columns are present in the dataframe."""
    missing_cols = np.setdiff1d(required_columns, df.columns)
    if missing_cols.size > 0:
        raise ValueError(f"Missing columns: {missing_cols}. Please use the template sheet.")
def check_unexpected_columns(df, expected_columns):
    """Check for any unexpected columns in the dataframe."""
    unexpected_cols = np.setdiff1d(df.columns, expected_columns)
    if unexpected_cols.size > 0:
        raise ValueError(f"Unexpected columns: {unexpected_cols}. Please use the template sheet.")
def check_missing_data(df, required_columns):
    """Check for missing data in required columns, list missing columns, and report the first 30 entries with missing data."""
    df_check = df[required_columns].copy()
    missing_data_summary = df_check.isna().sum()
    # Check if there are any missing values at all
    if missing_data_summary.sum() > 0:
        # Get columns with missing data
        missing_columns = missing_data_summary[missing_data_summary > 0].index.tolist()
        error_msg = (f"Missing entries found in required columns: {', '.join(missing_columns)}")
        raise ValueError(error_msg)
        
def check_one_study(df):
    if len(df.study.unique())>1:
        raise ValueError(f"More than one study in the file: {df.study.unique()}")

def check_unique_ids(df):
    """Check if Sample Identities (sample_id) are unique within each study within the dataframe."""
    grouped = df.groupby('study')
    for study, group in grouped:
        if not group['sample_id'].is_unique:
            duplicates = group.loc[group.duplicated(subset=['sample_id']), 'sample_id'].unique()
            raise ValueError(f"In study '{study}', sample_id is not unique: {duplicates}")


def check_clinical_identity(df, base_cols):
    """Check the uniqueness of clinical identifiers and their correct assignments within each study, raise errors if checks fail."""
    show_cols = base_cols + ['manifest_id', 'study']

    for study, group in df.groupby('study'):
        for identifier, related_field in [('GP2ID', 'clinical_id'), ('clinical_id', 'GP2ID')]:
            dups = group.loc[group[identifier].duplicated(), identifier].unique()
            if dups.size > 0:
                details = group.loc[group[identifier].isin(dups), show_cols].copy()
                grouped_details = details.drop_duplicates(subset=[identifier, related_field]).groupby(identifier).size()
                problem_ids = list(grouped_details[grouped_details != 1].index)
                if problem_ids:
                    error_msg = (
                        f"In study '{study}', FAIL: {identifier} assigned to different {related_field}. "
                        f"Issues with {identifier}: {problem_ids}"
                    )
                    raise ValueError(error_msg)

def validate_allowed_values(df):
    """Validate if values in specific columns match the allowed values."""
    allowed_values = {
        'GP2_phenotype': ['PD', 'Control', 'Prodromal', 'PSP', 'CBD/CBS', 'MSA', 'DLB', 'LBD',
                          'AD', 'FTD', "VaD", "VaPD", 'Population Control', 'Undetermined-MCI',
                          'Undetermined-Dementia', 'Mix', 'Other'],
        'study_type': ['Case(/Control)', 'Prodromal', 'Genetically Enriched', 'Population Cohort', 'Brain Bank', 'Monogenic'],
        'biological_sex_for_qc': ['Male', 'Female', 'Other/Unknown/Not Reported'],
        'race_for_qc': ['American Indian or Alaska Native', 'Asian', 'White',
                        'Black or African American', 'Multi-racial', 'Native Hawaiian or Other Pacific Islander',
                        'Other', 'Unknown', 'Not Reported'],
        'family_history_for_qc': ['Yes', 'No', 'Not Reported', 'Unknown'],
        # 'family_history_other_for_qc': ['Yes', 'No', 'Not Reported', 'Unknown'],
        'region_for_qc': ['ABW', 'AFG', 'AGO', 'AIA', 'ALA', 'ALB', 'AND', 'ARE', 'ARG', 'ARM', 'ASM', 'ATA', 'ATF', 'ATG', 'AUS', 'AUT', 'AZE',
                          'BDI', 'BEL', 'BEN', 'BES', 'BFA', 'BGD', 'BGR', 'BHR', 'BHS', 'BIH', 'BLM', 'BLR', 'BLZ', 'BMU', 'BOL', 'BRA', 'BRB',
                          'BRN', 'BTN', 'BVT', 'BWA', 'CAF', 'CAN', 'CCK', 'CHE', 'CHL', 'CHN', 'CIV', 'CMR', 'COD', 'COG', 'COK', 'COL', 'COM',
                          'CPV', 'CRI', 'CUB', 'CUW', 'CXR', 'CYM', 'CYP', 'CZE', 'DEU', 'DJI', 'DMA', 'DNK', 'DOM', 'DZA', 'ECU', 'EGY', 'ERI',
                          'ESH', 'ESP', 'EST', 'ETH', 'FIN', 'FJI', 'FLK', 'FRA', 'FRO', 'FSM', 'GAB', 'GBR', 'GEO', 'GGY', 'GHA', 'GIB', 'GIN',
                          'GLP', 'GMB', 'GNB', 'GNQ', 'GRC', 'GRD', 'GRL', 'GTM', 'GUF', 'GUM', 'GUY', 'HKG', 'HMD', 'HND', 'HRV', 'HTI', 'HUN',
                          'IDN', 'IMN', 'IND', 'IOT', 'IRL', 'IRN', 'IRQ', 'ISL', 'ISR', 'ITA', 'JAM', 'JEY', 'JOR', 'JPN', 'KAZ', 'KEN', 'KGZ',
                          'KHM', 'KIR', 'KNA', 'KOR', 'KWT', 'LAO', 'LBN', 'LBR', 'LBY', 'LCA', 'LIE', 'LKA', 'LSO', 'LTU', 'LUX', 'LVA', 'MAC',
                          'MAF', 'MAR', 'MCO', 'MDA', 'MDG', 'MDV', 'MEX', 'MHL', 'MKD', 'MLI', 'MLT', 'MMR', 'MNE', 'MNG', 'MNP', 'MOZ', 'MRT',
                          'MSR', 'MTQ', 'MUS', 'MWI', 'MYS', 'MYT', 'NAM', 'NCL', 'NER', 'NFK', 'NGA', 'NIC', 'NIU', 'NLD', 'NOR', 'NPL', 'NRU',
                          'NZL', 'OMN', 'PAK', 'PAN', 'PCN', 'PER', 'PHL', 'PLW', 'PNG', 'POL', 'PRI', 'PRK', 'PRT', 'PRY', 'PSE', 'PYF', 'QAT',
                          'REU', 'ROU', 'RUS', 'RWA', 'SAU', 'SDN', 'SEN', 'SGP', 'SGS', 'SHN', 'SJM', 'SLB', 'SLE', 'SLV', 'SMR', 'SOM', 'SPM',
                          'SRB', 'SSD', 'STP', 'SUR', 'SVK', 'SVN', 'SWE', 'SWZ', 'SXM', 'SYC', 'SYR', 'TCA', 'TCD', 'TGO', 'THA', 'TJK', 'TKL',
                          'TKM', 'TLS', 'TON', 'TTO', 'TUN', 'TUR', 'TUV', 'TWN', 'TZA', 'UGA', 'UKR', 'UMI', 'URY', 'USA', 'UZB', 'VAT', 'VCT',
                          'VEN', 'VGB', 'VIR', 'VNM', 'VUT', 'WLF', 'WSM', 'YEM', 'ZAF', 'ZMB', 'ZWE'], # Complete region codes here
        'manifest_id':[f'm{i}' for i in range(1,100)],
        'SampleRepNo': [f's{i}' for i in range(1,100)],
    }

    for column, allowed in allowed_values.items():
        unallowed = df[column].dropna().unique()
        unallowed = [item for item in unallowed if item not in allowed]
        if unallowed:
            raise ValueError(f"Unallowed values detected in {column}: {unallowed}")


    # List of age-related columns to check for numeric (float) values
    age_columns = ['age', 'age_of_onset', 'age_at_diagnosis', 'age_at_death', 'age_at_last_follow_up']

    for col in age_columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise ValueError(f"Column {col} contains non-numeric values.")

        # Additionally, check for non-float values in the column
        non_float_values = df[~df[col].apply(lambda x: isinstance(x, (int, float)) or pd.isna(x))]
        if not non_float_values.empty:
            raise ValueError(f"Non-float values detected in {col}: {non_float_values[col].tolist()}")


def validate_specific_conditions(df):
    # study_arm should be assigned to one study_type
    dv=df.drop_duplicates(['study', 'study_arm', 'study_type']).groupby(['study', 'study_arm']).size()
    if len(dv[dv>1])>0:
        raise ValueError('The same study_arm assigned to two or more study_type')

    # diagnosis assigned to one GP2_phenotype
    dd=df.drop_duplicates(['study', 'diagnosis', 'GP2_phenotype']).groupby(['study', 'diagnosis']).size()
    if len(dd[dd>1])>0:
        raise ValueError('The same diagnosis assigned to two or more GP2_phenotype')

    # study_type=='Monogenic' and GP2_phenotype!=Control, then family_history is required
    dm = df[(df.study_type=='Monogenic')&(df.GP2_phenotype!='Control')].copy()
    if not dm.empty:
        print("Monogenic study_type deteched. Checking family history completeness...")
        check_missing_data(dm, ['family_history_for_qc'])

    # GP2_phenotype=='LBD' is allowed only if study_type=='Brain Bank'
    db = df[(df.study_type!='Brain Bank')&(df.GP2_phenotype=='LBD')].copy()
    if not db.empty:
        raise ValueError('LBD is allowed only if study_type=="Brain Bank"')

    # GP2_phenotype=='Prodromal' is only allowed for study_type=='Prodromal'
    dnp = df[(df.study_type!='Prodromal')&(df.GP2_phenotype=='Prodromal')].copy()
    if not dnp.empty:
        raise ValueError('Prodromal-GP2_phenotype is only allowed for study_type=="Prodromal"')

    # GP2_phenotype=='Control' needs to be warned if provided in the prodromal cohort
    dp = df[(df.study_type=='Prodromal')&(df.GP2_phenotype=='Control')].copy()
    if not dp.empty:
        print('Control-GP2_phenotype assigned for study_type=="Prodromal". Is it rather "Prodromal"?')


##### This is the main function #####
def base_check(df, master_file=False):
    base_cols = ['study', 'GP2ID', 'clinical_id', 'GP2sampleID', 'sample_id', 'study_type', 'GP2_phenotype']
    required_cols = base_cols + ['study_arm', 'diagnosis', 'biological_sex_for_qc',
                                 'race_for_qc', 'family_history_for_qc', 'region_for_qc',
                                 'manifest_id', 'SampleRepNo', 'Genotyping_site']
    all_cols = required_cols + [
        "family_index", "family_index_relationship", "sample_type", "DNA_volume",
        "DNA_conc", "r260_280", "Plate_name", "Plate_position", "race", 'sex',
        "age", "age_of_onset", "age_at_diagnosis", "age_at_death", "age_at_last_follow_up",
        "family_history_pd", "family_history_other", "family_history_details", "region",
        "comment", "alternative_id1", "alternative_id2", 'GP2_phenotype_for_qc', 'filename',
    ]

    # Perform checks
    check_columns_exist(df, all_cols)
    check_unexpected_columns(df, all_cols)
    check_missing_data(df, required_cols)
    
    if not master_file: # skip if master_file
        check_one_study(df)
    
    check_unique_ids(df)
    check_clinical_identity(df, base_cols)
    validate_allowed_values(df)
    validate_specific_conditions(df)

    print('> All checks passed!')
    print('Study arms and phenotype summaries')
    print(df.groupby(['study_arm', 'study_type', 'diagnosis', 'GP2_phenotype']).size())
    # print comments if multiple diagnosis in the same study_arm
    if df.drop_duplicates(['study_arm', 'diagnosis']).groupby(['study_arm']).size().max()>1:
        print('\n=============== !! WARNING !! ===============\nMultiple diagnoses in the same study_arm')
        print('Please check if they really are in the same study_arm')
        print('If they are differently recruited, please separate the study_arm accordingly')