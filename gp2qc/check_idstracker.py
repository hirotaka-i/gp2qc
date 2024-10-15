import json
import pandas as pd
from google.cloud import storage

def check_idstracker(bucket, study, df):
    """
    Merge the current manifest (df) with the GP2 ID data from GP2IDSMAPPER.json.

    Args:
    - bucket (google.cloud.storage.bucket.Bucket): The GCS bucket containing the GP2IDSMAPPER.json file.
    - study (str): The study name to fetch from the GP2IDSMAPPER.json file.
    - df (pd.DataFrame): The current manifest DataFrame to be merged.

    Returns:
    - pd.DataFrame or None: Returns a DataFrame of missing rows if any, otherwise None.
    """
    
    # Load the ID data from GP2IDSMAPPER.json
    blob_id = bucket.blob('IDSTRACKER/GP2IDSMAPPER.json')
    masterids = json.loads(blob_id.download_as_text())
    
    # Fetch the relevant mapping for the current study
    if study not in masterids:
        raise ValueError(f"The study '{study}' was not found in GP2IDSMAPPER.json.")
    
    t = masterids[study]

    # Extract keys and values for sample_id, GP2sampleID, and clinical_id
    keys = list(t.keys())
    values_split = [value for value in t.values()]
    GP2sampleIDs = [x[0] for x in values_split]  # First part of the list
    clinical_ids = [x[1] for x in values_split]  # Second part of the list

    # Create DataFrame for the current study
    tt = pd.DataFrame({
        'sample_id': keys,
        'GP2sampleID': GP2sampleIDs,
        'clinical_id': clinical_ids
    })

    # Merge with the current manifest
    testmerge = df.merge(tt, on=['sample_id', 'GP2sampleID', 'clinical_id'], how='left', indicator=True)
    df_unmatched = testmerge[testmerge['_merge'] == 'left_only']

    # Check for missing rows
    if df_unmatched.empty:
        print('> All IDs are consistent with the ID system.')
    else:
        df_unmatched[ ['sample_id', 'GP2sampleID', 'clinical_id', 'manifest_id', 'GP2_phenotype']].to_csv('unmatched_ids.csv', index=False)
        raise ValueError("!! Some IDs are not compatible with the ID system!!! >  'unmatched_ids.csv' saved. Please check.")
        