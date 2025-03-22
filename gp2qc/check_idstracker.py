import json
import pandas as pd
from google.cloud import storage

def check_idstracker(bucket, study, df):
    """
    Merge the current manifest (df) with the GP2 ID data from GP2IDSMAPPER.json.
    """
    
    blob_id = bucket.blob('IDSTRACKER/GP2IDSMAPPER.json')
    masterids = json.loads(blob_id.download_as_text())

    study_k = "PPMI" if study in ["PPMI-N", "PPMI-G"] else study # PPMI-N/G's GP2ID stored as PPMI
    if study_k not in masterids:
        raise ValueError(f"The study '{study}' was not found in GP2IDSMAPPER.json.")
    
    t = masterids[study_k]
    
    keys = list(t.keys())
    values_split = [value for value in t.values()]
    GP2sampleIDs = [x[0] for x in values_split]
    clinical_ids = [x[1] for x in values_split]
    
    tt = pd.DataFrame({
        'sample_id': keys,
        'GP2sampleID': GP2sampleIDs,
        'clinical_id': clinical_ids
    })
    
    # modify for PPMI: 1. Consistency check and then prepare both PPMI-N and PPMI-G
    if study in ["PPMI-N", "PPMI-G"]: # modify GP2ID to match with df
        tt['GP2sampleID'] = tt['GP2sampleID'].str.replace('PPMI_', f'{study}_')
    
    testmerge = df.merge(tt, on=['sample_id', 'GP2sampleID', 'clinical_id'], how='left', indicator=True)
    df_unmatched = testmerge[testmerge['_merge'] == 'left_only']
    
    if df_unmatched.empty:
        print('> All IDs are consistent with the ID system.')
    else:
        df_unmatched[['sample_id', 'GP2sampleID', 'clinical_id', 'manifest_id', 'GP2_phenotype']].to_csv('unmatched_ids.csv', index=False)
        raise ValueError("!! Some IDs are not compatible with the ID system!!! > 'unmatched_ids.csv' saved. Please check.")
