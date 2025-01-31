import json
import pandas as pd
from google.cloud import storage

def check_ppmi_consistency(masterids):
    """
    Check the consistency of PPMI-N and PPMI-G sample_id, clinical_id, and GP2sampleID mappings.
    """
    if "PPMI-N" not in masterids or "PPMI-G" not in masterids:
        raise ValueError("Both PPMI-N and PPMI-G must be present in GP2IDSMAPPER.json for consistency check.")
    
    master_ppmin = masterids["PPMI-N"]
    master_ppmig = masterids["PPMI-G"]
    
    ppmin_df = pd.DataFrame({
        'sample_id': list(master_ppmin.keys()),
        'clinical_id': [v[1] for v in master_ppmin.values()],
        'GP2sampleID': [v[0] for v in master_ppmin.values()]
    })
    
    ppmig_df = pd.DataFrame({
        'sample_id': list(master_ppmig.keys()),
        'clinical_id': [v[1] for v in master_ppmig.values()],
        'GP2sampleID': [v[0] for v in master_ppmig.values()]
    })
    
    merged_master = ppmin_df.merge(ppmig_df, on=['sample_id', 'clinical_id'], how='outer', indicator=True)
    inconsistent_entries = merged_master[merged_master['_merge'] != 'both']
    
    if not inconsistent_entries.empty:
        print(inconsistent_entries)
        raise ValueError("Mismatch found between PPMI-N and PPMI-G sample_id and clinical_id mappings.")
    
    expected_gp2sampleid_n = ppmin_df['clinical_id'].apply(lambda x: f"PPMI-N_{x}")
    expected_gp2sampleid_g = ppmig_df['clinical_id'].apply(lambda x: f"PPMI-G_{x}")
    
    inconsistent_gp2sampleid_n = ppmin_df[ppmin_df['GP2sampleID'] != expected_gp2sampleid_n]
    inconsistent_gp2sampleid_g = ppmig_df[ppmig_df['GP2sampleID'] != expected_gp2sampleid_g]
    
    if not inconsistent_gp2sampleid_n.empty or not inconsistent_gp2sampleid_g.empty:
        print(pd.concat([inconsistent_gp2sampleid_n, inconsistent_gp2sampleid_g]))
        raise ValueError("Inconsistent GP2sampleID format detected for PPMI-N or PPMI-G.")

def check_idstracker(bucket, study, df):
    """
    Merge the current manifest (df) with the GP2 ID data from GP2IDSMAPPER.json.
    """
    
    blob_id = bucket.blob('IDSTRACKER/GP2IDSMAPPER.json')
    masterids = json.loads(blob_id.download_as_text())
    
    if study in ["PPMI-N", "PPMI-G"]:
        check_ppmi_consistency(masterids)
    
    if study not in masterids:
        raise ValueError(f"The study '{study}' was not found in GP2IDSMAPPER.json.")
    
    t = masterids[study]
    
    keys = list(t.keys())
    values_split = [value for value in t.values()]
    GP2sampleIDs = [x[0] for x in values_split]
    clinical_ids = [x[1] for x in values_split]
    
    tt = pd.DataFrame({
        'sample_id': keys,
        'GP2sampleID': GP2sampleIDs,
        'clinical_id': clinical_ids
    })
    
    # modify for PPMI
    if study in ["PPMI-N", "PPMI-G"]:
        if study == "PPMI-N":
            to_replace = "PPMI-G_"
        elif study == "PPMI-G":
            to_replace = "PPMI-N_"
        tt2 = tt.copy()
        tt2['GP2sampleID'] = tt2['GP2sampleID'].str.replace(to_replace, f'{study}_')
        tt = pd.concat([tt, tt2], ignore_index=True)
    
    testmerge = df.merge(tt, on=['sample_id', 'GP2sampleID', 'clinical_id'], how='left', indicator=True)
    df_unmatched = testmerge[testmerge['_merge'] == 'left_only']
    
    if df_unmatched.empty:
        print('> All IDs are consistent with the ID system.')
    else:
        df_unmatched[['sample_id', 'GP2sampleID', 'clinical_id', 'manifest_id', 'GP2_phenotype']].to_csv('unmatched_ids.csv', index=False)
        raise ValueError("!! Some IDs are not compatible with the ID system!!! > 'unmatched_ids.csv' saved. Please check.")
