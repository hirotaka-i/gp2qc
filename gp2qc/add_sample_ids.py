import json
from datetime import datetime
from google.cloud import storage

# Fixed bucket and storage client
bucket_name = 'eu-samplemanifest'
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)

def add_sample_ids(df):
    """
    Adds entries from a DataFrame to GP2IDSMAPPER.json.
    
    Args:
        df (pandas.DataFrame): DataFrame containing 'study', 'sample_id', 'GP2sampleID', 'clinical_id' columns.
    """
    
    if df.shape[0]!=df.drop_duplicates(['study', 'sample_id']).shape[0]:
        raise ValueError("Duplicate sample IDs found in the DataFrame.")

    if df.shape[0]!=df.drop_duplicates(['GP2sampleID']).shape[0]:
        raise ValueError("Duplicate GP2 sample IDs found in the DataFrame.")
    
    # check if the string in the GP2sampleID starts with the string in the study column
    mismatch = df.apply(lambda row: not row['GP2sampleID'].startswith(row['study']), axis=1)
    if mismatch.any():
        mismatched_rows = df[mismatch]
        raise ValueError(f"GP2sampleID does not start with the study name for these rows:\n{mismatched_rows}")
        
    print("Starting entry addition process...")
    print('sample_ids to add:')
    print(df.study.value_counts())
    
    # Load the ID data from GP2IDSMAPPER.json
    blob_id = bucket.blob('IDSTRACKER/GP2IDSMAPPER.json')
    try:
        masterids = json.loads(blob_id.download_as_text())
    except Exception as e:
        print(f"Error loading ID data: {e}")
        return
    
    # Backup the current GP2IDSMAPPER.json
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    new_blob_name = f'IDSTRACKER/ARCHIVE/GP2IDSMAPPER_before_add_{timestamp}.json'
    
    try:
        bucket.copy_blob(blob_id, bucket, new_blob_name)
        print(f"Original GP2IDSMAPPER.json copied to gs://{bucket_name}/{new_blob_name}")
    except Exception as e:
        print(f"Error copying file: {e}")
        return

    # Process the DataFrame and add to the JSON
    for _, row in df.iterrows():
        study = row['study']
        sample_id = row['sample_id']
        gp2sampleid = row['GP2sampleID']
        clinical_id = row['clinical_id']
        
        # Check if the study exists in the master file
        if study not in masterids:
            masterids[study] = {}
        
        # check if the sample_id already exists in the study
        if sample_id in masterids[study]:
            raise ValueError(f"Sample ID {sample_id} already exists in study {study}.")
        
        # check if the GP2sampleID already exists in the study
        if gp2sampleid in [x[0] for x in masterids[study].values()]:
            raise ValueError(f"GP2sampleID {gp2sampleid} already exists in study {study}.")    

        # Add or update the sample_id for the study
        masterids[study][sample_id] = [gp2sampleid, clinical_id]
        # print(f"Added/Updated {sample_id} for study {study}.")
    
    # Save the updated GP2IDSMAPPER.json
    try:
        blob_id.upload_from_string(json.dumps(masterids, indent=4))
        print("Updated GP2IDSMAPPER.json saved successfully.")
    except Exception as e:
        print(f"Error saving updated GP2IDSMAPPER.json: {e}")
        return

    print("Entry addition process completed.")
