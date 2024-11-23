import json
from google.cloud import storage
import pandas as pd

# Fixed bucket and storage client
bucket_name = 'eu-samplemanifest'
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)

def get_gp2idsmapper():
    """
    get IDs from GP2IDSMAPPER.json.
    
    Args:
        df (pandas.DataFrame): DataFrame containing 'study', 'sample_id', 'GP2sampleID', 'clinical_id' columns.
    """
    # Load the ID data from GP2IDSMAPPER.json
    blob_id = bucket.blob('IDSTRACKER/GP2IDSMAPPER.json')
    try:
        masterids = json.loads(blob_id.download_as_text())
    except Exception as e:
        print(f"Error loading ID data: {e}")
        return
    
    # make the df from masterids
    df = []
    for study in masterids:
        for sample_id in masterids[study]:
            df.append([study, sample_id, masterids[study][sample_id][0], masterids[study][sample_id][1]])
    df = pd.DataFrame(df, columns=['study', 'sample_id', 'GP2sampleID', 'clinical_id'])
    return df