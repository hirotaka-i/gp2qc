# gp2qc/processing.py

import pandas as pd
from google.cloud import storage
from io import BytesIO

# Initialize the Google Cloud Storage bucket
bucket = storage.Client().bucket('eu-samplemanifest')

# Function to read a file from Google bucket and process it
def read_file_and_process(file_name, mid):
    blob = bucket.blob(file_name)
    content = blob.download_as_bytes()
    df = pd.read_excel(BytesIO(content), dtype={"sample_id":'string', 'clinical_id':'string'})
    if '_selfQCV2_' in file_name:
        manifest_id = df.manifest_id.unique()[0]
        if mid == manifest_id:
            save_file_name = file_name.replace('.xlsx', '.csv')
        else:
            raise ValueError(f'manifest_id={manifest_id} in data: Not consistent with mid({mid})')
    elif '_selfQCV3_' in file_name:
        df['manifest_id'] = mid
        save_file_name = file_name.replace('.xlsx', f'_{mid}.csv')
    else:
        raise ValueError('Not selfQCV2 or V3')
    df['filename'] = save_file_name
    print(save_file_name, 'is loaded')
    return df