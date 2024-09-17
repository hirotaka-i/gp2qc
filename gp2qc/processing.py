# gp2qc/processing.py

import pandas as pd
from google.cloud import storage
from io import BytesIO

class GP2SampleManifesstProcessor:
    def __init__(self, bucket_name):
        # Initialize the Google Cloud Storage client and bucket
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def list_blobs(self, study):
        """
        List blobs in the Google Cloud Storage bucket with a specific study prefix.
        """
        self.study = study
        blobs = self.bucket.list_blobs(prefix=f"{study}/{study}")
        file_list = [blob.name for blob in blobs]
        print(f"Blobs in bucket for study {study}: {file_list}")
        return file_list

    def read_file_and_process(self, file_name, mid):
        """
        Read a file from Google Cloud Storage and process it into a pandas DataFrame.
        """
        self.file_name = self
        self.mid = mid
        # Retrieve the file from the bucket
        blob = self.bucket.blob(file_name)
        content = blob.download_as_bytes()

        # Read the content as an Excel file
        self.df = pd.read_excel(BytesIO(content), dtype={"sample_id": 'string', 'clinical_id': 'string'})

        # Process the file based on its type
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

        # Add filename column to the dataframe
        df['filename'] = save_file_name
        print(f"{save_file_name} is loaded")