# gp2qc/processing.py

import pandas as pd
from google.cloud import storage
from io import BytesIO
from .base_check import base_check

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
        print('Choose the one you would like to read and specify the manifest_id')
        print('test modification')

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
        df = pd.read_excel(BytesIO(content), dtype={"sample_id": 'string', 'clinical_id': 'string'})

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
        self.df = df.copy()
        self.df_original = df.copy()
        print(f"{save_file_name} is loaded")
    
    def base_check(self):
        """
        Perform the base check on the processed DataFrame.
        """
        if not hasattr(self, 'df'):
            raise ValueError("No data loaded. Please read_file_and_process first.")
        elif self.df.equals(self.df_original):
            base_check(self.df_original)
            self.base_checked = True
        else:
            print("WARNING!! base_check on the modified dataframe.")
            base_check(self.df)
            self.base_checked = True
            

    def show_phenotype_summary(self):
        """
        Show a summary of the phenotype data in the processed DataFrame.
        """
        if not hasattr(self, 'df'):
            raise ValueError("No data loaded. Please read_file_and_process first.")
        else:
            print(self.df.groupby(['study_arm', 'study_type', 'diagnosis', 'GP2_phenotype']).size())
    
    def read_previous_manifests(self, master_sheet_path):
        # read from master sheet
        mf = pd.read_csv(master_sheet_path, low_memory=False)
        mf = mf[mf.study == self.study]
        mid_in_mf = mf.manifest_id.unique()

        # check the new manifest in the finalized folder
        folder_path = f'/content/drive/Shareddrives/EUR_GP2/CIWG/sample_manifest/finalized/{self.study}'
        d = pd.DataFrame({'path': glob.glob(f'{folder_path}/*.csv')})
        d['filename'] = [path.split('/')[-1] for path in d['path']]
        d['mid'] = [filename.replace('.csv', '').split('_')[-1] for filename in d['filename']]
        if not d.mid.is_unique:
            raise ValueError('Multiple manifests with the same mid in the finalized folder')
        
        # Load the ones not in the mf
        paths = d[~d.mid.isin(mid_in_mf)].path.tolist()
        if len(paths) > 0:
            for path_i in paths:
                print('New manifests in finalized folder not yet in the master sheet')
                print(' Add', path_i)
                df_i = pd.read_csv(path_i)
                mf = pd.concat([mf, df_i], ignore_index=True)
        else:
            print('No new manifest to add in the finalized folder')

        print('N of samples from all the previous submission')
        print(mf.manifest_id.value_counts())

        return mf
