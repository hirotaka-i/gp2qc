# gp2qc/processing.py

import pandas as pd
from google.cloud import storage
from io import BytesIO
from .base_check import base_check
import glob

class GP2SampleManifesstProcessor:
    def __init__(self, bucket_name):
        # Initialize the Google Cloud Storage client and bucket
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.base_checked = False

    def list_blobs(self, study):
        """
        List blobs in the Google Cloud Storage bucket with a specific study prefix
        and allow the user to choose a file by number.
        """
        self.study = study
        blobs = self.bucket.list_blobs(prefix=f"{study}/{study}")
        file_list = [blob.name for blob in blobs]
        
        # Display the files with numbers
        print(f"\nBlobs in bucket for study {study}:")
        for i, file_name in enumerate(file_list, 1):
            print(f"{i}. {file_name}")
        
        # Ask the user to choose a file
        file_choice = input("\nEnter the number corresponding to the file you want to process: ")

        # Validate the input and return the chosen file
        try:
            file_index = int(file_choice) - 1  # Convert input to index
            if 0 <= file_index < len(file_list):
                self.file_name = file_list[file_index]
                print(f"Load: {self.file_name}")
                # Retrieve the file from the bucket
                blob = self.bucket.blob(self.file_name)
                content = blob.download_as_bytes()

                # Read the content as an Excel file
                self.df = pd.read_excel(BytesIO(content), dtype={"sample_id": 'string', 'clinical_id': 'string'})

            else:
                print("Invalid selection. Please choose a valid file number.")
        except ValueError:
            print("Invalid input. Please enter a number.")

    def assign_manifest_id(self, mid):
        """
        Read a file from Google Cloud Storage and process it into a pandas DataFrame.
        """
        self.mid = mid

        # Process the file based on its type
        if '_selfQCV2_' in self.file_name:
            manifest_id = self.df.manifest_id.unique()[0]
            if mid == manifest_id:
                save_file_name = self.file_name.replace('.xlsx', '.csv')
            else:
                raise ValueError(f'manifest_id={manifest_id} in data: Not consistent with mid({mid})')
        elif '_selfQCV3_' in self.file_name:
            self.df['manifest_id'] = mid
            save_file_name = self.file_name.replace('.xlsx', f'_{mid}.csv')
        else:
            raise ValueError('Not selfQCV2 or V3')
        
        # Add filename column to the dataframe
        self.df['filename'] = save_file_name
        self.df_original = self.df.copy()
        print(f"manifest_id={mid} assigned to the data.")
    
    def base_check(self):
        """
        Perform the base check on the processed DataFrame.
        """
        if not hasattr(self, 'df'):
            raise ValueError("No data loaded. Please read_file_and_process first.")
        elif not hasattr(self, 'df_original'):
            raise ValueError("manifest_id needs to be assigned. Please assign_manifest_id first.")
        
        try :
            if self.df.equals(self.df_original):
                base_check(self.df_original)
            else:
                print("\nWARNING!! base_check on the modified dataframe.\n")
                base_check(self.df)
        except:
            raise ValueError("df seems to be not a pandas DataFrame.")
            

    def show_phenotype_summary(self):
        """
        Show a summary of the phenotype data in the processed DataFrame.
        """
        if not hasattr(self, 'df'):
            raise ValueError("No data loaded. Please read_file_and_process first.")
        else:
            print(self.df.groupby(['study_arm', 'study_type', 'diagnosis', 'GP2_phenotype']).size())