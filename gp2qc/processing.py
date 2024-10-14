# gp2qc/processing.py

import pandas as pd
from google.cloud import storage
from io import BytesIO
from .base_check import base_check
import glob
import re


def extract_qc_version_and_number(file_name):
    """
    Extracts the selfQC version and the number (up to 8 digits) from the file name.

    Args:
    - file_name (str): The name of the file containing the desired information.

    Returns:
    - tuple: A tuple containing the selfQC version and the number, or (None, None) if not found.
    """
    # Regular expression to match 'selfQCV' followed by any number and then a number up to 8 digits
    pattern = r'(selfQC(?:V\d+)?)_(\d{4}-\d{2}-\d{2}|\d{8})'

    # Search for the pattern in the file name
    match = re.search(pattern, file_name)

    if match:
        self_qc = match.group(1)  # Get 'selfQCV' with any version number
        number = match.group(2)   # Get the number up to 8 digits
        return f'{self_qc}_{number}'
    else:
        raise ValueError(f"selfQCVx and date required in the {file_name}")

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
        blobs1 = list(self.bucket.list_blobs(prefix=f"{study}/{study}"))
        # Check if 'study' contains a hyphen, and only then list blobs with the modified prefix
        if '-' in study:
            blobs2 = list(self.bucket.list_blobs(prefix=f"{study.split('-')[0]}/{study}"))
        else:
            blobs2 = []  # Empty list if no hyphen
        
        # Create the list of file names, concatenating blobs1 and blobs2
        file_list = [blob.name for blob in blobs1 + blobs2]
        
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
        extract_file_name = extract_qc_version_and_number(self.file_name)
        self.save_file_name = f'{self.study}_{extract_file_name}_{self.mid}.csv'

        # Process the file based on its type
        if '_selfQCV2_' in self.file_name:
            manifest_ids = self.df.manifest_id.unique()
            if len(manifest_ids)>1:
                raise ValueError(f'multiple manifest ID in the file: {manifest_ids}')
            manifest_id = manifest_ids[0]
            if mid != manifest_id:
                raise ValueError(f'manifest_id={manifest_id} in data: Not consistent with mid({mid})')
        elif '_selfQCV3_' in self.file_name:
            self.df['manifest_id'] = mid
            save_file_name = self.file_name.replace('.xlsx', f'_{mid}.csv')
        else:
            raise ValueError('Not selfQCV2 or V3')
        
        # Add filename column to the dataframe
        self.df['filename'] = self.save_file_name
        self.df_original = self.df.copy()
        print(f"manifest_id={mid} assigned to the data.")
    
    def basic_check(self):
        """
        Perform the base check on the processed DataFrame.
        """
        if not hasattr(self, 'df'):
            raise ValueError("No data loaded. Please read_file_and_process first.")
        elif not hasattr(self, 'df_original'):
            raise ValueError("manifest_id needs to be assigned. Please assign_manifest_id first.")

        # make sure df and df_original are pd.DataFrame.
        if not isinstance(self.df, pd.DataFrame):
            raise TypeError("df is not a pandas DataFrame.")
        if not isinstance(self.df_original, pd.DataFrame):
            raise TypeError("df_original is not a pandas DataFrame.")

        # base_check
        if self.df.equals(self.df_original):
            base_check(self.df_original)
        else:
            print("\nWARNING!! base_check on the modified dataframe.\n")
            base_check(self.df)
            

    def show_phenotype_summary(self):
        """
        Show a summary of the phenotype data in the processed DataFrame.
        """
        if not hasattr(self, 'df'):
            raise ValueError("No data loaded. Please read_file_and_process first.")
        else:
            print(self.df.groupby(['study_arm', 'study_type', 'diagnosis', 'GP2_phenotype']).size())
