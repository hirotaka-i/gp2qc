import os
import glob
import pandas as pd
import numpy as np
from google.cloud import storage

#### Sub function to the "check_inconsistencies" function
def find_inconsistency(df, col_to_check):
    # Identify GP2IDs with inconsistent values
    inconsistent_gp2ids = df.groupby('GP2ID')[col_to_check].nunique(dropna=False)
    inconsistent_gp2ids = inconsistent_gp2ids[inconsistent_gp2ids > 1].index.tolist()

    # Filter only those rows with inconsistent GP2IDs
    t = df[df.GP2ID.isin(inconsistent_gp2ids)][['GP2ID', 'SampleRepNo', col_to_check]].copy()


    if t.empty:
        return pd.DataFrame()

    # pivot the table
    t_pivot = t.pivot(index='GP2ID', columns='SampleRepNo', values=col_to_check)
    t_pivot.columns = [f"{col_to_check}_{col}" for col in t_pivot.columns]
    # t_prob = t_pivot[t_pivot.apply(lambda row: len(set(row.dropna())) > 1, axis=1)]
    t_prob = t_pivot
    t_prob_process = t_prob.copy() # process changes the row of t_prob so keep the original
    t_prob = t_prob.reset_index()
    t_prob['study'] = t_prob.GP2ID.str.split('_').str[0]
    return t_prob

class StudyManifestHandler:
    def __init__(self, study, master_sheet_path, bucket_name='eu-samplemanifest'):
        """
        Initialize the handler with the study and the path to the master sheet.
        
        Args:
            study (str): The study code.
            master_sheet_path (str): Path to the master sheet CSV file.
            bucket_name (str): Name of the GCS bucket. Defaults to 'eu-samplemanifest'.
        """
        self.study = study
        self.master_sheet_path = master_sheet_path
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.get_bucket(bucket_name)
        self.mf = pd.DataFrame()  # Attribute to store the previous manifests

    def load_previous_manifests(self):
        """
        Loads previous manifests from the master sheet and checks for new manifests in the finalized folder.
        Stores the result in the `self.mf` attribute.
        """
        try:
            mf = pd.read_csv(self.master_sheet_path, low_memory=False)
        except FileNotFoundError:
            raise FileNotFoundError(f"Master sheet path '{self.master_sheet_path}' not found.")
        
        mf = mf[mf['study'] == self.study]
        mid_in_mf = mf['manifest_id'].unique()

        folder_path = f'/content/drive/Shareddrives/EUR_GP2/CIWG/sample_manifest/finalized/{self.study}'

        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Finalized folder path '{folder_path}' not found. Please created the folder.")

        files = glob.glob(os.path.join(folder_path, '*.csv'))

        if not files:
            print(f'No manifest files found in {folder_path}')
            self.mf = mf  # Assign the master sheet data to the attribute
            return

        d = pd.DataFrame({'path': files})
        d['filename'] = d['path'].apply(lambda x: os.path.basename(x))
        d['mid'] = d['filename'].apply(lambda x: x.replace('.csv', '').split('_')[-1])

        if not d['mid'].is_unique:
            raise ValueError('Multiple manifests with the same mid in the finalized folder')

        new_manifest_paths = d[~d['mid'].isin(mid_in_mf)]['path'].tolist()

        if new_manifest_paths:
            print('New manifests in finalized folder not yet in the master sheet:')
            for path_i in new_manifest_paths:
                print(f' Adding: {path_i}')
                df_i = pd.read_csv(path_i)
                mf = pd.concat([mf, df_i], ignore_index=True)
        else:
            print('No new manifest to add from the finalized folder')
        
        if mf.empty:
            print('No manifests found in the master sheet and finalized folder. No consistency check needed.')
        else:
            print(f'Number of samples from all previous submissions:\n{mf["manifest_id"].value_counts()}')
        
            self.mf = mf  # Store the result in the class attribute

    def combine_study_manifests(self, df):
        """
        Combines the current manifest DataFrame with the previous manifest DataFrame stored in `self.mf`.

        Args:
            df (pd.DataFrame): The current manifest DataFrame.

        Returns:
            pd.DataFrame: Combined DataFrame.
        """
        if self.mf.empty:
            raise ValueError("Previous manifests (mf) are empty. Forgot load_previous_manifests?")
        else:
            if len(np.union1d(df.study.unique(), self.mf.study.unique())) > 1:
                raise ValueError('Different study names detected')

            mids = df.manifest_id.unique()
            if len(mids) > 1:
                raise ValueError(f'More than one mid in the current df: {mids}')
            else:
                mid = mids[0]
                print(f'manifest_id of the current df: {mid}')
                mid_no = self.mf.manifest_id.str.replace('m', '').astype(int).max() + 1
                if mid != f'm{mid_no}':
                    raise ValueError(f'manifest_id should be m{mid_no}?')

            print(f'Combined with: {self.mf.manifest_id.unique()}')
            df_all = pd.concat([self.mf, df], ignore_index=True)

            rm_cols = np.intersect1d(df_all.columns, ['GP2_PHENO', 'GP2_family_id', 'alternative_id3', 'alternative_id4'])
            if len(rm_cols) > 0:
                df_all = df_all.drop(columns=rm_cols)

            self.df_all = df_all
    
    def check_inconsistencies(self, columns_to_check):
        """
        Check for inconsistencies in the provided columns of the combined DataFrame.
        
        Args:
            columns_to_check (list): List of columns to check for inconsistencies.
        """
        self.inconsistency = False  # Flag to indicate if inconsistencies are found
        for col_to_check in columns_to_check:
            dt_prob = find_inconsistency(self.df_all, col_to_check)
            if len(dt_prob) > 0:
                file_path = f'inconsistency_{col_to_check}.csv'
                print(f'FAIL: {col_to_check} {len(dt_prob)} entries are inconsistent --> File saved')
                dt_prob.to_csv(file_path, index=False)
                self.inconsistency = True
            else:
                print(f'PASS: {col_to_check}')

        if not self.inconsistency:
            print('All checks passed. No inconsistencies found. Please save the file')