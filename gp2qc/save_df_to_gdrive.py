import os
from .base_check import base_check

def save_df_to_gdrive(processor, root_path='/content/drive/Shareddrives/EUR_GP2/CIWG/sample_manifest/finalized'):
    """
    Save the manifest DataFrame from GP2SampleManifestProcessor to the specified path.
    
    Args:
        processor (GP2SampleManifestProcessor): An instance of the class containing `self.df`.
        root_path (str): Path to save the combined manifest DataFrame.
    """    
    base_check(processor.df)  
    
    # Get the file name from the dataframe
    file_name = processor.df['filename'].unique()

    if len(file_name) > 1:
        raise ValueError(f'More than one file name: {file_name}')

    # Construct the full save path
    if processor.save_file_name != file_name[0]
        raise ValueError(f'{processor.save_file_name} is different from the filename in the df: {file_name[0]}')
    
    save_path = os.path.join(root_path, processor.save_file_name)

    # Check if the subdirectory exists, if not, raise an error
    subdirectory_path = os.path.dirname(save_path)
    if not os.path.exists(subdirectory_path):
        raise ValueError(f"Subdirectory {subdirectory_path} does not exist.")
    
    else:
        # Check if the file exists
        if os.path.exists(save_path):
            user_input = input(f"The file {processor.save_file_name} already exists. Do you want to overwrite it? (yes/no): ").strip().lower()
            if user_input != 'yes':
                print("File not overwritten.")
                return
        
    processor.df.to_csv(save_path, index=False)
    print(f'Saving to the gdrive: {save_path}')