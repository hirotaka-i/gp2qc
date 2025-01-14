import json
from datetime import datetime
from google.cloud import storage

# Fixed bucket and storage client
bucket_name = 'eu-samplemanifest'
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)

def remove_sample_ids(ids, study_code):
    """
    Removes specified sample IDs from GP2IDSMAPPER.json for a given study code.
    
    Args:
        ids (list): List of sample IDs to remove.
        study_code (str): The study code associated with the sample IDs.
    """
    
    # Load the ID data from GP2IDSMAPPER.json
    blob_id = bucket.blob('IDSTRACKER/GP2IDSMAPPER.json')
    try:
        masterids = json.loads(blob_id.download_as_text())
    except Exception as e:
        print(f"Error loading ID data: {e}")
        return

    # Backup the current GP2IDSMAPPER.json
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    new_blob_name = f'IDSTRACKER/ARCHIVE/GP2IDSMAPPER_before_rm_{study_code}_{timestamp}.json'
    
    try:
        bucket.copy_blob(blob_id, bucket, new_blob_name)
        print(f"Original GP2IDSMAPPER.json copied to gs://{bucket_name}/{new_blob_name}")
    except Exception as e:
        print(f"Error copying file: {e}")
        return

    # Process ID removals
    if study_code in masterids:
        study_ids = masterids[study_code]
        sample_ids = set(ids)
        master_ids_set = set(study_ids.keys())

        # Check if all sample IDs match the master ID set
        if sample_ids == master_ids_set:
            masterids.pop(study_code)
            print(f"All sample IDs from {study_code} matched and were removed.")
        else:
            ids_to_remove = sample_ids & master_ids_set
            ids_not_in_master = sample_ids - master_ids_set
            
            # Raise an error if there are sample IDs that are not found in the master file
            if ids_not_in_master:
                raise ValueError(f"The following {len(ids_not_in_master)} sample IDs are not in the master file: {ids_not_in_master}")
            
            # Remove the IDs that exist in both sets
            for sample_id in ids_to_remove:
                study_ids.pop(sample_id, None)
            
            print(f"{len(ids_to_remove)} sample IDs have been deleted for {study_code}.")

        # Save the updated masterids to GP2IDSMAPPER.json
        try:
            blob_id.upload_from_string(json.dumps(masterids, indent=4))
            print("Updated GP2IDSMAPPER.json saved successfully.")
        except Exception as e:
            print(f"Error saving updated IDs: {e}")
            return
        print("ID removal process completed.")

    else:
        print(f"No matching study code {study_code} found in GP2IDSMAPPER.json.")
        print("No changes were made.")

