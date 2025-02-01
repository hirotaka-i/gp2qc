import json
from datetime import datetime
from google.cloud import storage

# Fixed bucket and storage client
bucket_name = 'eu-samplemanifest'
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)

def remove_sample_ids_from_study(masterids, sample_ids, study_code):
    """
    Removes sample IDs from the given study in masterids if they exist.
    """
    if study_code in masterids:
        study_ids = masterids[study_code]
        sample_ids_set = set(sample_ids)
        master_ids_set = set(study_ids.keys())

        ids_to_remove = sample_ids_set & master_ids_set
        ids_not_in_master = sample_ids_set - master_ids_set
        
        if ids_not_in_master:
            raise ValueError(f"The following {len(ids_not_in_master)} sample IDs are not in the master file: {ids_not_in_master}")

        for sample_id in ids_to_remove:
            study_ids.pop(sample_id, None)

        print(f"{len(ids_to_remove)} sample IDs have been deleted for {study_code}.")

def remove_sample_ids(sample_ids, study_code):
    """
    Removes specified sample IDs from GP2IDSMAPPER.json for a given study code.
    Additionally, if the study_code is "PPMI-N" or "PPMI-G", removes those IDs from both.
    """
    # Load the ID data from GP2IDSMAPPER.json
    blob_id = bucket.blob('IDSTRACKER/GP2IDSMAPPER.json')
    try:
        masterids = json.loads(blob_id.download_as_text())
    except Exception as e:
        print(f"Error loading ID data: {e}")
        return

    # Remove sample IDs from the specified study
    remove_sample_ids_from_study(masterids, sample_ids, study_code)

    # If the study is "PPMI-N" or "PPMI-G", also remove sample IDs from both studies
    if study_code in {"PPMI-N", "PPMI-G"}:
        other_study = "PPMI-G" if study_code == "PPMI-N" else "PPMI-N"
        print(f'Also remove sample_ids from {other_study}')
        remove_sample_ids_from_study(masterids, sample_ids, other_study)

    # Backup the current GP2IDSMAPPER.json after successful removal
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    new_blob_name = f'IDSTRACKER/ARCHIVE/GP2IDSMAPPER_before_rm_{study_code}_{timestamp}.json'
    
    try:
        bucket.copy_blob(blob_id, bucket, new_blob_name)
        print(f"Original GP2IDSMAPPER.json copied to gs://{bucket_name}/{new_blob_name}")
    except Exception as e:
        print(f"Error copying file: {e}")
        return

    # Save the updated masterids to GP2IDSMAPPER.json
    try:
        blob_id.upload_from_string(json.dumps(masterids, indent=4))
        print("Updated GP2IDSMAPPER.json saved successfully.")
    except Exception as e:
        print(f"Error saving updated IDs: {e}")
        return
    
    print("ID removal process completed.")
