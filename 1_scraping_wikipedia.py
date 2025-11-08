#################################################################################################################################################################
###############################   1.  IMPORTING MODULES AND INITIALIZING VARIABLES   ############################################################################
#################################################################################################################################################################

from dotenv import load_dotenv
import os
import requests
import json
import pandas as pd
import glob

pd.options.mode.chained_assignment = None

# Load environment variables
load_dotenv()

###############################   HEADERS (DON'T CHANGE)   #######################################################################################################
headers = {
    'Authorization': 'Bearer ' + os.getenv('BRIGHTDATA_API_KEY'),
    'Content-Type': 'application/json',
}

headers_status = {
    'Authorization': 'Bearer ' + os.getenv('BRIGHTDATA_API_KEY'),
}

# Load keywords from Excel
keywords = pd.read_excel("keywords.xlsx")

#################################################################################################################################################################
###############################   2.  IF SnapshotID IS NOT SET, TRIGGER CREATION OF THE SNAPSHOT   ##############################################################
#################################################################################################################################################################

snapshot_file = os.getenv("SNAPSHOT_STORAGE_FILE")
dataset_folder = os.getenv("DATASET_STORAGE_FOLDER")

# Ensure dataset folder exists
if not os.path.exists(dataset_folder):
    os.makedirs(dataset_folder)

# Ensure snapshot file exists
if not os.path.exists(snapshot_file):
    open(snapshot_file, "w").close()

file_exists = os.path.isfile(snapshot_file)

if not file_exists or os.path.getsize(snapshot_file) == 0:
    print("⚙️  No snapshot found. Creating a new BrightData snapshot...")

    params = {
        "dataset_id": "gd_lr9978962kkjr3nx49",
        "include_errors": "true",
        "type": "discover_new",
        "discover_by": "keyword",
    }

    json_data = []
    for ind in keywords.index:
        json_data.append({
            "keyword": keywords.loc[ind, "Keyword"],
            "pages_load": str(keywords.loc[ind, "Pages"])
        })

    response = requests.post(
        'https://api.brightdata.com/datasets/v3/trigger',
        params=params,
        headers=headers,
        json=json_data
    )

    print("Status Code:", response.status_code)
    print("Response Text:", response.text)

    try:
        result = response.json()
    except Exception as e:
        print("❌ Error parsing JSON. Response was not valid JSON.")
        print("Raw response:", response.text)
        raise e

    # ✅ Write snapshot ID to file
    snapshot_id = str(result.get("snapshot_id", "")).strip()
    if snapshot_id:
        with open(snapshot_file, "w") as f:
            f.write(snapshot_id)
        print(f"✅ Snapshot ID saved to {snapshot_file} ({snapshot_id})")
    else:
        print("❌ Failed to get snapshot_id from BrightData response.")
        exit(1)

else:
    #################################################################################################################################################################
    ###############################   3.  IF SnapshotID IS SET, GET BACK THE CRAWLED DATA   #########################################################################
    #################################################################################################################################################################

    print("📂 Using existing snapshot file:", snapshot_file)

    # Remove any old dataset files
    files = glob.glob(dataset_folder + "*")
    for f in files:
        os.remove(f)

    # Read snapshot ID
    with open(snapshot_file, "r") as f:
        snapshot_id = f.read().strip()

    if not snapshot_id:
        print("❌ snapshot.txt is empty! Please delete it and rerun the script.")
        exit(1)

    print(f"🔍 Checking progress for snapshot: {snapshot_id}")

    response = requests.get(
        'https://api.brightdata.com/datasets/v3/progress/' + snapshot_id,
        headers=headers_status
    )

    if response.status_code != 200:
        print(f"❌ Failed to get snapshot progress: {response.status_code}")
        print(response.text)
        exit(1)

    status = response.json().get('status', 'unknown')
    print("📊 Snapshot status:", status)

    if status == "ready":
        print("✅ Snapshot is ready! Fetching dataset content...")

        response = requests.get(
            'https://api.brightdata.com/datasets/v3/snapshot/' + snapshot_id,
            headers=headers
        )

        if response.status_code == 200:
            data_path = os.path.join(dataset_folder, "data.txt")
            with open(data_path, "wb") as f:
                f.write(response.content)
            print(f"✅ Dataset saved successfully at: {data_path}")
        else:
            print(f"❌ Failed to download dataset: {response.status_code}")
            print(response.text)
    else:
        print("⏳ Snapshot is not ready yet. Please try again later.")
