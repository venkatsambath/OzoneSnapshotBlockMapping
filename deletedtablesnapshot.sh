#!/bin/bash
set -x
# Set the main OM DB location
OM_DB="/var/lib/hadoop-ozone/om/data/om.db"
TEMP_DIR="/tmp/ozone_snapshots_deleted_table"

# Create temp directories
rm -rvf ${TEMP_DIR}/* # Clean previous runs
mkdir -p ${TEMP_DIR}

# Function to process a single snapshot entry
process_single_snapshot() {
  local snapshot_entry="$1"

  # Extract snapshot details
  local snapshot_id=$(echo "$snapshot_entry" | jq -r '.snapshotId')
  local snapshot_name=$(echo "$snapshot_entry" | jq -r '.name // "unnamed"')
  local volume_name=$(echo "$snapshot_entry" | jq -r '.volumeName')
  local bucket_name=$(echo "$snapshot_entry" | jq -r '.bucketName')
  local bucket_path="/${volume_name}/${bucket_name}"
  local status=$(echo "$snapshot_entry" | jq -r '.snapshotStatus // "unknown"')


  # Create unique temp files for this snapshot
  local deleted_table="${TEMP_DIR}/${snapshot_id}_deleted_table.txt"

  echo "Processing snapshot ID: $snapshot_id" >&2

  # Create directory for this snapshot
  local snapshot_dir="${TEMP_DIR}/${snapshot_id}"
  mkdir -p "${snapshot_dir}"

  # Copy snapshot DB (simplified error handling)
  if ! cp -r "/var/lib/hadoop-ozone/om/data/db.snapshots/checkpointState/om.db-${snapshot_id}" "${snapshot_dir}/om.db"; then
    echo "Error copying DB for ${snapshot_id}" >&2
    return 1
  fi

  #To get keys from fileTable:
  ozone debug ldb --db=${snapshot_dir}/om.db scan --cf=deletedTable | jq -r 'to_entries[] | select(.value.omKeyInfoList[].keyLocationVersions[0].locationVersionMap["0"] != []) | "\(.value.omKeyInfoList[].volumeName)/\(.value.omKeyInfoList[].bucketName)/\(.value.omKeyInfoList[].keyName) \(.value.omKeyInfoList[].keyLocationVersions[].locationVersionMap["0"][].blockID.containerBlockID.containerID) \(.value.omKeyInfoList[].keyLocationVersions[].locationVersionMap["0"][].blockID.containerBlockID.localID)"' > "${deleted_table}"

}

# Main processing function with concurrency control
process_snapshots() {
  echo "Extracting snapshot information..."
  ozone debug ldb --db="${OM_DB}" scan --cf=snapshotInfoTable > "${TEMP_DIR}/snapshots_info.json"

  # Read all snapshot entries into an array
  mapfile -t snapshot_entries < <(jq -c '.[]' "${TEMP_DIR}/snapshots_info.json")

  # Set concurrency level (adjust based on your system)
  local max_jobs=4
  local current_jobs=0

  for entry in "${snapshot_entries[@]}"; do
    # Limit concurrent jobs
    if (( current_jobs >= max_jobs )); then
      wait -n
      ((current_jobs--))
    fi

    # Start job in background
    process_single_snapshot "$entry" &
    ((current_jobs++))
  done

  # Wait for remaining jobs
  wait

  # Aggregate results
  echo "Aggregating results..."
  cat ${TEMP_DIR}/*deleted_table.txt > "${TEMP_DIR}/deleted_table_final.txt"

  # Write final results
  echo "Details saved in ${TEMP_DIR}/deleted_table_final.txt"
}

# Main execution
echo "Starting Ozone snapshot analysis for blocks mapping..."
process_snapshots
echo "Analysis complete"
