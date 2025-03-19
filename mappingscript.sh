#!/bin/bash
set -x
# Set the main OM DB location
OM_DB="/var/lib/hadoop-ozone/om/data/om.db"
TEMP_DIR="/tmp/ozone_snapshots_analysis"

# Create temp directories
rm -rvf ${TEMP_DIR}/* # Clean previous runs

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
  local file_table="${TEMP_DIR}/${snapshot_id}_file_table.txt"
  local key_table="${TEMP_DIR}/${snapshot_id}_key_table.txt"

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
  ozone debug ldb --db=${snapshot_dir}/om.db scan --cf=fileTable | jq -r 'to_entries[] | select(.value.keyLocationVersions[0].locationVersionMap["0"] != []) | "\(.value.volumeName)/\(.value.bucketName)/\(.value.keyName) \(.value.keyLocationVersions[].locationVersionMap["0"][].blockID.containerBlockID.containerID) \(.value.keyLocationVersions[].locationVersionMap["0"][].blockID.containerBlockID.localID)"' > "${file_table}"

  #To get keys from keyTable:
  ozone debug ldb --db=/var/lib/hadoop-ozone/om/data/om.db scan --cf=keyTable | jq -r 'to_entries[] | "\(.value.volumeName)/\(.value.bucketName)/\(.value.keyName) \(.value.keyLocationVersions[0].locationVersionMap["0"][0].blockID.containerBlockID.containerID) \(.value.keyLocationVersions[0].locationVersionMap["0"][0].blockID.containerBlockID.localID)"'  > "${key_table}"

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
  cat "${TEMP_DIR}/*file_table.txt > "${TEMP_DIR}/file_table_final.txt"
  cat "${TEMP_DIR}/*key_table.txt > "${TEMP_DIR}/key_table_final.txt"

  # Write final results
  echo "Details saved in ${TEMP_DIR}/file_table_final.txt ${TEMP_DIR}/key_table_final.txt"
}

# Main execution
echo "Starting Ozone snapshot analysis for blocks mapping..."
process_snapshots
echo "Analysis complete"
