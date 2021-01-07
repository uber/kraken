#!/bin/bash
# shellcheck disable=SC2006,SC2013,SC2016
# set -eux

function usage() {
  echo "s3_to_sql_migrator.sh numJobs dockerTagPath dbUser dbPass dbHost dbPort dbName"
}

function waitForCmd() {
  while [[ "$(pgrep -f "$1" | wc -l)" -ge "$2" ]]; do
    echo "Max jobs of $2 reached. Waiting for commands to complete..."
    sleep 10
  done
}

function waitForJobs() {
  local runningJobs=0
  while true; do
    local lastRunning=$runningJobs
    runningJobs=$(jobs | grep -c Running)
    if [[ "$runningJobs" -gt 0 ]]; then
      if [[ "$runningJobs" != "$lastRunning" ]]; then
        echo "Waiting for $runningJobs jobs to complete..."
      fi
      sleep 10
    else
      return
    fi
  done
}

function addTagToSQL() {
  local baseRepo=$1
  local tagPath=$2
  local tagFile=$3
  local repo=$4
  local tag=$5
  local sqlFile=$6
  local totalTags=$7

  if ! $s3Cmd cp "s3://$bucket/$tagPath" "$tagFile" &> "$tagFile.out"; then
    echo "[$baseRepo] ERROR copying tag $tagPath!"
    rm "$tagFile" &> /dev/null
    return
  fi

  local imageID=""
  imageID=$(cat "$tagFile")
  echo "INSERT INTO $dbName.tags (repository,tag,image_id,created_at,updated_at) VALUES('$repo','$tag','$imageID',NOW(),NOW());" >> "$sqlFile"

  (( sqlCnt=$(wc -l "$sqlFile" | awk '{print $1}') ))
  if ! (( sqlCnt % 100 )); then
    echo "[$baseRepo] Added $sqlCnt/$totalTags tags"
  fi
}

function scanRun() {
    local path=$1
    local outputPath=$2

    local doneFile=$outputPath/DONE
    local tagPathFile=$outputPath/tags
    local sqlFile=$outputPath/insert.sql

    local baseRepo=""
    baseRepo=$(echo "$path" | sed -e 's/^.*\/repositories\/]*//' | sed -e 's/\/_manifests\/.*//')

    if [[ -f $doneFile ]]; then
      # Uncomment this for more detailed output
      # echo "[$baseRepo] Already migrated $path!"
      return
    fi

    echo "[$baseRepo] Migrating $path..."
    mkdir -p "$outputPath" > /dev/null

    $s3Cmd ls --recursive "$path" | awk '{print $4}' | grep 'current/link' 2>/dev/null > "$tagPathFile"
    (( totalTags=$(wc -l "$tagPathFile" | awk '{print $1}') ))
    echo "[$baseRepo] Found $totalTags tags"
    local repo=""
    local tag=""
    local imageID=""
    local tagFile=""

    if (( totalTags > 0)); then
        echo "[$baseRepo] Building SQL commands..."
        for tagPath in $(cat "$tagPathFile"); do
          repo=$(echo "$tagPath" | sed -e 's/^.*\/repositories\/]*//' | sed -e 's/\/_manifests\/.*//')
          tag=$(echo "$tagPath" | sed -e 's/^.*\/_manifests\/tags\/]*//' | sed -e 's/\/current\/link//')
          tagFile="$outputPath/$(stringifyPath "$repo")__$tag"

          if [[ -f $tagFile ]] && [[ -f $sqlFile ]]; then
            if grep "$repo" "$sqlFile" | grep "$tag" > /dev/null; then
              # Uncomment this for more detailed output
              # echo "[$baseRepo] $repo:$tag has already been added to SQL file. Skipping..."
              continue
            fi
            # If we downloaded it but didn't put it in the SQL file, delete it in case something went wrong
            rm "$tagFile" > /dev/null
            rm "$tagFile.out" > /dev/null
          fi

          waitForCmd "$s3Cmd" "$maxJobs" &> /dev/null
          addTagToSQL "$baseRepo" "$tagPath" "$tagFile" "$repo" "$tag" "$sqlFile" "$totalTags" &
        done
        sleep 10
        waitForCmd "$path" "1"

        # Check for duplicate lines
        sort -u "$sqlFile" > "${sqlFile}.tmp"
        mv "${sqlFile}.tmp" "$sqlFile"
        
        (( totalSQL=$(wc -l "$sqlFile" | awk '{print $1}') ))
        echo "[$baseRepo] Created $totalSQL SQL insert statements"

        if [[ "$totalTags" == "$totalSQL" ]]; then
          echo "[$baseRepo] Inserting tags for into database..."
          # Filter out duplicate errors (SQL error 1062) because we may be re-running the script to catch updates
          $dbCmd -u "$dbUser" -p"$dbPass" -h "$dbHost" -P "$dbPort" -f < "$sqlFile" 2>&1 | grep -v 'ERROR 1062'
          echo "[$baseRepo] Database updated"
        else
          echo "[$baseRepo] Mismatch between number of tags ($totalTags) and number of SQL statements ($totalSQL)!"
          return
        fi
        echo "[$baseRepo] Migration complete for $path"
    fi
    touch "$doneFile"
}

function stringifyPath() {
  echo "$1" | sed 's/\//_/g'
}

if [[ $# -ne 7 ]]; then
  usage;
  exit 1
fi
maxJobs=$1
dockerTagPath=$(echo "$2" | sed -e 's/s3:\/\///')
dbUser=$3
dbPass=$4
dbHost=$5
dbPort=$6
dbName=$7

bucket=$(echo "$dockerTagPath" | awk -F'/' '{print $1}')

s3Cmd="aws s3"
if ! command -v aws > /dev/null; then
  echo "ERROR: This script requires the 'aws' command-line utility to be installed."
  echo "       Please install before continuing."
  exit 1
fi

dbCmd="mysql"
if ! command -v mysql > /dev/null; then
  dbCmd="mycli"
  if ! command -v mycli > /dev/null; then
    echo "ERROR: This script requires either the 'mysql' or 'mycli' command-line untilities to be installed."
    echo "       Please install either tool before continuing."
    exit 1
  fi
fi

if ! $s3Cmd ls > /dev/null; then
  exit $?
fi

echo "Scanning for tags in $dockerTagPath using max $maxJobs jobs"
echo ""
echo "USE THE FOLLOWING COMMAND TO STOP THE SCRIPT"
echo '============> kill $(pgrep -f "aws s3") $(pgrep -f s3_to_sql_migrator.sh) <============'
echo ""
sleep 5

scanOut=$(pwd)/$(stringifyPath "$bucket")
echo "Starting scan at `date`"
for namespace in $($s3Cmd ls "$dockerTagPath" 2>/dev/null | grep -E '^\s+PRE .*' | awk '{print $2}'); do
  for repo in $($s3Cmd ls "${dockerTagPath}${namespace}" 2>/dev/null | grep -E '^\s+PRE .*' | awk '{print $2}'); do
    pathToScan=${dockerTagPath}${namespace}${repo}
    # For repos that only have a single level, use the namespace
    if [[ $(basename "$repo") == "_manifests" ]]; then
      pathToScan=${dockerTagPath}${namespace}
    fi
    outputPath=$scanOut/$(stringifyPath "$pathToScan")
    waitForCmd "$s3Cmd" "$maxJobs"
    scanRun "$pathToScan" "$outputPath" &
  done
done
waitForJobs
echo "Scan complete at `date`"
