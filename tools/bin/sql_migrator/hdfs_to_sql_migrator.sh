#!/bin/bash
# shellcheck disable=SC2006,SC2013,SC2016
# set -eux
# Copyright (c) 2016-2020 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
function usage() {
  echo "hdfs_to_sql_migrator.sh numJobs hdfsCluster dockerTagPath dbUser dbPass dbHost dbPort dbName"
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

  if ! $hdfsCmd -copyToLocal "$tagPath" "$tagFile" &> "$tagFile.out"; then
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

    $hdfsCmd -find "$path" -name "link" 2>/dev/null 1> "$tagPathFile"
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

          waitForCmd "org.apache.hadoop.fs.FsShell" "$maxJobs" &> /dev/null
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

if [[ $# -ne 8 ]]; then
  usage;
  exit 1
fi
maxJobs=$1
hdfsCluster=$2
dockerTagPath=$3
dbUser=$4
dbPass=$5
dbHost=$6
dbPort=$7
dbName=$8

hdfsCmd="hdfs dfs"
if ! command -v hdfs > /dev/null; then
  echo "ERROR: This script requires the 'hdfs' command-line utility to be installed"
  echo "       Please install before continuing."
  exit 1
fi

dbCmd="mysql"
if ! command -v mysql > /dev/null; then
  echo "ERROR: This script requires either the 'mysql' command-line utilitiy to be installed"
  echo "       Please install before continuing."
  exit 1
fi

echo "Scanning for tags in $dockerTagPath using max $maxJobs jobs"
echo ""
echo "USE THE FOLLOWING COMMAND TO STOP THE SCRIPT"
echo '============> kill $(pgrep -f org.apache.hadoop.fs.FsShell) $(pgrep -f hdfs_to_sql_migrator.sh) <============'
echo ""
sleep 5

scanOut=$(pwd)/$(stringifyPath "$hdfsCluster")
echo "Starting scan at `date`"
for namespace in $($hdfsCmd -ls "$dockerTagPath" 2>/dev/null | grep -E '^d.*' | awk '{print $8}'); do
  for repo in $($hdfsCmd -ls "$namespace" 2>/dev/null | grep -E '^d.*' | awk '{print $8}'); do
    pathToScan=$repo
    # For repos that only have a single level, use the namespace
    if [[ $(basename "$repo") == "_manifests" ]]; then
      pathToScan=$namespace
    fi
    outputPath=$scanOut/$(stringifyPath "$pathToScan")
    waitForCmd "org.apache.hadoop.fs.FsShell" "$maxJobs"
    scanRun "$pathToScan" "$outputPath" &
  done
done
waitForJobs
echo "Scan complete at `date`"
