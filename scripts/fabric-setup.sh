#!/usr/bin/env bash
# Creates the Fabric workspace and connects it to this repository, through the Fabric REST API. Safe to re-run: every
# step looks for what already exists first.
#
#   scripts/capacity.sh resume && scripts/fabric-setup.sh
#
# Needs `az login` (a Fabric administrator of the tenant) and GITHUB_PAT in .env: a fine-grained token limited to this
# repository with Contents read and write.
set -euo pipefail
cd "$(dirname "$0")/.."

WORKSPACE=${WORKSPACE:-am-energy-dev}
CAPACITY=${CAPACITY:-amenergyf4}
OWNER=${OWNER:-v-nc}
REPO=${REPO:-fabric-am-energy}
BRANCH=${BRANCH:-main}
FOLDER=${FOLDER:-/fabric}
RUNTIME=${RUNTIME:-2.0}
CONNECTION=${CONNECTION:-github-$REPO}

api=https://api.fabric.microsoft.com/v1
fab() { az rest --resource https://api.fabric.microsoft.com "$@" 2> >(grep -v -e Warning -e SyntaxWarning -e excludedActions >&2); }
tmp=$(mktemp -d) && chmod 700 "$tmp" && trap 'rm -rf "$tmp"' EXIT

capacity_id=$(fab --url "$api/capacities" --query "value[?displayName=='$CAPACITY'].id | [0]" -o tsv)
[ -n "$capacity_id" ] || { echo "capacity $CAPACITY not found or not active (scripts/capacity.sh resume)"; exit 1; }

ws=$(fab --url "$api/workspaces" --query "value[?displayName=='$WORKSPACE'].id | [0]" -o tsv)
if [ -z "$ws" ]; then
  ws=$(fab --method post --url "$api/workspaces" --query id -o tsv --body "{
    \"displayName\": \"$WORKSPACE\", \"capacityId\": \"$capacity_id\",
    \"description\": \"fabric-am-energy portfolio project (simulated data). Items are versioned in GitHub $OWNER/$REPO$FOLDER.\"}")
  echo "created workspace $WORKSPACE ($ws)"
else
  echo "workspace $WORKSPACE exists ($ws)"
fi

fab --method patch --url "$api/workspaces/$ws/spark/settings" --body "{\"environment\": {\"runtimeVersion\": \"$RUNTIME\"}}" \
  --query "{runtime: environment.runtimeVersion}" -o tsv | sed 's/^/spark runtime /'

conn=$(fab --url "$api/connections" --query "value[?displayName=='$CONNECTION'].id | [0]" -o tsv)
if [ -z "$conn" ]; then
  set -a; . ./.env; set +a
  : "${GITHUB_PAT:?GITHUB_PAT missing in .env}"
  # The token goes into a private temp file, never onto the command line or into the output.
  python3 - "$tmp/conn.json" <<EOF
import json, os, sys
json.dump({
  "connectivityType": "ShareableCloud",
  "displayName": "$CONNECTION",
  "connectionDetails": {
    "type": "GitHubSourceControl",
    "creationMethod": "GitHubSourceControl.Contents",
    "parameters": [{"dataType": "Text", "name": "url", "value": "https://github.com/$OWNER/$REPO"}],
  },
  "credentialDetails": {"credentials": {"credentialType": "Key", "key": os.environ["GITHUB_PAT"]}},
}, open(sys.argv[1], "w"))
EOF
  conn=$(fab --method post --url "$api/connections" --body "@$tmp/conn.json" --query id -o tsv)
  echo "created connection $CONNECTION ($conn)"
else
  echo "connection $CONNECTION exists ($conn)"
fi

provider=$(fab --url "$api/workspaces/$ws/git/connection" --query gitConnectionState -o tsv)
if [ "$provider" = "NotConnected" ]; then
  fab --method post --url "$api/workspaces/$ws/git/connect" --body "{
    \"gitProviderDetails\": {\"gitProviderType\": \"GitHub\", \"ownerName\": \"$OWNER\", \"repositoryName\": \"$REPO\",
                             \"branchName\": \"$BRANCH\", \"directoryName\": \"$FOLDER\"},
    \"myGitCredentials\": {\"source\": \"ConfiguredConnection\", \"connectionId\": \"$conn\"}}" -o none
  echo "connected workspace to $OWNER/$REPO@$BRANCH:$FOLDER"
fi
if [ "$(fab --url "$api/workspaces/$ws/git/connection" --query gitConnectionState -o tsv)" = "Connected" ]; then
  fab --method post --url "$api/workspaces/$ws/git/initializeConnection" --body '{"initializationStrategy": "PreferWorkspace"}' \
    --query "{requiredAction: requiredAction, remoteCommit: remoteCommitHash}" -o jsonc
fi
fab --url "$api/workspaces/$ws/git/connection" --query "{state: gitConnectionState, details: gitProviderDetails, sync: gitSyncDetails}" -o jsonc
