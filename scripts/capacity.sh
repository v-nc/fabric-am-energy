#!/usr/bin/env bash
# Paid Fabric capacity for the lab: create it once, then keep it paused whenever nobody is working.
# A paused capacity costs nothing for compute (OneLake storage is still billed, at cents per GB and month).
#
#   scripts/capacity.sh budget     # monthly cost budget with e-mail alerts at 50/80/100 % (do this first)
#   scripts/capacity.sh create     # resource group + F4 capacity, then pauses it
#   scripts/capacity.sh resume     # start working
#   scripts/capacity.sh pause      # stop working
#   scripts/capacity.sh status
#   scripts/capacity.sh resize F8  # temporarily bigger (or F2 to save)
#
# Needs `az login` as a user who owns the subscription. Settings can be overridden with environment variables.
set -euo pipefail

RG=${RG:-rg-am-energy}
NAME=${NAME:-amenergyf4}            # lowercase letters and digits only
LOCATION=${LOCATION:-swedencentral} # the tenant's Fabric home region
SKU=${SKU:-F4}
BUDGET=${BUDGET:-20}                # in the subscription's billing currency
API=2023-11-01

sub=$(az account show --query id -o tsv)
admin=${ADMIN:-$(az ad signed-in-user show --query userPrincipalName -o tsv)}
url="https://management.azure.com/subscriptions/$sub/resourceGroups/$RG/providers/Microsoft.Fabric/capacities/$NAME"

state() { az rest --method get --url "$url?api-version=$API" --query "{state: properties.state, sku: sku.name, admins: properties.administration.members}" -o jsonc; }

case "${1:-status}" in
  budget)
    start=$(date -u +%Y-%m-01)
    az rest --method put \
      --url "https://management.azure.com/subscriptions/$sub/providers/Microsoft.Consumption/budgets/am-energy?api-version=2023-05-01" \
      --body "$(cat <<EOF
{
  "properties": {
    "category": "Cost",
    "amount": $BUDGET,
    "timeGrain": "Monthly",
    "timePeriod": { "startDate": "${start}T00:00:00Z" },
    "notifications": {
      "actual50":  { "enabled": true, "operator": "GreaterThanOrEqualTo", "threshold": 50,  "contactEmails": ["$admin"], "thresholdType": "Actual" },
      "actual80":  { "enabled": true, "operator": "GreaterThanOrEqualTo", "threshold": 80,  "contactEmails": ["$admin"], "thresholdType": "Actual" },
      "actual100": { "enabled": true, "operator": "GreaterThanOrEqualTo", "threshold": 100, "contactEmails": ["$admin"], "thresholdType": "Actual" },
      "forecast100": { "enabled": true, "operator": "GreaterThanOrEqualTo", "threshold": 100, "contactEmails": ["$admin"], "thresholdType": "Forecasted" }
    }
  }
}
EOF
)" --query "{name: name, amount: properties.amount, notify: '$admin'}" -o jsonc
    ;;
  create)
    az provider register --namespace Microsoft.Fabric --wait
    az group create --name "$RG" --location "$LOCATION" --query "{group: name, location: location}" -o jsonc
    az rest --method put --url "$url?api-version=$API" --body "$(cat <<EOF
{
  "location": "$LOCATION",
  "sku": { "name": "$SKU", "tier": "Fabric" },
  "properties": { "administration": { "members": ["$admin"] } }
}
EOF
)" -o none
    echo "waiting for the capacity to be provisioned…"
    until [ "$(az rest --method get --url "$url?api-version=$API" --query properties.provisioningState -o tsv)" = "Succeeded" ]; do sleep 10; done
    az rest --method post --url "$url/suspend?api-version=$API" -o none
    state
    ;;
  resume) az rest --method post --url "$url/resume?api-version=$API" -o none && state ;;
  pause)  az rest --method post --url "$url/suspend?api-version=$API" -o none && state ;;
  resize)
    az rest --method patch --url "$url?api-version=$API" --body "{\"sku\": {\"name\": \"${2:?usage: resize F2|F4|F8}\", \"tier\": \"Fabric\"}}" -o none && state
    ;;
  status) state ;;
  delete)
    read -rp "Delete capacity $NAME? Workspaces on it lose their Fabric items after 7 days. [y/N] " ok
    [ "$ok" = y ] && az rest --method delete --url "$url?api-version=$API"
    ;;
  *) sed -n '2,13p' "$0"; exit 1 ;;
esac
