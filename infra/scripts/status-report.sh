#!/usr/bin/env bash
# AirLake: Infrastructure status report. READ-ONLY, makes no changes on AWS.
#
# Run from repository root directory (directory containing infra/), after running: aws login --profile airlake
#   bash infra/scripts/status-report.sh | tee status-report.txt
#   bash infra/scripts/status-report.sh --plan | tee status-report.txt
#
# Options:
#   --plan  run additional "terraform plan" (read-only) to check if the latest apply completed
#   --ce    call Cost Explorer (approx. $0.01 per API call)
#
# Script DOES NOT print secrets: does not read SSM values, does not print access key IDs.

export MSYS_NO_PATHCONV=1
REGION="${AWS_REGION:-ap-southeast-2}"
RUN_PLAN=0
RUN_CE=0
for a in "$@"; do
  case "$a" in
    --plan) RUN_PLAN=1 ;;
    --ce) RUN_CE=1 ;;
  esac
done

[ -d infra ] || { echo "Please run this script from the repo root directory (the directory containing infra/)."; exit 1; }

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null | tr -d '\r')
if [ -z "$ACCOUNT_ID" ]; then
  echo "Not logged into AWS or session has expired. Run: aws login --profile airlake"
  exit 1
fi
STATE_BUCKET="airlake-tfstate-${ACCOUNT_ID}"
LAKE="airlake-${ACCOUNT_ID}-lake"

section() { printf '\n==== %s ====\n' "$1"; }
run() { echo "\$ $*"; "$@" 2>&1 | tr -d '\r' | head -n 60; echo; }

# ---------------------------------------------------------------
section "0. Context"
echo "Timestamp : $(date '+%Y-%m-%d %H:%M:%S %z')"
echo "Account   : $ACCOUNT_ID"
echo "Region    : $REGION"
echo "Identity  : $(aws sts get-caller-identity --query Arn --output text | tr -d '\r')"

# ---------------------------------------------------------------
section "1. Terraform stacks status (state on S3)"
for entry in "00-bootstrap:bootstrap" "10-foundation:foundation" "20-compute:compute"; do
  dir="${entry%%:*}"
  key="${entry##*:}/terraform.tfstate"
  echo "--- $dir (state key: $key)"
  info=$(aws s3api head-object --bucket "$STATE_BUCKET" --key "$key" --region "$REGION" \
    --query '[LastModified,ContentLength]' --output text 2>&1 | tr -d '\r')
  if echo "$info" | grep -qiE 'error|not found|404'; then
    echo "State: NOT FOUND on S3 (not applied yet, or state remains local in infra/$dir)"
  else
    echo "State last modified / size (bytes): $info"
    state_json=$(aws s3 cp "s3://$STATE_BUCKET/$key" - --region "$REGION" 2>/dev/null)
    echo "Managed resource count: $(echo "$state_json" | grep -c '"mode": "managed"')"
    echo "Resource types (count):"
    echo "$state_json" | grep -A1 '"mode": "managed"' | grep -oE '"type": "[a-z0-9_]+"' \
      | sed 's/"type": //' | sort | uniq -c | sort -rn | head -25
  fi
  if [ "$RUN_PLAN" = 1 ] && [ -d "infra/$dir/.terraform" ]; then
    tmp=$(mktemp)
    (cd "infra/$dir" && terraform plan -detailed-exitcode -input=false -no-color -lock=false > "$tmp" 2>&1)
    rc=$?
    case $rc in
      0) echo "Plan: MATCHES code (latest apply completed, no pending changes)" ;;
      2) echo "Plan: UNAPPLIED changes exist (incomplete apply, or code modified after last apply):"
         grep -E '^Plan:' "$tmp" ;;
      *) echo "Plan: ERROR (code $rc), last 15 lines:"; tail -n 15 "$tmp" ;;
    esac
    rm -f "$tmp"
  elif [ "$RUN_PLAN" = 1 ]; then
    echo "Plan: skipped (terraform init not run in infra/$dir on this machine)"
  fi
done

# ---------------------------------------------------------------
section "2. 20-compute: running resources and latest error reasons"
echo "EC2 (all instances in region; columns: id, type, state, state reason):"
run aws ec2 describe-instances --region "$REGION" \
  --filters Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].[InstanceId,InstanceType,State.Name,StateReason.Message]' --output text
echo "Recent Auto Scaling activities for airlake-spark (Failed with reason):"
run aws autoscaling describe-scaling-activities --region "$REGION" \
  --auto-scaling-group-name airlake-spark --max-items 5 \
  --query 'Activities[].[StartTime,StatusCode,StatusMessage]' --output text
echo "ALB and WAF:"
run aws elbv2 describe-load-balancers --region "$REGION" \
  --query 'LoadBalancers[].[LoadBalancerName,State.Code]' --output text
run aws wafv2 list-web-acls --scope REGIONAL --region "$REGION" --query 'WebACLs[].Name' --output text

# ---------------------------------------------------------------
section "3. Terraform outputs and main resources"
if [ -d infra/10-foundation/.terraform ]; then
  echo "\$ terraform output (10-foundation)"
  (cd infra/10-foundation && terraform output 2>&1 | tr -d '\r' | head -n 40)
else
  echo "(10-foundation not initialized on this machine, using AWS commands below)"
fi
echo "Region: $REGION"
echo "Bucket:"
run aws s3api list-buckets --query "Buckets[?starts_with(Name,'airlake')].Name" --output text
echo "Glue database:"
run aws glue get-databases --region "$REGION" --query 'DatabaseList[].Name' --output text
echo "Athena workgroup (name, state, bytes scanned cutoff per query limit):"
run aws athena get-work-group --region "$REGION" --work-group airlake \
  --query 'WorkGroup.[Name,State,Configuration.BytesScannedCutoffPerQuery]' --output text
echo "IAM role:"
run aws iam list-roles --query "Roles[?starts_with(RoleName,'airlake')].RoleName" --output text
echo "SSM parameters (name, type, version). Version 1 means actual value is not set yet:"
run aws ssm describe-parameters --region "$REGION" \
  --parameter-filters Key=Name,Option=BeginsWith,Values=/airlake \
  --query 'Parameters[].[Name,Type,Version]' --output text

# ---------------------------------------------------------------
section "4. AWS access: users, MFA, access keys"
users=$(aws iam list-users --query 'Users[].UserName' --output text 2>&1 | tr -d '\r')
if echo "$users" | grep -qiE 'denied|error'; then
  echo "Unable to read IAM user list: $users"
else
  [ -z "$users" ] && echo "(No IAM users found; session might be a role or federated user)"
  for u in $users; do
    mfa=$(aws iam list-mfa-devices --user-name "$u" --query 'length(MFADevices)' --output text | tr -d '\r')
    keys=$(aws iam list-access-keys --user-name "$u" --query 'AccessKeyMetadata[].Status' --output text | tr -d '\r')
    flag=""
    [ "$mfa" = "0" ] && flag="$flag [WARNING: MFA not enabled]"
    [ -n "$keys" ] && flag="$flag [WARNING: access keys present: $keys]"
    echo "- $u | MFA device count: $mfa$flag"
  done
fi
echo "IAM group:"
run aws iam list-groups --query 'Groups[].GroupName' --output text

# ---------------------------------------------------------------
section "5. Costs and billable resources"
echo "Budget (name, limit, actual spend, forecasted spend):"
run aws budgets describe-budgets --account-id "$ACCOUNT_ID" \
  --query 'Budgets[].[BudgetName,BudgetLimit.Amount,CalculatedSpend.ActualSpend.Amount,CalculatedSpend.ForecastedSpend.Amount]' --output table
echo "Hourly billable resources. Expected to be empty when not testing:"
echo "- NAT Gateway:"
run aws ec2 describe-nat-gateways --region "$REGION" --filter Name=state,Values=pending,available \
  --query 'NatGateways[].NatGatewayId' --output text
echo "- Unattached Elastic IPs:"
run aws ec2 describe-addresses --region "$REGION" --query 'Addresses[?AssociationId==null].PublicIp' --output text
echo "- Unattached EBS volumes (available):"
run aws ec2 describe-volumes --region "$REGION" --filters Name=status,Values=available \
  --query 'Volumes[].VolumeId' --output text
echo "- Auto Scaling group:"
run aws autoscaling describe-auto-scaling-groups --region "$REGION" \
  --query 'AutoScalingGroups[].[AutoScalingGroupName,DesiredCapacity]' --output text
echo "S3 storage usage (object count and total size):"
for b in $(aws s3api list-buckets --query "Buckets[?starts_with(Name,'airlake')].Name" --output text | tr -d '\r'); do
  printf '%s: ' "$b"
  aws s3 ls "s3://$b" --recursive --summarize 2>/dev/null | tail -n 2 | tr -d '\r' | tr '\n' ' '
  echo
done
if [ "$RUN_CE" = 1 ]; then
  echo "Cost Explorer, last 7 days (USD per day):"
  run aws ce get-cost-and-usage --time-period "Start=$(date -d '-7 days' +%F),End=$(date +%F)" \
    --granularity DAILY --metrics UnblendedCost \
    --query 'ResultsByTime[].[TimePeriod.Start,Total.UnblendedCost.Amount]' --output text
else
  echo "(Add --ce to view Cost Explorer. Remaining credits: check Billing -> Credits in AWS Console; CLI not supported)"
fi
cat <<'EOF'

How to stop or destroy when not in use:
  cd infra/20-compute && terraform destroy       # compute stack only
  DO NOT destroy infra/10-foundation (contains lake data, network, roles, state)
After destroying, re-run this script: sections 2 and 5 should be empty.
EOF

# ---------------------------------------------------------------
section "6. Cloud contracts: tables, S3 prefixes, write permissions"
for db in airlake_bronze airlake_silver airlake_gold; do
  echo "--- Glue $db (table names, S3 locations)"
  run aws glue get-tables --region "$REGION" --database-name "$db" \
    --query 'TableList[].[Name,StorageDescriptor.Location]' --output text
done
echo "--- Level 1 prefixes and warehouse in lake bucket"
run aws s3 ls "s3://$LAKE/" --region "$REGION"
run aws s3 ls "s3://$LAKE/warehouse/" --region "$REGION"
echo "--- Permissions for each EC2 role"
for r in spark trino superset kafka; do
  pol=$(aws iam list-role-policies --role-name "airlake-ec2-$r" --query 'PolicyNames' --output text 2>&1 | tr -d '\r')
  echo "airlake-ec2-$r: inline policy = $pol"
done
echo "Spark data write policy (Effect, Action, Resource):"
run aws iam get-role-policy --role-name airlake-ec2-spark --policy-name spark-data \
  --query 'PolicyDocument.Statement[].[Effect,Action,Resource]' --output json

echo
echo "==== End of report ===="