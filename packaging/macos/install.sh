#!/bin/zsh
set -euo pipefail
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

usage() {
  print -u2 "usage: sudo $0 --binary PATH --control-plane URL --workspace ID (--token-stdin | --token TOKEN) [--user USER] [--agent-id ID] [--labels LABEL,...] [--runner-group GROUP] [--parallelism N] [--cache-max-bytes N] [--cache-max-entry-bytes N] [--artifact-max-bytes N] [--artifact-max-entry-bytes N]"
  exit 64
}

binary=""
control_plane=""
workspace=""
agent_token=""
token_from_stdin="false"
agent_user="${SUDO_USER:-}"
agent_id="$(scutil --get LocalHostName 2>/dev/null || hostname -s)"
labels=""
runner_group=""
parallelism="1"
cache_max_bytes="10737418240"
cache_max_entry_bytes="2147483648"
artifact_max_bytes="10737418240"
artifact_max_entry_bytes="2147483648"

while (( $# > 0 )); do
  case "$1" in
    --binary) binary="${2:-}"; shift 2 ;;
    --control-plane) control_plane="${2:-}"; shift 2 ;;
    --workspace) workspace="${2:-}"; shift 2 ;;
    --token) agent_token="${2:-}"; shift 2 ;;
    --token-stdin) token_from_stdin="true"; shift ;;
    --user) agent_user="${2:-}"; shift 2 ;;
    --agent-id) agent_id="${2:-}"; shift 2 ;;
    --labels) labels="${2:-}"; shift 2 ;;
    --runner-group) runner_group="${2:-}"; shift 2 ;;
    --parallelism) parallelism="${2:-}"; shift 2 ;;
    --cache-max-bytes) cache_max_bytes="${2:-}"; shift 2 ;;
    --cache-max-entry-bytes) cache_max_entry_bytes="${2:-}"; shift 2 ;;
    --artifact-max-bytes) artifact_max_bytes="${2:-}"; shift 2 ;;
    --artifact-max-entry-bytes) artifact_max_entry_bytes="${2:-}"; shift 2 ;;
    *) usage ;;
  esac
done

if [[ "$token_from_stdin" == "true" ]]; then
  [[ -z "$agent_token" ]] || usage
  if ! IFS= read -r agent_token; then
    [[ -n "$agent_token" ]] || { print -u2 "agent token stdin was empty"; exit 64; }
  fi
fi

[[ "$EUID" == 0 ]] || { print -u2 "install must run as root"; exit 77; }
[[ -f "$binary" && -x "$binary" ]] || usage
[[ -n "$control_plane" && -n "$workspace" && -n "$agent_token" ]] || usage
[[ -n "$agent_user" && "$agent_user" != "root" ]] || {
  print -u2 "choose a dedicated non-root macOS user with --user"
  exit 77
}
id "$agent_user" >/dev/null 2>&1 || { print -u2 "user '$agent_user' does not exist"; exit 67; }
[[ "$labels" != *$'\n'* && "$labels" != *$'\r'* ]] || usage
[[ "$runner_group" != *$'\n'* && "$runner_group" != *$'\r'* ]] || usage
[[ "$parallelism" == <-> && "$parallelism" -ge 1 ]] || usage
[[ "$cache_max_bytes" == <-> && "$cache_max_bytes" -ge 1 ]] || usage
[[ "$cache_max_entry_bytes" == <-> && "$cache_max_entry_bytes" -ge 1 ]] || usage
(( cache_max_bytes >= cache_max_entry_bytes )) || usage
[[ "$artifact_max_bytes" == <-> && "$artifact_max_bytes" -ge 1 ]] || usage
[[ "$artifact_max_entry_bytes" == <-> && "$artifact_max_entry_bytes" -ge 1 ]] || usage
(( artifact_max_bytes >= artifact_max_entry_bytes )) || usage
command -v node >/dev/null 2>&1 || {
  print -u2 "Node.js is required for JavaScript actions; install Node 24 before the agent"
  exit 69
}

script_dir="${0:A:h}"
template="$script_dir/com.gitzero.agent.plist"
destination="/Library/LaunchDaemons/com.gitzero.agent.plist"
installed_binary="/usr/local/libexec/gitzero-agent"
work_root="/Library/Application Support/GitZero/work"
log_root="/Library/Logs/GitZero"
temporary_plist="$(mktemp /tmp/gitzero-agent.XXXXXX.plist)"
trap 'rm -f "$temporary_plist"' EXIT

install -d -o root -g wheel -m 0755 "${installed_binary:h}"
install -o root -g wheel -m 0755 "$binary" "$installed_binary"
install -d -o "$agent_user" -g staff -m 0700 "$work_root"
install -d -o "$agent_user" -g staff -m 0750 "$log_root"
install -m 0600 "$template" "$temporary_plist"

plutil -replace ProgramArguments.0 -string "$installed_binary" "$temporary_plist"
plutil -replace EnvironmentVariables.GITZERO_CONTROL_PLANE -string "$control_plane" "$temporary_plist"
plutil -replace EnvironmentVariables.GITZERO_WORKSPACE_ID -string "$workspace" "$temporary_plist"
plutil -replace EnvironmentVariables.GITZERO_AGENT_TOKEN -string "$agent_token" "$temporary_plist"
plutil -replace EnvironmentVariables.GITZERO_AGENT_ID -string "$agent_id" "$temporary_plist"
plutil -replace EnvironmentVariables.GITZERO_AGENT_NAME -string "$agent_id" "$temporary_plist"
plutil -replace EnvironmentVariables.GITZERO_WORK_ROOT -string "$work_root" "$temporary_plist"
if [[ -n "$labels" ]]; then
  plutil -insert EnvironmentVariables.GITZERO_LABELS -string "$labels" "$temporary_plist"
fi
if [[ -n "$runner_group" ]]; then
  plutil -insert EnvironmentVariables.GITZERO_RUNNER_GROUP -string "$runner_group" "$temporary_plist"
fi
plutil -replace EnvironmentVariables.GITZERO_MAX_PARALLELISM -string "$parallelism" "$temporary_plist"
plutil -replace EnvironmentVariables.GITZERO_CACHE_MAX_BYTES -string "$cache_max_bytes" "$temporary_plist"
plutil -replace EnvironmentVariables.GITZERO_CACHE_MAX_ENTRY_BYTES -string "$cache_max_entry_bytes" "$temporary_plist"
plutil -replace EnvironmentVariables.GITZERO_ARTIFACT_MAX_BYTES -string "$artifact_max_bytes" "$temporary_plist"
plutil -replace EnvironmentVariables.GITZERO_ARTIFACT_MAX_ENTRY_BYTES -string "$artifact_max_entry_bytes" "$temporary_plist"
plutil -insert UserName -string "$agent_user" "$temporary_plist"
plutil -lint "$temporary_plist"
install -o root -g wheel -m 0600 "$temporary_plist" "$destination"

launchctl bootout system/com.gitzero.agent >/dev/null 2>&1 || true
launchctl bootstrap system "$destination"
launchctl enable system/com.gitzero.agent
launchctl kickstart -k system/com.gitzero.agent
print "GitZero agent installed as $agent_user (agent id: $agent_id)."
