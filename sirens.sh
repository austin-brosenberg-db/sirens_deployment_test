#!/usr/bin/env bash

set -o errexit   # abort on nonzero exitstatus
set -o nounset   # abort on unbound variable
set -o pipefail  # don't hide errors within pipes
#set -x

log() {
    echo "${*}" 1>&2
}

logError() {
    echo "${*}" 1>&2
    exit 1
}

checkPrereqs() {
  log "Checking pre-requisites..."
  # check python
  python3 -V | grep "Python 3.11\|Python 3.12" > /dev/null 2>&1 && rc=$? || rc=$?
  [[ $rc -gt 0 ]] && logError "Python 3.11 or 3.12 is required, please install: https://www.python.org/downloads/release/python-3119/"

  # check terraform
  which terraform > /dev/null 2>&1 && rc=$? || rc=$?
  [[ $rc -gt 0 ]] && logError "Terraform is required, please install: https://developer.hashicorp.com/terraform/install"

  log "All pre-requisites satisfied"
}

gitSetup() {
  gitBranch="deploy_${runID}"
  log "Creating branch ${gitBranch} ..."
  git checkout -b "${gitBranch}"
  echo "${gitBranch}"
}

setUpVenv() {
  # create venv
  mkdir -p venvs
  venvName="venvs/venv_${runID}"
  log "Creating Virtual Env ${venvName} ..."
  python3 -m venv "${venvName}"
  source "${venvName}"/bin/activate

  # install python modules
  log "Installing Python module dependencies ..."
  pip3 install -r requirements_dev.txt
  pip3 install -r requirements.txt
  pip3 install setuptools

  pip3 install databricks-sdk
  pip3 install inquirer

  log "Finished creating virtual env with dependencies"
}

buildWheel() {
  rm -r build 2>/dev/null || true
  python3 setup.py clean
  python3 setup.py bdist_wheel
  mkdir -p lib
  cp dist/* lib/
}

usage() {
  log "usage: sirens.sh [--env_prep_only]"
  log "--env_prep_only     only prepares Python env and builds wheel"
}

print_help() {
  log "Interactively generates sirens.config and deploys resources to Databricks"
  usage
}

do_env_prep_only=false

optspec=":h-:"
while getopts "$optspec" optchar; do
    case "${optchar}" in
       -)
            case "${OPTARG}" in
                env_prep_only)
                    do_env_prep_only=true
                    ;;
                *)
                    log "Non-option argument: '-${OPTARG}'" >&2
                    log ""
                    usage
                    exit 1
            esac;;
       h)
            print_help
            exit 0
            ;;
       *)
            if [ "$OPTERR" != 1 ] || [ "${optspec:0:1}" = ":" ]; then
                log "Non-option argument: '-${OPTARG}'" >&2
                log ""
                usage
                exit 1
            fi
            ;;
  esac
done

runID=$(date -u +%Y%m%d%H%M%S)

checkPrereqs
gitBranch="$(gitSetup)"
setUpVenv
buildWheel

if $do_env_prep_only; then
  log Finished prepping env and building wheel!
else
  python3 sirens.py auto_deploy
  log Finished deploy!
fi


