#!/bin/bash

# enable !! command completion
set -o history -o histexpand

source "$(dirname ${0})/colors.sh"

# Allow controlled exit on error
function exit_on_err() {
    "$@"
    EXIT_CODE=$?
    if [ ${EXIT_CODE} -ne 0 ]; then
        >&2 echo "\"${@}\" command failed with exit code ${EXIT_CODE}."
        exit ${EXIT_CODE}
    fi
}

# tools definitions
K8S_CLIENT=${K8S_CLIENT:-kubectl}
OP_SDK=${OP_SDK:-operator-sdk}

# Bundle definition
IMAGE_REPO=${IMAGE_REPO:-${1}}
if [[ -z ${IMAGE_REPO} ]]; then
   red "Error: target repository was not set via a command line argument or the IMAGE_REPO environment variable"
   exit 1
fi
# Default bundle dir
OUTPUT_DIR=./tools/bundle
# Bundle image
BUNDLE_NAME=${BUNDLE_NAME:-ocs-osd-deployer-bundle}
BUNDLE_VERSION=${BUNDLE_VERSION:-latest}
BUNDLE_IMAGE=${BUNDLE_IMAGE:-${IMAGE_REPO}/${BUNDLE_NAME}:${BUNDLE_VERSION}}

# Deployer image
DEPLOYER_NAME=${DEPLOYER_NAME:-ocs-osd-deployer}
DEPLOYER_VERSION=${DEPLOYER_VERSION:-latest}
DEPLOYER_IMAGE=${DEPLOYER_IMAGE:-${IMAGE_REPO}/${DEPLOYER_NAME}:${DEPLOYER_VERSION}}

# Deploy target
TARGET_NAMESPACE=${TARGET_NAMESPACE:-openshift-storage}

# CSV environment variables
ADDON_NAME=${ADDON_NAME:-ocs-converged}
BUNDLE_FILE=${OUTPUT_DIR}/manifests/ocs-osd-deployer.clusterserviceversion.yaml
CLUSTER_SIZE=${CLUSTER_SIZE:-1}
ENABLE_MCG_FLAG=${ENABLE_MCG_FLAG:-false}

# Generate the deployer image 
blue "Generate deployer image: ${DEPLOYER_IMAGE}"
exit_on_err make docker-build IMG=${DEPLOYER_IMAGE}
exit_on_err make docker-push IMG=${DEPLOYER_IMAGE}

# Generate the olm bundle image
blue "Generate and push the olm bundle image: ${BUNDLE_IMAGE}"
exit_on_err make bundle IMG=${DEPLOYER_IMAGE} OUTPUT_DIR=${OUTPUT_DIR}
exit_on_err sed -i "s|- name: ADDON_NAME|- {name: ADDON_NAME, value: ${ADDON_NAME}}|" ${BUNDLE_FILE}
exit_on_err sed -i "s|- name: SOP_ENDPOINT|- {name: SOP_ENDPOINT, value: \'${SOP_ENDPOINT}\'}|" ${BUNDLE_FILE}
exit_on_err make bundle-build BUNDLE_IMG=${BUNDLE_IMAGE}
exit_on_err make docker-push IMG=${BUNDLE_IMAGE}

# Install operator prereqs on cluster
# In install olm if needed
blue "Checking for existing olm"
if ${K8S_CLIENT} get namespace olm >/dev/null 2>&1
then
    blue "olm found"
elif ${K8S_CLIENT} get namespace openshift-operator-lifecycle-manager &> /dev/null
then
    blue "olm found"
else 
    blue "olm not found, installing olm in target cluster"
    exit_on_err ${OP_SDK} olm install     
fi

blue "Creating ${TARGET_NAMESPACE} on cluster"
exit_on_err ${K8S_CLIENT} create namespace ${TARGET_NAMESPACE} --dry-run=client -o yaml | ${K8S_CLIENT} -n ${TARGET_NAMESPACE} apply -f -

blue "Creating catalogsource containing needed dependencies on cluster"
exit_on_err ${K8S_CLIENT} -n ${TARGET_NAMESPACE} apply -f ./tools/yamls/ocs-catalogsource.yaml

blue "Creating secrets needed for the operator to run without error"
exit_on_err ${K8S_CLIENT} -n ${TARGET_NAMESPACE} create secret generic addon-${ADDON_NAME}-parameters --from-literal=size=${CLUSTER_SIZE} --from-literal=enable-mcg=${ENABLE_MCG_FLAG} --dry-run=client -o yaml | ${K8S_CLIENT} -n ${TARGET_NAMESPACE} apply -f -
exit_on_err ${K8S_CLIENT} -n ${TARGET_NAMESPACE} create secret generic ${ADDON_NAME}-pagerduty --from-literal=PAGERDUTY_KEY=${PD_KEY} --dry-run=client -o yaml | ${K8S_CLIENT} -n ${TARGET_NAMESPACE} apply -f -
exit_on_err ${K8S_CLIENT} -n ${TARGET_NAMESPACE} create secret generic ${ADDON_NAME}-deadmanssnitch --from-literal=SNITCH_URL=${SNITCH_URL} --dry-run=client -o yaml | ${K8S_CLIENT} -n ${TARGET_NAMESPACE} apply -f -

# Deploy the bundle
blue "Deploy/Run the ${BUNDLE_NAME}:${BUNDLE_VERSION} in the cluster (at ${TARGET_NAMESPACE})"

exit_on_err ${OP_SDK} run bundle ${BUNDLE_IMAGE} -n ${TARGET_NAMESPACE}
