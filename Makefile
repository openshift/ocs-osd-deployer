# Current Operator version
VERSION ?= 0.0.1
# Default bundle image tag
IMAGE_TAG_BASE ?= controller
BUNDLE_IMG ?= $(IMAGE_TAG_BASE)-bundle:v$(VERSION)
# Options for 'bundle-build'
ifneq ($(origin CHANNELS), undefined)
BUNDLE_CHANNELS := --channels=$(CHANNELS)
endif
ifneq ($(origin DEFAULT_CHANNEL), undefined)
BUNDLE_DEFAULT_CHANNEL := --default-channel=$(DEFAULT_CHANNEL)
endif
BUNDLE_METADATA_OPTS ?= $(BUNDLE_CHANNELS) $(BUNDLE_DEFAULT_CHANNEL)

OUTPUT_DIR ?= bundle
BUNDLE_FLAGS = --output-dir=$(OUTPUT_DIR)

# Image URL to use all building/pushing image targets
IMG ?= ocs-osd-deployer:latest
# Produce CRDs that work back to Kubernetes 1.11 (no version conversion)
CRD_OPTIONS ?= "crd:trivialVersions=true"

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

OS = $(shell go env GOOS)
ARCH = $(shell go env GOARCH)


all: manager readinessServer

export_env_vars:
export NAMESPACE = openshift-storage
export ADDON_NAME = ocs-converged
export SOP_ENDPOINT = https://red-hat-storage.github.io/ocs-sop/sop/OSD/{{ .GroupLabels.alertname }}.html

# Run tests
ENVTEST_ASSETS_DIR = $(shell pwd)/testbin
test: generate fmt vet manifests
	mkdir -p $(ENVTEST_ASSETS_DIR)
	test -f $(ENVTEST_ASSETS_DIR)/setup-envtest.sh || curl -sSLo $(ENVTEST_ASSETS_DIR)/setup-envtest.sh https://raw.githubusercontent.com/kubernetes-sigs/controller-runtime/v0.6.3/hack/setup-envtest.sh
	source $(ENVTEST_ASSETS_DIR)/setup-envtest.sh; fetch_envtest_tools $(ENVTEST_ASSETS_DIR); setup_envtest_env $(ENVTEST_ASSETS_DIR); go test ./... -coverprofile cover.out

# Build manager binary
manager: generate fmt vet
	go build -o bin/manager main.go

# Build readiness probe binary
readinessServer: fmt vet
	go build -o bin/readinessServer readinessProbe/main.go

# Run against the configured Kubernetes cluster in ~/.kube/config
run: generate fmt vet manifests export_env_vars
	kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -
	kubectl create secret generic addon-${ADDON_NAME}-parameters -n ${NAMESPACE} --from-literal=size=1 --from-literal=enable-mcg=false --dry-run=client -oyaml | kubectl apply -f -
	kubectl create secret generic ${ADDON_NAME}-pagerduty -n ${NAMESPACE} --from-literal PAGERDUTY_KEY="test-key" --dry-run=client -oyaml | kubectl apply -f -
	kubectl create secret generic ${ADDON_NAME}-deadmanssnitch -n ${NAMESPACE} --from-literal SNITCH_URL="https://test-url" --dry-run=client -oyaml | kubectl apply -f -
	kubectl create configmap rook-ceph-operator-config -n ${NAMESPACE} --dry-run=client -oyaml | kubectl apply -f -
	echo -e "apiVersion: operators.coreos.com/v1alpha1" \
	      "\nkind: ClusterServiceVersion" \
		  "\nmetadata:" \
		  "\n  name: ocs-operator-0.1" \
		  "\n  namespace: ${NAMESPACE}" \
		  "\nspec:" \
		  "\n  displayName: ocs operator" \
		  "\n  install:" \
		  "\n    spec:" \
		  "\n      deployments:" \
		  "\n      - name: test" \
		  "\n        spec:" \
		  "\n          selector:" \
		  "\n            matchLabels:" \
		  "\n              app: test" \
		  "\n          template:" \
		  "\n            metadata:" \
		  "\n              labels:" \
		  "\n                app: test" \
		  "\n            spec:" \
		  "\n              containers:" \
		  "\n              - name: test" \
		  "\n    strategy: deployment" | kubectl apply -f -
	go run ./main.go

# Install CRDs into a cluster
install: manifests kustomize
	$(KUSTOMIZE) build config/crd | kubectl apply -f -
	./shim/shim.sh install

# Uninstall CRDs from a cluster
uninstall: manifests kustomize
	$(KUSTOMIZE) build config/crd | kubectl delete -f -
	./shim/shim.sh uninstall

# Deploy controller in the configured Kubernetes cluster in ~/.kube/config
deploy: manifests kustomize
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default | kubectl apply -f -

# Generate manifests e.g. CRD, RBAC etc.
manifests: controller-gen
	$(CONTROLLER_GEN) $(CRD_OPTIONS) rbac:roleName=manager-role webhook paths="./..." output:crd:artifacts:config=config/crd/bases

# Run go fmt against code
fmt:
	go fmt ./...

# Run go vet against code
vet:
	go vet ./...

# Generate code
generate: controller-gen
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

# Build the docker image
docker-build: test
	docker build . -t ${IMG}

# Push the docker image
docker-push:
	docker push ${IMG}

# find or download controller-gen
# download controller-gen if necessary
controller-gen:
ifeq (, $(shell which controller-gen))
	@{ \
	set -e ;\
	CONTROLLER_GEN_TMP_DIR=$$(mktemp -d) ;\
	cd $$CONTROLLER_GEN_TMP_DIR ;\
	go mod init tmp ;\
	go get sigs.k8s.io/controller-tools/cmd/controller-gen@v0.4.1 ;\
	rm -rf $$CONTROLLER_GEN_TMP_DIR ;\
	}
CONTROLLER_GEN=$(GOBIN)/controller-gen
else
CONTROLLER_GEN=$(shell which controller-gen)
endif

kustomize:
ifeq (, $(shell which kustomize))
	@{ \
	set -e ;\
	KUSTOMIZE_GEN_TMP_DIR=$$(mktemp -d) ;\
	cd $$KUSTOMIZE_GEN_TMP_DIR ;\
	go mod init tmp ;\
	go get sigs.k8s.io/kustomize/kustomize/v3@v3.9.1 ;\
	rm -rf $$KUSTOMIZE_GEN_TMP_DIR ;\
	}
KUSTOMIZE=$(GOBIN)/kustomize
else
KUSTOMIZE=$(shell which kustomize)
endif

# Generate bundle manifests and metadata, then validate generated files.
.PHONY: bundle
bundle: manifests kustomize
	operator-sdk generate kustomize manifests -q
	cd config/manager && $(KUSTOMIZE) edit set image controller=$(IMG)
	$(KUSTOMIZE) build config/manifests | operator-sdk generate bundle -q --extra-service-accounts prometheus-k8s --overwrite --version $(VERSION) $(BUNDLE_METADATA_OPTS) $(BUNDLE_FLAGS)
	cp config/metadata/* $(OUTPUT_DIR)/metadata/
	operator-sdk bundle validate $(OUTPUT_DIR)

# Build the bundle image.
.PHONY: bundle-build
bundle-build:
	docker build -f bundle.Dockerfile -t $(BUNDLE_IMG) .

.PHONY: opm
OPM = ./bin/opm
opm:
ifeq (,$(wildcard $(OPM)))
ifeq (,$(shell which opm 2>/dev/null))
	@{ \
	set -e ;\
	mkdir -p $(dir $(OPM)) ;\
	curl -sSLo $(OPM) https://github.com/operator-framework/operator-registry/releases/download/v1.15.1/$(OS)-$(ARCH)-opm ;\
	chmod +x $(OPM) ;\
	}
else 
OPM = $(shell which opm)
endif
endif
BUNDLE_IMGS ?= $(BUNDLE_IMG) 
CATALOG_IMG ?= $(IMAGE_TAG_BASE)-catalog:v$(VERSION) ifneq ($(origin CATALOG_BASE_IMG), undefined) FROM_INDEX_OPT := --from-index $(CATALOG_BASE_IMG) endif 
.PHONY: catalog-build
catalog-build: opm
	$(OPM) index add --container-tool docker --mode semver --tag $(CATALOG_IMG) --bundles $(BUNDLE_IMGS) $(FROM_INDEX_OPT)

.PHONY: catalog-push
catalog-push: ## Push the catalog image.
	$(MAKE) docker-push IMG=$(CATALOG_IMG)
