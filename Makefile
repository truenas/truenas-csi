# TrueNAS CSI Driver Makefile
# Includes targets for both standard Kubernetes and OpenShift certification builds

# Image configuration
REGISTRY ?= quay.io/truenas_solutions
DRIVER_IMAGE ?= $(REGISTRY)/truenas-csi
OPERATOR_IMAGE ?= $(REGISTRY)/truenas-csi-operator
BUNDLE_IMAGE ?= $(REGISTRY)/truenas-csi-operator-bundle
VERSION ?= 1.3.0
IMG_TAG ?= v$(VERSION)

# Go configuration
GO ?= go
LDFLAGS ?= -X github.com/truenas/truenas-csi/pkg/driver.DRIVER_VERSION=$(IMG_TAG)
GOFLAGS ?= -v

# Formatting. gofumpt is a strict superset of gofmt; golangci-lint runs the same
# formatter, so keep this version in step with .golangci.yml.
GOFUMPT_VERSION ?= v0.11.0
GOFUMPT ?= $(GO) run mvdan.cc/gofumpt@$(GOFUMPT_VERSION)

# Helm chart. CHART_REPO_URL is where the packaged charts are served from, which
# is what `helm repo add` resolves the tarballs against.
HELM ?= helm
CHART_DIR ?= charts/truenas-csi
CHART_REPO_DIR ?= charts
CHART_REPO_URL ?= https://raw.githubusercontent.com/truenas/truenas-csi/master/charts
CHART_RENDER ?= bin/chart-rendered.yaml
# Values that only satisfy the chart's required fields, so rendering does not
# depend on a real appliance.
CHART_TEST_VALUES ?= --set truenas.url=wss://truenas.example --set truenas.defaultPool=tank --set truenas.apiKey=example

.PHONY: all
all: build

##@ General

.PHONY: help
help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: build
build: ## Build the CSI driver binary
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o bin/truenas-csi cmd/main.go

.PHONY: test
test: ## Run unit tests
	$(GO) test ./... -v

.PHONY: test-sanity
test-sanity: ## Run CSI sanity tests
	$(GO) test ./test/sanity/... -v

.PHONY: fmt
fmt: ## Format all Go code with gofumpt (both modules)
	$(GOFUMPT) -l -w .

.PHONY: fmt-check
fmt-check: ## Fail if any Go file is not gofumpt-formatted
	@out=$$($(GOFUMPT) -l .); \
	if [ -n "$$out" ]; then echo "Not gofumpt-formatted:"; echo "$$out"; exit 1; fi

.PHONY: lint
lint: ## Run linter
	golangci-lint run ./...

##@ Helm Chart

.PHONY: chart-lint
chart-lint: ## Lint the Helm chart
	$(HELM) lint $(CHART_DIR) $(CHART_TEST_VALUES)

.PHONY: verify-chart
verify-chart: ## Fail if the Helm chart and deploy/truenas-csi-driver.yaml disagree
	@mkdir -p $(dir $(CHART_RENDER))
	$(HELM) template truenas-csi $(CHART_DIR) --namespace truenas-csi $(CHART_TEST_VALUES) > $(CHART_RENDER)
	$(GO) run ./hack/chartcheck $(CHART_RENDER) deploy/truenas-csi-driver.yaml

.PHONY: chart-package
chart-package: chart-lint verify-chart ## Package the chart and refresh the repo index
	$(HELM) package $(CHART_DIR) --destination $(CHART_REPO_DIR)
	$(HELM) repo index $(CHART_REPO_DIR) --url $(CHART_REPO_URL)
	@echo "Commit $(CHART_REPO_DIR)/index.yaml and the new .tgz to publish the chart."

.PHONY: clean
clean: ## Clean build artifacts
	rm -rf bin/

.PHONY: bump-version
bump-version: ## Update all hardcoded version refs (e.g. make bump-version VERSION=1.2.0)
	@echo "Setting version to $(VERSION)"
	sed -i 's/^VERSION ?= .*/VERSION ?= $(VERSION)/' Makefile operator/Makefile
	sed -i 's#\(truenas-csi-operator:v\)[0-9][0-9.]*#\1$(VERSION)#' operator/config/manifests/bases/operator.clusterserviceversion.yaml
	sed -i 's#\(truenas-csi:v\)[0-9][0-9.]*#\1$(VERSION)#' operator/config/manager/manager.yaml
	sed -i 's/^\( *newTag: *\)v[0-9][0-9.]*/\1v$(VERSION)/' operator/config/manager/kustomization.yaml
	sed -i 's/^version: .*/version: $(VERSION)/; s/^appVersion: .*/appVersion: "$(VERSION)"/' $(CHART_DIR)/Chart.yaml
	@echo "Done. Dockerfile 'version' labels are set from the VERSION build-arg at build time."
	@echo "Run 'make chart-package' to publish the chart at the new version."
	@echo "Next: make bundle VERSION=$(VERSION) USE_IMAGE_DIGESTS=true CHANNELS=stable DEFAULT_CHANNEL=stable (then finalize the CSV name/replaces)."

##@ Docker Builds (Standard)

.PHONY: docker-build
docker-build: ## Build standard Docker image (Alpine-based)
	docker build --build-arg VERSION=$(VERSION) -t $(DRIVER_IMAGE):$(IMG_TAG) .

.PHONY: docker-push
docker-push: ## Push standard Docker image
	docker push $(DRIVER_IMAGE):$(IMG_TAG)

##@ OpenShift / Red Hat Certification Builds

.PHONY: build-ubi
build-ubi: ## Build UBI-based driver image for Red Hat certification
	docker build --pull -f Dockerfile.ubi --provenance=false --sbom=false --build-arg VERSION=$(VERSION) -t $(DRIVER_IMAGE):$(IMG_TAG) .

.PHONY: push-ubi
push-ubi: ## Push UBI-based driver image
	docker push $(DRIVER_IMAGE):$(IMG_TAG)

.PHONY: push-latest
push-latest: ## Push all images with 'latest' tag (required for integration tests and default deployments)
	docker tag $(DRIVER_IMAGE):$(IMG_TAG) $(DRIVER_IMAGE):latest
	docker push $(DRIVER_IMAGE):latest
	docker tag $(OPERATOR_IMAGE):$(IMG_TAG) $(OPERATOR_IMAGE):latest
	docker push $(OPERATOR_IMAGE):latest
	docker tag $(BUNDLE_IMAGE):$(IMG_TAG) $(BUNDLE_IMAGE):latest
	docker push $(BUNDLE_IMAGE):latest

.PHONY: operator-build
operator-build: ## Build the UBI-based operator image for Red Hat certification
	cp LICENSE operator/LICENSE
	cd operator && docker build --pull -f Dockerfile.ubi --provenance=false --sbom=false --build-arg VERSION=$(VERSION) -t $(OPERATOR_IMAGE):$(IMG_TAG) .
	rm -f operator/LICENSE

.PHONY: operator-push
operator-push: ## Push the operator image
	docker push $(OPERATOR_IMAGE):$(IMG_TAG)

.PHONY: bundle-build
bundle-build: ## Build the OLM bundle image
	cd operator && docker build -f bundle.Dockerfile --provenance=false --sbom=false -t $(BUNDLE_IMAGE):$(IMG_TAG) .

.PHONY: bundle-push
bundle-push: ## Push the OLM bundle image
	docker push $(BUNDLE_IMAGE):$(IMG_TAG)

##@ OpenShift Deployment

.PHONY: deploy-openshift
deploy-openshift: ## Deploy to OpenShift using the operator
	kubectl apply -f deploy/openshift/scc.yaml
	cd operator && $(MAKE) deploy IMG=$(OPERATOR_IMAGE):$(IMG_TAG)

.PHONY: undeploy-openshift
undeploy-openshift: ## Undeploy from OpenShift
	cd operator && $(MAKE) undeploy
	kubectl delete -f deploy/openshift/scc.yaml --ignore-not-found

##@ Red Hat Certification

.PHONY: preflight-container
preflight-container: ## Run Red Hat preflight check on container image
	preflight check container $(DRIVER_IMAGE):$(IMG_TAG)

.PHONY: preflight-operator
preflight-operator: ## Run Red Hat preflight check on operator bundle
	preflight check operator $(BUNDLE_IMAGE):$(IMG_TAG)

.PHONY: scorecard
scorecard: ## Run operator-sdk scorecard tests
	cd operator && operator-sdk scorecard bundle/

##@ Build All

.PHONY: build-all
build-all: build-ubi operator-build bundle-build ## Build all images (driver, operator, bundle)

.PHONY: push-all
push-all: push-ubi operator-push bundle-push ## Push all versioned images (excludes 'latest' tag)

##@ Release

.PHONY: release
release: build-all push-all push-latest ## Build and push all images including 'latest' tag
	@echo ""
	@echo "Release v$(VERSION) complete!"
	@echo "  - Driver:   $(DRIVER_IMAGE):$(IMG_TAG) and :latest"
	@echo "  - Operator: $(OPERATOR_IMAGE):$(IMG_TAG)"
	@echo "  - Bundle:   $(BUNDLE_IMAGE):$(IMG_TAG)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Run 'make scorecard' to validate the bundle"
	@echo "  2. Tag the git repository: git tag v$(VERSION) && git push --tags"
	@echo "  3. Submit to Red Hat certification (if applicable)"

##@ Operator SDK Targets

.PHONY: operator-generate
operator-generate: ## Run operator code generation
	cd operator && $(MAKE) generate

.PHONY: operator-manifests
operator-manifests: ## Generate operator manifests (CRD, RBAC)
	cd operator && $(MAKE) manifests

.PHONY: operator-run
operator-run: ## Run operator locally (for development)
	cd operator && $(MAKE) run
