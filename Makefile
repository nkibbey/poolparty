IMG_NAME := docker.io/nkibbey/poolparty
OCI_TOOL := docker

define DOCKERFILE
FROM golang:1-alpine
COPY bin/* /go/bin/
ENTRYPOINT ["/go/bin/poolparty"]
endef
export DOCKERFILE # Export for use with echo

G_COMMIT := $(shell if [ -n "$(CI_COMMIT_SHA)" ]; then echo "$(CI_COMMIT_SHA)"; else git describe --all --dirty --long; fi)

# VER: The version string derived from the last tag and commit count since.
# Logic: git describe --tags --abbrev=0 (latest tag) + git rev-list HEAD ^<tag> --count (+X commits since)
# If no tag exists, falls back to "0.0.0+$(shell git rev-list HEAD --count)"
GIT_TAG ?= $(shell git describe --tags --abbrev=0 2>/dev/null)
ifeq ($(GIT_TAG),)
    VER := 0.0.0+$(shell git rev-list HEAD --count)
else
    COMMITS_SINCE := $(shell git rev-list HEAD ^$(GIT_TAG) --count)
    ifeq ($(COMMITS_SINCE),0)
        VER := $(GIT_TAG)
    else
        VER := $(GIT_TAG)+$(COMMITS_SINCE)
    endif
endif

BUILD_FLAGS :=  -trimpath -ldflags "-X 'main.Version=$(VER)' -X 'main.GitCommit=$(G_COMMIT)' -X 'main.BuildTime=$$(date)'"

# --- Targets ---

.PHONY: build oci clean all

build:
	@mkdir -p bin
	CGO_ENABLED=0 go build $(BUILD_FLAGS) -o bin ./...
	

oci: build
	@echo "$$DOCKERFILE" > Dockerfile
	$(OCI_TOOL) build -t $(IMG_NAME):$(VER) -t $(IMG_NAME):latest .
	@rm Dockerfile

clean:
	rm -rf bin

all: clean build oci
