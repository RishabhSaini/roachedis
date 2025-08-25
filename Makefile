# Makefile for the Go Key-Value Store Project (CDC Version)

# Define image names for our services
SERVER_IMAGE_NAME=kv-server-app
HYDRATOR_IMAGE_NAME=kv-hydrator-app

# Variables for gofmt
GOFMT := gofmt
GOFILES := $(shell find . -type f -name '*.go')

.PHONY: all
all:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  init          - Initializes the Go module and downloads dependencies (run once)."
	@echo "  build         - Builds the container images for the server and hydrator."
	@echo "  compose       - Builds images if needed, then starts the full environment."
	@echo "  test          - Runs the comprehensive Go test client against the live environment."
	@echo "  down          - Stops and removes the entire environment."
	@echo "  format        - Formats all Go files in the project."


# Target to initialize the Go module and download dependencies
.PHONY: init
init:
	@echo "--- Initializing Go module and downloading dependencies... ---"
	@go mod init kvstore-cdc
	@go mod tidy
	@echo "--- Go module initialized. ---"


# Target to build both service images
.PHONY: build
build:
	@echo "--- Building API Server image... ---"
	@podman build -t $(SERVER_IMAGE_NAME) -f server/Containerfile .

	@echo "--- Building Cache Hydrator image... ---"
	@podman build -t $(HYDRATOR_IMAGE_NAME) -f hydrator/Containerfile .


# Target to start the entire geo-distributed environment using podman-compose
.PHONY: compose
compose: build
	@echo "--- Starting the CDC environment with Podman Compose... ---"
	@podman-compose up -d
	@echo "\n--> Environment is starting in the background."
	@echo "--> API Server available at: http://localhost:8080"
	@echo "--> CockroachDB UI at http://localhost:8180"
	@echo "--> Waiting for cluster and hydrator to initialize..."
	@sleep 20


# Target to run the comprehensive Go test client
.PHONY: test
test:
	@echo "--- Running Comprehensive Go Test Client... ---"
	@go run kv_test_go.go


# Target to stop and remove all containers defined in podman-compose.yml
.PHONY: down
down:
	@echo "--- Stopping and removing all environment containers... ---"
	@podman-compose down
	@echo "--- Cleanup complete. ---"


# Target to format all Go files recursively
.PHONY: format
format:
	@echo "Running gofmt on all Go files..."
	$(GOFMT) -w $(GOFILES)
	@echo "Go files have been formatted."
