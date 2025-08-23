# Makefile for the Go Key-Value Store Project (using Podman)

# Variables for Podman container names to ensure consistency
ROACH_CONTAINER_NAME=roach-kv-store
REDIS_CONTAINER_NAME=redis-kv-store

# Default target runs when you just type 'make'. It lists the available commands.
.PHONY: all
all:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  init     - Initializes the Go module and downloads dependencies."
	@echo "  setup    - Starts the CockroachDB and Redis Podman containers."
	@echo "  run      - Builds and runs the main key-value server."
	@echo "  test     - Builds and runs the test client against the server."
	@echo "  clean    - Stops and removes the Podman containers."
	@echo ""
	@echo "Example Workflow:"
	@echo "1. In one terminal, run 'make init' (only needed once)."
	@echo "2. Then run 'make setup' to start the databases."
	@echo "3. In the same terminal, run 'make run' to start the server."
	@echo "4. In a second terminal, run 'make test' to test the server."
	@echo "5. When finished, run 'make clean' to stop the services."


# Target to initialize the Go module and download dependencies
.PHONY: init
init:
	@echo "--- Initializing Go module and downloading dependencies... ---"
	@go mod init kvstore
	@go mod tidy
	@echo "--- Go module initialized. ---"


# Target to set up the required services (CockroachDB and Redis)
.PHONY: setup
setup:
	@echo "--- Starting CockroachDB container ($(ROACH_CONTAINER_NAME))... ---"
	@podman run -d --name=$(ROACH_CONTAINER_NAME) \
		-p 26257:26257 \
		-p 8081:8080 \
		cockroachdb/cockroach:latest-v23.2 start-single-node --insecure

	@echo "--- Starting Redis container ($(REDIS_CONTAINER_NAME))... ---"
	@podman run -d --name=$(REDIS_CONTAINER_NAME) \
		-p 6379:6379 \
		redis:latest

	@echo "\n--> Waiting for services to initialize (5 seconds)..."
	@sleep 5
	@echo "--> Setup complete. CockroachDB UI is now on http://localhost:8081"


# Target to build and run the main Go server application.
# Assumes the server code is in a file named 'kv_store_go.go'.
.PHONY: run
run:
	@echo "--- Building and running the key-value server... (Press Ctrl+C to stop) ---"
	@go run kv_store_go.go


# Target to build and run the Go test client.
# Assumes the test client code is in a file named 'kv_test_go.go'.
.PHONY: test
test:
	@echo "--- Building and running the test client... ---"
	@go run kv_test_go.go


# Target to stop and remove the Podman containers to clean up the environment.
# The '-' before the commands ignores errors (e.g., if a container is not running).
.PHONY: clean
clean:
	@echo "--- Stopping and removing Podman containers... ---"
	@-podman stop $(ROACH_CONTAINER_NAME) 2>/dev/null || true
	@-podman rm $(ROACH_CONTAINER_NAME) 2>/dev/null || true
	@-podman stop $(REDIS_CONTAINER_NAME) 2>/dev/null || true
	@-podman rm $(REDIS_CONTAINER_NAME) 2>/dev/null || true
	@echo "--- Cleanup complete. ---"

