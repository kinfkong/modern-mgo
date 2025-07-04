# Makefile for modern-mgo Project Tests

.PHONY: help test test-unit test-integration test-all setup-test-db teardown-test-db clean test-verbose test-coverage test-race

# Default target
help:
	@echo "Available targets:"
	@echo "  setup-test-db     - Start MongoDB test database using Docker"
	@echo "  teardown-test-db  - Stop and remove MongoDB test database"
	@echo "  test-unit         - Run unit tests"
	@echo "  test-integration  - Run integration tests"
	@echo "  test-all          - Run all tests"
	@echo "  test              - Alias for test-all"
	@echo "  test-verbose      - Run tests with verbose output"
	@echo "  test-coverage     - Run tests with coverage report"
	@echo "  test-race         - Run tests with race detection"
	@echo "  mongo-express     - Start MongoDB Express UI"
	@echo "  stop-mongo-express - Stop MongoDB Express UI"
	@echo "  clean             - Clean up test artifacts and Docker volumes"
	@echo "  help              - Show this help message"

# MongoDB connection string for tests
MONGODB_TEST_URL := mongodb://localhost:27018/modern_mgo_test

# Start MongoDB test database
setup-test-db:
	@echo "Starting MongoDB test database..."
	docker compose -f docker-compose.test.yml up -d mongodb-test
	@echo "Waiting for MongoDB to be ready..."
	@sleep 5
	@echo "MongoDB test database is ready at $(MONGODB_TEST_URL)"

# Stop and remove MongoDB test database
teardown-test-db:
	@echo "Stopping MongoDB test database..."
	docker compose -f docker-compose.test.yml down
	@echo "MongoDB test database stopped"

# Run unit tests
test-unit: setup-test-db
	@echo "Running unit tests..."
	cd .. && MONGODB_TEST_URL=$(MONGODB_TEST_URL) go test -v -short ./...
	@$(MAKE) teardown-test-db

# Run integration tests
test-integration: setup-test-db
	@echo "Running integration tests..."
	cd .. && MONGODB_TEST_URL=$(MONGODB_TEST_URL) go test -v -run Integration ./...
	@$(MAKE) teardown-test-db

# Run all tests
test-all: setup-test-db
	@echo "Running all tests..."
	cd .. && MONGODB_TEST_URL=$(MONGODB_TEST_URL) go test -v ./...
	@$(MAKE) teardown-test-db

# Alias for test-all
test: test-all

# Run tests with verbose output
test-verbose: setup-test-db
	@echo "Running tests with verbose output..."
	cd .. && MONGODB_TEST_URL=$(MONGODB_TEST_URL) go test -v -count=1 ./...
	@$(MAKE) teardown-test-db

# Run tests with coverage
test-coverage: setup-test-db
	@echo "Running tests with coverage..."
	cd .. && MONGODB_TEST_URL=$(MONGODB_TEST_URL) go test -v -coverprofile=coverage.out ./...
	cd .. && go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"
	@$(MAKE) teardown-test-db

# Run tests with race detection
test-race: setup-test-db
	@echo "Running tests with race detection..."
	cd .. && MONGODB_TEST_URL=$(MONGODB_TEST_URL) go test -v -race ./...
	@$(MAKE) teardown-test-db

# Run specific test by name
test-specific: setup-test-db
	@if [ -z "$(TEST)" ]; then \
		echo "Usage: make test-specific TEST=TestName"; \
		$(MAKE) teardown-test-db; \
		exit 1; \
	fi
	@echo "Running specific test: $(TEST)"
	cd .. && MONGODB_TEST_URL=$(MONGODB_TEST_URL) go test -v -run "$(TEST)" ./...
	@$(MAKE) teardown-test-db

# Start MongoDB Express for database inspection
mongo-express: setup-test-db
	@echo "Starting MongoDB Express..."
	docker compose -f docker-compose.test.yml up -d mongo-express
	@echo "MongoDB Express available at http://localhost:8081"
	@echo "Username: admin, Password: admin"

# Stop MongoDB Express
stop-mongo-express:
	@echo "Stopping MongoDB Express..."
	docker compose -f docker-compose.test.yml stop mongo-express

# Clean up test artifacts and Docker volumes
clean: teardown-test-db
	@echo "Cleaning up test artifacts..."
	rm -f ../coverage.out ../coverage.html
	docker volume prune -f
	@echo "Cleanup complete"

# Run benchmarks
benchmark: setup-test-db
	@echo "Running benchmarks..."
	cd .. && MONGODB_TEST_URL=$(MONGODB_TEST_URL) go test -v -bench=. -benchmem ./...
	@$(MAKE) teardown-test-db

# Continuous integration pipeline
ci: setup-test-db
	@echo "Running CI pipeline..."
	cd .. && go fmt ./...
	cd .. && go vet ./...
	cd .. && MONGODB_TEST_URL=$(MONGODB_TEST_URL) go test -v -race -coverprofile=coverage.out ./...
	@$(MAKE) teardown-test-db
	@echo "CI pipeline completed successfully" 