# modern-mgo Project Makefile
# This delegates to the test infrastructure in the test/ directory

.PHONY: help test test-unit test-integration test-all test-coverage test-race setup-test-db teardown-test-db clean

# Default target
help:
	@$(MAKE) -C test help

# Test targets
test:
	@$(MAKE) -C test test

test-unit:
	@$(MAKE) -C test test-unit

test-integration:
	@$(MAKE) -C test test-integration

test-all:
	@$(MAKE) -C test test-all

test-coverage:
	@$(MAKE) -C test test-coverage

test-race:
	@$(MAKE) -C test test-race

test-verbose:
	@$(MAKE) -C test test-verbose

benchmark:
	@$(MAKE) -C test benchmark

# Database management
setup-test-db:
	@$(MAKE) -C test setup-test-db

teardown-test-db:
	@$(MAKE) -C test teardown-test-db

mongo-express:
	@$(MAKE) -C test mongo-express

stop-mongo-express:
	@$(MAKE) -C test stop-mongo-express

# Cleanup
clean:
	@$(MAKE) -C test clean

# CI pipeline
ci:
	@$(MAKE) -C test ci

# Pass through specific test execution
test-specific:
	@$(MAKE) -C test test-specific TEST=$(TEST) 