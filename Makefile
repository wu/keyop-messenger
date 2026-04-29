.PHONY: all build test test-integration test-fast test-coverage bench lint clean release help

# Default target
all: test build

# Build the project (if applicable)
# Since this is primarily a library, we'll verify it compiles.
build:
	go build ./...

# Run tests with race detection
test:
	go test -race -v ./...

# Run integration tests (requires build tag)
test-integration:
	go test -race -v -tags integration -timeout 60s ./...

# Run tests without race detection (faster)
test-fast:
	go test -v ./...

# Run tests with coverage
test-coverage:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

# Run benchmarks
bench:
	go test -run='^$$' -bench=. -benchmem -benchtime=3s ./...

# Lint the project
# This assumes golangci-lint is installed.
lint:
	golangci-lint run ./...

# Run all checks required before a release
release: test test-integration lint bench
	@echo "All release checks passed!"

# Clean build artifacts and test cache
clean:
	go clean
	rm -f coverage.out

# Display help information
help:
	@echo "Available targets:"
	@echo "  all              - Run tests and build (default)"
	@echo "  build            - Verify compilation of all packages"
	@echo "  test             - Run all unit tests with race detection"
	@echo "  test-integration - Run integration tests (build tag: integration)"
	@echo "  test-fast        - Run all tests without race detection"
	@echo "  test-coverage    - Run all tests and show coverage"
	@echo "  bench            - Run benchmarks"
	@echo "  lint             - Run golangci-lint"
	@echo "  release          - Run all pre-release checks (test, test-integration, lint, bench)"
	@echo "  clean            - Clean build artifacts and coverage files"
	@echo "  help             - Show this help message"
