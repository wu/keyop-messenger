.PHONY: all build test clean lint help

# Default target
all: test build

# Build the project (if applicable)
# Since this is primarily a library, we'll verify it compiles.
build:
	go build ./...

# Run tests with race detection
test:
	go test -race -v ./...

# Run tests without race detection (faster)
test-fast:
	go test -v ./...

# Run tests with coverage
test-coverage:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

# Lint the project
# This assumes golangci-lint is installed.
lint:
	golangci-lint run ./...

# Clean build artifacts and test cache
clean:
	go clean
	rm -f coverage.out

# Display help information
help:
	@echo "Available targets:"
	@echo "  all           - Run tests and build (default)"
	@echo "  build         - Verify compilation of all packages"
	@echo "  test          - Run all tests with race detection"
	@echo "  test-fast     - Run all tests without race detection"
	@echo "  test-coverage - Run all tests and show coverage"
	@echo "  lint          - Run golangci-lint"
	@echo "  clean         - Clean build artifacts and coverage files"
	@echo "  help          - Show this help message"
