.PHONY: all build test test-integration test-fast coverage bench lint security loc clean release help

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
	go test -race -v -tags integration -timeout 5m ./...

# Run tests without race detection (faster)
test-fast:
	go test -v ./...

# Run tests with coverage, excluding the example, internal/testutil, and gen packages
COVER_PKGS := $(shell go list ./... | grep -Ev '/(example|internal/testutil|gen/)$$')
coverage:
	go test -race -tags integration -timeout 5m -coverprofile=coverage.out $(COVER_PKGS)
	@grep -v -E '^github\.com/wu/keyop-messenger/(example|internal/testutil|gen/)' coverage.out > coverage-filtered.out
	go tool cover -func=coverage-filtered.out
	@rm -f coverage-filtered.out

# Run benchmarks
bench:
	go test -run='^$$' -bench=. -benchmem -benchtime=3s ./...

# Lint the project
# This assumes golangci-lint is installed.
lint:
	golangci-lint run ./...

# Run static analysis security scanners.
# This assumes govulncheck and gosec are installed:
#   go install golang.org/x/vuln/cmd/govulncheck@latest
#   go install github.com/securego/gosec/v2/cmd/gosec@latest
security:
	@echo "== govulncheck (known vulnerabilities in reachable code) =="
	govulncheck ./...
	@echo "== gosec (security-focused static analysis) =="
	gosec ./...

# Count lines of code by category (requires cloc).
# Hand-written: Go files excluding tests and generated code.
# Test: *_test.go files. Generated: the gen/ tree (protobuf output).
loc:
	@command -v cloc >/dev/null 2>&1 || { echo "cloc is not installed (e.g. 'brew install cloc')"; exit 1; }
	@echo "== Hand-written code (Go, excludes tests and generated) =="
	@cloc --quiet --include-lang=Go --fullpath --not-match-d='/gen/' --not-match-f='_test\.go$$' .
	@echo "== Test code (*_test.go) =="
	@cloc --quiet --include-lang=Go --match-f='_test\.go$$' .
	@echo "== Generated code (gen/) =="
	@cloc --quiet --include-lang=Go gen/

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
	@echo "  coverage         - Run tests and show coverage (excludes example and testutil)"
	@echo "  bench            - Run benchmarks"
	@echo "  lint             - Run golangci-lint"
	@echo "  security         - Run govulncheck and gosec"
	@echo "  loc              - Count lines of code by category (requires cloc)"
	@echo "  release          - Run all pre-release checks (test, test-integration, lint, bench)"
	@echo "  clean            - Clean build artifacts and coverage files"
	@echo "  help             - Show this help message"
