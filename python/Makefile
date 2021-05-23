.DEFAULT_GOAL := help

VENV := venv
VERSION := $(shell cargo pkgid | cut -d\# -f2 | cut -d: -f2)

.PHONY: setup
setup: ## Setup the requirements
	pip install maturin==0.10.6

.PHONY: develop
develop: setup ## Install Python binding of delta-rs
	maturin develop

.PHONY: format
format: setup ## Format the code
	cargo fmt
	black .
	isort .

.PHONY: check-rust
check-rust: develop ## Run check on Rust
	cargo clippy
	cargo fmt -- --check

.PHONY: check-python
check-python: ## Run check on Python
	isort --check-only .
	black --check .
	mypy --exclude "$(VENV)|docs|tests" .

.PHONY: unit-test
unit-test: ## Run unit tests
	py.test --cov tests -m 'not integration'

.PHONY: build-documentation
documentation: ## Build documentation with Sphinx
	sphinx-build -Wn -b html -d ./docs/build/doctrees ./docs/source ./docs/build/html

.PHONY: publish
publish: install ## Publish Python version
	maturin publish -b pyo3 --target $(TARGET) --no-sdist

.PHONY: clean
clean: ## Run clean
	cargo clean
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'