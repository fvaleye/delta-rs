.DEFAULT_GOAL := help

VENV := venv
VERSION := $(shell cargo pkgid | cut -d\# -f2 | cut -d: -f2)

.PHONY: venv
venv: ## Setup the virtual env
	source $(VENV)/bin/activate;

.PHONY: setup
setup: venv ## Setup the requirements
	pip install maturin==0.10.6

.PHONY: install
install: setup ## Install Python binding of delta-rs
	maturin build --manylinux off
	ls -lh ../target/wheels
	pip install $(shell printf ../target/wheels/deltalake-$(VERSION)-cp36-abi3-*.whl)'[devel,pandas]'

.PHONY: format
format: ## Format the code
	cargo fmt
	black --exclude=$(VENV) .
	isort --profile black .

.PHONY: check-rust
check-rust: ## Run check on Rust
	cargo clippy
	cargo fmt -- --check

.PHONY: check-python
check-python: ## Run check on Python
	isort --profile black --check-only .
	black --exclude=$(VENV) . --check
	mypy --exclude "$(VENV)|docs|tests" .

.PHONY: unit-test
unit-test: venv ## Run unit tests
	source venv/bin/activate;
	py.test --cov tests -m 'not integration'

.PHONY: build-documentation
documentation: ## Build documentation with Sphinx
	sphinx-build -Wn -b html -d ./docs/build/doctrees ./docs/source ./docs/build/html

.PHONY: publish
publish: setup ## Publish Python version
	maturin publish -b pyo3 --target $(TARGET) --no-sdist

.PHONY: clean
clean: ## Run clean
	cargo clean
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'