.PHONY: install
install: ## Install dependencies and pre-commit hooks with uv
	@echo "Creating virtual environment and installing dependencies with uv"
	@uv sync --dev
	@uv run pre-commit install

.PHONY: check
check: ## Run code quality tools.
	@echo "Checking uv lock file consistency"
	@uv lock --check
	@echo "Running pre-commit"
	@uv run pre-commit run -a
	@echo "Running mypy"
	@uv run mypy

.PHONY: test
test: ## Test the code with pytest
	@echo "Running pytest"
	@uv run pytest --doctest-modules

.PHONY: build
build: clean-build ## Build wheel and source distributions
	@echo "Building package"
	@uv build

.PHONY: clean-build
clean-build: ## clean build artifacts
	@rm -rf dist

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
