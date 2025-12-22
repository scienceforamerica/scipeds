#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_NAME = scipeds
PYTHON_VERSION = 3.11.10
PYTHON_INTERPRETER = python

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Set up python interpreter environment
.PHONY: create_environment
create_environment:
	conda create --name $(PROJECT_NAME) python=$(PYTHON_VERSION) -y
	@echo ">>> conda env created. Activate with:\nconda activate $(PROJECT_NAME)"

## Install Python Dependencies
.PHONY: requirements
requirements:
	$(PYTHON_INTERPRETER) -m pip install -U pip
	$(PYTHON_INTERPRETER) -m pip install -r requirements/dev.txt

## Format source code with ruff
.PHONY: format
format:
	ruff format
	ruff check --fix

## Lint using ruff (use `make format` to do formatting)
.PHONY: lint
lint:
	ruff check
	mypy scipeds pipeline

## Build distribution packages
.PHONY: build
build: 
	python -m build
	ls -l dist

## Delete all compiled Python files
.PHONY: clean
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

#################################################################################
# PROJECT RULES                                                                 #
#################################################################################

## Run tests
.PHONY: test
test:
	pytest -v scipeds

## Run pipeline tests
.PHONY: test-pipeline
test-pipeline:
	pytest -v pipeline

## Get test coverage
.PHONY: test-coverage
test-coverage:
	coverage run -m pytest pipeline/ scipeds/
	coverage report -m 

## Generate test assets
.PHONY: test-assets
test-assets:
	python pipeline/db.py write-test-db

## Debug tests
.PHONY: test-debug
test-debug:
	pytest -v scipeds --pdb

## Install requirements for building docs
.PHONY: docs-requirements
docs-requirements:
	pip install -r requirements/docs.txt

## Build static version of docs
.PHONY: docs-build
docs-build:
	cd docs && mkdocs build --clean

## Serve docs locally
.PHONY: docs-serve
docs-serve:
	cd docs && mkdocs serve --clean

## Download raw data files from Cloud Storage
.PHONY: download-raw
download-raw:
	python pipeline/download.py download-from-bucket

## Process all raw files, write interims, and create duckdb database
.PHONY: process
process:
	python pipeline/completions.py
	python pipeline/institutions.py
	python pipeline/fall_enrollment.py
	python pipeline/db.py write-db --overwrite


#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys; \
lines = '\n'.join([line for line in sys.stdin]); \
matches = re.findall(r'\n## (.*)\n[\s\S]+?\n([a-zA-Z_-]+):', lines); \
print('Available rules:\n'); \
print('\n'.join(['{:25}{}'.format(*reversed(match)) for match in matches]))
endef
export PRINT_HELP_PYSCRIPT

help:
	@python -c "${PRINT_HELP_PYSCRIPT}" < $(MAKEFILE_LIST)
