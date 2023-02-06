include .env
export

CHANGED_FILES := $(shell git ls-files --modified --other --exclude-standard)
CHANGED_FILES_IN_BRANCH := $(shell git diff --name-only $(shell git merge-base origin/main HEAD))

.PHONY : install_deps setup pre-commit pre-commit-in-branch pre-commit-all test help

install_deps:  ## Install all dependencies.
	pip install -r requirements.txt -r dev-requirements.txt
	pip install -e .

setup:  ## Install all dependencies and setup pre-commit
	make install_deps
	pre-commit install
	make .env

.env:  ## Generate .env file
	@cp .env.example $@

test:  ## Run tests.
	make unit_test
	make functional_test

unit_test:  ## Run unit tests.
	pytest --cov=dbt tests/unit

functional_test: .env  ## Run functional tests.
	pytest tests/functional

pre-commit:  ## check modified and added files (compared to last commit!) with pre-commit.
	pre-commit run --files $(CHANGED_FILES)

pre-commit-in-branch:  ## check changed since origin/main files with pre-commit.
	pre-commit run --files $(CHANGED_FILES_IN_BRANCH)

pre-commit-all:  ## Check all files in working directory with pre-commit.
	pre-commit run --all-files

help:  ## Show this help.
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m  %-30s\033[0m %s\n", $$1, $$2}'
