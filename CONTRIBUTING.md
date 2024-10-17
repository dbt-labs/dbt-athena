# Contributing

## Requirements

* Python>=3.8 - [docs](https://www.python.org/)
* Hatch - [docs](https://hatch.pypa.io/dev/)

## Getting started

### Hatch

This repository uses `hatch` as its primary development tool.
`hatch` will store its virtual environments in its own user space unless you configure it.
We strongly recommend that you configure `hatch` to store its virtual environments in an explicit location.
This has two benefits:

* this path is predictable and easily discoverable, making it much easier to use with IDEs
* the default environment uses a hash for the name whereas the explicit environment will use
a predictable and human-readable name

For example, we configure `hatch` to store its virtual environments in the project itself (first option below).
This is akin to running `python -m venv venv` from the project root.
Many folks prefer to store virtual environments in a central location separate from the project (second option below).

```toml
# MacOS   : ~/Library/Application Support/hatch/config.toml
# Windows : %USERPROFILE%\AppData\Local\hatch\config.toml
# Unix    : ~.config/hatch/config.toml

# this will create the virtual environment at `dbt-athena/dbt-athena/.hatch/dbt-athena
[dirs.env]
virtual = ".hatch"

# this will create the virtual environment at `~/.hatch/dbt-athena`
[dirs.env]
virtual = "~/.hatch"
```

You can find the full docs [here](https://hatch.pypa.io/dev/config/hatch/) if you'd like to learn more about `hatch`.

### Initial setup

You will need to perform these steps the first time you contribute.
If you plan on contributing in the future (we would really appreciate that!),
most of this should persist and be reusable at that point in time.

<!-- markdownlint-disable MD013 -->
* Fork the `dbt-athena` repo into your own user space on GitHub - [docs](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo)
* Install `hatch` on your local machine - [docs](https://hatch.pypa.io/dev/install/)
* Clone the fork to your local machine - [docs](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository)
* Navigate to the `dbt-athena` package directory
  * There are two packages in this repository. Don't worry about `dbt-athena-community`,
  it will automatically remain in sync with `dbt-athena`
* Setup your development environment with `hatch run setup`:
  1. Create a `hatch` virtual environment
  2. Install all dependencies
  3. Install pre-commit hooks
  4. Create a `test.env` stub file (formerly `.env`)
* Adjust the `test.env` file by configuring the environment variables to match your Athena development environment
<!-- markdownlint-restore -->

```shell
# install `hatch`
pip install hatch

# clone your fork
git clone https://github.com/<user>/dbt-athena

# navigate to the dbt-athena package
cd dbt-athena

# setup your development environment (formerly `make setup`)
hatch run setup
```

## Running tests and checks

There are many checks that are collectively referred to as Code Quality checks as well as 2 different types of testing:

* **code quality checks**: these checks include static analysis, type checking, and other code quality assurances
* **unit testing**: these tests are fast tests that don't require a platform connection
* **integration testing**: these tests are more thorough and require an AWS account with an Athena instance configured
  * Details of the Athena instance also need to be configured in your `test.env` file

These tests and checks can be run as follows:

```shell
# run all pre-commit checks
hatch run code-quality

# run unit tests (formerly `make unit_test`)
hatch run unit-tests

# run integration tests (formerly `make integration_test`)
hatch run integration-tests

# run unit tests and integration tests, formerly `make test`
hatch run all-tests

# run specific integration tests
hatch run integration-tests tests/functional/my/test_file.py
```

## Submitting a pull request

<!-- markdownlint-disable MD013 -->
* Create a commit with your changes and push them back up to your fork (e.g. `https://github.com/<user>/dbt-athena`)
* Create a [pull request](https://github.com/dbt-labs/dbt-athena/compare) on GitHub - [docs](https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork)
  * The pull request title and commit messages should adhere to [conventional commits](https://www.conventionalcommits.org)
  * The pull request body should describe _motivation_
<!-- markdownlint-restore -->

### General Guidelines

* Keep your Pull Request small and focused on a single feature or bug fix
* Make sure your change is well tested
  * Add new tests for completely new features or bug fixes
  * Add scenarios to existing tests if extending a feature
* Make sure your change is well documented
  * Mention when something is not obvious, or is being used for a specific purpose
  * Provide a link to the GitHub bug in the docstring when writing a new test demonstrating the bug
* Provide a clear description in your pull request to allow the reviewer to understand the context of your changes
  * Use a "self-review" to walk the reviewer through your thought process in a specific area
  * Use a "self-review" to ask a question about how to handle a specific problem
* Use a draft pull request during development and mark it as Ready for Review when you're ready
  * Ideally CI is also passing at this point, but you may also be looking for feedback on how to resolve an issue
