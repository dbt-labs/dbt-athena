# Contributing

## Requirements

* Python from 3.8 to 3.12
* Virtual Environment or any other Python environment manager

## Getting started

* Clone of fork the repo
* Run `make setup`, it will:
  1. Install all dependencies
  2. Install pre-commit hooks
  3. Generate your `.env` file

* Adjust `.env` file by configuring the environment variables to match your Athena development environment.

## Running tests

We have 2 different types of testing:

* **unit testing**: you can run this type of tests running `make unit_test`
* **functional testing**: you must have an AWS account with Athena setup in order to launch this type of tests and have
  a `.env` file in place with the right values.
  You can run this type of tests running `make functional_test`

All type of tests can be run using `make`:

```bash
make test
```

### Pull Request

* Create a commit with your changes and push them to a
  [fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo).
<!-- markdownlint-disable-next-line MD013 -->
* Create a [pull request on Github](https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).
* Pull request title and message (and PR title and description) must adhere to
  [conventional commits](https://www.conventionalcommits.org).
* Pull request body should describe _motivation_.
<!-- markdownlint-restore -->

### General Guidelines

* Keep your Pull Request small and focused on a single feature or bug fix.
* Make sure your code is well tested.
* Make sure your code is well documented.
* Provide a clear description of your Pull Request to allow the reviewer to understand the context of your changes.
* Consider the usage of draft Pull Request and switch to ready for review only when the CI pass or is ready for
  feedback.
* Be sure to have pre-commit installed or run `make pre-commit` before pushing your changes.
