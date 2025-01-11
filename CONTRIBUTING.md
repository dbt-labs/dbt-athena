# Contributing

This repository as moved into the `dbt-labs/dbt-adapters` monorepo found
[here](https://www.github.com/dbt-labs/dbt-adapters).
Please refer to that repo for a guide on how to contribute to `dbt-athena`.

If you have already opened a pull request and need to migrate it to the new repo,
you will need to follow these steps:

1. Fork `dbt-labs/dbt-adapters` and pull it down locally
2. Migrate your feature branch from your fork of `dbt-labs/dbt-athena` to your fork of `dbt-labs/dbt-adapters`
3. Create a new pull request in `dbt-labs/dbt-adapters` based on your feature branch

Steps 1 and 3 are manual.
Steps 2 can be accomplished by running this in your local fork of `dbt-labs/dbt-adapters`:

```shell
_USER="<your-user>"
_BRANCH="<your-feature-branch>"

# create a remote for your fork of dbt-athena
git remote add athena-fork https://github.com/$_USER/dbt-athena.git
git fetch athena-fork

# update your feature branch
git rebase main athena-fork/$_BRANCH

# merge your feature branch from the dbt-athena repo into the dbt-adapters repo
git checkout -b dbt-athena/$_BRANCH  # prefixing dbt-athena/ namespaces your feature branch in the new repo
git merge athena-fork/$_BRANCH
git push origin dbt-athena/$_BRANCH

# remove the remote that was created by this process
git remote remove athena-fork
```
