# How to release

* Open a pull request with a manual bump on `dbt-athena/dbt/adapters/athena/__version__.py`
* Create a new release from <https://github.com/dbt-labs/dbt-athena/releases>
  * Be sure to use the same version as in the `__version__.py` file
  * Be sure to start the release with `v` e.g. v1.6.3
  * Tag with the same name of the release e.g. v1.6.3
  * Be sure to clean up release notes grouping by semantic commit type,
    e.g. all feat commits should under the same section
* Once the new release is made be sure that the new package version is available on PyPI
  in [PyPI](https://pypi.org/project/dbt-athena/)
