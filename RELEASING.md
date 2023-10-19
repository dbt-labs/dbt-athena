# How to make a release

* open a Pull Request with a manual bump of in `main/dbt/adapters/athena/__version__.py`
* create a new release from <https://github.com/dbt-athena/dbt-athena/releases>
  * be sure to use the same version as in the `__version__.py` file
  * be sure to start the release with `v` e.g. v1.6.3
  * tag with the same name of the release e.g. v1.6.3
  * be sure to clean up release notes grouping by semantic commit type,
    e.g. all feat commits should under the same section
* Once the new release is made be sure that the new package version is available in PyPI
  in [dbt-athena-community](https://pypi.org/project/dbt-athena-comunity/)
