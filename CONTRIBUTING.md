## Exposing a new lookml configuration in dbt2looker

If you'd like to expose some new lookml config (e.g. a dimension or measure field) you can follow the pattern in this commit: https://github.com/lightdash/dbt2looker/commit/b9e799969aad2930efd1bbc3d01b5a2a77db9d60

In general:
* Update `models.py` with the new `schema.yml` fields you'd like to expose
* Map new fields to lookml in `generator.py`
* Update the `/examples` directory with an example of your feature in the dbt `pages.yml` and the `pages.view` output

### Local testing

Instead of setting up the example project with valid config, etc, the easiest
is to point to a locally available, valid DBT project where manifest.json and
catalog.json is already generated.


Make your changes in this repo locally, then run `poetry install`.

Then generate lookml files with the local version of dbt2looker with the following command:

```bash
poetry run dbt2looker --tag YOUR_TAG --target-dir TARGET_DIR_OF_TEST_PROJECT --project-dir PROJECT_DIR_OF_TEST_PROJECT --output-dir LOOKML_OUTPUT_DIR_OF_TEST_PROJECT
```
