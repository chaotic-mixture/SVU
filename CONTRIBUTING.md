# Contributing

Thanks for helping improve SVU. The public project is an experimental research candidate. Contributions should preserve transparent methods, traceable inputs and clearly stated limitations.

## Set up and check changes

Use Python 3.11 or 3.12. From the repository root:

```text
python -m pip install --require-hashes -r requirements.lock
python -m pytest tests -q
python -m scripts.demo --output reports/demo
```

For chart-template or interaction changes, also follow the browser regression steps in [the reproduction guide](docs/REPRODUCIBILITY.md#offline-browser-regression). The gallery uses synthetic data and is not evidence of current market conditions.

## Methods and data

Describe changes to formulas, category weights, basket membership, source selection or date alignment in the pull request. Keep methodological changes separate from maintenance where practical. Test invalid inputs and missing observations as well as normal cases. Never describe synthetic fixtures as market observations.

Do not silently replace one provider's data with another provider's quote. Changes to provider, unit, timestamp, calendar or licence can change the meaning of a series. Document the source and comparability limits.

## Dependencies and public files

For dependency changes, update `requirements.txt` and `constraints.txt`, regenerate `requirements.lock` with `python -m scripts.lock_dependencies`, then review the hashes and audit output.

The supported public distribution is defined file by file in `config/public_release.json`. Add each required source file, test, template and document to that manifest. Before proposing a release, export to a new directory with `python -m scripts.export_public --output dist/candidate` and verify the resulting tree.

Do not commit downloaded provider data, databases, generated reports, credentials, private paths or unrelated workspace files. The project licence does not grant rights to redistribute third-party data.

## Bug and security reports

Include the command, Python version, expected behavior, observed behavior and a minimal synthetic input where possible. For provider failures, name the provider and error type, but remove credentials and private paths. Never post secrets in an issue. Use GitHub private vulnerability reporting for security issues if it is enabled.
