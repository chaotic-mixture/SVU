# Contributing

Use Python 3.11 or 3.12. Install with `python -m pip install --require-hashes -r requirements.lock`, then run `python -m pytest tests -q` and `python -m scripts.demo`. For dependency changes, update `requirements.txt` and `constraints.txt`, regenerate hashes with `python -m scripts.lock_dependencies`, and scan the resulting lock. For chart changes, run the synthetic gallery and browser checks in [the reproduction guide](docs/REPRODUCIBILITY.md).

Changes to formulas, category weights, entity membership or source policy must be described explicitly and reviewed separately from maintenance fixes. Test invalid input as well as expected values. Synthetic fixtures must state `SIMULATED` and must never be described as market evidence.

The supported public file list is `config/public_release.json`. Add every required module, template, configuration and test to that list. Run the export and verify the exported tree independently before proposing a release. Do not commit downloaded data, databases, generated reports, private paths or credentials.

For bug reports, include the command, Python version, expected behavior and a minimal synthetic input. For source failures, include the source identifier and error type without private paths or credentials. Do not post a secret in an issue; use a private maintainer contact or GitHub private vulnerability reporting if enabled.
