# diagnostic-v0.1-candidate

This is a local source candidate, not a published release.

Final source review on 2026-09-08 added regression coverage for English labels after chart interaction, language persistence, diagnostic table headings, and scheduler recovery after a failed run. Shutdown now waits for an active refresh to complete, and missing refresh dependencies fail visibly at startup. Windows launch prefers a project virtual environment. Scheduled refresh is ordinary Python tied to the server lifecycle; it does not use AI.

- Public files are selected individually; exported trees do not contain repository history, market databases or internal working notes.
- Added a synthetic offline example, ordered official-data rebuild, public export checks and CI configuration.
- Reject invalid prices, duplicate dates/columns, invalid bases and ambiguous category membership before index computation. Undefined correlations are represented as null; zero remaining weights are marked as skipped.
- Daily refresh supports the registered ECB identifiers, fills historical gaps, retains raw responses on revision conflicts and returns failure when a source or derived output fails. A revision comparison allows only relative floating-point roundoff of 1e-12.
- Daily, domain and snapshot outputs share a read-only SQLite backup and are published as a complete generation through an atomic pointer switch. Failed refreshes preserve the previous published generation; a local report server follows the current pointer.
- Dynamic domain trends are named Domain ICATI. Existing machine keys remain compatibility aliases.
- Requests was upgraded from 2.32.3 to 2.34.2 after reviewing the upstream [release history](https://requests.readthedocs.io/en/latest/community/updates/) and [security advisory](https://github.com/psf/requests/security/advisories/GHSA-gc5v-m9x4-r6x2). The upstream temporary-file issue concerns direct use of the extraction utility, not ordinary HTTP calls; the upgrade is dependency maintenance, not evidence of an exploit in SVU.
- Pytest was upgraded to 9.0.3 for its upstream temporary-directory security fix; see the [pytest changelog](https://docs.pytest.org/en/latest/changelog.html). Python dependencies have a full artifact hash lock, and browser tooling uses a pnpm integrity lock. Dated scans on 2026-09-07 found no known vulnerabilities in either dependency set.
- Rebuild validates input manifests and hashes. Refresh retains distinct raw response revisions, rejects duplicate daily observations and reuses ECB reference responses within a run.
- Fixed tooltip recovery after an empty daily window, mobile page overflow and large-series domain chart extrema calculations. Added synthetic full-page gallery and automated Chromium interaction checks at 1440 and 390 pixels.

On 2026-09-07, an isolated live reconstruction, live incremental refresh, 58 historical endpoint probes and real-data browser checks all passed. The complete official basket still ends on 2025-12-30; newer individual observations do not extend it. Local tests ran on Windows; Linux runtime and remote GitHub CI execution remain unverified. Remote history and data redistribution rights are outside this source candidate. Test success is not an economic claim.
