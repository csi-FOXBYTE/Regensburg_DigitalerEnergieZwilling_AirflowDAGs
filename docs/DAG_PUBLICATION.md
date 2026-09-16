# DAG publication

The development repository is
[Regensburg_DigitalerEnergieZwilling_AirflowDAGs](https://github.com/csi-FOXBYTE/Regensburg_DigitalerEnergieZwilling_AirflowDAGs).
Its **Publish DAGs** GitHub Actions workflow publishes source `main` to `main` in
[Regensburg_DigitalerEnergieZwilling_DAGs](https://github.com/csi-FOXBYTE/Regensburg_DigitalerEnergieZwilling_DAGs).
The destination root is the DAG bundle; configure `GitDagBundle` without `subdir`.

## One-time authentication setup

Create a fine-grained personal access token with:

- Resource owner: `csi-FOXBYTE`.
- Repository access: only `Regensburg_DigitalerEnergieZwilling_DAGs`.
- Repository permission **Contents: Read and write** (Metadata read access is implicit).

Obtain organization approval if required. The token's user must have write access,
and destination branch rules must permit that user to push directly to `main`.

In the **development/source repository**, open **Settings → Secrets and variables
→ Actions → New repository secret** and save the token as **`DAG_PUBLISH_TOKEN`**.
The built-in `GITHUB_TOKEN` is scoped to the source repository and cannot provide
this cross-repository write access. See GitHub's
[token documentation](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens).

## Publish from GitHub

Once the workflow is committed to the source repository's default branch, open
**Actions → Publish DAGs → Run workflow** and choose `main`. The workflow always
exports the latest source `main`, even if another workflow branch is selected.
It runs only on manual dispatch.

The export includes:

- All committed contents of `dags/`, directly at the destination root.
- `LICENSE`, `COPYING`, `COPYING.LESSER`, and `NOTICE`.
- The checked-in `SBOM.cdx.json` and `SBOM.csv`.
- `sbom.config.json`, `requirements-sbom.txt`, and `scripts/generate_sbom.py`.
- The source `.gitignore` and this document as `PUBLICATION.md`.

Each run compares this snapshot with destination `main` and commits additions,
modifications, and deletions. Destination files outside the snapshot are removed,
except `.github/`, which is preserved for destination-specific automation. Make
published code changes in the source repository: direct destination edits are
overwritten on the next publication. The DAG README becomes the root README;
local services, environment files, test data, and source Git history are not exported.
The exported `.airflowignore` also excludes `scripts/` so Airflow does not import
the SBOM generation tool as a DAG module.

Existing destination history is preserved. Identical content produces no commit.
An empty destination gets its first commit on `main`; a nonempty destination must
already have a `main` branch. Pushes are never forced. If another writer updates
the destination during publication, the push fails; run the workflow again to
compare against its latest state. The commit message records the source revision.

## SBOM and Open CoDE publication files

The license texts, notice, deployment README, and SBOMs accompany the DAG source
for subsequent publication on Open CoDE. This workflow only pushes to the GitHub
destination above.

Publication copies the checked-in SBOMs unchanged. They describe the recorded
Airflow environment, reachable DAG dependencies, and referenced container images;
they are not a fresh scan at publication time. Their source repository paths and
revision remain provenance for that inventory, even though the bundle is flattened.
Container contents are not expanded. Regenerate and review the SBOMs in the source
repository when dependencies or image declarations change, then commit them before
publishing.

The exported tooling also supports regeneration from the destination root. First
provision a compatible Airflow environment using the deployment README, then run:

```bash
python3 -m venv .sbom-env
.sbom-env/bin/python -m pip install -r requirements-sbom.txt
.sbom-env/bin/python scripts/generate_sbom.py --python /absolute/path/to/airflow/bin/python
```

`sbom.config.json` retains the source project's identity and metadata paths.
An intentional inventory for a separately maintained fork should update those
declarations before regeneration. Destination edits will otherwise be replaced by
the next publication from the source repository.
