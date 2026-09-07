# Production ML Artifacts

Only runtime-approved ML artifacts belong here.

Requirements for any future artifact in this directory:

- `deployed=true`
- `allowed_runtime=true`
- `feature_contract_version` matches the runtime contract
- `schema_hash` matches the runtime contract
- `feature_count` matches the runtime feature list
- companion metadata and encoder files are present when required

No model is deployed here by this cleanup.
