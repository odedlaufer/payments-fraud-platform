# features

`definitions.yml` is the single definition of every feature (ADR-003). The `features` package loads and validates it; the Flink jobs and the dbt offline model are generated from or checked against it, so the online and offline paths compute the same values.

```bash
uv run features validate          # load and validate definitions.yml
uv run features catalogue         # regenerate docs/feature-catalogue.md
uv run features catalogue --check # fail if the catalogue is stale
```

Changing a feature's meaning requires bumping its `version`, so logged online values (FR-D3) can always be matched to the definition that produced them.
