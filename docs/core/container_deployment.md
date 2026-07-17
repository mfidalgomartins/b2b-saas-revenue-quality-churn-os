# Reproducible container release

The container packages the full batch release job: deterministic data generation, analytical builds, governance validation, release gate, dashboard and PDF report. It does not run a web server.

## Build and run

```bash
docker build --tag revenue-quality-os:local .
docker run --rm revenue-quality-os:local
```

The image uses Python 3.12.13 on Debian Bookworm, pinned by multi-architecture OCI digest. Python dependencies are installed from `requirements-dev.lock` with SHA-256 verification, and the editable project install runs without dependency resolution or build isolation. Runtime execution uses unprivileged UID/GID `10001` and requires no operating-system packages.

To retain generated artifacts, bind the output directories:

```bash
mkdir -p data/raw data/processed reports outputs
docker run --rm \
  --mount type=bind,src="$PWD/data",dst=/app/data \
  --mount type=bind,src="$PWD/reports",dst=/app/reports \
  --mount type=bind,src="$PWD/outputs",dst=/app/outputs \
  revenue-quality-os:local
```

On Linux, the bind-mounted directories must be writable by UID `10001`. The repository is copied into the image without generated data, reports or outputs; every release therefore starts from source and the pinned environment rather than inheriting local artifacts.

## Supply-chain controls

- `requirements.lock` contains the hashed runtime dependency closure.
- `requirements-dev.lock` adds the exact QA, SQL-parity and security toolchain needed by the release gate.
- `make lock` is the only supported lock refresh command and requires `pip-tools` from the existing development environment.
- CI installs the development lock with `--require-hashes`, rebuilds every artifact, verifies PDF byte reproducibility, and builds/smoke-tests the same non-root container.
- Dependabot opens grouped weekly updates for Python and GitHub Actions dependencies; dependency changes still have to pass the complete release workflow.

The container deliberately includes the development closure because governed validation check 20 executes the repository's complete test suite, including DuckDB SQL/Python semantic parity. This is a release-verification image, not a minimal serving image.
