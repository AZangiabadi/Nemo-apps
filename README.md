# NEMO Apps

This repository contains two self-contained versions of the Columbia Nano
Initiative NEMO tools application.

## Applications

- `nemo_tools_app_v2/` is the maintained production application. Its source,
  Docker configuration, web assets, invoice assets, and setup documentation are
  contained within that directory.
- `nemo_tools_app_v1/` is the archived legacy application. It remains available
  for rollback and reference and is also self-contained.

For current development and deployment:

```bash
cd nemo_tools_app_v2
```

See `nemo_tools_app_v2/README.md` for setup, testing, and Docker commands.

For the legacy application:

```bash
cd nemo_tools_app_v1
```

See `nemo_tools_app_v1/README.md` for its setup and Docker commands.

## Repository-level files

The `.github/` directory contains CI and dependency-update configuration for
V2. The root `.gitignore` applies to the whole repository. Application code and
runtime configuration belong inside the corresponding V1 or V2 directory.

Local parent-level archives, generated reports, imports, backups, secrets, and
old job state are not required to run V1 or V2. They are excluded from Git and
should be retained or deleted according to local data-retention needs.
