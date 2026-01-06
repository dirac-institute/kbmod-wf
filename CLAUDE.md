# CLAUDE.md - AI Assistant Context File

This file provides context for AI assistants (like Claude) working on the kbmod-wf project. It contains essential information about the project structure, architecture, and development practices.

## Project Overview

**kbmod-wf** is a Parsl-based workflow orchestration system for running KBMOD (Kernel-Based Moving Object Detection) across multi-night astronomical survey data at SLAC's USDF SLURM environment.

**Primary Users**: Astronomers and researchers at SLAC National Accelerator Laboratory
**Compute Environment**: SLURM-based HPC cluster with limited network access
**Key Technologies**: Python, Parsl, SLURM, Rubin Observatory LSST stack

## Architecture

### Workflow Execution

```
Parent SLURM Job (72hr max)
  ↓
Parsl DataFlowKernel
  ↓
HighThroughputExecutor
  ↓
Child SLURM Jobs (GPU/CPU tasks)
```

**Workflow Stages**:
1. `create_manifest` - Create ImageCollection manifest from Butler data repository
2. `ic_to_wu` - Convert ImageCollections to WorkUnits
3. `reproject_wu` - Reproject WorkUnits for heliocentric correction
4. `kbmod_search` - Run KBMOD GPU search on WorkUnits

### Monitoring Infrastructure

- **Parsl MonitoringHub**: Collects task metrics to SQLite database (`monitoring.db`)
- **Checkpointing**: Enabled with `checkpoint_mode='task_exit'`
- **Logging**: Centralized logs in `$HOME/rubin-user/parsl/workflow_output/kbmod/workflow/run_logs/`

### Dashboard (In Development)

The dashboard project adds a web-based UI for workflow monitoring and management:
- **Backend**: FastAPI (to be implemented)
- **Frontend**: React/Vue.js or Vanilla JS (to be implemented)
- **Deployment**: Embedded in parent SLURM job or separate service job
- **Access**: SSH tunneling (due to network restrictions)

See [PLAN.md](PLAN.md) for full dashboard implementation plan.

## Key Files and Directories

### Core Workflow Files

- **`src/kbmod_wf/multi_night_workflow.py`**: Main workflow orchestration logic
- **`src/kbmod_wf/resource_configs/usdf_configuration.py`**: USDF SLURM configuration including Parsl executor setup
- **`src/kbmod_wf/workflow_tasks/`**: Individual Parsl app definitions for each workflow stage

### Configuration Files

- **`scripts/parsl_configurator.py`**: CLI tool to generate workflow configs from templates
- **`scripts/generic_runtime_config.toml`**: Template for runtime configuration
- **`scripts/generic_parent_parsl_sbatch.sh`**: Template for parent SLURM job script
- **`scripts/generic_search_config.yaml`**: Template for KBMOD search parameters

### Utility Modules

- **`src/kbmod_wf/utilities/logger_utilities.py`**: Logging setup and management
- **`src/kbmod_wf/utilities/configuration_utilities.py`**: Configuration parsing and validation
- **`src/kbmod_wf/utilities/download_utilities.py`**: Data download and staging helpers

### Dashboard Files (To Be Created)

- **`src/kbmod_wf/dashboard/`**: Dashboard implementation (not yet created)
- **`docs/dashboard/`**: Dashboard documentation

## Environment and Dependencies

### Python Environment

- **Python Version**: 3.10+
- **Primary Framework**: Parsl (workflow orchestration)
- **LSST Stack**: Rubin Observatory LSST Science Pipelines (loaded via `setup lsst_distrib`)
- **KBMOD**: Installed separately or via conda

### SLURM Configuration

- **Login Node**: sdf-login.slac.stanford.edu
- **Compute Partitions**: roma (GPU), milano (CPU)
- **Accounts**: rubin:commissioning, rubin:developers
- **Parent Job Limits**: 72 hours max walltime
- **Network**: Limited outbound access, SSH tunneling required for services

### File Paths at USDF

- **Home**: `$HOME` (e.g., `/sdf/home/u/username/`)
- **Rubin Stack**: `$HOME/rubin-user/lsst_stack_v28_0_1/`
- **Workflow Output**: `$HOME/rubin-user/parsl/workflow_output/kbmod/workflow/run_logs/`
- **Staging Dirs**: Varies by user, typically in `/sdf/data/rubin/shared/...`

## Development Workflow

### Setting Up Local Development

```bash
# Clone repository
git clone https://github.com/dirac-institute/kbmod-wf.git
cd kbmod-wf

# Create conda environment
conda create -n kbmod-wf-dev python=3.10
conda activate kbmod-wf-dev

# Install in development mode
pip install -e '.[dev]'
pre-commit install
```

### Testing on USDF

```bash
# SSH to USDF
ssh user@sdf-login.slac.stanford.edu

# Load LSST stack
source $HOME/rubin-user/lsst_stack_v28_0_1/loadLSST.bash
setup lsst_distrib

# Navigate to kbmod-wf
cd $HOME/rubin-user/parsl/kbmod-wf

# Install/update
pip install -e .
```

### Submitting a Workflow

```bash
# Generate configuration
python scripts/parsl_configurator.py \
    --basedir /path/to/staging \
    --reflex-distances 39.0 \
    --nworkers 32

# Submit to SLURM
cd /path/to/staging
sbatch parent_parsl_sbatch.sh

# Monitor
squeue -u $USER
tail -f run_logs/<run_id>/kbmod.log
```

## Common Development Tasks

### Adding a New Workflow Task

1. Create Parsl app in `src/kbmod_wf/workflow_tasks/`
2. Import and register in `multi_night_workflow.py`
3. Update configuration templates if new parameters needed
4. Add tests in `tests/`

### Modifying SLURM Resource Allocation

Edit `src/kbmod_wf/resource_configs/usdf_configuration.py`:
- `gpu_executor_config`: GPU task settings (partition, walltime, cores)
- `cpu_executor_config`: CPU task settings
- `monitoring_config`: MonitoringHub settings

### Debugging Failed Workflows

1. Check SLURM job status: `squeue -u $USER`
2. Check parent job output: `cat slurm-<jobid>.out`
3. Check Parsl logs: `tail -f run_logs/<run_id>/kbmod.log`
4. Inspect monitoring.db: `sqlite3 monitoring.db "SELECT * FROM task WHERE status='failed';"`
5. Check task-specific logs in `run_logs/<run_id>/`

### Dashboard Development

See [PLAN.md](PLAN.md) for full implementation plan. Key points:
- Use FastAPI for backend
- Leverage existing `parsl_configurator.py` for config generation
- Read from Parsl's `monitoring.db` (read-only)
- Write dashboard state to separate `state.db`
- Deploy embedded in parent job or as separate service

## Important Constraints and Considerations

### SLURM Environment Constraints

- **No long-running non-SLURM processes**: All services must run within SLURM jobs
- **Limited network access**: SSH tunneling required for web services
- **Parent job time limit**: 72 hours maximum
- **Node access**: Compute nodes not directly accessible from outside

### Parsl Specifics

- **Checkpoint mode**: `task_exit` - tasks restart from checkpoints after failure
- **MonitoringHub**: Runs in parent job, writes to SQLite `monitoring.db`
- **Resource specification**: Each task can specify cores, memory, walltime
- **Executors**: GPU (roma partition) and CPU (milano partition) executors

### Data Considerations

- **ImageCollections**: `.collection` files in staging directory
- **WorkUnits**: Intermediate format, large (can be cleaned up after run)
- **Results**: Saved as Parquet files, not ECSV
- **Butler repo**: Usually at `/repo/main` or similar shared path

### Security and Access

- **No secrets in code**: Use environment variables or separate config files
- **SSH keys**: Required for SLURM submission and tunneling
- **SLURM accounts**: Specify correct account (rubin:commissioning, etc.)

## Testing

### Unit Tests

```bash
pytest tests/
```

### Integration Tests

Currently limited. Testing primarily done on USDF with real workflows.

**TODO**: Add integration test framework for workflow stages.

### Dashboard Tests

**TODO**: Add dashboard tests in Phase 1 implementation.

## Git Workflow

### Branch Strategy

- **main**: Stable production code
- **feature branches**: Named descriptively (e.g., `add-dashboard-mvp`, `fix-reproject-memory`)
- **Pull requests**: Required for merging to main

### Commit Messages

Follow conventional commits format:
```
<type>: <description>

[optional body]

[optional footer]
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`

Example:
```
feat: Add dashboard workflow submission API

Implement WorkflowBuilder wrapper around parsl_configurator.py
to enable workflow submission from dashboard UI.

Closes #123
```

### Pre-commit Hooks

Configured in `.pre-commit-config.yaml`. Runs:
- Black (code formatting)
- Flake8 (linting)
- isort (import sorting)
- Other checks

## Deployment

### Production Deployment at USDF

1. User clones/updates kbmod-wf in their USDF home directory
2. User activates LSST stack environment
3. User installs kbmod-wf: `pip install -e .`
4. User generates config: `python scripts/parsl_configurator.py ...`
5. User submits job: `sbatch parent_parsl_sbatch.sh`
6. Workflow executes, monitored via logs or dashboard (when available)

### Dashboard Deployment

**Phase 1 (MVP)**: Embedded in parent SLURM job
**Phase 2+**: Optional separate service job for multi-workflow monitoring

See [docs/dashboard/mvp-setup.md](docs/dashboard/mvp-setup.md) for details.

## Resources and Links

### Documentation

- **Main Docs**: https://kbmod-wf.readthedocs.io/
- **Parsl Docs**: https://parsl.readthedocs.io/
- **KBMOD Docs**: https://github.com/dirac-institute/kbmod
- **Dashboard Plan**: [PLAN.md](PLAN.md)

### Repositories

- **kbmod-wf**: https://github.com/dirac-institute/kbmod-wf
- **kbmod**: https://github.com/dirac-institute/kbmod
- **Rubin Science Pipelines**: https://github.com/lsst

### SLAC/USDF Resources

- **USDF Docs**: https://usdf.slac.stanford.edu/
- **S3DF User Guide**: https://s3df.slac.stanford.edu/
- **SLURM at SLAC**: Internal documentation

## Troubleshooting Common Issues

### Parsl Config Deprecation Warnings

**Issue**: Deprecation warnings about `provider` and `launcher` in Parsl config

**Solution**: Update to new Parsl config format (see PR #78 for example)

### GPU Out of Memory

**Issue**: GPU tasks fail with OOM errors

**Solution**: Reduce batch size in search config or request more GPU memory in executor config

### WorkUnit Cleanup Failures

**Issue**: WorkUnits not cleaned up even with cleanup enabled

**Solution**: Check for filtered in-memory WorkUnits (see PR #77 for fix)

### Checkpoint Recovery Issues

**Issue**: Workflow doesn't resume from checkpoint after restart

**Solution**: Verify `checkpoint_mode='task_exit'` in config and checkpoint files exist in run directory

## AI Assistant Guidelines

When working on this project:

1. **Check PLAN.md** for dashboard implementation strategy before adding dashboard features
2. **Respect SLURM constraints**: No long-running processes outside SLURM, limited network
3. **Reuse existing code**: Use `parsl_configurator.py` rather than reimplementing config logic
4. **Test on USDF**: Parsl and SLURM behavior can be environment-specific
5. **Follow existing patterns**: Match code style and structure of existing workflow tasks
6. **Update documentation**: Add docs for each phase as features are implemented
7. **Preserve compatibility**: Maintain CLI workflow submission alongside dashboard
8. **Security**: Never commit secrets, be careful with file permissions at USDF

## Recent Changes

- **2026-01-06**: Added comprehensive dashboard implementation plan (PLAN.md)
- **PR #78**: Fixed Parsl config deprecation issues
- **PR #77**: Fixed WorkUnit cleanup for filtered in-memory WorkUnits
- **PR #70**: Changed EBD resampling to use 100 points instead of 10
- **PR #69**: Save results as Parquet instead of ECSV

## TODO / Known Issues

- [ ] Implement dashboard (see PLAN.md)
- [ ] Add integration tests for workflow stages
- [ ] Improve error messages for common failure modes
- [ ] Add workflow performance profiling tools
- [ ] Document optimal resource allocation strategies

---

**Last Updated**: 2026-01-06

**Maintained By**: DIRAC Institute, University of Washington

**For Questions**: Open an issue on GitHub or contact the KBMOD team
