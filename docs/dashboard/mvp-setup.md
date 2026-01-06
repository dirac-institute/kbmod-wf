# MVP Dashboard Setup Guide

This guide walks you through deploying the MVP (Phase 1) read-only monitoring dashboard for kbmod-wf workflows at USDF.

## Prerequisites

- Access to SLAC's USDF SLURM environment
- Python 3.10+ environment with kbmod-wf installed
- Active or completed workflow with Parsl monitoring.db

## Installation

### 1. Install Dashboard Dependencies

```bash
# SSH to USDF
ssh user@sdf-login.slac.stanford.edu

# Activate your kbmod environment
source $HOME/rubin-user/lsst_stack_v28_0_1/loadLSST.bash
setup lsst_distrib

# Navigate to kbmod-wf repository
cd $HOME/rubin-user/parsl/kbmod-wf

# Install dashboard dependencies
pip install -e '.[dashboard]'
```

### 2. Verify Installation

```bash
# Check that dashboard module is available
python -c "from kbmod_wf.dashboard import server; print('Dashboard ready!')"
```

## Running the Dashboard

### Option A: Standalone Mode (For Testing)

Run the dashboard to monitor an existing workflow:

```bash
# Navigate to a workflow run directory
cd $HOME/rubin-user/parsl/workflow_output/kbmod/workflow/run_logs/<run_id>

# Start dashboard
python -m kbmod_wf.dashboard.server \
    --run-dir="$(pwd)" \
    --port=8050 \
    --log-file="dashboard.log"
```

The dashboard will start and display:
```
Dashboard running on http://<hostname>:8050
Monitoring: /path/to/run_dir/monitoring.db
Press Ctrl+C to stop
```

### Option B: Embedded Mode (Production)

For new workflows, the dashboard will automatically start when enabled in the workflow configuration.

**Step 1: Enable dashboard in runtime config**

Edit your `runtime_config.toml`:

```toml
[dashboard]
enabled = true
port = 8050
```

**Step 2: Submit workflow as usual**

```bash
sbatch parent_parsl_sbatch.sh
```

The dashboard will start automatically when the workflow begins.

## Accessing the Dashboard

### Setup SSH Tunnel

Since USDF compute nodes have limited network access, you'll access the dashboard via SSH tunnel.

**Step 1: Find the compute node**

```bash
# Check your running jobs
squeue -u $USER -o "%i %P %j %N"

# Example output:
# JOBID PARTITION NAME NODELIST
# 12345 roma      kbmod-wf sdfrome023
```

Note the node name (e.g., `sdfrome023`).

**Step 2: Create SSH tunnel from your local machine**

```bash
# Replace 'sdfrome023' with your actual node name
# Replace 'user' with your USDF username
ssh -L 8050:sdfrome023:8050 user@sdf-login.slac.stanford.edu
```

**Step 3: Access dashboard in browser**

Open your browser and navigate to:
```
http://localhost:8050
```

You should see the KBMOD-WF Dashboard interface.

## Dashboard Features (MVP)

### Workflow Overview Page

- **Active Workflows**: List of running workflows
- **Progress Bars**: Overall completion percentage
- **Task Summary**: Counts by status (pending/running/completed/failed)
- **Recent Activity**: Latest task completions and errors

### Task List View

- **Status Filter**: Filter tasks by status
- **Search**: Search tasks by name
- **Sort**: Sort by start time, duration, status
- **Details**: Click task to view details

### Task Detail View

- **Metadata**: Task ID, status, start/end time, duration
- **Logs**: Searchable execution logs
- **Resources**: Memory usage, CPU/GPU utilization (if available)
- **Dependencies**: Upstream and downstream tasks

## Troubleshooting

### Dashboard won't start

**Check Python version:**
```bash
python --version  # Should be 3.10+
```

**Check dependencies:**
```bash
pip list | grep -E "fastapi|uvicorn|sse-starlette"
```

**Check port availability:**
```bash
# Check if port 8050 is already in use
netstat -tuln | grep 8050

# If in use, try a different port
python -m kbmod_wf.dashboard.server --port=8051
```

### Cannot connect to dashboard

**Verify SSH tunnel:**
```bash
# From local machine, check if tunnel is active
netstat -an | grep 8050
```

**Check firewall settings:**
```bash
# On USDF, check if dashboard is listening
curl http://localhost:8050
```

### No data showing in dashboard

**Check monitoring.db exists:**
```bash
ls -lh monitoring.db
```

**Verify database has data:**
```bash
sqlite3 monitoring.db "SELECT COUNT(*) FROM task;"
```

**Check dashboard logs:**
```bash
tail -f dashboard.log
```

### Performance issues

**Reduce polling frequency:**

Edit the dashboard config to poll less frequently (default: 5 seconds):

```python
# In server.py or config
POLL_INTERVAL = 10  # seconds
```

**Limit log display:**

If logs are very large, limit the number of lines displayed in the UI.

## Configuration Reference

### Command Line Options

```bash
python -m kbmod_wf.dashboard.server --help
```

Options:
- `--run-dir`: Path to workflow run directory (default: current directory)
- `--port`: Port to bind to (default: 8050)
- `--host`: Host to bind to (default: 0.0.0.0)
- `--log-file`: Path to dashboard log file (default: dashboard.log)
- `--dev`: Enable development mode with auto-reload (default: False)
- `--monitor-dir`: Monitor multiple runs in directory (default: None)

### Environment Variables

- `KBMOD_DASHBOARD_PORT`: Override default port
- `KBMOD_DASHBOARD_LOG_LEVEL`: Set log level (DEBUG, INFO, WARNING, ERROR)

## Next Steps

Once you've successfully deployed the MVP dashboard:

1. **Provide Feedback**: Report issues or suggestions on [GitHub Issues](https://github.com/dirac-institute/kbmod-wf/issues)
2. **Phase 2 Preview**: Workflow execution features (coming soon)
3. **Phase 3 Preview**: Retry management features (coming soon)

## Support

For help with the dashboard:

- Check the [FAQ](faq.md) *(coming soon)*
- Search [GitHub Issues](https://github.com/dirac-institute/kbmod-wf/issues)
- Ask on [GitHub Discussions](https://github.com/dirac-institute/kbmod-wf/discussions)

---

**Note**: This is the MVP (Phase 1) documentation. Features from Phase 2+ will be documented as they are implemented.
