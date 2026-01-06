# KBMOD-WF Dashboard Implementation Plan

## Executive Summary

This plan outlines the implementation of a lightweight, SLURM-compatible monitoring and control dashboard for kbmod-wf workflows at SLAC's USDF. The dashboard leverages Parsl's existing MonitoringHub infrastructure while providing a web-based interface for workflow execution, monitoring, retry management, and failure investigation.

## 1. Architecture Overview

### 1.1 System Components

```
┌─────────────────────────────────────────────────────────────────┐
│                        USDF SLURM Environment                    │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ Parent SLURM Job (72hr max)                            │    │
│  │                                                         │    │
│  │  ┌──────────────────┐      ┌────────────────────────┐ │    │
│  │  │ Parsl Workflow   │─────▶│ MonitoringHub          │ │    │
│  │  │ Orchestrator     │      │ (SQLite: monitoring.db)│ │    │
│  │  └──────────────────┘      └────────────────────────┘ │    │
│  │          │                            │                │    │
│  │          │                            ▼                │    │
│  │          │                  ┌────────────────────────┐ │    │
│  │          │                  │ Dashboard Backend      │ │    │
│  │          │                  │ (FastAPI/Flask)        │ │    │
│  │          │                  └────────────────────────┘ │    │
│  │          │                            │                │    │
│  │          ▼                            ▼                │    │
│  │  ┌──────────────────┐      ┌────────────────────────┐ │    │
│  │  │ Child SLURM Jobs │      │ State Manager          │ │    │
│  │  │ (GPU/CPU tasks)  │      │ (Retry Queue, etc.)    │ │    │
│  │  └──────────────────┘      └────────────────────────┘ │    │
│  └────────────────────────────────────────────────────────┘    │
│                                      │                          │
│                                      │ HTTP (port forwarding)   │
└──────────────────────────────────────┼──────────────────────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │ User Browser    │
                              │ (via SSH tunnel)│
                              └─────────────────┘
```

### 1.2 Deployment Models

**Model A: Embedded Dashboard (Recommended)**
- Dashboard runs within the parent SLURM job process
- Launches alongside the workflow in multi_night_workflow.py
- Automatically terminates when workflow completes
- Simplest integration with existing workflow

**Model B: Companion Service Job**
- Dashboard runs in a separate long-lived SLURM job
- Monitors multiple workflow runs across their lifecycle
- Requires shared filesystem access to monitoring.db
- Better for managing multiple concurrent workflows

**Recommendation:** Start with Model A for MVP, migrate to Model B for production.

## 2. Component Design

### 2.1 Dashboard Backend

**Technology Stack:**
- **Framework:** FastAPI (lightweight, async, auto-docs)
- **Database:** SQLite (Parsl's monitoring.db + dashboard state.db)
- **Process Management:** Python threading or asyncio
- **Authentication:** Optional - SSH tunnel provides security layer

**Core Modules:**

```
dashboard/
├── api/
│   ├── workflows.py      # Workflow execution endpoints
│   ├── monitoring.py     # Task status and metrics endpoints
│   ├── control.py        # Retry queue and task control
│   └── analysis.py       # Failure investigation endpoints
├── data/
│   ├── parsl_monitor.py  # Query Parsl monitoring.db
│   ├── log_parser.py     # Parse and search log files
│   ├── state_manager.py  # Manage retry queue and dashboard state
│   └── workflow_builder.py # Generate workflow configurations
├── models/
│   ├── task.py           # Task status models
│   ├── workflow.py       # Workflow configuration models
│   └── retry.py          # Retry queue models
└── server.py             # FastAPI application entry point
```

### 2.2 Dashboard Frontend

**Technology Stack:**
- **Framework:** React or Vue.js (modern, component-based)
- **Alternative:** Vanilla JS + Tailwind CSS (minimal dependencies)
- **Visualization:** Plotly.js or Chart.js for metrics
- **WebSocket:** For real-time updates (Server-Sent Events as fallback)

**Views:**

1. **Workflow Overview Dashboard**
   - Active workflows list
   - Overall progress bars
   - Resource utilization (CPU/GPU blocks)
   - Recent errors/warnings

2. **Workflow Detail View**
   - Task dependency graph (D3.js or Cytoscape.js)
   - Task status table (pending/running/completed/failed)
   - Timeline view
   - Metrics: task duration, memory usage, GPU utilization

3. **Task Detail View**
   - Task metadata (inputs, outputs, config)
   - Execution logs (searchable, filterable)
   - Stdout/stderr
   - Resource usage graphs
   - Retry history

4. **Workflow Execution Form**
   - Configuration file upload (TOML)
   - Parameter overrides (staging dir, helio_guess_dists, etc.)
   - Pre-flight validation
   - Launch button

5. **Retry Management**
   - Failed tasks queue
   - Bulk retry selection
   - Retry with modified config
   - Clear/reset options

### 2.3 Data Layer

**Parsl Monitoring Database Schema (Read-Only Access):**

Parsl's monitoring.db contains tables:
- `workflow` - Workflow run metadata
- `task` - Task records (status, start/end times, etc.)
- `try` - Individual task execution attempts
- `status` - Status transitions
- `resource` - Resource usage metrics
- `node` - Worker node information

**Dashboard State Database (state.db):**

```sql
CREATE TABLE retry_queue (
    id INTEGER PRIMARY KEY,
    workflow_id TEXT,
    task_id TEXT,
    task_name TEXT,
    original_config TEXT,
    modified_config TEXT,
    retry_count INTEGER,
    status TEXT, -- 'queued', 'retrying', 'completed', 'failed'
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE workflow_submissions (
    id INTEGER PRIMARY KEY,
    config_path TEXT,
    runtime_config TEXT,
    sbatch_job_id TEXT,
    status TEXT, -- 'pending', 'running', 'completed', 'failed'
    created_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE TABLE bookmarks (
    id INTEGER PRIMARY KEY,
    workflow_id TEXT,
    task_id TEXT,
    note TEXT,
    created_at TIMESTAMP
);
```

### 2.4 Parsl Integration

**Monitoring Hook:**

Extend the workflow to expose runtime state:

```python
# In multi_night_workflow.py
def workflow_runner_with_dashboard(env=None, runtime_config={}, enable_dashboard=True):
    # ... existing setup ...

    if enable_dashboard:
        from kbmod_wf.dashboard import DashboardServer
        dashboard = DashboardServer(
            dfk=dfk,
            run_dir=dfk.run_dir,
            port=runtime_config.get('dashboard_port', 8050)
        )
        dashboard.start_in_background()

    # ... existing workflow logic ...

    if enable_dashboard:
        dashboard.stop()
```

**Task Introspection:**

Access task futures and their status programmatically:

```python
# Store futures in a registry accessible to dashboard
from kbmod_wf.dashboard.state_manager import TaskRegistry

registry = TaskRegistry()
registry.register_future('create_manifest', create_manifest_future)
registry.register_future('reproject_wu_0', reproject_future)
# ... etc
```

## 3. Data Flow

### 3.1 Monitoring Flow

```
Parsl Tasks → MonitoringHub → monitoring.db
                    ↓
            Dashboard Backend (polling every 5-10s)
                    ↓
            In-memory cache
                    ↓
            WebSocket → Frontend (push updates)
```

### 3.2 Control Flow (Retry Example)

```
User (Frontend)
    ↓
API POST /api/retry/{task_id}
    ↓
State Manager → retry_queue table
    ↓
Workflow Orchestrator (polls retry_queue)
    ↓
Submits new task via Parsl
    ↓
Updates retry_queue status
    ↓
Frontend receives update via WebSocket
```

### 3.3 Workflow Execution Flow

```
User uploads config.toml
    ↓
API POST /api/workflows/submit
    ↓
Validate config
    ↓
Generate sbatch script (or trigger direct Parsl load)
    ↓
Submit to SLURM (sbatch) or start in current job
    ↓
Store submission in workflow_submissions table
    ↓
Monitor via existing monitoring flow
```

## 4. SLURM Integration Strategy

### 4.1 Dashboard Lifecycle Management

**Option 1: Embedded (Recommended for MVP)**

Modify `generic_parent_parsl_sbatch.sh`:

```bash
# Start dashboard in background
python -m kbmod_wf.dashboard.server \
    --run-dir="$run_dir" \
    --port=8050 \
    --log-file="$run_dir/dashboard.log" &
DASHBOARD_PID=$!

# Run workflow
python "$kbmodwfdir/src/kbmod_wf/multi_night_workflow.py" \
    --runtime-config="____tomlfile____" \
    --env="usdf"

# Cleanup
kill $DASHBOARD_PID
```

**Option 2: Separate Service Job**

Create `dashboard_service_sbatch.sh`:

```bash
#!/bin/bash
#SBATCH --job-name=kbmod-dashboard
#SBATCH --time=168:00:00  # 7 days
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --account=rubin:commissioning
#SBATCH --partition=roma

python -m kbmod_wf.dashboard.server \
    --monitor-dir="$HOME/rubin-user/parsl/workflow_output" \
    --port=8050 \
    --persist
```

### 4.2 Network Access Strategy

USDF has limited but available network access within the cluster.

**Access Methods:**

1. **SSH Tunnel (Primary)**
   ```bash
   # From local machine
   ssh -L 8050:sdfrome001:8050 user@sdf-login.slac.stanford.edu
   # Access at http://localhost:8050
   ```

2. **SLURM Node Port Forwarding**
   - Dashboard binds to specific SLURM node hostname
   - Users SSH to login node, then tunnel to compute node

3. **Shared Filesystem Dashboard**
   - Generate static HTML snapshots to shared directory
   - User views latest snapshot (no real-time updates)
   - Fallback for restricted network scenarios

### 4.3 Workflow Submission from Dashboard

**Method 1: Direct SLURM Submission**

Dashboard calls `sbatch` to submit new parent jobs:

```python
import subprocess

def submit_workflow(config_path: str, env: str = "usdf"):
    # Generate sbatch script
    sbatch_script = generate_sbatch_script(config_path, env)

    # Submit via sbatch
    result = subprocess.run(
        ["sbatch", sbatch_script],
        capture_output=True,
        text=True
    )

    # Parse job ID
    job_id = parse_job_id(result.stdout)
    return job_id
```

**Method 2: In-Process Launch (Embedded Dashboard Only)**

If dashboard is embedded in parent job:

```python
def launch_workflow_in_process(runtime_config: dict, env: str):
    # Import workflow runner
    from kbmod_wf.multi_night_workflow import workflow_runner

    # Launch in separate thread
    import threading
    thread = threading.Thread(
        target=workflow_runner,
        args=(env, runtime_config)
    )
    thread.start()

    return thread
```

**Recommendation:** Use Method 1 for production (cleaner isolation).

## 5. Monitoring Approach

### 5.1 Real-Time Updates

**WebSocket/SSE Implementation:**

```python
# FastAPI SSE endpoint
from fastapi import FastAPI
from sse_starlette.sse import EventSourceResponse

@app.get("/api/stream/tasks")
async def task_stream():
    async def event_generator():
        while True:
            tasks = get_tasks_from_monitoring_db()
            yield {
                "event": "tasks",
                "data": json.dumps(tasks)
            }
            await asyncio.sleep(5)  # Update every 5 seconds

    return EventSourceResponse(event_generator())
```

### 5.2 Metrics Collection

**Data Sources:**

1. **Parsl monitoring.db** (primary)
   - Task start/end times
   - Status transitions
   - Try count
   - Resource usage (CPU, memory, GPU)

2. **Log files** (secondary)
   - Error messages
   - Warning patterns
   - Custom kbmod metrics (e.g., "Number of results found: X")

3. **SLURM accounting** (tertiary)
   - Job walltime
   - Queue times
   - Node allocation history

**Key Metrics:**

- **Workflow-level:**
  - Total tasks (by stage)
  - Completion percentage
  - Estimated time remaining
  - Average task duration
  - Failure rate

- **Task-level:**
  - Execution time
  - Memory usage (peak)
  - GPU utilization
  - Retry count
  - Queue time vs. run time

- **System-level:**
  - Active SLURM blocks
  - Pending vs. running tasks
  - Resource utilization (CPU/GPU)

### 5.3 Log Aggregation

**Log Parser:**

```python
import re
from pathlib import Path

class LogParser:
    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.parsl_log = run_dir / "kbmod.log"

    def get_task_logs(self, task_id: str):
        # Extract logs for specific task
        pattern = rf"\[.*{task_id}.*\]"
        lines = []
        with open(self.parsl_log) as f:
            for line in f:
                if re.search(pattern, line):
                    lines.append(line)
        return lines

    def search_errors(self, query: str = None):
        # Search for error patterns
        error_pattern = r"ERROR|Exception|Traceback"
        errors = []
        with open(self.parsl_log) as f:
            for line in f:
                if re.search(error_pattern, line):
                    if query is None or query in line:
                        errors.append(line)
        return errors
```

## 6. Storage and Persistence Strategy

### 6.1 Database Location

**Parsl monitoring.db:**
- Location: `{run_dir}/monitoring.db`
- Managed by: Parsl MonitoringHub
- Access: Read-only from dashboard
- Lifetime: Persists after workflow completion

**Dashboard state.db:**
- Location: `{run_dir}/dashboard_state.db`
- Managed by: Dashboard backend
- Access: Read-write from dashboard
- Lifetime: Persists for retry queue, cleared on new run

### 6.2 File-Based State

**Checkpoint Integration:**

Parsl checkpoints are stored in `{run_dir}/checkpoint/`:
- Each task checkpoint is a pickled Python object
- Dashboard can inspect checkpoint directory to show recovery status

**Configuration Archives:**

Store submitted configurations:
```
$HOME/rubin-user/parsl/workflow_configs/
├── 2026-01-06_run001/
│   ├── runtime_config.toml
│   ├── search_config.yaml
│   ├── sbatch_script.sh
│   └── metadata.json
```

### 6.3 Data Retention

**Policy:**
- Keep monitoring.db for completed workflows: 90 days
- Keep dashboard_state.db: 30 days
- Archive logs: Compress after 7 days, delete after 90 days
- Checkpoint files: Keep only for active/recent workflows (7 days)

## 7. Retry and Failure Handling

### 7.1 Retry Queue Implementation

**Retry Workflow:**

1. User selects failed task(s) in UI
2. Dashboard adds to retry_queue table
3. Background thread polls retry_queue
4. For queued retry:
   - Load task configuration
   - Apply any user modifications
   - Submit new task via Parsl
   - Update retry_queue status
5. Monitor new task execution
6. Update retry_queue on completion

**Code Example:**

```python
class RetryManager:
    def __init__(self, dfk, state_db):
        self.dfk = dfk
        self.state_db = state_db

    def queue_retry(self, task_id: str, modified_config: dict = None):
        # Add to retry queue
        self.state_db.execute(
            "INSERT INTO retry_queue (task_id, modified_config, status) "
            "VALUES (?, ?, 'queued')",
            (task_id, json.dumps(modified_config), )
        )

    def process_retry_queue(self):
        # Get queued retries
        queued = self.state_db.execute(
            "SELECT * FROM retry_queue WHERE status = 'queued'"
        ).fetchall()

        for retry in queued:
            # Update status
            self.state_db.execute(
                "UPDATE retry_queue SET status = 'retrying' WHERE id = ?",
                (retry['id'],)
            )

            # Resubmit task
            # (Implementation depends on task type)
            future = self._resubmit_task(retry)

            # Monitor future
            self._monitor_retry(retry['id'], future)
```

### 7.2 Failure Investigation

**Features:**

1. **Error Categorization:**
   - Parse log errors into categories (GPU OOM, timeout, data missing, etc.)
   - Show frequency of each error type
   - Suggest fixes based on error pattern

2. **Task Comparison:**
   - Compare successful vs. failed tasks
   - Identify differences in config, inputs, or environment
   - Highlight anomalies (e.g., higher memory usage)

3. **Log Search:**
   - Full-text search across all logs
   - Filter by task, time range, log level
   - Highlight context around errors

4. **Dependency Tracing:**
   - Show upstream dependencies of failed task
   - Check if failure was caused by bad input from parent task
   - Visualize task dependency graph with failure highlights

## 8. Implementation Phases

### Phase 1: MVP - Read-Only Monitoring (2-3 weeks)

**Objectives:**
- View active workflow progress
- Inspect task status and logs
- Basic metrics visualization

**Deliverables:**
- [ ] Backend API for reading Parsl monitoring.db
- [ ] Log file parser
- [ ] Frontend: Workflow overview dashboard
- [ ] Frontend: Task detail view
- [ ] SSH tunnel access documentation

**Tasks:**
1. Set up FastAPI project structure
2. Implement Parsl monitoring.db reader
3. Create API endpoints for tasks, workflows
4. Build React/Vue frontend (or static HTML)
5. Deploy as embedded service in parent job
6. Test on USDF with sample workflow

### Phase 2: Workflow Execution (1-2 weeks)

**Objectives:**
- Submit new workflows from dashboard
- Configure workflow parameters via UI
- Validate configurations

**Deliverables:**
- [ ] Workflow submission API
- [ ] Configuration validation
- [ ] SBATCH script generation
- [ ] Frontend: Workflow execution form
- [ ] Integration with SLURM submission

**Tasks:**
1. Implement workflow submission API
2. Create configuration validator
3. Build SBATCH script generator
4. Create frontend form for workflow config
5. Test end-to-end submission flow

### Phase 3: Retry Management (2 weeks)

**Objectives:**
- Queue failed tasks for retry
- Modify task configurations
- Bulk retry operations

**Deliverables:**
- [ ] Dashboard state database (state.db)
- [ ] Retry queue implementation
- [ ] Retry processing background worker
- [ ] Frontend: Retry management UI

**Tasks:**
1. Design and create state.db schema
2. Implement RetryManager class
3. Create retry queue polling worker
4. Build retry UI components
5. Test retry workflows

### Phase 4: Advanced Features (2-3 weeks)

**Objectives:**
- Enhanced failure investigation
- Real-time notifications
- Performance optimization
- Multi-workflow management

**Deliverables:**
- [ ] Error categorization engine
- [ ] WebSocket/SSE real-time updates
- [ ] Task dependency visualization
- [ ] Multi-workflow dashboard
- [ ] Performance metrics dashboard

**Tasks:**
1. Implement log analysis and error categorization
2. Add WebSocket support for real-time updates
3. Create task dependency graph visualization (D3.js)
4. Build multi-workflow overview
5. Add resource utilization graphs

### Phase 5: Production Hardening (1-2 weeks)

**Objectives:**
- Reliability improvements
- Security hardening
- Documentation
- Deployment automation

**Deliverables:**
- [ ] Error handling and recovery
- [ ] Authentication (optional)
- [ ] User documentation
- [ ] Deployment scripts
- [ ] Monitoring and alerting

**Tasks:**
1. Add comprehensive error handling
2. Implement authentication if needed
3. Write user guide and API docs
4. Create automated deployment scripts
5. Set up health checks and alerting

## 9. Key Technical Decisions and Trade-offs

### 9.1 Embedded vs. Separate Service

**Decision: Start with Embedded, Migrate to Separate**

**Rationale:**
- Embedded is simpler for MVP (no separate job management)
- Separate service better for production (persistent, multi-workflow)
- Migration path is straightforward

**Trade-offs:**
- Embedded: Lifetime limited to parent job (max 72hr)
- Separate: Requires managing additional SLURM job
- Embedded: One dashboard per workflow run
- Separate: One dashboard for all runs (better resource utilization)

### 9.2 Real-Time vs. Polling Updates

**Decision: Polling for MVP, Real-Time for Production**

**Rationale:**
- Polling is simpler to implement (HTTP only)
- Real-time requires WebSocket/SSE support
- USDF network may have restrictions on WebSocket

**Trade-offs:**
- Polling: 5-10s latency, higher server load
- Real-time: Immediate updates, more complex implementation
- Recommendation: Start with polling, add real-time in Phase 4

### 9.3 Direct Parsl Integration vs. Database-Only

**Decision: Hybrid - Database for Monitoring, API for Control**

**Rationale:**
- Database is reliable for historical data
- Direct Parsl integration needed for dynamic control (retries, submissions)
- Hybrid approach balances simplicity and power

**Trade-offs:**
- Database-only: Simpler, but limited to monitoring
- Full integration: Powerful, but tightly coupled to Parsl internals
- Hybrid: Best of both, moderate complexity

### 9.4 Technology Stack Choices

**Backend: FastAPI**
- Pros: Modern, async, auto-docs, lightweight
- Cons: Requires Python 3.7+
- Alternative: Flask (more mature, larger ecosystem)

**Frontend: React vs. Vanilla JS**
- React: Component-based, rich ecosystem, better for complex UI
- Vanilla: No build step, simpler deployment, faster iteration
- Recommendation: Vanilla JS for MVP, React for production

**Database: SQLite**
- Pros: No separate server, file-based, Parsl compatibility
- Cons: Limited concurrency, not for high-scale
- Suitable for this use case (single writer, few readers)

### 9.5 Security Considerations

**Authentication:**
- MVP: Rely on SSH tunnel for security (no auth in dashboard)
- Production: Add token-based auth or integrate with SLAC SSO
- Dashboard should not expose sensitive data (credentials, etc.)

**Authorization:**
- Read-only operations: Open to all authenticated users
- Write operations (submit, retry): Restricted to workflow owners
- Admin operations: SLURM account admins only

**Data Protection:**
- monitoring.db contains no secrets (only task metadata)
- Configuration files may contain paths (not credentials)
- Logs may contain debugging info (sanitize before display)

## 10. Success Metrics

**Usability:**
- Time to identify failed task: < 30 seconds
- Time to retry failed task: < 2 minutes
- Time to submit new workflow: < 5 minutes

**Reliability:**
- Dashboard uptime: > 99% (while parent job running)
- Database query latency: < 500ms (p95)
- UI responsiveness: < 200ms (local rendering)

**Adoption:**
- Active users: All kbmod-wf users at USDF
- Workflow submissions via dashboard: > 50% (vs. manual sbatch)
- Positive user feedback: > 80% satisfaction

## 11. Future Enhancements

### 11.1 Advanced Monitoring
- Anomaly detection for task performance
- Predictive ETA based on historical data
- Resource usage forecasting

### 11.2 Workflow Optimization
- Suggest optimal resource allocation
- Auto-scaling SLURM blocks based on queue depth
- Task coalescing for small tasks

### 11.3 Integration Expansions
- Slack/email notifications for workflow events
- Integration with SLAC's monitoring infrastructure
- Export metrics to Prometheus/Grafana

### 11.4 Collaborative Features
- Workflow sharing and templates
- Comments and annotations on tasks
- Team dashboards for shared projects

## 12. Risk Mitigation

**Risk: Network access restrictions**
- Mitigation: SSH tunnel access, static HTML snapshot fallback
- Contingency: Pure CLI alternative for fully restricted environments

**Risk: Parsl API changes**
- Mitigation: Primarily use monitoring.db (stable interface)
- Contingency: Version pinning, compatibility layer

**Risk: SLURM job preemption**
- Mitigation: Embedded dashboard restarts with workflow (via checkpointing)
- Contingency: Separate service job provides continuity

**Risk: Performance overhead**
- Mitigation: Lightweight backend, polling interval tuning
- Contingency: Disable dashboard if interfering with workflow

## 13. Development Environment Setup

### 13.1 Local Development

```bash
# Clone repository
git clone https://github.com/dirac-institute/kbmod-wf.git
cd kbmod-wf

# Create virtual environment
conda create -n kbmod-wf-dev python=3.10
conda activate kbmod-wf-dev

# Install dependencies
pip install -e '.[dev]'
pip install fastapi uvicorn sse-starlette

# Install dashboard dependencies
pip install -r dashboard/requirements.txt

# Run dashboard locally (against sample monitoring.db)
python -m kbmod_wf.dashboard.server \
    --run-dir ./sample_run_logs \
    --port 8050 \
    --dev
```

### 13.2 Testing on USDF

```bash
# SSH to USDF
ssh user@sdf-login.slac.stanford.edu

# Load environment
source $HOME/rubin-user/lsst_stack_v28_0_1/loadLSST.bash
setup lsst_distrib

# Install kbmod-wf with dashboard
cd $HOME/rubin-user/parsl/kbmod-wf
pip install -e '.[dashboard]'

# Submit test workflow with dashboard
sbatch --comment="dashboard-test" test_parent_parsl_sbatch.sh

# Check dashboard port
squeue -u $USER -o "%i %P %j %N"
# Note the node name, e.g., sdfrome023

# From local machine, create SSH tunnel
ssh -L 8050:sdfrome023:8050 user@sdf-login.slac.stanford.edu

# Access dashboard at http://localhost:8050
```

## 14. Documentation Requirements

### 14.1 User Documentation
- Quick start guide (5 minutes to first dashboard)
- Workflow submission tutorial
- Retry management guide
- Troubleshooting common issues
- FAQ

### 14.2 Developer Documentation
- Architecture overview (this document)
- API reference (auto-generated from FastAPI)
- Database schema
- Adding new features guide
- Testing guide

### 14.3 Operational Documentation
- Deployment procedures
- Monitoring and alerting setup
- Backup and recovery
- Performance tuning
- Security best practices

---

## Conclusion

This implementation plan provides a comprehensive roadmap for building a Grafana-style dashboard for kbmod-wf workflows at USDF. The phased approach allows for incremental value delivery, starting with a read-only monitoring MVP and progressing to full workflow management capabilities.

The architecture is designed to work within SLURM constraints by:
1. Running as part of the parent SLURM job (embedded model)
2. Leveraging existing Parsl infrastructure (MonitoringHub)
3. Using SSH tunnels for network access
4. Storing state in file-based databases (SQLite)

By following this plan, the kbmod-wf team will have a powerful, user-friendly tool for executing, monitoring, and managing multi-night workflows at scale.

---

## Critical Files for Implementation

- `/home/user/kbmod-wf/src/kbmod_wf/multi_night_workflow.py` - Core workflow file to integrate dashboard startup and shutdown hooks
- `/home/user/kbmod-wf/src/kbmod_wf/resource_configs/usdf_configuration.py` - USDF SLURM configuration including MonitoringHub setup to reference for dashboard integration
- `/home/user/kbmod-wf/scripts/generic_parent_parsl_sbatch.sh` - SBATCH script template to modify for launching dashboard alongside workflow
- `/home/user/kbmod-wf/src/kbmod_wf/utilities/logger_utilities.py` - Logging infrastructure to understand for log parsing implementation
- `/home/user/kbmod-wf/src/kbmod_wf/utilities/configuration_utilities.py` - Configuration management patterns to follow for dashboard config validation
