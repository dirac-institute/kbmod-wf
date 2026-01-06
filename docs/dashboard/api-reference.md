# Dashboard API Reference

**Version**: MVP (Phase 1)
**Base URL**: `http://localhost:8050/api`

This document describes the REST API endpoints for the KBMOD-WF Dashboard. The API follows RESTful conventions and returns JSON responses.

---

## Phase 1: Monitoring Endpoints (MVP - Current)

### GET /api/workflows

Get list of all workflows.

**Response:**
```json
{
  "workflows": [
    {
      "workflow_id": "abc123",
      "run_dir": "/path/to/run",
      "start_time": "2026-01-06T12:00:00Z",
      "status": "running",
      "tasks_total": 150,
      "tasks_completed": 75,
      "tasks_failed": 2
    }
  ]
}
```

### GET /api/workflows/{workflow_id}

Get details for a specific workflow.

**Parameters:**
- `workflow_id` (path): Workflow identifier

**Response:**
```json
{
  "workflow_id": "abc123",
  "run_dir": "/path/to/run",
  "start_time": "2026-01-06T12:00:00Z",
  "end_time": null,
  "status": "running",
  "config": {
    "basedir": "/path/to/staging",
    "reflex_distances": [39.0],
    "nworkers": 32
  },
  "metrics": {
    "tasks_total": 150,
    "tasks_pending": 50,
    "tasks_running": 23,
    "tasks_completed": 75,
    "tasks_failed": 2,
    "avg_task_duration": 120.5,
    "estimated_completion": "2026-01-06T15:30:00Z"
  }
}
```

### GET /api/tasks

Get list of tasks for a workflow.

**Query Parameters:**
- `workflow_id` (optional): Filter by workflow
- `status` (optional): Filter by status (pending, running, completed, failed)
- `limit` (optional): Limit number of results (default: 100)
- `offset` (optional): Pagination offset (default: 0)

**Response:**
```json
{
  "tasks": [
    {
      "task_id": "task_001",
      "task_name": "create_manifest",
      "status": "completed",
      "start_time": "2026-01-06T12:01:00Z",
      "end_time": "2026-01-06T12:02:30Z",
      "duration": 90.0,
      "try_count": 1
    }
  ],
  "total": 150,
  "limit": 100,
  "offset": 0
}
```

### GET /api/tasks/{task_id}

Get detailed information about a specific task.

**Parameters:**
- `task_id` (path): Task identifier

**Response:**
```json
{
  "task_id": "task_001",
  "task_name": "create_manifest",
  "workflow_id": "abc123",
  "status": "completed",
  "start_time": "2026-01-06T12:01:00Z",
  "end_time": "2026-01-06T12:02:30Z",
  "duration": 90.0,
  "try_count": 1,
  "resources": {
    "memory_mb": 2048,
    "cpu_percent": 45.2,
    "gpu_percent": null
  },
  "dependencies": {
    "upstream": [],
    "downstream": ["task_002", "task_003"]
  }
}
```

### GET /api/tasks/{task_id}/logs

Get execution logs for a specific task.

**Parameters:**
- `task_id` (path): Task identifier

**Query Parameters:**
- `lines` (optional): Number of log lines to return (default: 1000)
- `search` (optional): Search query to filter logs

**Response:**
```json
{
  "task_id": "task_001",
  "logs": [
    {
      "timestamp": "2026-01-06T12:01:05Z",
      "level": "INFO",
      "message": "Starting manifest creation..."
    },
    {
      "timestamp": "2026-01-06T12:02:25Z",
      "level": "INFO",
      "message": "Manifest created successfully"
    }
  ],
  "total_lines": 45
}
```

### GET /api/stream/tasks

Server-Sent Events (SSE) endpoint for real-time task updates.

**Response:** SSE stream

```
event: tasks
data: {"task_id": "task_001", "status": "running"}

event: tasks
data: {"task_id": "task_001", "status": "completed"}
```

---

## Phase 2: Workflow Execution Endpoints (Planned)

> **Note**: These endpoints will be implemented in Phase 2. Documentation will be added when implemented.

### POST /api/workflows/validate

Validate workflow configuration before submission.

**TODO**: Add documentation in Phase 2

### POST /api/workflows/submit

Submit a new workflow for execution.

**TODO**: Add documentation in Phase 2

### GET /api/workflows/{workflow_id}/config

Get the configuration for a workflow.

**TODO**: Add documentation in Phase 2

---

## Phase 3: Retry Management Endpoints (Planned)

> **Note**: These endpoints will be implemented in Phase 3. Documentation will be added when implemented.

### POST /api/retry/queue

Add a task to the retry queue.

**TODO**: Add documentation in Phase 3

### GET /api/retry/queue

Get list of tasks in retry queue.

**TODO**: Add documentation in Phase 3

### POST /api/retry/{task_id}

Retry a specific failed task.

**TODO**: Add documentation in Phase 3

### DELETE /api/retry/{task_id}

Remove a task from the retry queue.

**TODO**: Add documentation in Phase 3

---

## Phase 4: Analysis Endpoints (Planned)

> **Note**: These endpoints will be implemented in Phase 4. Documentation will be added when implemented.

### GET /api/analysis/errors

Get categorized error analysis.

**TODO**: Add documentation in Phase 4

### GET /api/analysis/compare/{task_id}

Compare a task with similar tasks.

**TODO**: Add documentation in Phase 4

### GET /api/analysis/dependencies/{task_id}

Get task dependency graph.

**TODO**: Add documentation in Phase 4

---

## Error Responses

All endpoints may return the following error responses:

### 400 Bad Request
```json
{
  "error": "Invalid parameter",
  "detail": "workflow_id is required"
}
```

### 404 Not Found
```json
{
  "error": "Not found",
  "detail": "Task with id 'task_001' not found"
}
```

### 500 Internal Server Error
```json
{
  "error": "Internal server error",
  "detail": "Database connection failed"
}
```

---

## Rate Limiting

> **Note**: Rate limiting will be implemented in Phase 5.

**TODO**: Add rate limiting documentation in Phase 5

---

## Authentication

> **Note**: Authentication is optional and will be implemented in Phase 5.

**TODO**: Add authentication documentation in Phase 5

---

## Changelog

### Phase 1 (MVP - 2026-01-06)
- Initial API design
- Monitoring endpoints for workflows and tasks
- SSE endpoint for real-time updates

### Phase 2 (Planned)
- **TODO**: Add workflow execution endpoints
- **TODO**: Add configuration validation

### Phase 3 (Planned)
- **TODO**: Add retry management endpoints
- **TODO**: Add retry queue management

### Phase 4 (Planned)
- **TODO**: Add analysis and comparison endpoints
- **TODO**: Add dependency graph endpoints

### Phase 5 (Planned)
- **TODO**: Add authentication endpoints
- **TODO**: Add rate limiting
- **TODO**: Add health check endpoints

---

## Auto-Generated Documentation

For the most up-to-date API documentation, you can access the auto-generated Swagger/OpenAPI docs when running the dashboard:

- **Swagger UI**: `http://localhost:8050/docs`
- **ReDoc**: `http://localhost:8050/redoc`
- **OpenAPI JSON**: `http://localhost:8050/openapi.json`

---

**Last Updated**: 2026-01-06 (Phase 1 - MVP)
