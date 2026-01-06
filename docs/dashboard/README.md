# KBMOD-WF Dashboard Documentation

## Overview

The KBMOD-WF Dashboard is a web-based interface for monitoring and executing multi-night KBMOD workflows at SLAC's USDF SLURM environment. It provides real-time workflow monitoring, task progress tracking, failure investigation, and retry management capabilities.

## Documentation Structure

- **[MVP Setup Guide](mvp-setup.md)** - Quick start guide for deploying the MVP dashboard
- **[User Guide](user-guide.md)** - How to use the dashboard for workflow management *(Phase 2)*
- **[API Reference](api-reference.md)** - REST API documentation *(Phase 1 - Initial)*
- **[Architecture](architecture.md)** - Technical architecture and design decisions *(Phase 4)*
- **[Development Guide](development.md)** - Contributing and local development setup *(Phase 5)*

## Quick Links

- [Implementation Plan](../../PLAN.md) - Full implementation roadmap
- [GitHub Repository](https://github.com/dirac-institute/kbmod-wf)
- [Main kbmod-wf Documentation](https://kbmod-wf.readthedocs.io/)

## Features by Phase

### Phase 1: MVP - Read-Only Monitoring (Current)

- ✅ View active workflow progress
- ✅ Inspect task status and logs
- ✅ Basic metrics visualization
- ✅ SSH tunnel access

### Phase 2: Workflow Execution (Planned)

- ⏳ Submit workflows from dashboard
- ⏳ Configure parameters via UI (using `parsl_configurator.py`)
- ⏳ Pre-flight validation

### Phase 3: Retry Management (Planned)

- ⏳ Queue failed tasks for retry
- ⏳ Modify task configurations
- ⏳ Bulk retry operations

### Phase 4: Advanced Features (Planned)

- ⏳ Enhanced failure investigation
- ⏳ Real-time WebSocket updates
- ⏳ Task dependency visualization
- ⏳ Multi-workflow management

### Phase 5: Production Hardening (Planned)

- ⏳ Error handling and recovery
- ⏳ Optional authentication
- ⏳ Deployment automation
- ⏳ Monitoring and alerting

## Getting Started

For MVP deployment, see the [MVP Setup Guide](mvp-setup.md).

For production deployment, see the [Deployment Guide](deployment.md) *(coming in Phase 5)*.

## Support

- **Issues**: [GitHub Issues](https://github.com/dirac-institute/kbmod-wf/issues)
- **Discussions**: [GitHub Discussions](https://github.com/dirac-institute/kbmod-wf/discussions)

## Contributing

See the [Development Guide](development.md) for information on contributing to the dashboard *(coming in Phase 5)*.
