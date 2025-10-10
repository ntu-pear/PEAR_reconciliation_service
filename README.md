# PEAR Reconciliation Service

Reconciliation Service is responsible for ensuring data consistency across the PEAR system by detecting and resolving data drift between authoritative and eventual consistent services.

## Overview

The reconciliation service monitors data consistency between:
- **Activity Service** (authoritative) ↔ **Scheduler Service** (eventual)
- **Patient Service** (authoritative) ↔ **Scheduler Service** (eventual)

## Features

- **Scheduled Reconciliation** - Hourly integrity checks
- **Drift Detection** - Identifies missing, stale, and orphaned records
- **Automatic Resolution** - Republishes events to fix inconsistencies
- **Monitoring & Metrics** - Comprehensive observability
- **Field Mapping** - Handles different column names between services

## Quick Start

1. **Create a Conda Virtual Environment::**
   ```bash
   conda create -n pear_reconciliation_service python=3.9.19
   conda activate pear_reconciliation_service
   ```

2. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment:**
   ```bash
   cp .env.example .env
   # Edit .env with the actual variables from Confluence
   ```

4. **Run the Service:**
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```

## API Endpoints

- `GET /reconciliation/run` - Trigger manual reconciliation
- `GET /reconciliation/status` - Check reconciliation status
- `GET /reconciliation/history` - View reconciliation history
- `GET /health` - Health check

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Activity      │    │   Patient       │    │   Scheduler     │
│   Service       │    │   Service       │    │   Service       │
│ (Authoritative) │    │ (Authoritative) │    │ (Eventually     │
│                 │    │                 │    │  Consistent)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────▼─────────────┐
                    │   Reconciliation Service  │
                    │                           │
                    │ • Drift Detection         │
                    │ • Event Republishing      │
                    │ • Monitoring              │
                    └───────────────────────────┘
```

## Configuration

Set these environment variables:

- `ACTIVITY_SERVICE_URL` - Activity service base URL
- `PATIENT_SERVICE_URL` - Patient service base URL  
- `SCHEDULER_SERVICE_URL` - Scheduler service base URL
- `RABBITMQ_URL` - RabbitMQ connection string
- `RECONCILIATION_SCHEDULE` - Cron expression for scheduling

## Development

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest

# Run with auto-reload
uvicorn app.main:app --reload --port 9000
```
