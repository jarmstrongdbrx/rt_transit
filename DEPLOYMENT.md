# Multi-Bundle Deployment Guide

This project has been restructured to use multiple Databricks Asset Bundles for better organization and deployment control.

## Bundle Structure

### 1. Infrastructure Bundle (`bundles/infrastructure/`)
**Purpose**: Creates foundational infrastructure components
**Resources**:
- Catalog schema
- Managed volume for raw data storage
- Database instance
- Database catalog

**Dependencies**: None (must be deployed first)

### 2. Ingestion Job Bundle (`bundles/ingestion-job/`)
**Purpose**: Sets up raw data ingestion job
**Resources**:
- Raw data ingestion job (independent)

**Dependencies**: Infrastructure bundle must be deployed first

### 3. ETL Pipeline Bundle (`bundles/etl/`)
**Purpose**: Sets up data processing pipeline
**Resources**:
- ETL pipeline (DLT) - processes data collected by the job

**Dependencies**: Ingestion job bundle must be deployed and running first

### 4. Synced Tables Bundle (`bundles/database/`)
**Purpose**: Creates synced tables for real-time access
**Resources**:
- Synced tables that reference silver tables

**Dependencies**: Infrastructure and Ingestion bundles must be deployed first, AND the DLT pipeline must be running to create the source silver tables

### 5. App Bundle (`bundles/app/`)
**Purpose**: Deploys the Streamlit application
**Resources**:
- Streamlit application with database permissions

**Dependencies**: All other bundles must be deployed first (deployed last)

## Deployment Options

### Full Deployment (Recommended)
Deploy all bundles in the correct order:
```bash
./deploy.sh
```

**Idempotent Behavior**: The deployment script automatically detects existing resources and skips steps that are already complete. You can safely run the deployment multiple times.

### Staged Deployment
Deploy bundles individually in order:

1. Infrastructure first:
```bash
./deploy.sh --infrastructure
```

2. Then ingestion job:
```bash
./deploy.sh --ingestion-job
```

3. Then ETL pipeline:
```bash
./deploy.sh --etl
```

4. Then synced tables:
```bash
./deploy.sh --database
```

5. Finally app:
```bash
./deploy.sh --app
```

### Validation Only
Validate all bundle configurations without deploying:
```bash
./deploy.sh --validate
```

### Start Services Only
Start ingestion job and DLT pipeline (after deployment):
```bash
./deploy.sh --start-services
```

**Process**:
1. Deploys ingestion job bundle and starts the job
2. Waits 30 seconds for initial data collection
3. Deploys ETL pipeline bundle and starts the pipeline
4. Waits 120 seconds for data processing into silver tables

### Deploy App Only
Deploy just the Streamlit application (after other bundles):
```bash
./deploy.sh --app
```

## Destruction

### Full Destruction
Destroy all bundles and resources:
```bash
./destroy.sh
```

**Process**:
1. Destroys app bundle (Streamlit application)
2. Destroys synced tables bundle
3. Destroys ETL pipeline bundle
4. Destroys ingestion job bundle
5. Destroys infrastructure bundle (schema, volumes, database instance, database catalog)

### Force Destruction
Destroy all bundles without confirmation:
```bash
./destroy.sh --force
```

### Stop Services First
Stop running jobs and pipelines before destruction:
```bash
./destroy.sh --stop-services
```

### Destroy Individual Bundles
Destroy specific bundles only:
```bash
./destroy.sh --app-only           # Destroy only app bundle
./destroy.sh --database-only      # Destroy only synced tables bundle
./destroy.sh --etl-only           # Destroy only ETL pipeline bundle
./destroy.sh --ingestion-only     # Destroy only ingestion job bundle
./destroy.sh --infrastructure-only # Destroy only infrastructure bundle
```

### Force Deployment
Override deployment locks and conflicts:
```bash
./deploy.sh --force
```

### Clear Deployment Locks
Clear stuck deployment locks:
```bash
./deploy.sh --clear-locks
```

## Bundle Structure

```
bundles/
├── infrastructure/
│   └── databricks.yml          # Infrastructure bundle configuration
├── ingestion-job/
│   ├── databricks.yml          # Ingestion job bundle configuration
│   ├── resources/              # Resource definitions
│   │   └── gtfs_rt.job.yml    # Raw data ingestion job
│   └── src/                    # Source code for ingestion
│       └── raw/
│           └── gtfs_rt_raw_ingest.py
├── etl/
│   ├── databricks.yml          # ETL pipeline bundle configuration
│   ├── resources/              # Resource definitions
│   │   └── gtfs_rt.pipeline.yml # DLT pipeline for data processing
│   └── src/                    # Source code for pipeline
│       └── pipeline/
│           └── gtfs.py
├── database/
│   └── databricks.yml          # Synced Tables bundle configuration
└── app/
    ├── databricks.yml          # App bundle configuration
    └── src/                    # Source code for app
        └── app/                # Streamlit app files
            ├── app.py
            ├── app.yaml
            └── requirements.txt
```

**Legacy files (deprecated)**:
- `resources/` - Original resource definitions
- `databricks.yml` - Original single bundle configuration
- `src/` - Original shared source directory (now copied to individual bundles)

## Migration from Single Bundle

If you were previously using the single `databricks.yml` bundle:

1. The original bundle has been split into four focused bundles in separate directories
2. A backup of the original configuration is created as `databricks.yml.backup`
3. Each bundle now lives in its own directory under `bundles/`
4. The deployment script automatically handles the correct deployment order
5. All existing functionality is preserved across the new bundles

**Directory Structure Change**:
- Old: Single `databricks.yml` file with shared `src/` directory
- New: `bundles/{infrastructure,ingestion,database,app}/databricks.yml` with bundle-specific source code

**Source Code Distribution**:
- Each bundle now contains its own copy of the required source files
- This ensures proper sync root path compliance for Databricks bundles
- Original `src/` directory remains for reference but is no longer used by bundles

## Troubleshooting

### Deployment Lock Errors
If you encounter "Failed to acquire deployment lock" errors:

```bash
# Option 1: Use force deployment
./deploy.sh --force

# Option 2: Clear locks first, then deploy normally
./deploy.sh --clear-locks
./deploy.sh

# Option 3: Manual force deployment for specific bundle
cd bundles/infrastructure && databricks bundle deploy --target dev --force --force-lock --auto-approve
```

**Common causes**:
- Previous deployment was interrupted
- Multiple deployments running simultaneously
- Databricks workspace connection issues

### Bundle Validation Errors
```bash
# Validate specific bundle (run from bundle directory)
cd bundles/infrastructure && databricks bundle validate --target dev
cd bundles/ingestion && databricks bundle validate --target dev
cd bundles/database && databricks bundle validate --target dev
cd bundles/app && databricks bundle validate --target dev
```

### Manual Bundle Deployment
```bash
# Deploy specific bundle manually (in order, run from bundle directory)
cd bundles/infrastructure && databricks bundle deploy --target dev --auto-approve
cd bundles/ingestion && databricks bundle deploy --target dev --auto-approve
cd bundles/database && databricks bundle deploy --target dev --auto-approve  # Synced tables
cd bundles/app && databricks bundle deploy --target dev --auto-approve
```

### Manual Job/App Management
```bash
# Start ingestion job (collects raw data)
cd bundles/ingestion && databricks bundle run gtfs_rt_ingestion_job --target dev

# Start DLT pipeline (processes collected data)
cd bundles/ingestion && databricks pipelines start-update --pipeline-name gtfs_rt_ingestion

# Start Streamlit app
cd bundles/app && databricks apps start gtfs-rt-app --target dev
```

## Benefits of Multi-Bundle Structure

1. **Clear Separation of Concerns**: Each bundle has a specific purpose
2. **Better Dependency Management**: Deploy in the correct order automatically
3. **Easier Troubleshooting**: Issues can be isolated to specific bundles
4. **Flexible Deployment**: Deploy only what you need
5. **Improved Maintainability**: Smaller, focused configuration files
6. **App Isolation**: Streamlit app is deployed separately after data infrastructure is ready
7. **Idempotent Deployments**: Automatically skips steps when resources already exist

## Deployment Order

The bundles must be deployed in this specific order due to dependencies:

1. **Infrastructure** → Creates schema, volumes, database instance, and database catalog
2. **Ingestion Job** → Deploys and starts job, waits 30s for data collection
3. **ETL Pipeline** → Deploys and starts pipeline, waits 120s for data processing
4. **Synced Tables** → Creates synced tables that depend on the silver tables from the pipeline
5. **App** → Deploys the Streamlit application

**Critical Dependencies**: 
- The database catalog must exist before ingestion can write data
- The ingestion job must run first to collect raw data (30s wait)
- The DLT pipeline then processes raw data into silver tables (120s wait)
- Synced tables can only be deployed after silver tables exist
