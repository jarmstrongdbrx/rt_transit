# GTFS Real-time Transit Data Platform

A comprehensive real-time transit data platform built on Databricks that ingests, processes, and visualizes GTFS Real-time data from Helsinki Region Transport (HSL). The platform provides live vehicle tracking, performance analytics, and service monitoring through an interactive Streamlit dashboard.

## 🚌 Overview

This platform continuously ingests real-time transit data and provides:

- **Live Vehicle Tracking**: Real-time positions of buses, trams, and metro vehicles
- **Performance Analytics**: Route reliability, delay analysis, and service metrics  
- **Service Monitoring**: Active service alerts and disruption notifications
- **Interactive Dashboard**: Web-based visualization for operations teams

## 🏗️ Architecture

The platform follows a modern data lakehouse architecture:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   GTFS-RT APIs  │───▶│  Raw Ingestion   │───▶│   Bronze Layer  │
│  (HSL Helsinki) │    │   (Protobuf)     │    │   (JSON Files)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
┌─────────────────┐    ┌──────────────────┐             │
│ Streamlit App   │◀───│  Silver Tables   │◀────────────┘
│   Dashboard     │    │  (Cleaned Data)  │
└─────────────────┘    └──────────────────┘
```

### Components

1. **Raw Data Ingestion** (`src/raw/gtfs_rt_raw_ingest.py`)
   - Fetches GTFS-RT protobuf data from HSL APIs
   - Converts to JSON and stores in Unity Catalog volumes
   - Handles three data types: Vehicle Positions, Trip Updates, Service Alerts

2. **Data Pipeline** (`src/pipeline/gtfs.py`)
   - Databricks Delta Live Tables (DLT) pipeline
   - Creates bronze and silver streaming tables
   - Provides cleaned, analytics-ready data

3. **Streamlit Dashboard** (`src/app/app.py`)
   - Interactive web application for data visualization
   - Real-time vehicle map with live updates
   - Performance analytics and service monitoring

## 🚀 Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python 3.8+ for local development
- Access to HSL GTFS-RT feeds (public APIs)

### Deployment

This project uses multiple Databricks Asset Bundles for deployment. Use the provided deployment script for easy setup:

```bash
# Deploy all bundles in the correct order
./deploy.sh

# Or deploy individual bundles in order:
./deploy.sh --infrastructure    # Infrastructure first
./deploy.sh --ingestion-job     # Then ingestion job
./deploy.sh --etl              # Then ETL pipeline
./deploy.sh --database         # Then synced tables
./deploy.sh --app              # Finally the app
```

### Configuration

Key configuration is managed through:

- `shared-variables.yml` - Centralized configuration variables for all bundles
- `bundles/*/databricks.yml` - Individual bundle configurations
- `bundles/*/resources/*.yml` - Resource definitions for each bundle

#### Databricks Resources Deployed

The multi-bundle configuration deploys the following resources across five bundles:

**Infrastructure Resources:**
- **Database Instance** (`demo-db`) - CU_1 capacity Lakebase database for real-time queries
- **Unity Catalog Schema** (`prod_lakehouse.vehicle`) - Organized data storage for vehicle analytics
- **Unity Catalog Volume** (`prod_lakehouse.vehicle.gtfs_rt_raw`) - Managed storage for raw GTFS-RT data

**Data Processing Pipeline:**
- **DLT Pipeline** (`gtfs_rt_pipeline`) - Serverless Delta Live Tables pipeline with:
  - Photon engine enabled for optimized performance
  - Advanced edition with continuous processing
  - Bronze and silver table transformations from `src/pipeline/gtfs.py`
  - Automatic schema evolution and data quality monitoring

**Data Ingestion Jobs:**
- **GTFS-RT Job** (`gtfs_rt_job`) - Multi-task job orchestrating real-time data collection:
  - `service_alerts_raw` - Polls HSL service alerts every 60 seconds
  - `trip_updates_raw` - Polls HSL trip updates every 5 seconds  
  - `vehicle_positions_raw` - Polls HSL vehicle positions every 0.25 seconds
  - `gtfs_rt_pipeline` - Triggers DLT pipeline processing
  - Environment with `gtfs-realtime-bindings==1.0.0` dependency

**Data Synchronization:**
- **Synced Database Tables** - Continuous sync from lakehouse to Lakebase for low-latency access:
  - `lb_vehicle_positions` - Real-time vehicle positions (primary key: `vehicle_id`, timeseries: `feed_timestamp`)
  - `lb_trip_updates` - Trip predictions and delays (composite key: `trip_id`, `stop_id`, `stop_sequence`)
  - `lb_service_alerts` - Service disruptions (primary key: `alert_id`, timeseries: `feed_timestamp`)
  - `lb_service_alerts_entities` - Affected routes/stops per alert (composite key: `alert_id`, `affected_route_id`, `affected_stop_id`)

**Application Resources:**
- **Streamlit App** (`gtfs-rt-app`) - Interactive dashboard with database connectivity permissions

This architecture provides end-to-end real-time transit data processing: from ingestion → transformation → synchronization → visualization, with continuous processing and low-latency access optimized for dashboard queries.

## 📊 Data Sources

The platform ingests three types of real-time data from HSL:

| Data Type | Update Frequency | HSL Endpoint |
|-----------|------------------|--------------|
| Vehicle Positions | 0.25 seconds | `/realtime/vehicle-positions/v2/hsl` |
| Trip Updates | 5 seconds | `/realtime/trip-updates/v2/hsl` |
| Service Alerts | 60 seconds | `/realtime/service-alerts/v2/hsl` |

## 🗂️ Data Schema

### Bronze Tables (Raw JSON)
- `bronze_vehicle_positions` - Raw vehicle position data
- `bronze_trip_updates` - Raw trip update data  
- `bronze_service_alerts` - Raw service alert data

### Silver Tables (Cleaned & Flattened)
- `silver_vehicle_positions` - Cleaned vehicle positions with geospatial data
- `silver_trip_updates` - Flattened trip predictions by stop
- `silver_service_alerts` - Parsed alerts with affected entities

## 📱 Dashboard Features

### Vehicle Positions Tab
- **Interactive Map**: Live vehicle positions with route-based coloring
- **Fleet Statistics**: Active vehicles, routes, and average speeds
- **Occupancy Status**: Real-time passenger load information
- **Route Filtering**: Focus on specific transit routes

### Performance Analytics Tab  
- **System Metrics**: Reliability percentages and delay statistics
- **Route Analysis**: Performance comparison across routes
- **Stop Heatmap**: Delay patterns by location
- **Trend Analysis**: Hourly performance patterns

### Service Alerts Tab
- **Active Alerts**: Current service disruptions
- **Impact Analysis**: Affected routes and stops
- **Alert Categories**: Organized by cause and effect

### Project Structure

```
rt_transit/
├── bundles/                   # Multi-bundle Databricks Asset Bundles
│   ├── infrastructure/        # Infrastructure components
│   │   └── databricks.yml    # Catalog, schema, volume, database instance
│   ├── ingestion-job/         # Raw data ingestion
│   │   ├── databricks.yml    # Job bundle configuration
│   │   ├── resources/        # Resource definitions
│   │   │   └── gtfs_rt.job.yml
│   │   └── src/              # Source code for ingestion
│   │       └── raw/
│   │           └── gtfs_rt_raw_ingest.py
│   ├── etl/                  # Data processing pipeline
│   │   ├── databricks.yml    # ETL bundle configuration
│   │   ├── resources/        # Resource definitions
│   │   │   └── gtfs_rt.pipeline.yml
│   │   └── src/              # Source code for pipeline
│   │       ├── pipeline/
│   │       │   └── gtfs.py
│   │       └── raw/
│   │           └── gtfs_rt_raw_ingest.py
│   ├── database/             # Synced tables
│   │   └── databricks.yml    # Synced tables configuration
│   └── app/                  # Streamlit application
│       ├── databricks.yml    # App bundle configuration
│       └── src/              # Source code for app
│           └── app/
│               ├── app.py
│               ├── app.yaml
│               └── requirements.txt
├── resources/                 # Legacy resource definitions (deprecated)
│   ├── gtfs_rt.job.yml
│   └── gtfs_rt.pipeline.yml
├── shared-variables.yml       # Shared configuration variables
├── deploy.sh                  # Multi-bundle deployment script
├── destroy.sh                 # Multi-bundle destruction script
└── README.md
```

## 🔐 Authentication

The Streamlit app uses OAuth authentication to connect to Databricks:

- **Environment Variables**: Configure `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` (automatically set by Databricks Apps)
- **Database Access**: Uses PostgreSQL-compatible connection to Databricks SQL
- **Auto-refresh**: Tokens are automatically refreshed as needed

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **HSL (Helsinki Region Transport)** for providing open GTFS-RT data
- **Databricks** for the lakehouse platform and DLT capabilities
- **Streamlit** for the interactive dashboard framework

---

**Built with ❤️ for better public transit** 🚌🚊🚇
