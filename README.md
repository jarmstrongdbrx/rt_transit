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

This project uses Databricks Asset Bundles for deployment:

```bash
# Deploy to development environment
databricks bundle deploy --target dev

# Start the data ingestion jobs
databricks bundle run gtfs_rt_job --target dev

# Launch the Streamlit app
databricks apps start gtfs-rt-app --target dev
```

### Configuration

Key configuration is managed through:

- `databricks.yml` - Main bundle configuration
- `resources/gtfs_rt.job.yml` - Data ingestion job definitions  
- `resources/gtfs_rt.pipeline.yml` - DLT pipeline configuration

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
├── databricks.yml              # Main bundle configuration
├── resources/                  # Databricks resource definitions
│   ├── gtfs_rt.job.yml        # Data ingestion jobs
│   └── gtfs_rt.pipeline.yml   # DLT pipeline config
├── src/
│   ├── app/                   # Streamlit dashboard
│   │   ├── app.py            # Main application
│   │   ├── app.yaml          # App configuration  
│   │   └── requirements.txt  # Python dependencies
│   ├── pipeline/             # Data processing
│   │   └── gtfs.py          # DLT pipeline definitions
│   └── raw/                  # Data ingestion
│       └── gtfs_rt_raw_ingest.py  # Raw data ingestion script
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
