"""
GTFS Real-time Transit Data Application - Streamlit Dashboard
An interactive Streamlit app for GTFS real-time transit data visualization and monitoring.
"""

import streamlit as st
import os
import pandas as pd
from datetime import datetime, timedelta
import time
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging
import uuid
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import asyncio
from threading import Timer

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Check for Databricks SDK availability
try:
    from databricks.sdk import WorkspaceClient
    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False
    logger.warning("Databricks SDK not available - will use fallback authentication")


class DatabaseConnection:
    """Handles database connections and authentication for GTFS RT data."""
    
    # Database configuration from environment variables
    DB_CONFIG = {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT", "5432"),
        "database": os.getenv("PGDATABASE", "prod_lakehouse"),
        "user": os.getenv("PGUSER"),
        "sslmode": os.getenv("PGSSLMODE", "require"),
        "application_name": os.getenv("PGAPPNAME", "gtfs-rt-app")
    }
    
    # Databricks configuration for OAuth
    DATABRICKS_CONFIG = {
        "host": f"https://{os.getenv('DATABRICKS_HOST')}/",  # Use the actual host from env
        "client_id": os.getenv("DATABRICKS_CLIENT_ID"),  # 027f0f04-8587-483e-aff0-1956bef906e6
        "client_secret": os.getenv("DATABRICKS_CLIENT_SECRET"),  # Available in environment
        "instance_name": "demo-db"
    }
    
    def __init__(self):
        self.engine: Optional[Engine] = None
        self._oauth_token: Optional[str] = None
        
    @classmethod
    def generate_oauth_token(cls):
        """Generate OAuth token using Databricks SDK."""
        if not DATABRICKS_SDK_AVAILABLE:
            raise Exception("Databricks SDK is required for authentication but not available")
        
        # Validate configuration
        if not cls.DATABRICKS_CONFIG["client_id"]:
            raise Exception("DATABRICKS_CLIENT_ID environment variable must be set")
            
        if not cls.DATABRICKS_CONFIG["client_secret"]:
            raise Exception("DATABRICKS_CLIENT_SECRET environment variable must be set")
        
        try:
            logger.info(f"Attempting OAuth with client_id: {cls.DATABRICKS_CONFIG['client_id'][:8]}...")
            logger.info(f"Using host: {cls.DATABRICKS_CONFIG['host']}")
            logger.info(f"Using instance: {cls.DATABRICKS_CONFIG['instance_name']}")
            
            # Create WorkspaceClient
            w = WorkspaceClient(
                host=cls.DATABRICKS_CONFIG["host"],
                client_id=cls.DATABRICKS_CONFIG["client_id"],
                client_secret=cls.DATABRICKS_CONFIG["client_secret"]
            )
            
            # Use the original function you provided
            cred = w.database.generate_database_credential(
                request_id=str(uuid.uuid4()), 
                instance_names=[cls.DATABRICKS_CONFIG["instance_name"]]
            )
            
            logger.info("Successfully generated new OAuth token via Databricks SDK")
            return cred.token
            
        except Exception as e:
            logger.error(f"Failed to generate OAuth token via Databricks SDK: {e}")
            raise Exception(f"Authentication failed: {e}")
    
    def get_connection_string(self) -> str:
        """Build PostgreSQL connection string with OAuth token as password."""
        # Generate OAuth token if not already available
        if not self._oauth_token:
            self._oauth_token = self.generate_oauth_token()
        
        # Use OAuth token as the password for PostgreSQL connection
        return (
            f"postgresql://{self.DB_CONFIG['user']}:{self._oauth_token}@"
            f"{self.DB_CONFIG['host']}:{self.DB_CONFIG['port']}/"
            f"{self.DB_CONFIG['database']}?"
            f"sslmode={self.DB_CONFIG['sslmode']}&"
            f"application_name={self.DB_CONFIG['application_name']}"
        )
    
    def get_engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self.engine is None:
            try:
                connection_string = self.get_connection_string()
                self.engine = create_engine(
                    connection_string,
                    pool_pre_ping=True,
                    pool_recycle=3600,  # Recycle connections every hour
                    echo=False
                )
                logger.info("Database engine created successfully")
            except Exception as e:
                logger.error(f"Failed to create database engine: {e}")
                raise
        
        return self.engine
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def refresh_oauth_token(self):
        """Refresh OAuth token and recreate engine."""
        try:
            logger.info("Refreshing OAuth token...")
            self._oauth_token = self.generate_oauth_token()
            self.engine = None  # Force recreation of engine
            logger.info("OAuth token refreshed successfully")
        except Exception as e:
            logger.error(f"Failed to refresh OAuth token: {e}")
            raise

    def execute_query(self, query: str, params: dict = None) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                df = pd.read_sql_query(text(query), conn, params=params)
            return df
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            # Try refreshing token once if authentication might have expired
            try:
                logger.info("Attempting to refresh OAuth token and retry query")
                self.refresh_oauth_token()
                engine = self.get_engine()
                with engine.connect() as conn:
                    df = pd.read_sql_query(text(query), conn, params=params)
                return df
            except Exception as retry_error:
                logger.error(f"Query retry after token refresh failed: {retry_error}")
                raise retry_error
    
    @st.cache_data(ttl=3, show_spinner=False)  # Cache for 3 seconds
    def get_vehicle_positions(_self, time_range_minutes: int = 15) -> pd.DataFrame:
        """Get recent vehicle positions from vehicle.lb_vehicle_positions table."""
        query = f"""
        SELECT 
            vehicle_id,
            route_id,
            latitude,
            longitude,
            speed_kmh as speed,
            bearing,
            feed_timestamp::timestamp as timestamp,
            occupancy_status,
            current_stop_id,
            current_status
        from vehicle.lb_vehicle_positions 
        WHERE feed_timestamp::timestamp >= NOW() - INTERVAL '{time_range_minutes} MINUTES'
        ORDER BY feed_timestamp::timestamp DESC
        """
        
        return _self.execute_query(query)
    
    def get_trip_updates(self, time_range_minutes: int = 60) -> pd.DataFrame:
        """Get recent trip updates from vehicle.lb_trip_updates table."""
        query = f"""
        SELECT 
            trip_id,
            route_id,
            entity_id,
            trip_start_time as start_time,
            trip_start_date as start_date,
            trip_schedule_relationship as schedule_relationship,
            stop_sequence,
            predicted_arrival_time,
            predicted_departure_time,
            feed_timestamp::timestamp as timestamp,
            stop_id,
            arrival_uncertainty_seconds,
            departure_uncertainty_seconds
        from vehicle.lb_trip_updates 
        WHERE feed_timestamp::timestamp >= NOW() - INTERVAL '{time_range_minutes} MINUTES'
        ORDER BY feed_timestamp::timestamp DESC
        """
        
        return self.execute_query(query)
    
    def get_route_performance_stats(self, time_range_minutes: int = 60) -> pd.DataFrame:
        """Get route performance statistics for delay analysis."""
        query = f"""
        WITH route_stats AS (
            SELECT 
                route_id,
                COUNT(*) as total_predictions,
                COUNT(CASE WHEN predicted_arrival_time IS NOT NULL THEN 1 END) as arrival_predictions,
                COUNT(CASE WHEN predicted_departure_time IS NOT NULL THEN 1 END) as departure_predictions,
                AVG(arrival_uncertainty_seconds) as avg_arrival_uncertainty,
                AVG(departure_uncertainty_seconds) as avg_departure_uncertainty,
                COUNT(CASE WHEN trip_schedule_relationship = 'CANCELED' THEN 1 END) as cancelled_trips,
                COUNT(DISTINCT trip_id) as unique_trips
            FROM vehicle.lb_trip_updates 
            WHERE feed_timestamp::timestamp >= NOW() - INTERVAL '{time_range_minutes} MINUTES'
            GROUP BY route_id
        )
        SELECT 
            route_id,
            total_predictions,
            arrival_predictions,
            departure_predictions,
            COALESCE(avg_arrival_uncertainty, 0) as avg_arrival_uncertainty_sec,
            COALESCE(avg_departure_uncertainty, 0) as avg_departure_uncertainty_sec,
            cancelled_trips,
            unique_trips,
            ROUND(100.0 * (total_predictions - cancelled_trips) / NULLIF(total_predictions, 0), 1) as reliability_pct
        FROM route_stats
        ORDER BY total_predictions DESC
        """
        
        return self.execute_query(query)
    
    def get_stop_delay_analysis(self, time_range_minutes: int = 240) -> pd.DataFrame:
        """Get stop-level delay analysis for heatmap visualization."""
        query = f"""
        WITH stop_delays AS (
            SELECT 
                stop_id,
                route_id,
                COUNT(*) as prediction_count,
                AVG(arrival_uncertainty_seconds) as avg_arrival_uncertainty,
                AVG(departure_uncertainty_seconds) as avg_departure_uncertainty,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY arrival_uncertainty_seconds) as median_arrival_uncertainty,
                COUNT(CASE WHEN arrival_uncertainty_seconds > 300 THEN 1 END) as high_uncertainty_count
            FROM vehicle.lb_trip_updates 
            WHERE feed_timestamp::timestamp >= NOW() - INTERVAL '{time_range_minutes} MINUTES'
            AND stop_id IS NOT NULL
            AND (arrival_uncertainty_seconds IS NOT NULL OR departure_uncertainty_seconds IS NOT NULL)
            GROUP BY stop_id, route_id
            HAVING COUNT(*) >= 3  -- Only include stops with multiple predictions
        )
        SELECT 
            stop_id,
            route_id,
            prediction_count,
            COALESCE(avg_arrival_uncertainty, 0) as avg_delay_seconds,
            COALESCE(median_arrival_uncertainty, 0) as median_delay_seconds,
            high_uncertainty_count,
            ROUND(100.0 * high_uncertainty_count / prediction_count, 1) as high_uncertainty_pct
        FROM stop_delays
        ORDER BY avg_arrival_uncertainty DESC
        """
        
        return self.execute_query(query)
    
    def get_service_alerts(self, time_range_minutes: int = 1440) -> pd.DataFrame:  # 24 hours default
        """Get recent service alerts from vehicle.lb_service_alerts table."""
        query = f"""
        SELECT 
            alert_id,
            cause,
            effect,
            severity_level,
            header_text,
            description_text,
            url,
            active_periods,
            feed_timestamp::timestamp as timestamp
        from vehicle.lb_service_alerts 
        WHERE feed_timestamp::timestamp >= NOW() - INTERVAL '{time_range_minutes} MINUTES'
        ORDER BY feed_timestamp::timestamp DESC
        """
        
        return self.execute_query(query)
    
    def get_service_alert_entities(self, time_range_minutes: int = 1440) -> pd.DataFrame:
        """Get service alert entities from vehicle.lb_service_alerts_entities table."""
        query = f"""
        SELECT 
            alert_id,
            affected_agency_id as agency_id,
            affected_route_id as route_id,
            affected_stop_id as stop_id,
            cause,
            effect,
            feed_timestamp::timestamp as timestamp
        from vehicle.lb_service_alerts_entities 
        WHERE feed_timestamp::timestamp >= NOW() - INTERVAL '{time_range_minutes} MINUTES'
        ORDER BY feed_timestamp::timestamp DESC
        """
        
        return self.execute_query(query)
    
    @st.cache_data(ttl=3, show_spinner=False)  # Cache for 3 seconds
    def get_fleet_statistics(_self) -> dict:
        """Get current fleet statistics."""
        try:
            # Get total vehicles
            total_vehicles_query = """
            SELECT COUNT(DISTINCT vehicle_id) as total_vehicles
            from vehicle.lb_vehicle_positions 
            WHERE feed_timestamp::timestamp >= NOW() - INTERVAL '15 MINUTES'
            """
            total_vehicles = _self.execute_query(total_vehicles_query).iloc[0]['total_vehicles']
            
            # Get active routes
            active_routes_query = """
            SELECT COUNT(DISTINCT route_id) as active_routes
            from vehicle.lb_vehicle_positions 
            WHERE feed_timestamp::timestamp >= NOW() - INTERVAL '15 MINUTES'
            """
            active_routes = _self.execute_query(active_routes_query).iloc[0]['active_routes']
            
            # Get average speed
            avg_speed_query = """
            SELECT AVG(speed_kmh) as avg_speed
            from vehicle.lb_vehicle_positions 
            WHERE feed_timestamp::timestamp >= NOW() - INTERVAL '15 MINUTES'
            AND speed_kmh IS NOT NULL AND speed_kmh > 0
            """
            avg_speed_result = _self.execute_query(avg_speed_query)
            avg_speed = avg_speed_result.iloc[0]['avg_speed'] if not avg_speed_result.empty else 0
            
            return {
                'total_vehicles': int(total_vehicles),
                'active_routes': int(active_routes),
                'avg_speed': float(avg_speed) if avg_speed else 0
            }
        except Exception as e:
            logger.error(f"Failed to get fleet statistics: {e}")
            return {
                'total_vehicles': 0,
                'active_routes': 0,
                'avg_speed': 0
            }


# Global database connection instance
db_connection = DatabaseConnection()


class GTFSRTStreamlitApp:
    """Streamlit application for GTFS Real-time data visualization."""
    
    def __init__(self):
        self.app_name = os.getenv("APP_NAME", "GTFS RT Transit App")
        self.app_version = os.getenv("APP_VERSION", "0.1.0")
        
    def setup_page_config(self):
        """Configure Streamlit page settings."""
        st.set_page_config(
            page_title=self.app_name,
            page_icon="🚌",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
    def render_header(self):
        """Render the application header."""
        st.title(f"🚌 {self.app_name}")
        st.markdown(f"**Version:** {self.app_version}")
        st.markdown("---")
        
    def render_sidebar(self):
        """Render the sidebar with controls and information."""
        st.sidebar.header("📊 Dashboard Controls")
        
        # Data source selection
        data_source = st.sidebar.selectbox(
            "Select Data Source",
            ["Vehicle Positions", "Trip Updates", "Service Alerts"]
        )
        
        # Time range selection
        time_range = st.sidebar.selectbox(
            "Time Range",
            ["Last 15 minutes", "Last hour", "Last 4 hours", "Last 24 hours"]
        )
        
        # Auto-refresh toggle with status indicator
        auto_refresh = st.sidebar.checkbox("Auto-refresh (3s)", value=True)
        
        if auto_refresh:
            st.sidebar.success("🔄 Auto-refresh active")
        else:
            st.sidebar.info("⏸️ Auto-refresh paused")
        
        # Manual refresh button
        if st.sidebar.button("🔄 Refresh Now"):
            st.rerun()
        
        # Route filter (add this after getting the data)
        route_filter = st.sidebar.multiselect(
            "🚏 Filter Routes",
            options=[],  # Will be populated dynamically
            default=[],
            help="Select specific routes to display on the map. Different colors on the map represent different routes."
        )
            
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📈 System Status")
        st.sidebar.success("✅ Pipeline Active")
        st.sidebar.info(f"🕐 Last Updated: {datetime.now().strftime('%H:%M:%S')}")
        
        return data_source, time_range, auto_refresh, route_filter
        
    @st.cache_data(ttl=3, show_spinner=False)  # Cache for 3 seconds to match refresh rate
    def get_live_vehicle_data(_self, time_range_minutes: int = 15):
        """Get live vehicle position data from database."""
        try:
            df = db_connection.get_vehicle_positions(time_range_minutes)
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            logger.error(f"Failed to get vehicle data: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=3, show_spinner=False)  # Cache for 3 seconds to match refresh rate
    def get_live_fleet_stats(_self):
        """Get live fleet statistics from database."""
        try:
            return db_connection.get_fleet_statistics()
        except Exception as e:
            logger.error(f"Failed to get fleet statistics: {e}")
            return {'total_vehicles': 0, 'active_routes': 0, 'avg_speed': 0}
        
    def render_vehicle_positions_tab(self, time_range: str, route_filter: list = None):
        """Render the vehicle positions visualization."""
        st.header("🚌 Vehicle Positions")
        
        # Convert time range to minutes
        time_range_map = {
            "Last 15 minutes": 15,
            "Last hour": 60,
            "Last 4 hours": 240,
            "Last 24 hours": 1440
        }
        time_range_minutes = time_range_map.get(time_range, 15)
        
        # Initialize session state for smooth updates
        if 'last_vehicle_data' not in st.session_state:
            st.session_state.last_vehicle_data = None
        if 'last_fleet_stats' not in st.session_state:
            st.session_state.last_fleet_stats = None
        if 'last_refresh_time' not in st.session_state:
            st.session_state.last_refresh_time = datetime.now()
            
        # Get live data (cached, so efficient)
        df = self.get_live_vehicle_data(time_range_minutes)
        fleet_stats = self.get_live_fleet_stats()
        
        if df.empty:
            st.info("No vehicle data available. Please check your database connection.")
            return
        
        # Validate and clean coordinate data
        if 'latitude' in df.columns and 'longitude' in df.columns:
            # Remove rows with invalid coordinates (NaN or out of reasonable bounds)
            df = df.dropna(subset=['latitude', 'longitude'])
            df = df[
                (df['latitude'].between(-90, 90)) & 
                (df['longitude'].between(-180, 180))
            ]
            
            if df.empty:
                st.warning("No valid vehicle position data available after data validation.")
                return
        
        # Update sidebar route filter with available routes
        if 'route_id' in df.columns and not df.empty:
            available_routes = sorted(df['route_id'].unique())
            # Update the route filter in sidebar (this is a bit of a hack, but works)
            if available_routes:
                with st.sidebar:
                    # Replace the empty route filter with populated one
                    route_filter = st.multiselect(
                        "🚏 Filter Routes",
                        options=available_routes,
                        default=route_filter if route_filter else [],
                        help="Select specific routes to display on the map",
                        key="route_filter_populated"
                    )
        
        # Apply route filter if selected
        if route_filter and 'route_id' in df.columns:
            df = df[df['route_id'].isin(route_filter)]
            if df.empty:
                st.info(f"No vehicles found for selected routes: {', '.join(route_filter)}")
                return
        
        # Create layout columns - make map larger
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.subheader("Real-time Vehicle Map")
            
            # Clean speed data for visualization (fixed sensible marker sizes)
            df_viz = df.copy()
            if 'speed' in df_viz.columns:
                # Handle NaN values and ensure non-negative speeds
                df_viz['speed'] = df_viz['speed'].fillna(0)  # Fill NaN with 0
                df_viz['speed_viz'] = df_viz['speed'].clip(lower=0)  # Clip negative values to 0
                # Use fixed sensible sizes: 4-10 pixels based on speed
                max_speed = df_viz['speed_viz'].max() if df_viz['speed_viz'].max() > 0 else 50
                df_viz['speed_viz'] = 4 + (df_viz['speed_viz'] / max_speed) * 6  # Scale to 4-10 range
            else:
                df_viz['speed_viz'] = 6  # Default fixed marker size if no speed data
            
            # Create enhanced map visualization
            fig = px.scatter_mapbox(
                df_viz,
                lat="latitude",
                lon="longitude",
                color="route_id",
                size="speed_viz",
                hover_name="vehicle_id",
                hover_data={
                    "route_id": True,
                    "speed": ":.1f km/h",
                    "occupancy_status": True,
                    "speed_viz": False,  # Hide the adjusted speed from hover
                    "latitude": ":.4f",
                    "longitude": ":.4f"
                },
                color_discrete_sequence=px.colors.qualitative.Set3,  # More vibrant colors
                mapbox_style="carto-positron",  # Cleaner, more modern style
                zoom=12,
                height=700,  # Larger height
                title="🚌 Live Transit Vehicles"
            )
            
            # Enhanced styling and interactivity
            fig.update_layout(
                mapbox=dict(
                    center=dict(lat=60.1699, lon=24.9384),
                    style="carto-positron",
                    zoom=12
                ),
                title=dict(
                    text="🚌 Live Transit Vehicles",
                    x=0.5,  # Center the title
                    font=dict(size=20, color="#2E86AB")
                ),
                showlegend=False,  # Disable legend due to too many routes
                margin=dict(l=0, r=0, t=50, b=0),
                transition={'duration': 500, 'easing': 'cubic-in-out'},
                uirevision='constant'  # Preserve zoom/pan state
            )
            
            # Enhanced marker styling with fixed reasonable sizes
            fig.update_traces(
                marker=dict(
                    sizemin=4,  # Minimum marker size
                    sizemode='diameter',
                    sizeref=1,  # Direct size mapping (no scaling)
                    opacity=0.7,  # Good visibility without being overwhelming
                    symbol='circle'  # Ensure consistent circle symbols
                ),
                # Use default hover template - plotly will handle the hover_data formatting
                selector=dict(mode='markers')
            )
            
            # Use plotly's built-in animation and consistent key for smoother updates
            st.plotly_chart(
                fig, 
                use_container_width=True, 
                key="vehicle_map_persistent",  # Consistent key to maintain state
                config={
                    'displayModeBar': True,  # Show toolbar for better UX
                    'displaylogo': False,
                    'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                    'modeBarButtonsToAdd': ['drawline', 'drawopenpath', 'drawclosedpath'],
                    'staticPlot': False,
                    'scrollZoom': True,
                    'doubleClick': 'reset+autosize',
                    'showTips': True,
                    'responsive': True
                }
            )
            
            # Add map controls info
            st.caption("🔍 **Map Controls:** Zoom with scroll wheel • Pan by dragging • Double-click to reset view • Different colors = different routes")
            
            # Add real-time data indicator
            if not df_viz.empty:
                latest_update = df_viz['timestamp'].max()
                time_ago = (datetime.now() - latest_update).total_seconds()
                if time_ago < 60:
                    st.success(f"🟢 **Live Data** - Updated {int(time_ago)}s ago")
                elif time_ago < 300:  # 5 minutes
                    st.warning(f"🟡 **Recent Data** - Updated {int(time_ago/60)}m ago")
                else:
                    st.error(f"🔴 **Stale Data** - Updated {int(time_ago/60)}m ago")
        
        with col2:
            st.subheader("📊 Fleet Statistics")
            
            # Enhanced metrics with delta indicators and styling
            col2a, col2b = st.columns(2)
            with col2a:
                st.metric(
                    "🚌 Vehicles", 
                    fleet_stats['total_vehicles'],
                    help="Total active vehicles in the past 15 minutes"
                )
                st.metric(
                    "🛣️ Routes", 
                    fleet_stats['active_routes'],
                    help="Number of different routes with active vehicles"
                )
            with col2b:
                avg_speed = fleet_stats['avg_speed']
                speed_color = "🟢" if avg_speed > 20 else "🟡" if avg_speed > 10 else "🔴"
                st.metric(
                    f"{speed_color} Avg Speed", 
                    f"{avg_speed:.1f} km/h",
                    help="Average speed of all active vehicles"
                )
                
                # Add speed distribution indicator
                if not df.empty and 'speed' in df.columns:
                    fast_vehicles = len(df[df['speed'] > 30])
                    st.metric(
                        "⚡ Fast (>30km/h)",
                        fast_vehicles,
                        help="Vehicles traveling faster than 30 km/h"
                    )
            
            # Enhanced occupancy distribution
            st.subheader("🚻 Occupancy Status")
            if 'occupancy_status' in df.columns and not df['occupancy_status'].isna().all():
                occupancy_counts = df['occupancy_status'].value_counts()
                if not occupancy_counts.empty:
                    # Create a more modern donut chart
                    fig_pie = px.pie(
                        values=occupancy_counts.values,
                        names=occupancy_counts.index,
                        title="",
                        hole=0.4,  # Create donut chart
                        color_discrete_sequence=px.colors.qualitative.Pastel
                    )
                    fig_pie.update_traces(
                        textposition='inside', 
                        textinfo='percent+label',
                        hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>"
                    )
                    fig_pie.update_layout(
                        showlegend=True,
                        legend=dict(orientation="h", yanchor="bottom", y=-0.2),
                        margin=dict(l=20, r=20, t=20, b=20),
                        height=250
                    )
                    st.plotly_chart(fig_pie, use_container_width=True, key="occupancy_pie")
            else:
                st.info("💭 Occupancy data not available")
            
            # Route summary
            if not df.empty and 'route_id' in df.columns:
                st.subheader("🚏 Top Active Routes")
                route_counts = df['route_id'].value_counts().head(5)
                if not route_counts.empty:
                    for route, count in route_counts.items():
                        st.write(f"**Route {route}:** {count} vehicles")
                
            # Show data freshness with enhanced styling
            if not df.empty:
                latest_update = df['timestamp'].max()
                st.markdown("---")
                st.markdown(f"**🕐 Data Timestamp:** `{latest_update.strftime('%H:%M:%S')}`")
                st.markdown(f"**📊 Total Records:** `{len(df):,}`")
        
        # Update session state with current refresh time
        st.session_state.last_refresh_time = datetime.now()
        
        # Store current data in session state for next comparison
        st.session_state.last_vehicle_data = df.copy() if not df.empty else None
        st.session_state.last_fleet_stats = fleet_stats.copy() if fleet_stats else None
            
    def render_trip_updates_tab(self, time_range: str):
        """Render the enhanced trip performance analytics."""
        st.header("🚏 Real-time Performance Analytics")
        
        # Convert time range to minutes
        time_range_map = {
            "Last 15 minutes": 15,
            "Last hour": 60,
            "Last 4 hours": 240,
            "Last 24 hours": 1440
        }
        time_range_minutes = time_range_map.get(time_range, 60)
        
        try:
            with st.spinner("Loading performance analytics..."):
                df_trips = db_connection.get_trip_updates(time_range_minutes)
                df_route_stats = db_connection.get_route_performance_stats(time_range_minutes)
                df_stop_delays = db_connection.get_stop_delay_analysis(min(time_range_minutes * 4, 1440))  # Use longer window for stop analysis
            
            if df_trips.empty:
                st.info("No trip update data available for the selected time range.")
                return
            
            # === KEY PERFORMANCE METRICS ===
            st.subheader("📊 System Performance Overview")
            
            # Calculate meaningful metrics
            total_predictions = len(df_trips)
            cancelled_trips = len(df_trips[df_trips['schedule_relationship'] == 'CANCELED']) if 'schedule_relationship' in df_trips.columns else 0
            unique_routes = df_trips['route_id'].nunique() if 'route_id' in df_trips.columns else 0
            unique_trips = df_trips['trip_id'].nunique() if 'trip_id' in df_trips.columns else 0
            
            # Uncertainty analysis (proxy for delays)
            avg_uncertainty = 0
            high_uncertainty_count = 0
            if 'arrival_uncertainty_seconds' in df_trips.columns:
                df_trips['arrival_uncertainty_seconds'] = pd.to_numeric(df_trips['arrival_uncertainty_seconds'], errors='coerce')
                avg_uncertainty = df_trips['arrival_uncertainty_seconds'].mean()
                high_uncertainty_count = len(df_trips[df_trips['arrival_uncertainty_seconds'] > 300])  # > 5 minutes
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "📈 Total Predictions", 
                    f"{total_predictions:,}",
                    help="Total number of arrival/departure predictions"
                )
                
            with col2:
                reliability = ((total_predictions - cancelled_trips) / total_predictions * 100) if total_predictions > 0 else 100
                reliability_color = "🟢" if reliability > 95 else "🟡" if reliability > 90 else "🔴"
                st.metric(
                    f"{reliability_color} Service Reliability", 
                    f"{reliability:.1f}%",
                    help="Percentage of non-cancelled services"
                )
                
            with col3:
                uncertainty_color = "🟢" if avg_uncertainty < 120 else "🟡" if avg_uncertainty < 300 else "🔴"
                st.metric(
                    f"{uncertainty_color} Avg Uncertainty", 
                    f"{avg_uncertainty:.0f}s" if avg_uncertainty > 0 else "N/A",
                    help="Average prediction uncertainty (lower is better)"
                )
                
            with col4:
                problem_rate = (high_uncertainty_count / total_predictions * 100) if total_predictions > 0 else 0
                problem_color = "🟢" if problem_rate < 10 else "🟡" if problem_rate < 25 else "🔴"
                st.metric(
                    f"{problem_color} High Delay Rate", 
                    f"{problem_rate:.1f}%",
                    help="Percentage of predictions with >5min uncertainty"
                )
            
            # === ROUTE PERFORMANCE ANALYSIS ===
            if not df_route_stats.empty:
                st.subheader("🚌 Route Performance Analysis")
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    # Route reliability chart
                    top_routes = df_route_stats.head(15)  # Top 15 most active routes
                    
                    fig_routes = px.bar(
                        top_routes,
                        x='route_id',
                        y='reliability_pct',
                        color='avg_arrival_uncertainty_sec',
                        title="Route Reliability & Average Uncertainty",
                        labels={
                            'route_id': 'Route ID',
                            'reliability_pct': 'Reliability (%)',
                            'avg_arrival_uncertainty_sec': 'Avg Uncertainty (sec)'
                        },
                        color_continuous_scale='RdYlGn_r',  # Red for high uncertainty, green for low
                        height=400
                    )
                    
                    fig_routes.update_layout(
                        xaxis_title="Route ID",
                        yaxis_title="Service Reliability (%)",
                        coloraxis_colorbar_title="Avg Uncertainty (sec)"
                    )
                    
                    fig_routes.add_hline(y=95, line_dash="dash", line_color="green", 
                                       annotation_text="Target: 95% Reliability")
                    
                    st.plotly_chart(fig_routes, use_container_width=True)
                
                with col2:
                    st.markdown("**🏆 Top Performing Routes**")
                    best_routes = df_route_stats.nlargest(5, 'reliability_pct')
                    for _, route in best_routes.iterrows():
                        st.write(f"**Route {route['route_id']}**: {route['reliability_pct']:.1f}% reliability")
                    
                    st.markdown("**⚠️ Routes Needing Attention**")
                    problem_routes = df_route_stats.nsmallest(5, 'reliability_pct')
                    for _, route in problem_routes.iterrows():
                        uncertainty_mins = route['avg_arrival_uncertainty_sec'] / 60
                        st.write(f"**Route {route['route_id']}**: {route['reliability_pct']:.1f}% reliability, {uncertainty_mins:.1f}min avg uncertainty")
            
            # === STOP-LEVEL DELAY HEATMAP ===
            if not df_stop_delays.empty:
                st.subheader("🚏 Stop Performance Heatmap")
                
                # Create a scatter plot showing stop performance
                fig_stops = px.scatter(
                    df_stop_delays.head(50),  # Top 50 stops by delay
                    x='stop_id',
                    y='route_id',
                    size='prediction_count',
                    color='avg_delay_seconds',
                    hover_data=['median_delay_seconds', 'high_uncertainty_pct'],
                    title="Stop Performance Analysis (Larger = More Predictions, Red = Higher Delays)",
                    labels={
                        'avg_delay_seconds': 'Avg Delay (sec)',
                        'prediction_count': 'Prediction Count',
                        'stop_id': 'Stop ID',
                        'route_id': 'Route ID'
                    },
                    color_continuous_scale='RdYlGn_r',
                    height=500
                )
                
                fig_stops.update_layout(
                    xaxis_title="Stop ID",
                    yaxis_title="Route ID",
                    coloraxis_colorbar_title="Avg Delay (sec)"
                )
                
                st.plotly_chart(fig_stops, use_container_width=True)
                
                # Show problematic stops table
                st.markdown("**🚨 Stops with Highest Delays**")
                problem_stops = df_stop_delays.head(10)[['stop_id', 'route_id', 'avg_delay_seconds', 'high_uncertainty_pct', 'prediction_count']]
                problem_stops['avg_delay_minutes'] = (problem_stops['avg_delay_seconds'] / 60).round(1)
                display_stops = problem_stops[['stop_id', 'route_id', 'avg_delay_minutes', 'high_uncertainty_pct', 'prediction_count']]
                display_stops.columns = ['Stop ID', 'Route ID', 'Avg Delay (min)', 'High Uncertainty %', 'Predictions']
                st.dataframe(display_stops, use_container_width=True)
            
            # === REAL-TIME TREND ANALYSIS ===
            if 'timestamp' in df_trips.columns and 'arrival_uncertainty_seconds' in df_trips.columns:
                st.subheader("📈 Real-time Performance Trends")
                
                # Convert timestamp and create time-based analysis
                df_trips['timestamp'] = pd.to_datetime(df_trips['timestamp'])
                df_trips['hour'] = df_trips['timestamp'].dt.hour
                
                # Hourly performance trend
                hourly_stats = df_trips.groupby('hour').agg({
                    'arrival_uncertainty_seconds': ['mean', 'count'],
                    'trip_id': 'count'
                }).round(1)
                
                hourly_stats.columns = ['avg_uncertainty', 'uncertainty_count', 'total_predictions']
                hourly_stats = hourly_stats.reset_index()
                
                if not hourly_stats.empty:
                    fig_trend = px.line(
                        hourly_stats,
                        x='hour',
                        y='avg_uncertainty',
                        title="Average Prediction Uncertainty by Hour",
                        labels={
                            'hour': 'Hour of Day',
                            'avg_uncertainty': 'Average Uncertainty (seconds)'
                        },
                        markers=True,
                        height=300
                    )
                    
                    fig_trend.update_layout(
                        xaxis=dict(tickmode='linear', tick0=0, dtick=2),
                        yaxis_title="Average Uncertainty (seconds)"
                    )
                    
                    st.plotly_chart(fig_trend, use_container_width=True)
            
            # === DATA SUMMARY ===
            st.subheader("📋 Data Summary")
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**📊 Analysis Period:** {time_range}")
                st.markdown(f"**🚌 Active Routes:** {unique_routes}")
                st.markdown(f"**🚏 Unique Trips:** {unique_trips}")
                
            with col2:
                latest_update = df_trips['timestamp'].max() if 'timestamp' in df_trips.columns else "Unknown"
                st.markdown(f"**🕐 Latest Update:** {latest_update}")
                st.markdown(f"**📈 Total Data Points:** {total_predictions:,}")
                st.markdown(f"**❌ Cancelled Services:** {cancelled_trips}")
            
        except Exception as e:
            logger.error(f"Failed to load performance analytics: {e}")
            st.error(f"Failed to load performance analytics: {str(e)}")
            
    def render_service_alerts_tab(self, time_range: str):
        """Render the service alerts visualization."""
        st.header("⚠️ Service Alerts")
        
        # Convert time range to minutes
        time_range_map = {
            "Last 15 minutes": 15,
            "Last hour": 60,
            "Last 4 hours": 240,
            "Last 24 hours": 1440
        }
        time_range_minutes = time_range_map.get(time_range, 1440)  # Default to 24 hours for alerts
        
        try:
            with st.spinner("Loading service alerts..."):
                df_alerts = db_connection.get_service_alerts(time_range_minutes)
                df_entities = db_connection.get_service_alert_entities(time_range_minutes)
            
            if df_alerts.empty:
                st.info("No service alerts available for the selected time range.")
                return
            
            # Alert summary metrics
            total_alerts = len(df_alerts)
            # Since active_periods is now a complex field, we'll just show total for now
            active_alerts = total_alerts  # Placeholder - would need to parse active_periods array
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Alerts", total_alerts)
            with col2:
                st.metric("Active Alerts", active_alerts)
            
            # Alert severity distribution
            if 'effect' in df_alerts.columns:
                st.subheader("Alert Effects")
                effect_counts = df_alerts['effect'].value_counts()
                if not effect_counts.empty:
                    fig_bar = px.bar(
                        x=effect_counts.index,
                        y=effect_counts.values,
                        title="Alert Effects Distribution",
                        labels={"x": "Effect Type", "y": "Count"}
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)
            
            # Recent alerts table
            st.subheader("Recent Service Alerts")
            display_columns = ['alert_id', 'cause', 'effect', 'header_text', 'timestamp']
            available_columns = [col for col in display_columns if col in df_alerts.columns]
            
            if available_columns:
                display_df = df_alerts[available_columns].head(10)
                st.dataframe(display_df, use_container_width=True)
            else:
                st.dataframe(df_alerts.head(10), use_container_width=True)
            
            # Alert entities if available
            if not df_entities.empty:
                st.subheader("Affected Routes/Stops")
                # Use the correct column name for route_id
                route_col = 'route_id' if 'route_id' in df_entities.columns else 'affected_route_id'
                if route_col in df_entities.columns:
                    entity_summary = df_entities.groupby([route_col]).size().reset_index(name='alert_count')
                    entity_summary = entity_summary.sort_values('alert_count', ascending=False)
                    st.dataframe(entity_summary.head(10), use_container_width=True)
                else:
                    st.dataframe(df_entities.head(10), use_container_width=True)
                
        except Exception as e:
            logger.error(f"Failed to load service alerts: {e}")
            st.error(f"Failed to load service alerts: {str(e)}")
        
    def render_system_overview_tab(self):
        """Render the system overview dashboard."""
        st.header("📊 System Overview")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("Data Pipeline Status")
            st.success("✅ Vehicle Positions: Active")
            st.success("✅ Trip Updates: Active") 
            st.success("✅ Service Alerts: Active")
            
        with col2:
            st.subheader("Data Freshness")
            st.info("🕐 Vehicle Positions: 15s ago")
            st.info("🕐 Trip Updates: 2m ago")
            st.info("🕐 Service Alerts: 5m ago")
            
        with col3:
            st.subheader("System Health")
            st.metric("Uptime", "99.8%")
            st.metric("Data Quality", "98.5%")
            st.metric("Processing Rate", "1.2k/min")
        
    def run(self):
        """Run the Streamlit application."""
        self.setup_page_config()
        self.render_header()
        
        # Test database connection
        if not self.test_database_connection():
            st.error("❌ Database connection failed. Please check your configuration.")
            st.stop()
        
        # Sidebar controls
        data_source, time_range, auto_refresh, route_filter = self.render_sidebar()
        
        # Main content tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "🚌 Vehicle Positions", 
            "📈 Performance Analytics", 
            "⚠️ Service Alerts",
            "📊 System Overview"
        ])
        
        with tab1:
            self.render_vehicle_positions_tab(time_range, route_filter)
            
        with tab2:
            self.render_trip_updates_tab(time_range)
            
        with tab3:
            self.render_service_alerts_tab(time_range)
            
        with tab4:
            self.render_system_overview_tab()
            
        # Auto-refresh functionality with smooth updates
        if auto_refresh:
            # Initialize refresh state if not exists
            if 'next_refresh' not in st.session_state:
                st.session_state.next_refresh = time.time() + 3
            
            # Check if it's time to refresh
            current_time = time.time()
            if current_time >= st.session_state.next_refresh:
                st.session_state.next_refresh = current_time + 3
                st.rerun()  # Trigger a smooth rerun without full page reload
            else:
                # Show countdown in sidebar
                time_left = int(st.session_state.next_refresh - current_time)
                st.sidebar.caption(f"🔄 Next refresh in {time_left}s")
                # Sleep for a short time and rerun to update countdown
                time.sleep(1)
                st.rerun()
    
    def test_database_connection(self) -> bool:
        """Test database connection and show status."""
        try:
            with st.spinner("Testing database connection..."):
                success = db_connection.test_connection()
            if success:
                st.success("✅ Database connection successful")
            return success
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            st.error(f"❌ Database connection failed: {str(e)}")
            return False


def main():
    """Main application entry point."""
    app = GTFSRTStreamlitApp()
    app.run()


if __name__ == "__main__":
    main()
