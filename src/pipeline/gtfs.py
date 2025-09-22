"""
GTFS Real-time Bronze Streaming Tables

This module defines bronze streaming tables for GTFS Real-time data using 
Databricks Lakeflow Declarative Pipelines (Python API).

The raw data is ingested by the `gtfs_rt_raw_ingest.py` script and stored 
in Unity Catalog volumes with the following structure:
- Vehicle Positions: Updated every 0.25 seconds
- Trip Updates: Updated every 5 seconds  
- Service Alerts: Updated every 60 seconds
"""

import dlt
from pyspark.sql.functions import (
    current_timestamp, col, explode, when, isnan, isnull, 
    regexp_extract, to_timestamp, from_unixtime, coalesce,
    struct, array, lit, size, filter as spark_filter, concat
)
from pyspark.sql.types import *


# Configuration constants
VOLUME_BASE_PATH = "/Volumes/prod_lakehouse/vehicle/gtfs_rt_raw"


@dlt.table(
    name="bronze_vehicle_positions",
    comment="Bronze streaming table for GTFS-RT vehicle positions data from HSL",
    cluster_by_auto=True
)
def bronze_vehicle_positions():
    """
    Creates a streaming bronze table for vehicle position data from HSL GTFS-RT feed.
    
    Returns:
        DataFrame: Streaming DataFrame with vehicle positions data
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiline", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME_BASE_PATH}/dt=*/vehicle_positions_*.json")
        .select(
            "*",
            # Add ingestion metadata
            col("_metadata").alias("file_metadata"),
            current_timestamp().alias("ingestion_timestamp"),
            # Extract epoch time from filename for ordering
        )
    )


@dlt.table(
    name="bronze_trip_updates",
    comment="Bronze streaming table for GTFS-RT trip updates data from HSL",
    cluster_by_auto=True
)
def bronze_trip_updates():
    """
    Creates a streaming bronze table for trip update data from HSL GTFS-RT feed.
    
    Returns:
        DataFrame: Streaming DataFrame with trip updates data
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiline", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME_BASE_PATH}/dt=*/trip_updates_*.json")
        .select(
            "*",
            # Add ingestion metadata
            col("_metadata").alias("file_metadata"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )


@dlt.table(
    name="bronze_service_alerts",
    comment="Bronze streaming table for GTFS-RT service alerts data from HSL",
    cluster_by_auto=True
)
def bronze_service_alerts():
    """
    Creates a streaming bronze table for service alert data from HSL GTFS-RT feed.
    
    Returns:
        DataFrame: Streaming DataFrame with service alerts data
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiline", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME_BASE_PATH}/dt=*/service_alerts_*.json")
        .select(
            "*",
            # Add ingestion metadata
            col("_metadata").alias("file_metadata"),
            current_timestamp().alias("ingestion_timestamp"),
        )
    )


# ============================================================================
# SILVER TABLES - Cleaned and Enriched Data for Analytics
# ============================================================================

@dlt.table(
    name="silver_vehicle_positions",
    comment="Silver table with cleaned vehicle positions data optimized for analytics and visualization",
    cluster_by_auto=True
)
def silver_vehicle_positions():
    """
    Creates a cleaned silver table for vehicle positions with flattened structure
    optimized for geospatial analysis and real-time visualization.
    
    Returns:
        DataFrame: Cleaned vehicle positions with geospatial optimizations
    """
    return (
        spark.readStream
        .table("bronze_vehicle_positions")
        .select(
            explode("entity").alias("entity_exploded"),
            col("header"),
            col("dt"),
            col("file_metadata"),
            col("ingestion_timestamp")
        )
        .select(
            col("entity_exploded.id").alias("entity_id"),
            col("entity_exploded.vehicle.vehicle.id").alias("vehicle_id"),
            col("entity_exploded.vehicle.vehicle.label").alias("vehicle_label"),
            
            # Trip information
            col("entity_exploded.vehicle.trip.route_id").alias("route_id"),
            col("entity_exploded.vehicle.trip.direction_id").alias("direction_id"),
            col("entity_exploded.vehicle.trip.start_date").alias("trip_start_date"),
            col("entity_exploded.vehicle.trip.start_time").alias("trip_start_time"),
            col("entity_exploded.vehicle.trip.schedule_relationship").alias("schedule_relationship"),
            
            # Position data
            col("entity_exploded.vehicle.position.latitude").alias("latitude"),
            col("entity_exploded.vehicle.position.longitude").alias("longitude"),
            col("entity_exploded.vehicle.position.bearing").alias("bearing"),
            col("entity_exploded.vehicle.position.speed").alias("speed_mps"),
            col("entity_exploded.vehicle.position.odometer").alias("odometer"),
            
            # Vehicle status
            col("entity_exploded.vehicle.current_status").alias("current_status"),
            col("entity_exploded.vehicle.occupancy_status").alias("occupancy_status"),
            col("entity_exploded.vehicle.stop_id").alias("current_stop_id"),
            
            # Timestamps
            from_unixtime(col("entity_exploded.vehicle.timestamp").cast("bigint")).alias("vehicle_timestamp"),
            from_unixtime(col("header.timestamp")).alias("feed_timestamp"),
            
            # Metadata
            col("dt"),
            col("file_metadata"),
            col("ingestion_timestamp"),
            
            # Derived fields for analytics
            (col("entity_exploded.vehicle.position.speed") * 3.6).alias("speed_kmh"),  # Convert m/s to km/h
            when(col("entity_exploded.vehicle.position.bearing").isNotNull(), 
                 col("entity_exploded.vehicle.position.bearing")).otherwise(lit(None)).alias("bearing_degrees"),
            
            # PostGIS-friendly geometry (will be converted in app)
            struct(
                col("entity_exploded.vehicle.position.longitude").alias("x"),
                col("entity_exploded.vehicle.position.latitude").alias("y")
            ).alias("geometry_point")
        )
    )


@dlt.table(
    name="silver_trip_updates",
    comment="Silver table with flattened trip updates and stop time predictions for real-time arrival/departure analysis",
    cluster_by_auto=True
)
def silver_trip_updates():
    """
    Creates a flattened silver table for trip updates with stop-level predictions.
    Each row represents a stop time update for a specific trip.
    
    Returns:
        DataFrame: Flattened trip updates with stop time predictions
    """
    return (
        spark.readStream
        .table("bronze_trip_updates")
        .select(
            explode("entity").alias("entity_exploded"),
            col("header"),
            col("dt"),
            col("file_metadata"),
            col("ingestion_timestamp")
        )
        .select(
            # Entity identification
            col("entity_exploded.id").alias("entity_id"),
            
            # Trip information
            col("entity_exploded.trip_update.trip.route_id").alias("route_id"),
            col("entity_exploded.trip_update.trip.direction_id").alias("direction_id"),
            col("entity_exploded.trip_update.trip.start_date").alias("trip_start_date"),
            col("entity_exploded.trip_update.trip.start_time").alias("trip_start_time"),
            col("entity_exploded.trip_update.trip.schedule_relationship").alias("trip_schedule_relationship"),
            
            # Trip update timestamp
            from_unixtime(col("entity_exploded.trip_update.timestamp").cast("bigint")).alias("trip_update_timestamp"),
            
            # Stop time updates (flattened)
            explode("entity_exploded.trip_update.stop_time_update").alias("stop_update"),
            
            # Feed metadata
            from_unixtime(col("header.timestamp")).alias("feed_timestamp"),
            col("dt"),
            col("file_metadata"),
            col("ingestion_timestamp")
        )
        .select(
            col("entity_id"),
            col("route_id"),
            col("direction_id"),
            col("trip_start_date"),
            col("trip_start_time"),
            col("trip_schedule_relationship"),
            col("trip_update_timestamp"),
            col("feed_timestamp"),
            
            # Stop information
            col("stop_update.stop_id").alias("stop_id"),
            col("stop_update.stop_sequence").alias("stop_sequence"),
            col("stop_update.schedule_relationship").alias("stop_schedule_relationship"),
            
            # Arrival predictions
            from_unixtime(col("stop_update.arrival.time").cast("bigint")).alias("predicted_arrival_time"),
            col("stop_update.arrival.uncertainty").alias("arrival_uncertainty_seconds"),
            
            # Departure predictions  
            from_unixtime(col("stop_update.departure.time").cast("bigint")).alias("predicted_departure_time"),
            col("stop_update.departure.uncertainty").alias("departure_uncertainty_seconds"),
            
            # Stop time properties
            col("stop_update.stop_time_properties.assigned_stop_id").alias("assigned_stop_id"),
            
            # Metadata
            col("dt"),
            col("file_metadata"),
            col("ingestion_timestamp"),
            
            # Derived trip identifier for joining
            concat(col("route_id"), lit("_"), col("trip_start_date"), lit("_"), col("trip_start_time")).alias("trip_id")
        )
    )


@dlt.table(
    name="silver_service_alerts",
    comment="Silver table with parsed service alerts and affected entities for service disruption analysis",
    cluster_by_auto=True
)
def silver_service_alerts():
    """
    Creates a cleaned silver table for service alerts with parsed text content
    and flattened affected entities.
    
    Returns:
        DataFrame: Cleaned service alerts with affected routes/stops
    """
    return (
        spark.readStream
        .table("bronze_service_alerts")
        .select(
            explode("entity").alias("entity_exploded"),
            col("header"),
            col("dt"),
            col("file_metadata"), 
            col("ingestion_timestamp")
        )
        .select(
            # Alert identification
            col("entity_exploded.id").alias("alert_id"),
            
            # Alert metadata
            col("entity_exploded.alert.cause").alias("cause"),
            col("entity_exploded.alert.effect").alias("effect"),
            col("entity_exploded.alert.severity_level").alias("severity_level"),
            
            # Alert text content (extract first translation)
            col("entity_exploded.alert.header_text.translation")[0]["text"].alias("header_text"),
            col("entity_exploded.alert.header_text.translation")[0]["language"].alias("header_language"),
            col("entity_exploded.alert.description_text.translation")[0]["text"].alias("description_text"),
            col("entity_exploded.alert.description_text.translation")[0]["language"].alias("description_language"),
            col("entity_exploded.alert.url.translation")[0]["text"].alias("url"),
            
            # Active periods
            col("entity_exploded.alert.active_period").alias("active_periods"),
            
            # Informed entities (affected routes/stops)
            col("entity_exploded.alert.informed_entity").alias("informed_entities"),
            
            # Timestamps
            from_unixtime(col("header.timestamp")).alias("feed_timestamp"),
            
            # Metadata
            col("dt"),
            col("file_metadata"),
            col("ingestion_timestamp")
        )
    )


@dlt.table(
    name="silver_service_alerts_entities", 
    comment="Flattened table of service alerts with individual affected entities (routes, stops, agencies)",
    cluster_by_auto=True
)
def silver_service_alerts_entities():
    """
    Creates a flattened table where each row represents one affected entity
    (route, stop, or agency) per service alert.
    
    Returns:
        DataFrame: Service alerts with individual affected entities
    """
    return (
        spark.readStream
        .table("silver_service_alerts")
        .select(
            col("alert_id"),
            col("cause"),
            col("effect"), 
            col("severity_level"),
            col("header_text"),
            col("description_text"),
            col("url"),
            col("feed_timestamp"),
            explode("informed_entities").alias("entity"),
            col("dt"),
            col("ingestion_timestamp")
        )
        .select(
            col("alert_id"),
            col("cause"),
            col("effect"),
            col("severity_level"), 
            col("header_text"),
            col("description_text"),
            col("url"),
            col("feed_timestamp"),
            
            # Affected entity details
            col("entity.agency_id").alias("affected_agency_id"),
            col("entity.route_id").alias("affected_route_id"),
            col("entity.stop_id").alias("affected_stop_id"),
            
            col("dt"),
            col("ingestion_timestamp")
        )
    )