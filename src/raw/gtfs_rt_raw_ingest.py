#!/usr/bin/env python3

"""
GTFS-RT Raw Data Ingestion Script

This script continuously fetches GTFS Realtime data from HSL (Helsinki Region Transport)
and stores it as JSON files in a Unity Catalog volume for further processing.
"""

import os
import time
from datetime import datetime, timezone
import logging
import requests
import json
from databricks.sdk.runtime import dbutils
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

# Configuration constants
DEFAULT_GTFS_RT_URL = "https://realtime.hsl.fi/realtime/vehicle-positions/v2/hsl"
DEFAULT_VOLUME_BASE_PATH = "/Volumes/prod_lakehouse/vehicle/gtfs_rt_raw/"
DEFAULT_POLL_INTERVAL_SECONDS = 5
TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S%fZ"
REQUEST_TIMEOUT_SECONDS = 30

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_gtfs_rt_data(url: str, timeout: int = REQUEST_TIMEOUT_SECONDS) -> bytes:
    """
    Fetch GTFS Realtime data from the specified URL.
    
    Args:
        url: The GTFS-RT endpoint URL
        timeout: Request timeout in seconds
        
    Returns:
        The raw protobuf data as bytes
        
    Raises:
        requests.exceptions.RequestException: If the request fails
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        logger.debug(f"Successfully fetched {len(response.content)} bytes from {url}")
        return response.content
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from {url}: {e}")
        raise


def protobuf_to_json(protobuf_data: bytes) -> dict:
    """
    Convert GTFS-RT protobuf data to JSON dictionary.
    
    Args:
        protobuf_data: Raw protobuf bytes from GTFS-RT feed
        
    Returns:
        Dictionary representation of the protobuf message
        
    Raises:
        Exception: If protobuf parsing fails
    """
    try:
        # Parse the GTFS-RT FeedMessage
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(protobuf_data)
        
        # Convert to dictionary using protobuf's MessageToDict
        json_dict = MessageToDict(feed, preserving_proto_field_name=True)
        
        logger.debug(f"Successfully converted protobuf to JSON with {len(json_dict.get('entity', []))} entities")
        return json_dict
        
    except Exception as e:
        logger.error(f"Failed to parse protobuf data: {e}")
        raise


def save_json_to_file(json_data: dict, base_path: str) -> str:
    """
    Save JSON data to a timestamped file in the specified directory.
    
    Args:
        json_data: The JSON data to save
        base_path: Base directory path for saving files
        
    Returns:
        The full path of the saved file
        
    Raises:
        OSError: If file operations fail
    """
    # Ensure the directory exists
    os.makedirs(base_path, exist_ok=True)
    
    # Generate timestamped filename
    timestamp = datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)
    filename = f"vehicle_positions_{timestamp}.json"
    file_path = os.path.join(base_path, filename)
    
    # Write JSON data to file
    with open(file_path, "w", encoding='utf-8') as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Saved JSON data to {file_path}")
    return file_path


def run_continuous_ingestion(
    url: str = DEFAULT_GTFS_RT_URL,
    volume_path: str = DEFAULT_VOLUME_BASE_PATH,
    poll_interval: int = DEFAULT_POLL_INTERVAL_SECONDS,
    message_name: str = "vehicle_positions"
) -> None:
    """
    Continuously fetch GTFS-RT data and save to files when data changes.
    
    Args:
        url: GTFS-RT endpoint URL
        volume_path: Path to Unity Catalog volume for storing files
        poll_interval: Time between polling attempts in seconds
        message_name: Type of message (vehicle_positions, trip_updates, service_alerts)
    """
    logger.info(f"Starting GTFS-RT ingestion from {url}")
    logger.info(f"Saving to: {volume_path}")
    logger.info(f"Poll interval: {poll_interval} seconds")
    
    last_data = None
    consecutive_errors = 0
    max_consecutive_errors = 10
    
    while True:
        try:
            # Fetch new data
            current_data = fetch_gtfs_rt_data(url)
            
            # Only save if data has changed
            if current_data != last_data:
                # Convert protobuf to JSON
                json_data = protobuf_to_json(current_data)
                
                # Prepare additional directory structure: dt={current_date}/{epoch_time}.json
                now = datetime.now(timezone.utc)
                current_date = now.strftime("%Y-%m-%d")
                epoch_time = int(now.timestamp())
                dt_dir = f"dt={current_date}"
                # Compose the full directory path
                full_dir = os.path.join(volume_path, dt_dir)
                # Ensure the directory exists
                os.makedirs(full_dir, exist_ok=True)
                # File name includes message type and epoch time
                filename = f"{message_name}_{epoch_time}.json"
                file_path = os.path.join(full_dir, filename)
                # Write the JSON data to file
                with open(file_path, "w", encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, ensure_ascii=False)
                last_data = current_data
                consecutive_errors = 0  # Reset error counter on success
                logger.info(f"New GTFS-RT data saved to {file_path}")
            else:
                logger.debug("No new data; skipping write")
                
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"Error during ingestion (attempt {consecutive_errors}): {e}")
            
            # Exit if too many consecutive errors
            if consecutive_errors >= max_consecutive_errors:
                logger.critical(f"Too many consecutive errors ({max_consecutive_errors}). Exiting.")
                raise
        
        # Wait before next poll
        time.sleep(0.5)


def main() -> None:
    """
    Main entry point for the script.
    """
    import sys
    
    # Parse command line arguments for different data types
    # Expected format: gtfs_rt_url <url> message_name <name> poll_interval_seconds <interval>
    if len(sys.argv) >= 7:
        # Parse arguments from Databricks job
        args = {}
        for i in range(1, len(sys.argv), 2):
            if i + 1 < len(sys.argv):
                args[sys.argv[i]] = sys.argv[i + 1]
        
        url = args.get('gtfs_rt_url', DEFAULT_GTFS_RT_URL)
        message_name = args.get('message_name', 'vehicle_positions')
        poll_interval = float(args.get('poll_interval_seconds', DEFAULT_POLL_INTERVAL_SECONDS))
        volume_path = DEFAULT_VOLUME_BASE_PATH
        
        logger.info(f"Starting ingestion for {message_name} from {url}")
        logger.info(f"Poll interval: {poll_interval} seconds")
        logger.info(f"Volume path: {volume_path}")
    else:
        # Fallback to defaults
        url = DEFAULT_GTFS_RT_URL
        volume_path = DEFAULT_VOLUME_BASE_PATH
        poll_interval = DEFAULT_POLL_INTERVAL_SECONDS
        message_name = 'vehicle_positions'
    
    try:
        run_continuous_ingestion(url, volume_path, poll_interval, message_name)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Shutting down gracefully.")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()

