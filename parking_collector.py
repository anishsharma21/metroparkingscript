import os
import sys
import time
import json
import sqlite3
import logging
from datetime import datetime
from typing import Optional, Dict, Any
import requests
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv('OPEN_DATA_API_KEY')
BASE_URL = 'https://api.transport.nsw.gov.au/v1/carpark'
FACILITIES = [29, 30, 31, 32]  # Kellyville North, South, Bella Vista, Hills Showground
POLL_INTERVAL_SECONDS = 600  # 10 minutes
MAX_RETRIES = 5
RATE_LIMIT_DELAY = 0.5  # Delay between requests to respect rate limits
MAX_CONSECUTIVE_FAILURES = 3
DB_PATH = 'parking_data.db'

# Error codes that should trigger retry with backoff
RETRYABLE_STATUS_CODES = [500, 503]
# Error codes that should fail immediately
FATAL_STATUS_CODES = [401, 403, 404]


def init_database():
    """Initialize the SQLite database and create tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS parking_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            facility_id INTEGER NOT NULL,
            facility_name TEXT NOT NULL,
            message_date TEXT NOT NULL,
            collected_at TEXT NOT NULL,
            total_spots INTEGER,
            total_occupancy INTEGER,
            raw_response TEXT NOT NULL,
            UNIQUE(facility_id, message_date)
        )
    ''')

    # Create indexes for efficient querying
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_facility_id
        ON parking_data(facility_id)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_message_date
        ON parking_data(message_date)
    ''')

    conn.commit()
    conn.close()
    logger.info(f"Database initialized at {DB_PATH}")


def fetch_parking_data(facility_id: int, retry_count: int = 0) -> Optional[Dict[Any, Any]]:
    """
    Fetch parking data for a specific facility with exponential backoff retry logic.

    Args:
        facility_id: The facility ID to fetch data for
        retry_count: Current retry attempt (0-indexed)

    Returns:
        Dictionary containing the API response, or None if all retries failed
    """
    if not API_KEY:
        logger.error("OPEN_DATA_API_KEY not found in environment variables")
        return None

    url = f"{BASE_URL}?facility={facility_id}"
    headers = {
        'Authorization': f'apikey {API_KEY}'
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            data = response.json()

            # Check for error response structure
            if 'ErrorDetails' in data:
                logger.error(f"API returned error for facility {facility_id}: {data['ErrorDetails'].get('Message', 'Unknown error')}")
                return None

            logger.info(f"Successfully fetched data for facility {facility_id}")
            return data

        elif response.status_code in FATAL_STATUS_CODES:
            logger.error(f"Fatal error {response.status_code} for facility {facility_id}: {response.text}")
            return None

        elif response.status_code in RETRYABLE_STATUS_CODES or response.status_code == 403:
            if retry_count < MAX_RETRIES:
                wait_time = 2 ** retry_count  # Exponential backoff: 1, 2, 4, 8, 16 seconds
                logger.warning(f"Retryable error {response.status_code} for facility {facility_id}. Retry {retry_count + 1}/{MAX_RETRIES} in {wait_time}s")
                time.sleep(wait_time)
                return fetch_parking_data(facility_id, retry_count + 1)
            else:
                logger.error(f"Max retries ({MAX_RETRIES}) exceeded for facility {facility_id}")
                return None

        else:
            logger.error(f"Unexpected status code {response.status_code} for facility {facility_id}: {response.text}")
            return None

    except requests.exceptions.Timeout:
        if retry_count < MAX_RETRIES:
            wait_time = 2 ** retry_count
            logger.warning(f"Request timeout for facility {facility_id}. Retry {retry_count + 1}/{MAX_RETRIES} in {wait_time}s")
            time.sleep(wait_time)
            return fetch_parking_data(facility_id, retry_count + 1)
        else:
            logger.error(f"Max retries ({MAX_RETRIES}) exceeded due to timeouts for facility {facility_id}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception for facility {facility_id}: {str(e)}")
        return None

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response for facility {facility_id}: {str(e)}")
        return None


def save_parking_data(data: Dict[Any, Any]):
    """
    Save parking data to the SQLite database.

    Args:
        data: Dictionary containing the API response
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    facility_id = data.get('facility_id')
    facility_name = data.get('facility_name', '')
    message_date = data.get('MessageDate', '')
    collected_at = datetime.now().isoformat()
    total_spots = data.get('spots')

    # Extract total occupancy from the occupancy object
    occupancy = data.get('occupancy', {})
    total_occupancy = occupancy.get('total')

    # Store the entire response as JSON
    raw_response = json.dumps(data)

    try:
        cursor.execute('''
            INSERT INTO parking_data
            (facility_id, facility_name, message_date, collected_at, total_spots, total_occupancy, raw_response)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (facility_id, facility_name, message_date, collected_at, total_spots, total_occupancy, raw_response))

        conn.commit()
        logger.info(f"Saved data for facility {facility_id} ({facility_name}) - Spots: {total_spots}, Occupancy: {total_occupancy}")
    except sqlite3.IntegrityError:
        logger.warning(f"Duplicate data detected for facility {facility_id} at message_date {message_date} - skipping")
    finally:
        conn.close()


def poll_all_facilities() -> bool:
    """
    Poll all configured facilities and save their data.

    Returns:
        True if at least one facility was successfully polled, False if all failed
    """
    logger.info(f"Starting polling cycle for {len(FACILITIES)} facilities")
    success_count = 0

    for facility_id in FACILITIES:
        data = fetch_parking_data(facility_id)

        if data:
            save_parking_data(data)
            success_count += 1

        # Rate limiting: wait between requests to avoid hitting 5 req/sec limit
        if facility_id != FACILITIES[-1]:  # Don't wait after the last request
            time.sleep(RATE_LIMIT_DELAY)

    logger.info(f"Polling cycle complete: {success_count}/{len(FACILITIES)} facilities successful")
    return success_count > 0


def main():
    """Main execution loop."""
    if not API_KEY:
        logger.error("OPEN_DATA_API_KEY not found in .env file. Exiting.")
        sys.exit(1)

    logger.info("Starting NSW Car Park Data Collector")
    logger.info(f"Monitoring facilities: {FACILITIES}")
    logger.info(f"Poll interval: {POLL_INTERVAL_SECONDS} seconds ({POLL_INTERVAL_SECONDS // 60} minutes)")

    # Initialize database
    init_database()

    consecutive_failures = 0

    while True:
        try:
            success = poll_all_facilities()

            if success:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                logger.error(f"Polling interval failed. Consecutive failures: {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}")

                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    error_msg = f"CRITICAL: {MAX_CONSECUTIVE_FAILURES} consecutive polling intervals failed. Exiting."
                    logger.error(error_msg)
                    print(error_msg, file=sys.stderr)
                    sys.exit(1)

            # Wait for next polling interval
            logger.info(f"Waiting {POLL_INTERVAL_SECONDS} seconds until next poll...")
            time.sleep(POLL_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Exiting.")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {str(e)}", exc_info=True)
            consecutive_failures += 1

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                error_msg = f"CRITICAL: {MAX_CONSECUTIVE_FAILURES} consecutive errors. Exiting."
                logger.error(error_msg)
                print(error_msg, file=sys.stderr)
                sys.exit(1)

            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
