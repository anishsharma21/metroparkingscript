# NSW Car Park Data Collector

Automatically collects parking occupancy data from NSW Park&Ride facilities every 10 minutes, with interactive web-based visualization.

## Quick Start

**Collect Data:**
```bash
# Install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run the collector
python3 parking_collector.py
```

Press `Ctrl+C` to stop.

**Visualize Data:**
```bash
# Make sure you're in the virtual environment
source venv/bin/activate

# Launch the visualization web app
streamlit run visualize_parking.py
```

Opens automatically at http://localhost:8501

## Prerequisites

- Python 3.7+
- NSW Open Data API key (stored in `.env` file)

## Setup

1. Create a `.env` file:
```bash
OPEN_DATA_API_KEY=your_api_key_here
```

2. Install dependencies:
```bash
source venv/bin/activate
pip install -r requirements.txt
```

3. Run the script:
```bash
python3 parking_collector.py
```

## Monitored Facilities

- Park&Ride - Kellyville (north) - Facility ID 29
- Park&Ride - Kellyville (south) - Facility ID 30
- Park&Ride - Bella Vista - Facility ID 31
- Park&Ride - Hills Showground - Facility ID 32

## Interactive Visualization

The project includes a Streamlit web app for visualizing parking occupancy patterns.

**Features:**
- Interactive dual-axis charts showing hourly occupancy patterns
- Compare absolute occupancy (number of cars) vs. occupancy rate (%)
- Select any facility from a dropdown menu
- Choose custom date ranges for averaging
- View peak and lowest occupancy hours
- Explore data with interactive hover tooltips and zoom

**Running the app:**
```bash
source venv/bin/activate
streamlit run visualize_parking.py
```

The app will open automatically in your browser at http://localhost:8501

**What you'll see:**
- Hourly averages (0-23 hours) across your selected date range
- Blue line: Average number of occupied spots
- Orange line: Average occupancy rate as a percentage
- Summary metrics and key insights about peak times

## Data Storage

All data is stored in `parking_data.db` (SQLite database).

### Query Examples

View recent data:
```bash
sqlite3 parking_data.db "SELECT facility_name, message_date, total_spots, total_occupancy FROM parking_data ORDER BY message_date DESC LIMIT 10"
```

Count records per facility:
```bash
sqlite3 parking_data.db "SELECT facility_name, COUNT(*) as records FROM parking_data GROUP BY facility_id"
```

View all data for a specific facility:
```bash
sqlite3 parking_data.db "SELECT * FROM parking_data WHERE facility_id = 29"
```

## How It Works

- Polls the NSW Transport API every 10 minutes
- Collects occupancy data for all 4 facilities
- Stores complete API response + key metrics in SQLite
- Automatically prevents duplicate data (safe to restart anytime)
- Automatically retries on errors with exponential backoff
- Exits after 3 consecutive failed polling intervals
- Respects API rate limits (5 requests/second)

## Database Schema

```sql
parking_data (
  id                INTEGER PRIMARY KEY,
  facility_id       INTEGER,
  facility_name     TEXT,
  message_date      TEXT,
  collected_at      TEXT,
  total_spots       INTEGER,
  total_occupancy   INTEGER,
  raw_response      TEXT,
  UNIQUE(facility_id, message_date)
)
```

- **Indexes** on `facility_id` and `message_date` for fast queries
- **Unique constraint** on `(facility_id, message_date)` prevents duplicate data collection

## Error Handling

- Automatic retry with exponential backoff (up to 5 attempts)
- Rate limit protection with delays between requests
- Exits with error message to stderr after 3 consecutive failures
- All activity logged with timestamps

## API Limits

- Daily quota: 60,000 requests
- Rate limit: 5 requests/second
- Current usage: ~576 requests/day (4 facilities × 6 polls/hour × 24 hours)
