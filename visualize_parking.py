import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Page configuration
st.set_page_config(
    page_title="NSW Parking Data Visualization",
    page_icon="🚗",
    layout="wide"
)

# Database configuration
DB_PATH = 'parking_data.db'

# Facility mapping
FACILITIES = {
    29: "Park&Ride - Kellyville (north)",
    30: "Park&Ride - Kellyville (south)",
    31: "Park&Ride - Bella Vista",
    32: "Park&Ride - Hills Showground"
}


@st.cache_data(ttl=60)  # Refresh cache every 60 seconds to show new data
def load_data():
    """Load all parking data from SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT
            facility_id,
            facility_name,
            message_date,
            total_spots,
            total_occupancy
        FROM parking_data
        ORDER BY message_date
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Convert message_date to datetime
    df['message_date'] = pd.to_datetime(df['message_date'])

    # Extract hour from timestamp
    df['hour'] = df['message_date'].dt.hour

    # Extract date (without time)
    df['date'] = df['message_date'].dt.date

    # Extract day of week (Monday, Tuesday, etc.)
    df['day_of_week'] = df['message_date'].dt.day_name()

    # Calculate occupancy rate
    df['occupancy_rate'] = (df['total_occupancy'] / df['total_spots']) * 100

    return df


def calculate_time_averages(df, facility_id, day_filter="All Days", granularity="Hourly"):
    """Calculate average occupancy and rate by time interval for selected facility and day filter.

    Args:
        df: DataFrame with parking data
        facility_id: Facility ID to filter
        day_filter: Day of week to filter ("Today", "All Days", or specific day like "Monday")
        granularity: "Hourly" or "30 Minutes"

    Returns:
        Tuple of (averaged DataFrame, record count)
    """
    # Filter by facility
    filtered_df = df[df['facility_id'] == facility_id].copy()

    # Filter by day
    if day_filter == "Today":
        # Get today's date and filter to only today's data
        today = pd.Timestamp.now().date()
        filtered_df = filtered_df[filtered_df['date'] == today].copy()
    elif day_filter != "All Days":
        # Filter by day of week (Monday, Tuesday, etc.)
        filtered_df = filtered_df[filtered_df['day_of_week'] == day_filter].copy()

    if filtered_df.empty:
        return None, 0

    if granularity == "Hourly":
        # Group by hour and calculate averages
        time_avg = filtered_df.groupby('hour').agg({
            'total_occupancy': 'mean',
            'occupancy_rate': 'mean',
            'total_spots': 'mean'
        }).reset_index()
        time_avg.rename(columns={'hour': 'time_bin'}, inplace=True)

    else:  # 30 Minutes
        # Create 30-minute bins (0.0, 0.5, 1.0, 1.5, ..., 23.5)
        filtered_df['time_bin'] = filtered_df['message_date'].dt.hour + \
                                  (filtered_df['message_date'].dt.minute // 30) * 0.5

        time_avg = filtered_df.groupby('time_bin').agg({
            'total_occupancy': 'mean',
            'occupancy_rate': 'mean',
            'total_spots': 'mean'
        }).reset_index()

    # Round to 1 decimal place
    time_avg['total_occupancy'] = time_avg['total_occupancy'].round(1)
    time_avg['occupancy_rate'] = time_avg['occupancy_rate'].round(1)
    time_avg['total_spots'] = time_avg['total_spots'].round(0)

    record_count = len(filtered_df)

    return time_avg, record_count


def create_occupancy_chart(time_avg, facility_name, display_mode, granularity):
    """Create an occupancy chart based on the selected display mode and granularity."""

    # Format time labels based on granularity
    def format_time(x):
        if granularity == "Hourly":
            return f"{int(x):02d}:00"
        else:  # 30 Minutes
            hour = int(x)
            minute = "00" if x == hour else "30"
            return f"{hour:02d}:{minute}"

    # Create hover template based on granularity
    time_label = "Time" if granularity == "30 Minutes" else "Hour"

    if display_mode == "Absolute Occupancy":
        # Create single-axis chart for absolute occupancy
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=time_avg['time_bin'],
                y=time_avg['total_occupancy'],
                name="Occupied Spots",
                line=dict(color='#2563eb', width=4),
                mode='lines+markers',
                marker=dict(size=10, color='#2563eb'),
                fill='tozeroy',
                fillcolor='rgba(37, 99, 235, 0.1)',
                hovertemplate=f'<b>{time_label} %{{x}}</b><br>Occupied: %{{y:.1f}} spots<extra></extra>'
            )
        )

        fig.update_yaxes(
            title_text="<b>Average Occupied Spots</b>",
            title_font=dict(size=14, color='#2563eb'),
            gridcolor='#d1d5db',
            showgrid=True,
            rangemode='tozero',
            tickfont=dict(size=12, color='#1f2937')
        )

        chart_title = f"Average Occupancy (Absolute) - {facility_name}"

    elif display_mode == "Percentage Rate":
        # Create single-axis chart for percentage rate
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=time_avg['time_bin'],
                y=time_avg['occupancy_rate'],
                name="Occupancy Rate",
                line=dict(color='#dc2626', width=4),
                mode='lines+markers',
                marker=dict(size=10, color='#dc2626'),
                fill='tozeroy',
                fillcolor='rgba(220, 38, 38, 0.1)',
                hovertemplate=f'<b>{time_label} %{{x}}</b><br>Rate: %{{y:.1f}}%<extra></extra>'
            )
        )

        fig.update_yaxes(
            title_text="<b>Average Occupancy Rate (%)</b>",
            title_font=dict(size=14, color='#dc2626'),
            gridcolor='#d1d5db',
            showgrid=True,
            range=[0, 100],
            tickfont=dict(size=12, color='#1f2937')
        )

        chart_title = f"Average Occupancy Rate - {facility_name}"

    # Common x-axis configuration
    dtick = 1 if granularity == "Hourly" else 0.5
    fig.update_xaxes(
        title_text="Time of Day",
        title_font=dict(size=14, color='#1f2937'),
        dtick=dtick,
        range=[-0.5, 23.5],
        gridcolor='#d1d5db',
        showgrid=True,
        zeroline=True,
        zerolinecolor='#9ca3af',
        tickfont=dict(size=12, color='#1f2937'),
        tickformat=".1f" if granularity == "30 Minutes" else "d"
    )

    # Common layout configuration
    fig.update_layout(
        title={
            'text': chart_title,
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 20, 'color': '#111827'}
        },
        hovermode='x unified',
        height=500,
        showlegend=False,
        plot_bgcolor='#f9fafb',
        paper_bgcolor='#ffffff',
        font=dict(color='#1f2937')
    )

    return fig


def main():
    st.title("🚗 NSW Park&Ride Occupancy Analysis")
    st.markdown("Interactive visualization of parking facility occupancy patterns")

    # Load data
    try:
        df = load_data()
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.info("Make sure the parking_data.db file exists and contains data.")
        return

    if df.empty:
        st.warning("No data available in the database. Run the parking_collector.py script to collect data first.")
        return

    # Sidebar controls
    st.sidebar.header("📊 Visualization Controls")

    # Facility selection
    facility_id = st.sidebar.selectbox(
        "Select Facility",
        options=list(FACILITIES.keys()),
        format_func=lambda x: FACILITIES[x],
        index=0
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Day Filter")

    # Day of week selection
    day_filter = st.sidebar.radio(
        "Select day(s) to analyze",
        options=["Today", "All Days", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
        index=0,
        help="View today's current data, or analyze historical patterns by day of week"
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Display Options")

    # Display mode selection
    display_mode = st.sidebar.radio(
        "Metric to display",
        options=["Absolute Occupancy", "Percentage Rate"],
        index=1,  # Default to Percentage Rate
        help="Choose whether to view absolute number of occupied spots or occupancy rate as a percentage"
    )

    # Time granularity selection
    granularity = st.sidebar.radio(
        "Time granularity",
        options=["Hourly", "30 Minutes"],
        index=0,
        help="Choose time interval for averaging: hourly or every 30 minutes"
    )

    st.sidebar.markdown("---")

    # Calculate time averages based on day filter and granularity
    time_avg, record_count = calculate_time_averages(df, facility_id, day_filter, granularity)

    if time_avg is None or time_avg.empty:
        st.warning(f"No data available for {FACILITIES[facility_id]} with the selected day filter.")
        st.info("Try selecting a different day or facility.")
        return

    # Display summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Facility", FACILITIES[facility_id])

    with col2:
        st.metric("Data Points", f"{record_count:,}")

    with col3:
        avg_occupancy = time_avg['total_occupancy'].mean()
        st.metric("Avg Occupancy", f"{avg_occupancy:.0f} spots")

    with col4:
        avg_rate = time_avg['occupancy_rate'].mean()
        st.metric("Avg Rate", f"{avg_rate:.1f}%")

    # Display day filter info
    if day_filter == "Today":
        from datetime import datetime
        st.info(f"📊 Showing: **Today's data** ({datetime.now().strftime('%B %d, %Y')})")
    elif day_filter == "All Days":
        st.info(f"📊 Showing: **All Days** (combined patterns across all days of the week)")
    else:
        st.info(f"📊 Showing: **{day_filter}** patterns only")

    # Create and display the chart
    fig = create_occupancy_chart(time_avg, FACILITIES[facility_id], display_mode, granularity)
    st.plotly_chart(fig)

    # Display data table
    table_title = "View Time Averages Table" if granularity == "30 Minutes" else "View Hourly Averages Table"
    with st.expander(f"📋 {table_title}"):
        # Format the table for display
        display_df = time_avg.copy()

        # Format time column based on granularity
        if granularity == "Hourly":
            display_df['time_bin'] = display_df['time_bin'].apply(lambda x: f"{int(x):02d}:00")
            time_col_name = 'Hour'
        else:
            display_df['time_bin'] = display_df['time_bin'].apply(lambda x: f"{int(x):02d}:{('00' if x == int(x) else '30')}")
            time_col_name = 'Time'

        display_df.columns = [time_col_name, 'Avg Occupied Spots', 'Avg Occupancy Rate (%)', 'Total Spots']

        st.dataframe(
            display_df,
            hide_index=True,
            width='stretch'
        )

    # Additional insights
    st.markdown("---")
    st.subheader("📈 Key Insights")

    col1, col2 = st.columns(2)

    with col1:
        # Peak time
        peak_time = time_avg.loc[time_avg['total_occupancy'].idxmax()]

        # Format time based on granularity
        if granularity == "Hourly":
            peak_time_str = f"{int(peak_time['time_bin']):02d}:00"
        else:
            hour = int(peak_time['time_bin'])
            minute = "00" if peak_time['time_bin'] == hour else "30"
            peak_time_str = f"{hour:02d}:{minute}"

        st.markdown(f"""
        **Peak Occupancy:**
        - Time: **{peak_time_str}**
        - Occupied Spots: **{peak_time['total_occupancy']:.0f}**
        - Occupancy Rate: **{peak_time['occupancy_rate']:.1f}%**
        """)

    with col2:
        # Lowest time
        lowest_time = time_avg.loc[time_avg['total_occupancy'].idxmin()]

        # Format time based on granularity
        if granularity == "Hourly":
            lowest_time_str = f"{int(lowest_time['time_bin']):02d}:00"
        else:
            hour = int(lowest_time['time_bin'])
            minute = "00" if lowest_time['time_bin'] == hour else "30"
            lowest_time_str = f"{hour:02d}:{minute}"

        st.markdown(f"""
        **Lowest Occupancy:**
        - Time: **{lowest_time_str}**
        - Occupied Spots: **{lowest_time['total_occupancy']:.0f}**
        - Occupancy Rate: **{lowest_time['occupancy_rate']:.1f}%**
        """)

    # Sidebar info
    st.sidebar.markdown("---")
    st.sidebar.info("""
    **About this app:**

    This visualization shows parking occupancy patterns throughout the day.

    **Features:**
    - **Day Filter**: View today's current data, all days combined, or specific day patterns
    - **Metric**: Absolute spots (blue) or percentage rate (red)
    - **Granularity**: Hourly or 30-minute intervals

    **Examples:**
    - Select "Today" to see current day occupancy
    - Select "Monday" to see typical Monday patterns
    - Compare "Friday" vs "Saturday" to see weekday/weekend differences
    """)


if __name__ == "__main__":
    main()
