import pandas as pd
import streamlit as st
from datetime import datetime, time, timedelta
import requests
import json
import math
import os

def main():
    st.title("Bus Data Processor (API to CSV)")
    st.write("Fetch bus stop data from an API for a selected date range, process it to count loops, calculate mileage, and save a summary CSV.")

    # API endpoint selection
    API_ENDPOINTS = {
        "Stop Report": "https://avail360-api.myavail.cloud/StopReports/v1/CATA/",
        "Stop Report Detail": "https://avail360-api.myavail.cloud/StopReportsDetail/v1/CATA/"
    }
    
    # Available stops for dropdown selection
    AVAILABLE_STOPS = [
        "Jordan East Pk", 
        "Nittany Com Ctr", 
        "College_Allen", 
        "Pattee TC EB", 
        "Lot 83 West", 
        "Pattee TC WB", 
        "Schlow Lib_CATA"
    ]
    
    # Route mapping
    ROUTE_MAPPING = {
        "BL": 55,
        "WL": 57,
        "BL Gameday": 955,
        "WL Gameday": 957
    }

    st.header("Configuration")
    
    # API Source Selection
    api_source = st.selectbox(
        "API Data Source",
        options=list(API_ENDPOINTS.keys()),
        index=0,
        help="Select which API endpoint to fetch data from"
    )
    API_BASE_URL = API_ENDPOINTS[api_source]
    
    api_key = st.text_input("API Subscription Key", type="password", help="Enter your API subscription key")
    
    if not api_key:
        st.warning("Please enter your API subscription key to continue.")
        return

    loop_mileage = st.number_input("Loop Mileage (miles)", min_value=0.1, max_value=100.0, value=4.3, step=0.1, help="Enter the mileage for one complete loop")

    st.header("Route and Stop Selection")
    col1, col2 = st.columns(2)
    
    with col1:
        route_loop = st.selectbox("Route Loop", ["BL", "WL", "BL Gameday", "WL Gameday"], help="Select the route loop to analyze")
    
    with col2:
        # Direction is always 'L' for loop counting
        direction = "L"

    col3, col4 = st.columns(2)
    
    with col3:
        start_stop = st.selectbox("Start Stop", AVAILABLE_STOPS, index=3, help="Select the starting stop for loop counting")
    
    with col4:
        end_stop = st.selectbox("End Stop", AVAILABLE_STOPS, index=0, help="Select the ending stop for loop counting")

    # We no longer filter by stops - we need all stops to detect complete loops

    st.header("Date Range Selection")
    col5, col6 = st.columns(2)
    
    with col5:
        start_date = st.date_input("Start Date", value=datetime.now().date())
    
    with col6:
        end_date = st.date_input("End Date", value=datetime.now().date())

    if start_date > end_date:
        st.error("Start date cannot be after the end date.")
        return
    st.header("Route and Direction Selection")
    
    # Define the threshold date
    CUTOFF_DATE = datetime(2026, 1, 12).date()

    col1, col2 = st.columns(2)
    
    with col1:
        route_loop = st.selectbox("Route Loop", ["BL", "WL", "BL Gameday", "WL Gameday"], help="Select the route loop to analyze")
    
    with col2:
        # Check if start date is Jan 12, 2026 or later
        if start_date >= CUTOFF_DATE:
            direction = st.selectbox(
                "Direction", 
                options=["IB", "OB", "Both"], 
                index=0,
                help="Select direction for dates on or after Jan 12, 2026"
            )
        else:
            direction = "L"
            st.info("Direction defaulted to 'L' for dates prior to Jan 12, 2026.")
            st.disabled = True # Visual indicator that it's locked
    col_btn1, col_btn2 = st.columns([3, 1])
    
    with col_btn1:
        fetch_button = st.button("Fetch, Process, and Download Summary", type="primary")
    
    with col_btn2:
        if st.session_state.get('fetch_triggered', False):
            if st.button("🔄 Clear Results"):
                # Clear session state
                st.session_state['fetch_triggered'] = False
                if 'cached_data' in st.session_state:
                    del st.session_state['cached_data']
                if 'cached_data_key' in st.session_state:
                    del st.session_state['cached_data_key']
                if 'params' in st.session_state:
                    del st.session_state['params']
                st.rerun()
    
    if fetch_button:
        # Store parameters and trigger fetch
        st.session_state['fetch_triggered'] = True
        st.session_state['params'] = {
            'start_date': start_date,
            'end_date': end_date,
            'api_key': api_key,
            'api_base_url': API_BASE_URL,
            'loop_mileage': loop_mileage,
            'start_stop': start_stop,
            'end_stop': end_stop,
            'direction': direction,
            'route_filter': ROUTE_MAPPING[route_loop],
            'route_mapping': ROUTE_MAPPING
        }
    
    # Process and display data if fetch was triggered
    if st.session_state.get('fetch_triggered', False):
        params = st.session_state['params']
        run_full_process(
            params['start_date'],
            params['end_date'],
            params['api_key'],
            params['api_base_url'],
            params['loop_mileage'],
            params['start_stop'],
            params['end_stop'],
            params['direction'],
            params['route_filter'],
            params['route_mapping']
        )

def run_full_process(start_date, end_date, api_key, api_base_url, loop_mileage, start_stop, end_stop, direction_to_keep, route_filter, route_mapping):
    start_datetime = datetime.combine(start_date, time(6, 0))
    end_datetime = datetime.combine(end_date + timedelta(days=1), time(3, 0))

    # Check if we need to fetch data or if it's already cached
    cache_key = f"{start_date}_{end_date}_{api_base_url}"
    if 'cached_data_key' not in st.session_state or st.session_state['cached_data_key'] != cache_key:
        # Need to fetch new data
        with st.spinner("Fetching data from API..."):
            api_data = fetch_data_in_chunks(start_datetime, end_datetime, api_base_url, api_key)
        
        if not api_data:
            st.info("No data was returned from the API for the selected date range.")
            return
        
        # Cache the data
        st.session_state['cached_data'] = api_data
        st.session_state['cached_data_key'] = cache_key
    else:
        # Use cached data
        api_data = st.session_state['cached_data']
           
    with st.spinner("Processing loops..."):
        try:
            df = pd.DataFrame(api_data)
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])

            # Add raw data download button
            st.success("✅ Raw API data fetched successfully!")
            
            # Show raw data statistics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Records", len(df))
            with col2:
                st.metric("Unique Vehicles", len(df['Vehicle'].unique()))
            with col3:
                st.metric("Unique Routes", len(df['Route'].unique()))
            with col4:
                st.metric("Unique Stops", len(df['Stop_Name'].unique()))
            
            raw_csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download Raw API Data (CSV)",
                data=raw_csv,
                file_name=f"Raw_API_Data_{start_date}_{end_date}.csv",
                mime="text/csv",
                help="Download the complete raw API data before any filtering or processing"
            )
            
            # Add expandable raw data preview
            with st.expander("🔍 Preview Raw Data (First 100 rows)"):
                st.dataframe(df.head(100), use_container_width=True)

            # Debug: Show what routes are in the data
            st.write(f"Total records in dataset: {len(df)}")
            st.write(f"Available routes in data: {sorted(df['Route'].unique())}")
            st.write(f"Route data types: {df['Route'].dtype}")
            st.write(f"Looking for route: '{route_filter}' (type: {type(route_filter)})")

            # First, filter by route (BL=55, WL=57) - convert to int since API data has integer routes
            route_filter_int = int(route_filter)
            df_route_filtered = df[df['Route'] == route_filter_int].copy()
            
            st.write(f"Records after route filtering: {len(df_route_filtered)}")
            
            if df_route_filtered.empty:
                # Find the route loop name for the error message
                route_name = next((k for k, v in route_mapping.items() if v == route_filter_int), f"Route {route_filter}")
                st.error(f"No data found for Route '{route_filter}' ({route_name}).")
                return

            # Filter by direction only (NOT by stops - we need to see all stops to detect complete loops)
            if direction_to_keep == "Both":
                df_filtered = df_route_filtered[df_route_filtered['Direction'].isin(["IB", "OB"])].copy()
            else:
                df_filtered = df_route_filtered[df_route_filtered['Direction'] == direction_to_keep].copy()

            if df_filtered.empty:
                st.error(f"No data found for the specified stops and Direction '{direction_to_keep}' in Route '{route_filter}'.")
                return

            # Sort by Block first, then by Timestamp to ensure proper chronological order within blocks
            df_filtered.sort_values(by=['Block', 'Timestamp'], inplace=True)
            
            loop_events = get_loop_events(df_filtered, loop_mileage, start_stop, end_stop)
            
            if loop_events.empty:
                st.error("No complete loops were found in the data.")
                return

            save_loop_events(loop_events, loop_mileage)

        except Exception as e:
            st.error(f"An error occurred while processing the data: {e}")

def fetch_data_in_chunks(start_date, end_date, api_base_url, api_key):
    all_reports = []
    current_start = start_date
    total_chunks = math.ceil((end_date - start_date).total_seconds() / (24 * 3600))
    processed_chunks = 0

    progress_bar = st.progress(0)
    status_text = st.empty()

    while current_start < end_date:
        current_end = min(current_start + timedelta(hours=24), end_date)
        start_str = current_start.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_str = current_end.strftime('%Y-%m-%dT%H:%M:%SZ')
        url = f"{api_base_url}{start_str}/{end_str}?subscription-key={api_key}"

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and "result" in data and "Stop Reports" in data["result"]:
                reports = data["result"]["Stop Reports"]
                if reports:
                    all_reports.extend(reports)
        except requests.exceptions.RequestException as e:
            st.error(f"Failed to fetch data: {e}")
            return None
        except json.JSONDecodeError:
            st.error("Failed to decode API response.")
            return None

        processed_chunks += 1
        if total_chunks > 0:
            progress = (processed_chunks / total_chunks)
            progress_bar.progress(progress)
            status_text.text(f"Fetching... ({processed_chunks}/{total_chunks})")
        
        current_start = current_end
    
    progress_bar.empty()
    status_text.empty()
    
    return all_reports

def get_loop_events(df, loop_mileage, start_stop, end_stop):
    loop_events = []
    
    # Sort the entire dataframe by Block and Timestamp first to ensure proper processing order
    df_sorted = df.sort_values(['Block', 'Timestamp']).reset_index(drop=True)
    
    # Helper function for service day calculation
    def get_service_day(timestamp):
        ts = pd.to_datetime(timestamp)
        if ts.hour >= 6:
            return ts.date()
        else:
            return ts.date() - pd.Timedelta(days=1)
    
    # Group by Vehicle and Block to track each bus's journey
    for (vehicle, block, route), group in df_sorted.groupby(['Vehicle', 'Block', 'Route']):
        group_sorted = group.sort_values('Timestamp').reset_index(drop=True)
        
        # Track loop states for each trip
        trip_loops = {}  # trip_id -> {'waiting_for_end': False, 'start_time': None, 'start_index': None}
        
        for i in range(len(group_sorted)):
            current_row = group_sorted.iloc[i]
            current_stop = current_row['Stop_Name']
            current_trip = current_row['Trip']
            current_timestamp = pd.to_datetime(current_row['Timestamp'])
            
            # Initialize trip if we haven't seen it
            if current_trip not in trip_loops:
                trip_loops[current_trip] = {'waiting_for_end': False, 'start_time': None, 'start_index': None}
            
            # Check if this is a start stop visit
            if current_stop == start_stop:
                # Start a new potential loop
                trip_loops[current_trip]['waiting_for_end'] = True
                trip_loops[current_trip]['start_time'] = current_row['Timestamp']
                trip_loops[current_trip]['start_index'] = i
            
            # Check if this is an end stop visit and we're waiting for it
            elif current_stop == end_stop and trip_loops[current_trip]['waiting_for_end']:
                # Complete the loop!
                completion_timestamp = current_timestamp
                
                # Reset the waiting state for this trip (in case it does multiple loops)
                trip_loops[current_trip]['waiting_for_end'] = False
                
                # Determine service day (6 AM to 3 AM next day)
                service_day = get_service_day(completion_timestamp)
                
                # Count loops completed by this specific vehicle on this service day (across all blocks)
                daily_loops = len([event for event in loop_events 
                                 if event['Vehicle'] == vehicle
                                 and get_service_day(event['Loop_Completed_At']) == service_day])
                
                loop_count = daily_loops + 1
                
                loop_events.append({
                    'Vehicle': vehicle,
                    'Block': block,
                    'Route': route,
                    'Trip': current_trip,
                    'Start_Stop': start_stop,
                    'End_Stop': end_stop,
                    'Loop_Completed_At': completion_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'Loop_Count': loop_count,
                    'Total_Miles': round(loop_count * loop_mileage, 2),
                    'Trip_Flip': False,  # Normal loop completion
                    'End_Trip': current_trip
                })
        
        # After processing all records, check for incomplete loops that might have trip flips
        for trip_id, trip_state in trip_loops.items():
            if trip_state['waiting_for_end']:
                # This trip started a loop but never completed it
                # Check if the next trip in sequence has the end stop
                start_idx = trip_state['start_index']
                
                # Find where this trip ends (next different trip ID or end of data)
                next_trip_start_idx = None
                for j in range(start_idx + 1, len(group_sorted)):
                    if group_sorted.iloc[j]['Trip'] != trip_id:
                        next_trip_start_idx = j
                        break
                
                if next_trip_start_idx is not None:
                    # Check if the next trip's first stop is the end stop
                    next_trip_id = group_sorted.iloc[next_trip_start_idx]['Trip']
                    next_trip_first_stop = group_sorted.iloc[next_trip_start_idx]['Stop_Name']
                    
                    if next_trip_first_stop == end_stop:
                        # Trip flip detected!
                        completion_timestamp = pd.to_datetime(group_sorted.iloc[next_trip_start_idx]['Timestamp'])
                        service_day = get_service_day(completion_timestamp)
                        
                        # Count loops completed by this specific vehicle on this service day
                        daily_loops = len([event for event in loop_events 
                                         if event['Vehicle'] == vehicle
                                         and get_service_day(event['Loop_Completed_At']) == service_day])
                        
                        loop_count = daily_loops + 1
                        
                        loop_events.append({
                            'Vehicle': vehicle,
                            'Block': block,
                            'Route': route,
                            'Trip': trip_id,  # Count for the ORIGINAL trip where loop started
                            'Start_Stop': start_stop,
                            'End_Stop': end_stop,
                            'Loop_Completed_At': completion_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                            'Loop_Count': loop_count,
                            'Total_Miles': round(loop_count * loop_mileage, 2),
                            'Trip_Flip': True,  # Flag to indicate this was a trip flip
                            'End_Trip': next_trip_id  # The trip where the end stop was actually found
                        })
                        
                        # Mark as no longer waiting
                        trip_loops[trip_id]['waiting_for_end'] = False
    
    # Convert to DataFrame and sort by Block and then by completion time
    loop_events_df = pd.DataFrame(loop_events)
    if not loop_events_df.empty:
        # Sort by Block first, then by Loop_Completed_At to maintain chronological order within blocks
        loop_events_df['Loop_Completed_At_dt'] = pd.to_datetime(loop_events_df['Loop_Completed_At'])
        loop_events_df = loop_events_df.sort_values(['Block', 'Loop_Completed_At_dt'])
        loop_events_df = loop_events_df.drop('Loop_Completed_At_dt', axis=1)  # Remove temporary column
    
    return loop_events_df.reset_index(drop=True)

def save_loop_events(loop_events_df, loop_mileage):
    total_loops = len(loop_events_df)
    # Calculate total miles as loop mileage times total number of loop events
    total_miles = round(total_loops * loop_mileage, 2)
    
    # Count trip flips if the column exists
    trip_flip_count = loop_events_df['Trip_Flip'].sum() if 'Trip_Flip' in loop_events_df.columns else 0
    
    output_df = loop_events_df.copy()
    
    total_row = pd.DataFrame([{
        'Vehicle': 'Total',
        'Block': '',
        'Route': '',
        'Trip': '',
        'Start_Stop': '',
        'End_Stop': '',
        'Loop_Completed_At': '',
        'Loop_Count': total_loops,
        'Total_Miles': total_miles,
        'Trip_Flip': f'{trip_flip_count} trip flips',
        'End_Trip': ''
    }])
    
    final_df = pd.concat([output_df, total_row], ignore_index=True)
    
    # Ensure the index is reset and explicitly exclude it from CSV
    final_df = final_df.reset_index(drop=True)
    csv = final_df.to_csv(index=False)
    
    st.success(f"Processing complete! Total loop events: {total_loops}, Total miles: {total_miles:.2f}, Trip flips detected: {trip_flip_count}")
    
    st.download_button(
        label="Download Loop Events CSV",
        data=csv,
        file_name="Bus_Loop_Events.csv",
        mime="text/csv"
    )
    
    st.header("Summary Statistics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Loop Events", total_loops)
    
    with col2:
        st.metric("Total Miles", f"{total_miles:.2f}")
    
    with col3:
        unique_blocks = len(loop_events_df['Block'].unique())
        st.metric("Unique Blocks", unique_blocks)
    
    with col4:
        st.metric("Trip Flips", trip_flip_count, help="Loops completed with trip ID changes")
    
    st.header("Loop Events Data")
    st.dataframe(loop_events_df, use_container_width=True)

if __name__ == "__main__":
    main() 
