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

    API_BASE_URL = "https://avail360-api.myavail.cloud/StopReports/v1/CATA/"
    
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

    # Create stops to keep list based on user selection
    stops_to_keep = [start_stop, end_stop, "Nittany Com Ctr", "College_Allen"]
    # Remove duplicates while preserving order
    stops_to_keep = list(dict.fromkeys(stops_to_keep))

    st.header("Date Range Selection")
    col5, col6 = st.columns(2)
    
    with col5:
        start_date = st.date_input("Start Date", value=datetime.now().date())
    
    with col6:
        end_date = st.date_input("End Date", value=datetime.now().date())

    if start_date > end_date:
        st.error("Start date cannot be after the end date.")
        return

    if st.button("Fetch, Process, and Download Summary", type="primary"):
        run_full_process(start_date, end_date, api_key, API_BASE_URL, loop_mileage, stops_to_keep, direction, ROUTE_MAPPING[route_loop], ROUTE_MAPPING)

def run_full_process(start_date, end_date, api_key, api_base_url, loop_mileage, stops_to_keep, direction_to_keep, route_filter, route_mapping):
    start_datetime = datetime.combine(start_date, time(6, 0))
    end_datetime = datetime.combine(end_date + timedelta(days=1), time(3, 0))

    with st.spinner("Fetching data from API..."):
        api_data = fetch_data_in_chunks(start_datetime, end_datetime, api_base_url, api_key)

    if not api_data:
        st.info("No data was returned from the API for the selected date range.")
        return
           
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

            # Debug: Show vehicle 206 data before stop/direction filtering
            vehicle_206_before = df_route_filtered[df_route_filtered['Vehicle'] == 206]
            if len(vehicle_206_before) > 0:
                st.write(f"🔍 DEBUG: Vehicle 206 records before stop/direction filtering: {len(vehicle_206_before)}")
                trip_1454_before = vehicle_206_before[vehicle_206_before['Trip'] == 1454]
                st.write(f"🔍 DEBUG: Vehicle 206 Trip 1454 records before filtering: {len(trip_1454_before)}")
                if len(trip_1454_before) > 0:
                    st.write("🔍 DEBUG: Trip 1454 stops before filtering:")
                    st.dataframe(trip_1454_before[['Stop_Name', 'Direction', 'Timestamp']].sort_values('Timestamp'))
                    st.write(f"🔍 DEBUG: Stops to keep: {stops_to_keep}")
                    st.write(f"🔍 DEBUG: Direction to keep: '{direction_to_keep}'")
            
            # Then filter by stops and direction
            filter_condition = (
                (df_route_filtered['Stop_Name'].isin(stops_to_keep)) & 
                (df_route_filtered['Direction'] == direction_to_keep)
            )
            df_filtered = df_route_filtered[filter_condition].copy()
            
            # Debug: Show vehicle 206 data after stop/direction filtering
            vehicle_206_after = df_filtered[df_filtered['Vehicle'] == 206]
            if len(vehicle_206_after) > 0:
                st.write(f"🔍 DEBUG: Vehicle 206 records after stop/direction filtering: {len(vehicle_206_after)}")
                trip_1454_after = vehicle_206_after[vehicle_206_after['Trip'] == 1454]
                st.write(f"🔍 DEBUG: Vehicle 206 Trip 1454 records after filtering: {len(trip_1454_after)}")
                if len(trip_1454_after) > 0:
                    st.write("🔍 DEBUG: Trip 1454 stops after filtering:")
                    st.dataframe(trip_1454_after[['Stop_Name', 'Direction', 'Timestamp']].sort_values('Timestamp'))

            if df_filtered.empty:
                st.error(f"No data found for the specified stops and Direction '{direction_to_keep}' in Route '{route_filter}'.")
                return

            df_filtered.sort_values(by=['Block', 'Route', 'Timestamp'], inplace=True)
            
            loop_events = get_loop_events(df_filtered, loop_mileage, stops_to_keep[0], stops_to_keep[1])
            
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
    
    # IMPORTANT: Deduplicate records to avoid double-counting
    # Some API responses contain duplicate records with same Vehicle, Block, Route, Timestamp, Stop_Name
    # but different Trip IDs. We need to remove these duplicates before counting loops.
    df_deduped = df.drop_duplicates(subset=['Vehicle', 'Block', 'Route', 'Timestamp', 'Stop_Name'], keep='first')
    
    # Debug info about deduplication
    original_count = len(df)
    deduped_count = len(df_deduped)
    if original_count != deduped_count:
        st.write(f"⚠️ Removed {original_count - deduped_count} duplicate records from {original_count} total records")
    
    # Group by Vehicle and Block to track each bus's journey
    for (vehicle, block, route), group in df_deduped.groupby(['Vehicle', 'Block', 'Route']):
        group_sorted = group.sort_values('Timestamp').reset_index(drop=True)
        
        # Debug output for vehicle 206
        if vehicle == 206:
            st.write(f"🔍 DEBUG: Processing vehicle 206, block {block}, route {route}")
            st.write(f"Records for vehicle 206: {len(group_sorted)}")
            st.dataframe(group_sorted[['Trip', 'Stop_Name', 'Timestamp']].head(20))
        
        # Track trips and which stops they've visited with timestamps
        trip_data = {}  # trip_id -> {'stops': set, 'start_time': timestamp, 'end_time': timestamp}
        processed_trips = set()  # Keep track of trips we've already processed to avoid duplicates
        
        for i in range(len(group_sorted)):
            current_row = group_sorted.iloc[i]
            current_stop = current_row['Stop_Name']
            current_trip = current_row['Trip']
            current_timestamp = pd.to_datetime(current_row['Timestamp'])
            
            # Initialize trip if we haven't seen it
            if current_trip not in trip_data:
                trip_data[current_trip] = {'stops': set(), 'start_time': None, 'end_time': None}
            
            # Add this stop to the trip's visited stops
            trip_data[current_trip]['stops'].add(current_stop)
            
            # Record timestamps when we hit start or end stops (only first time)
            if current_stop == start_stop and trip_data[current_trip]['start_time'] is None:
                trip_data[current_trip]['start_time'] = current_row['Timestamp']
            elif current_stop == end_stop and trip_data[current_trip]['end_time'] is None:
                trip_data[current_trip]['end_time'] = current_row['Timestamp']
            
            # Debug for vehicle 206 trip 1454
            if vehicle == 206 and current_trip == 1454:
                st.write(f"🔍 DEBUG Trip 1454: Stop {current_stop}, Time {current_timestamp}")
                st.write(f"  - Stops visited so far: {trip_data[current_trip]['stops']}")
                st.write(f"  - Start time: {trip_data[current_trip]['start_time']}")
                st.write(f"  - End time: {trip_data[current_trip]['end_time']}")
                st.write(f"  - Looking for start: '{start_stop}', end: '{end_stop}'")
                st.write(f"  - Start in stops? {start_stop in trip_data[current_trip]['stops']}")
                st.write(f"  - End in stops? {end_stop in trip_data[current_trip]['stops']}")
            
            # Check if this trip has now completed a loop (visited both start and end stops)
            # and we haven't processed it yet
            if (start_stop in trip_data[current_trip]['stops'] and 
                end_stop in trip_data[current_trip]['stops'] and
                trip_data[current_trip]['start_time'] is not None and
                trip_data[current_trip]['end_time'] is not None and
                current_trip not in processed_trips):
                
                # Debug for vehicle 206 trip 1454
                if vehicle == 206 and current_trip == 1454:
                    st.write(f"🎉 DEBUG: Trip 1454 LOOP DETECTED!")
                
                # Mark this trip as processed to avoid duplicate processing
                processed_trips.add(current_trip)
                
                # Determine which timestamp to use for completion (the later of the two)
                start_ts = pd.to_datetime(trip_data[current_trip]['start_time'])
                end_ts = pd.to_datetime(trip_data[current_trip]['end_time'])
                completion_timestamp = max(start_ts, end_ts)
                
                # Determine service day (6 AM to 3 AM next day)
                if completion_timestamp.hour >= 6:
                    service_day = completion_timestamp.date()
                else:
                    service_day = completion_timestamp.date() - pd.Timedelta(days=1)
                
                # Count loops for this service day
                def get_service_day(timestamp):
                    ts = pd.to_datetime(timestamp)
                    if ts.hour >= 6:
                        return ts.date()
                    else:
                        return ts.date() - pd.Timedelta(days=1)
                
                daily_loops = len([event for event in loop_events 
                                 if event['Vehicle'] == vehicle and event['Block'] == block
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
                    'Total_Miles': round(loop_count * loop_mileage, 2)
                })
    
    return pd.DataFrame(loop_events).reset_index(drop=True)

def save_loop_events(loop_events_df, loop_mileage):
    total_loops = len(loop_events_df)
    # Calculate total miles as loop mileage times total number of loop events
    total_miles = round(total_loops * loop_mileage, 2)
    
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
        'Total_Miles': total_miles
    }])
    
    final_df = pd.concat([output_df, total_row], ignore_index=True)
    
    # Ensure the index is reset and explicitly exclude it from CSV
    final_df = final_df.reset_index(drop=True)
    csv = final_df.to_csv(index=False)
    
    st.success(f"Processing complete! Total loop events: {total_loops}, Total miles: {total_miles:.2f}")
    
    st.download_button(
        label="Download Loop Events CSV",
        data=csv,
        file_name="Bus_Loop_Events.csv",
        mime="text/csv"
    )
    
    st.header("Summary Statistics")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Loop Events", total_loops)
    
    with col2:
        st.metric("Total Miles", f"{total_miles:.2f}")
    
    with col3:
        unique_blocks = len(loop_events_df['Block'].unique())
        st.metric("Unique Blocks", unique_blocks)
    
    st.header("Loop Events Data")
    st.dataframe(loop_events_df, use_container_width=True)

if __name__ == "__main__":
    main() 