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

    st.header("1. Configuration")
    
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

    loop_mileage = st.number_input("Loop Mileage (miles)", min_value=0.1, max_value=100.0, value=4.3, step=0.1)

    st.header("2. Date and Route Selection")
    
    col_date1, col_date2 = st.columns(2)
    with col_date1:
        start_date = st.date_input("Start Date", value=datetime.now().date())
    with col_date2:
        end_date = st.date_input("End Date", value=datetime.now().date())

    if start_date > end_date:
        st.error("Start date cannot be after the end date.")
        return

    # Logic for dynamic direction based on date
    CUTOFF_DATE = datetime(2026, 1, 12).date()
    
    col_r1, col_r2 = st.columns(2)
    with col_r1:
        route_loop = st.selectbox("Route Loop", list(ROUTE_MAPPING.keys()), help="Select the route loop to analyze")
    
    with col_r2:
        if start_date >= CUTOFF_DATE:
            direction = st.selectbox("Direction", options=["IB", "OB", "Both"], index=0)
        else:
            direction = "L"
            st.text_input("Direction", value="L (Default for pre-Jan 12)", disabled=True)

    st.header("3. Stop Sequence")
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        start_stop = st.selectbox("Start Stop", AVAILABLE_STOPS, index=3)
    with col_s2:
        end_stop = st.selectbox("End Stop", AVAILABLE_STOPS, index=0)

    # Action Buttons
    col_btn1, col_btn2 = st.columns([3, 1])
    with col_btn1:
        fetch_button = st.button("Fetch, Process, and Download Summary", type="primary")
    
    with col_btn2:
        if st.session_state.get('fetch_triggered', False):
            if st.button("🔄 Clear Results"):
                for key in ['fetch_triggered', 'cached_data', 'cached_data_key', 'params']:
                    if key in st.session_state: del st.session_state[key]
                st.rerun()
    
    if fetch_button:
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
    
    if st.session_state.get('fetch_triggered', False):
        p = st.session_state['params']
        run_full_process(
            p['start_date'], p['end_date'], p['api_key'], p['api_base_url'],
            p['loop_mileage'], p['start_stop'], p['end_stop'],
            p['direction'], p['route_filter'], p['route_mapping']
        )

def run_full_process(start_date, end_date, api_key, api_base_url, loop_mileage, start_stop, end_stop, direction_to_keep, route_filter, route_mapping):
    start_datetime = datetime.combine(start_date, time(6, 0))
    end_datetime = datetime.combine(end_date + timedelta(days=1), time(3, 0))

    cache_key = f"{start_date}_{end_date}_{api_base_url}"
    if 'cached_data_key' not in st.session_state or st.session_state['cached_data_key'] != cache_key:
        with st.spinner("Fetching data from API..."):
            api_data = fetch_data_in_chunks(start_datetime, end_datetime, api_base_url, api_key)
        if not api_data:
            st.info("No data returned from API.")
            return
        st.session_state['cached_data'] = api_data
        st.session_state['cached_data_key'] = cache_key
    else:
        api_data = st.session_state['cached_data']
            
    with st.spinner("Processing loops..."):
        try:
            df = pd.DataFrame(api_data)
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])

            # Route Filtering
            route_filter_int = int(route_filter)
            df_route_filtered = df[df['Route'] == route_filter_int].copy()
            
            if df_route_filtered.empty:
                st.error(f"No data found for Route {route_filter_int}")
                return

            # Direction Filtering (Handling IB, OB, and Both)
            if direction_to_keep == "Both":
                df_filtered = df_route_filtered[df_route_filtered['Direction'].isin(["IB", "OB"])].copy()
            else:
                df_filtered = df_route_filtered[df_route_filtered['Direction'] == direction_to_keep].copy()

            if df_filtered.empty:
                st.error(f"No data found for Direction '{direction_to_keep}'")
                return

            df_filtered.sort_values(by=['Block', 'Timestamp'], inplace=True)
            loop_events = get_loop_events(df_filtered, loop_mileage, start_stop, end_stop)
            
            if loop_events.empty:
                st.error("No complete loops were found.")
                return

            save_loop_events(loop_events, loop_mileage)

        except Exception as e:
            st.error(f"Processing error: {e}")

def fetch_data_in_chunks(start_date, end_date, api_base_url, api_key):
    all_reports = []
    current_start = start_date
    total_hours = (end_date - start_date).total_seconds() / 3600
    
    progress_bar = st.progress(0)
    
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
                if reports: all_reports.extend(reports)
        except Exception as e:
            st.error(f"Fetch error: {e}")
            return None

        # Update Progress
        elapsed_hours = (current_end - start_date).total_seconds() / 3600
        progress_bar.progress(min(elapsed_hours / total_hours, 1.0))
        current_start = current_end
    
    progress_bar.empty()
    return all_reports

def get_loop_events(df, loop_mileage, start_stop, end_stop):
    loop_events = []
    df_sorted = df.sort_values(['Block', 'Timestamp']).reset_index(drop=True)
    
    def get_service_day(timestamp):
        ts = pd.to_datetime(timestamp)
        return ts.date() if ts.hour >= 6 else ts.date() - timedelta(days=1)
    
    for (vehicle, block, route), group in df_sorted.groupby(['Vehicle', 'Block', 'Route']):
        group_sorted = group.sort_values('Timestamp').reset_index(drop=True)
        trip_loops = {}
        
        for i in range(len(group_sorted)):
            row = group_sorted.iloc[i]
            stop, trip, ts = row['Stop_Name'], row['Trip'], pd.to_datetime(row['Timestamp'])
            
            if trip not in trip_loops:
                trip_loops[trip] = {'waiting': False, 'start_ts': None, 'start_idx': None}
            
            if stop == start_stop:
                trip_loops[trip] = {'waiting': True, 'start_ts': ts, 'start_idx': i}
            
            elif stop == end_stop and trip_loops[trip]['waiting']:
                trip_loops[trip]['waiting'] = False
                day = get_service_day(ts)
                
                daily_count = len([e for e in loop_events if e['Vehicle'] == vehicle and get_service_day(e['Loop_Completed_At']) == day])
                loop_count = daily_count + 1
                
                loop_events.append({
                    'Vehicle': vehicle, 'Block': block, 'Route': route, 'Trip': trip,
                    'Start_Stop': start_stop, 'End_Stop': end_stop,
                    'Loop_Completed_At': ts.strftime('%Y-%m-%d %H:%M:%S'),
                    'Loop_Count': loop_count, 'Total_Miles': round(loop_count * loop_mileage, 2),
                    'Trip_Flip': False, 'End_Trip': trip
                })

        # Check for Trip Flips
        for trip_id, state in trip_loops.items():
            if state['waiting']:
                idx = state['start_idx']
                next_rows = group_sorted.iloc[idx+1:]
                next_trip_data = next_rows[next_rows['Trip'] != trip_id].head(1)
                
                if not next_trip_data.empty and next_trip_data.iloc[0]['Stop_Name'] == end_stop:
                    ts_flip = pd.to_datetime(next_trip_data.iloc[0]['Timestamp'])
                    day_flip = get_service_day(ts_flip)
                    daily_count = len([e for e in loop_events if e['Vehicle'] == vehicle and get_service_day(e['Loop_Completed_At']) == day_flip])
                    loop_count = daily_count + 1
                    
                    loop_events.append({
                        'Vehicle': vehicle, 'Block': block, 'Route': route, 'Trip': trip_id,
                        'Start_Stop': start_stop, 'End_Stop': end_stop,
                        'Loop_Completed_At': ts_flip.strftime('%Y-%m-%d %H:%M:%S'),
                        'Loop_Count': loop_count, 'Total_Miles': round(loop_count * loop_mileage, 2),
                        'Trip_Flip': True, 'End_Trip': next_trip_data.iloc[0]['Trip']
                    })

    ev_df = pd.DataFrame(loop_events)
    if not ev_df.empty:
        ev_df['sort_ts'] = pd.to_datetime(ev_df['Loop_Completed_At'])
        ev_df = ev_df.sort_values(['Block', 'sort_ts']).drop('sort_ts', axis=1)
    return ev_df.reset_index(drop=True)

def save_loop_events(df, mileage):
    total_l = len(df)
    total_m = round(total_l * mileage, 2)
    flips = df['Trip_Flip'].sum() if 'Trip_Flip' in df.columns else 0
    
    summary_row = pd.DataFrame([{'Vehicle': 'Total', 'Loop_Count': total_l, 'Total_Miles': total_m, 'Trip_Flip': f'{flips} flips'}])
    final_csv = pd.concat([df, summary_row], ignore_index=True).to_csv(index=False)
    
    st.success(f"Processed {total_l} loops ({total_m} miles).")
    st.download_button("Download CSV", data=final_csv, file_name="Bus_Loops.csv", mime="text/csv")
    
    st.header("Summary")
    c1, c2, c3 = st.columns(3)
    c1.metric("Loops", total_l)
    c2.metric("Miles", total_m)
    c3.metric("Flips", flips)
    st.dataframe(df, use_container_width=True)

if __name__ == "__main__":
    main()
