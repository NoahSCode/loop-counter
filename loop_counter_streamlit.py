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
    
    LOOP_MILEAGE = 4.3
    START_STOP = "Pattee TC EB"
    END_STOP = "Jordan East Pk"
    STOPS_TO_KEEP = [START_STOP, END_STOP, "Nittany Com Ctr", "College_Allen"]
    DIRECTION_TO_KEEP = 'L'

    st.header("Configuration")
    api_key = st.text_input("API Subscription Key", type="password", help="Enter your API subscription key")
    
    if not api_key:
        st.warning("Please enter your API subscription key to continue.")
        return

    st.header("Date Range Selection")
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input("Start Date", value=datetime.now().date())
    
    with col2:
        end_date = st.date_input("End Date", value=datetime.now().date())

    if start_date > end_date:
        st.error("Start date cannot be after the end date.")
        return

    if st.button("Fetch, Process, and Download Summary", type="primary"):
        run_full_process(start_date, end_date, api_key, API_BASE_URL, LOOP_MILEAGE, STOPS_TO_KEEP, DIRECTION_TO_KEEP)

def run_full_process(start_date, end_date, api_key, api_base_url, loop_mileage, stops_to_keep, direction_to_keep):
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

            filter_condition = (df['Stop_Name'].isin(stops_to_keep)) & (df['Direction'] == direction_to_keep)
            df_filtered = df[filter_condition].copy()

            if df_filtered.empty:
                st.error(f"No data found for the specified stops and Direction '{direction_to_keep}'.")
                return

            df_filtered.sort_values(by=['Vehicle', 'Route', 'Timestamp'], inplace=True)
            
            loop_events = get_loop_events(df_filtered, loop_mileage)
            
            if loop_events.empty:
                st.error("No complete loops were found in the data.")
                return

            save_loop_events(loop_events)

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

def get_loop_events(df, loop_mileage):
    loop_events = []
    
    for (vehicle, route), group in df.groupby(['Vehicle', 'Route']):
        count = 0
        
        group_sorted = group.sort_values('Timestamp').reset_index(drop=True)
        
        for i in range(len(group_sorted)):
            current_row = group_sorted.iloc[i]
            current_stop = current_row['Stop_Name']
            
            if current_stop == "Pattee TC EB":
                count += 1
                loop_events.append({
                    'Vehicle': current_row['Vehicle'],
                    'Route': current_row['Route'],
                    'Trip': current_row['Trip'],
                    'Stop_Name': current_row['Stop_Name'],
                    'Timestamp': current_row['Timestamp'],
                    'Loop_Count': count,
                    'Total_Miles': round(count * loop_mileage, 2)
                })
    
    return pd.DataFrame(loop_events)

def save_loop_events(loop_events_df):
    total_loops = len(loop_events_df)
    total_miles = round(loop_events_df['Total_Miles'].sum(), 2)
    
    output_df = loop_events_df.copy()
    
    total_row = pd.DataFrame([{
        'Vehicle': 'Total',
        'Route': '',
        'Trip': '',
        'Stop_Name': '',
        'Timestamp': '',
        'Loop_Count': total_loops,
        'Total_Miles': total_miles
    }])
    
    final_df = pd.concat([output_df, total_row], ignore_index=True)
    
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
        unique_vehicles = len(loop_events_df['Vehicle'].unique())
        st.metric("Unique Vehicles", unique_vehicles)
    
    st.header("Loop Events Data")
    st.dataframe(loop_events_df, use_container_width=True)

if __name__ == "__main__":
    main() 