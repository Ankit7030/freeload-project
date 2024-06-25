# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 18:35:10 2024

@author: ankikul
"""

# -*- coding: utf-8 -*-
"""
Created on Fri May 24 10:47:11 2024

@author: ankikul
"""

import pandas as pd
from datetime import date, timedelta
import calendar
import argparse

# Set up argument parser
parser = argparse.ArgumentParser(description='Process relo and times files.')
parser.add_argument('relo_file', type=str, help='Path to the ReLo CSV file')
parser.add_argument('times_file', type=str, help='Path to the times CSV file')
args = parser.parse_args()

# Load the data
relo = pd.read_csv(args.relo_file)
relo['identifier'] = range(1, len(relo) + 1)
relo['Scheduled Truck Arrival - 1 date'] = pd.to_datetime(relo['Scheduled Truck Arrival - 1 date'])

times = pd.read_csv(args.times_file)
times['End'] = times['End'].replace('0:00', '23:59')

# Ensure 'day' column is correctly created
relo['day'] = relo['Scheduled Truck Arrival - 1 date'].dt.dayofweek.apply(lambda x: calendar.day_abbr[x])

relo['pickup_time'] = pd.to_datetime(relo['Scheduled Truck Arrival - 1 time']).dt.strftime('%H:%M')
relo['delivery_time'] = pd.to_datetime(relo['Scheduled Truck Arrival - 2 time']).dt.strftime('%H:%M')

relo['orig_key'] = relo.Lane.apply(lambda x: x.partition("->")[0]) + '|' + relo['day']
relo['dest_key'] = relo.Lane.apply(lambda x: x.partition("->")[2]) + '|' + relo['day']

times['key'] = times['Site'] + '|' + times['Day']
times['Start'] = pd.to_datetime(times['Start']).dt.strftime('%H:%M')
times['End'] = pd.to_datetime(times['End']).dt.strftime('%H:%M')

# Debugging step: Print times DataFrame to verify 'Start' and 'End' columns
print("Times DataFrame:\n", times.head())

# Merge dataframes on origin keys
merged_df = pd.merge(relo, times, how='left', left_on='orig_key', right_on='key')

# Debugging step: Print merged DataFrame to verify the merge
print("Merged DataFrame after first merge:\n", merged_df.head())

# Calculate Pickup Window Start 1 and Pickup Window End 1
merged_df['Pickup Window Start 1'] = pd.to_datetime(
    merged_df['Scheduled Truck Arrival - 1 date'].astype(str) + ' ' + merged_df['Scheduled Truck Arrival - 1 time'])

def check_band(row):
    pickup_time = row['pickup_time']
    start = row['Start']
    end = row['End']
    if start <= pickup_time <= end:
        return f"{start, end}"
    return None

merged_df['band'] = merged_df.apply(check_band, axis=1)

# Debugging step: Print merged DataFrame to verify the 'band' column
print("Merged DataFrame with 'band' column:\n", merged_df.head())

merged_df['pickup_start_hour'] = merged_df.apply(lambda row: row['Start'] if row['band'] else '', axis=1)
merged_df['pickup_end_hour'] = merged_df.apply(lambda row: row['End'] if row['band'] else '', axis=1)

# Apply the same calculation logic for Delivery Window Start 1 and End 1
merged_df = merged_df.dropna(subset=['band'])

merged_df['delivery_time'] = pd.to_datetime(merged_df['Scheduled Truck Arrival - 2 time']).dt.strftime('%H:%M')
merged_df = pd.merge(merged_df, times, how='left', left_on='dest_key', right_on='key', suffixes=('_pickup', '_delivery'))

# Debugging step: Print merged DataFrame to verify the second merge
print("Merged DataFrame after second merge:\n", merged_df.head())

def check_band_delivery(row):
    delivery_time = row['delivery_time']
    start = row['Start_delivery']
    end = row['End_delivery']
    # Ensure all values are strings and not empty
    if pd.isna(start) or pd.isna(end) or pd.isna(delivery_time) or start == '' or end == '' or delivery_time == '':
        return None
    if str(start) <= str(delivery_time) <= str(end):
        return f"{start, end}"
    return None

merged_df['band'] = merged_df.apply(check_band_delivery, axis=1)

merged_df['delivery_start_hour'] = merged_df.apply(lambda row: row['Start_delivery'] if row['band'] else '', axis=1)
merged_df['delivery_end_hour'] = merged_df.apply(lambda row: row['End_delivery'] if row['band'] else '', axis=1)

# Convert valid times to datetime and handle invalid entries
def safe_to_datetime(time_str, time_format='%H:%M'):
    try:
        return pd.to_datetime(time_str, format=time_format).time()
    except ValueError:
        return None

def parse_datetime(date_str, time_obj):
    if date_str is None or pd.isna(date_str) or time_obj is None or pd.isna(time_obj):
        return None
    try:
        return pd.to_datetime(date_str + ' ' + time_obj.strftime('%H:%M'))
    except ValueError:
        return None

merged_df['pickup_start_hour'] = merged_df['pickup_start_hour'].apply(lambda x: safe_to_datetime(x))
merged_df['pickup_end_hour'] = merged_df['pickup_end_hour'].apply(lambda x: safe_to_datetime(x))
merged_df['delivery_start_hour'] = merged_df['delivery_start_hour'].apply(lambda x: safe_to_datetime(x))
merged_df['delivery_end_hour'] = merged_df['delivery_end_hour'].apply(lambda x: safe_to_datetime(x))

# Calculate the final pickup and delivery windows
merged_df['Pickup Window End 1'] = merged_df.apply(lambda row: pd.to_datetime(str(row['Scheduled Truck Arrival - 1 date']) + ' ' + str(row['pickup_end_hour'])) if row['pickup_end_hour'] is not None else None, axis=1)
merged_df['Delivery Window Start 1'] = merged_df.apply(lambda row: parse_datetime(row['Scheduled Truck Arrival - 2 date'], row['delivery_start_hour']) if row['delivery_start_hour'] is not None else None, axis=1)
merged_df['Delivery Window End 1'] = merged_df.apply(lambda row: pd.to_datetime(str(row['Scheduled Truck Arrival - 2 date']) + ' ' + str(row['delivery_end_hour'])) if row['delivery_end_hour'] is not None else None, axis=1)

# Ensure the pickup time is at least 120 minutes
merged_df['Pickup Window End 1'] = merged_df.apply(lambda row: row['Pickup Window End 1'] if (row['Pickup Window End 1'] - row['Pickup Window Start 1']) >= timedelta(minutes=120) else row['Pickup Window Start 1'] + timedelta(minutes=120), axis=1)
merged_df['Delivery Window End 1'] = merged_df.apply(lambda row: row['Delivery Window End 1'] if (row['Delivery Window End 1'] - row['Delivery Window Start 1']) >= timedelta(minutes=120) else row['Delivery Window Start 1'] + timedelta(minutes=120), axis=1)

# Format the pickup and delivery window columns
merged_df['Pickup Window Start 1'] = merged_df['Pickup Window Start 1'].dt.strftime('%Y/%m/%d %H:%M')
merged_df['Pickup Window End 1'] = merged_df['Pickup Window End 1'].dt.strftime('%Y/%m/%d %H:%M')
merged_df['Delivery Window Start 1'] = merged_df['Delivery Window Start 1'].dt.strftime('%Y/%m/%d %H:%M')
merged_df['Delivery Window End 1'] = merged_df['Delivery Window End 1'].dt.strftime('%Y/%m/%d %H:%M')

# Drop rows where any of the key columns have None values
merged_df = merged_df.dropna(subset=['Pickup Window Start 1', 'Pickup Window End 1', 'Delivery Window Start 1', 'Delivery Window End 1'])

# Drop unnecessary columns
columns_to_drop = [
    'identifier', 'pickup_time', 'delivery_time', 'orig_key', 'dest_key', 'Site_pickup', 'Site_delivery', 'day_pickup', 'day_delivery',
    'Start_pickup', 'End_pickup', 'key_pickup', 'Start_delivery', 'End_delivery', 'key_delivery', 'band', 'Scheduled Truck Arrival - 1 datetime','day','	Day_pickup',	'pickup_start_hour','	pickup_end_hour','	Day_delivery','	delivery_start_hour',	'delivery_end_hour'
]

# Ensure that only existing columns are dropped
columns_to_drop = [col for col in columns_to_drop if col in merged_df.columns]
merged_df = merged_df.drop(columns_to_drop, axis=1)
columns=['	Day_pickup','	pickup_end_hour','	Day_delivery','	delivery_start_hour']
columns_1=[]
for col in columns:
    col=col.strip("\t")
    columns_1.append(col)
merged_df = merged_df.drop(columns_1, axis=1)

# Save the final dataframe to a CSV file
today = date.today()
year, week_number, _ = today.isocalendar()
filename = f'flexed_fixed_week_{week_number}.csv'
merged_df.to_csv(filename, index=False)

print("Data processing complete. Output saved to", filename)
