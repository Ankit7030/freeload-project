# -*- coding: utf-8 -*-
"""
Created on Sun Jun 16 22:10:56 2024

@author: ankikul
"""

import pandas as pd
from datetime import date
import calendar
import time
import argparse

# Set up argument parser
parser = argparse.ArgumentParser(description='Process ReLo and FMER files.')
parser.add_argument('relo_file', type=str, help='Path to the ReLo CSV file')
parser.add_argument('fmer_file', type=str, help='Path to the FMER CSV file')
args = parser.parse_args()

relo_file_path = args.relo_file
fmer_file_path = args.fmer_file

current_time = time.localtime()  # Get current time as a tuple
year = current_time[0]
month = current_time[1]
day = current_time[2]

formatted_date = f"{day:02d}-{month:02d}-{year}"  # Pad with zeros

# Reading the inputs
relo = pd.read_csv(relo_file_path)
relo['Corresponding CPT'] = pd.to_datetime(relo['Corresponding CPT'])
relo['Corresponding CPT'] = relo['Corresponding CPT'].dt.date
relo = relo[relo['Tour ID'].isnull()]
columns_to_keep = ['Load #', 'Lane', 'Corresponding CPT', 'Equipment Type']
relo = relo[columns_to_keep]

relo['orig'] = pd.Series([x.partition("->")[0] for x in relo['Lane']])
relo['dest'] = pd.Series([x.partition("->")[2] for x in relo['Lane']])

relo_pilot = relo.copy()
eligible_equipment = ['DROP_TRAILER']
relo_pilot['eligible_equipment'] = relo_pilot['Equipment Type'].apply(lambda x: 1 if x in eligible_equipment else 0)
relo_pilot = relo_pilot[relo_pilot['eligible_equipment'] == 1]

fmer = pd.read_csv(fmer_file_path)
fmer['Corresponding CPT'] = pd.to_datetime(fmer['Corresponding CPT'])
fmer['Corresponding CPT'] = fmer['Corresponding CPT'].dt.date
fmer = fmer[columns_to_keep]
fmer['orig'] = pd.Series([x.partition("->")[0] for x in fmer['Lane']])
fmer['dest'] = pd.Series([x.partition("->")[2] for x in fmer['Lane']])
fmer_pilot = fmer.copy()
fmer_pilot['eligible_equipment'] = fmer_pilot['Equipment Type'].apply(lambda x: 1 if x in eligible_equipment else 0)
fmer_pilot = fmer_pilot[fmer_pilot['eligible_equipment'] == 1]

final_columns = ['Load #', 'Lane', 'Corresponding CPT']
relo_pilot = relo_pilot[final_columns]

fmer_pilot = fmer_pilot[final_columns]
fmer_pilot = fmer_pilot.rename(columns={'Load #': 'identifier', 'Corresponding CPT': 'date', 'Lane': 'lane'})
relo_pilot = relo_pilot.rename(columns={'Load #': 'identifier', 'Corresponding CPT': 'date', 'Lane': 'lane'})

relo_input = f"relo_carts_{formatted_date}.xlsx"
fmer_input = f"fmer_{formatted_date}.xlsx"
relo_pilot.to_excel(relo_input)
fmer_pilot.to_excel(fmer_input)

# Load the actual input dataframes
relo_df = relo_pilot.copy()
fmer_df = fmer_pilot.copy()

# Ensure 'date' columns are in datetime format
relo_df['date'] = pd.to_datetime(relo_df['date'])
fmer_df['date'] = pd.to_datetime(fmer_df['date'])

# Find unique dates in both dataframes
unique_dates = pd.concat([relo_df['date'], fmer_df['date']]).unique()

# Prepare lists to store the consolidated outputs
all_common_lanes = []
all_cross_overs = []
all_non_flipped = []
summary_list = []

def split_lane(lane):
    if isinstance(lane, str) and '->' in lane:
        parts = lane.split('->')
        if len(parts) == 2:
            return parts[0], parts[1]
    return None, None

for date in unique_dates:
    print(f"Processing date: {date}")
    
    # Filter data for the specific date
    relo_filtered = relo_df[relo_df['date'] == date].copy()
    fmer_filtered = fmer_df[fmer_df['date'] == date].copy()

    # Apply split_lane function and handle invalid rows
    relo_filtered['split_lane'] = relo_filtered['lane'].apply(split_lane)
    fmer_filtered['split_lane'] = fmer_filtered['lane'].apply(split_lane)

    # Filter out invalid lanes
    relo_filtered = relo_filtered[relo_filtered['split_lane'].apply(lambda x: x != (None, None))]
    fmer_filtered = fmer_filtered[fmer_filtered['split_lane'].apply(lambda x: x != (None, None))]

    # Extract valid splits
    if relo_filtered.empty or fmer_filtered.empty:
        print(f"No valid lanes for processing on {date}.")
        continue

    relo_filtered['start_relo'], relo_filtered['end_relo'] = zip(*relo_filtered['split_lane'])
    fmer_filtered['start_fmer'], fmer_filtered['end_fmer'] = zip(*fmer_filtered['split_lane'])

    # Drop the temporary 'split_lane' column
    relo_filtered.drop(columns=['split_lane'], inplace=True)
    fmer_filtered.drop(columns=['split_lane'], inplace=True)

    # Identify common lanes ensuring no duplicates and one-to-one mapping
    common_lanes = pd.merge(relo_filtered, fmer_filtered, left_on=['start_relo', 'end_relo'], right_on=['start_fmer', 'end_fmer'])
    common_lanes = common_lanes[['identifier_x', 'identifier_y', 'lane_x']]
    common_lanes.columns = ['relo_identifier', 'fmer_identifier', 'lane']
    common_lanes = common_lanes.drop_duplicates(subset=['relo_identifier', 'fmer_identifier'])
    common_lanes['date'] = date

    used_relo_identifiers = set()
    used_fmer_identifiers = set()
    common_lanes_list = []
    for _, row in common_lanes.iterrows():
        if row['relo_identifier'] not in used_relo_identifiers and row['fmer_identifier'] not in used_fmer_identifiers:
            common_lanes_list.append(row)
            used_relo_identifiers.add(row['relo_identifier'])
            used_fmer_identifiers.add(row['fmer_identifier'])

    common_lanes = pd.DataFrame(common_lanes_list)
    all_common_lanes.append(common_lanes)
    
    print(f"Common lanes found: {len(common_lanes)}")

    # Keep track of used identifiers to avoid duplicates
    if not common_lanes.empty:
        used_relo_identifiers.update(common_lanes['relo_identifier'])
        used_fmer_identifiers.update(common_lanes['fmer_identifier'])

    # Remove common lanes from consideration in cross-overs
    relo_filtered = relo_filtered[~relo_filtered['identifier'].isin(used_relo_identifiers)]
    fmer_filtered = fmer_filtered[~fmer_filtered['identifier'].isin(used_fmer_identifiers)]

    cross_overs_list = []

    # Create a set of fmer lanes for quick lookup
    fmer_lanes = set(f"{row['start_fmer']}->{row['end_fmer']}" for _, row in fmer_filtered.iterrows())

    used_fmer_identifiers_for_crossovers = set()
    used_relo_identifiers_for_crossovers = set()

    # Identify cross-overs ensuring no duplicates
    for i, relo_row1 in relo_filtered.iterrows():
        if relo_row1['identifier'] in used_relo_identifiers_for_crossovers:
            continue
        for j, relo_row2 in relo_filtered.iterrows():
            if i >= j or relo_row2['identifier'] in used_relo_identifiers_for_crossovers:
                continue

            # Check if we can form an "X" pattern with these two `relo` lanes
            if relo_row1['start_relo'] != relo_row2['start_relo'] and relo_row1['end_relo'] != relo_row2['end_relo']:
                x_pattern1 = f"{relo_row1['start_relo']}->{relo_row2['end_relo']}"
                x_pattern2 = f"{relo_row2['start_relo']}->{relo_row1['end_relo']}"

                # Check if the "X" pattern exists in the `fmer` lanes
                if x_pattern1 in fmer_lanes and x_pattern2 in fmer_lanes:
                    fmer1 = fmer_filtered[fmer_filtered['lane'] == x_pattern1]
                    fmer2 = fmer_filtered[fmer_filtered['lane'] == x_pattern2]

                    if not fmer1.empty and not fmer2.empty and fmer1.iloc[0]['identifier'] not in used_fmer_identifiers_for_crossovers and fmer2.iloc[0]['identifier'] not in used_fmer_identifiers_for_crossovers:
                        cross_overs_list.append({
                            'relo_identifier1': relo_row1['identifier'],
                            'relo_identifier2': relo_row2['identifier'],
                            'fmer_identifier1': fmer1.iloc[0]['identifier'],
                            'fmer_identifier2': fmer2.iloc[0]['identifier'],
                            'relo_lane1': relo_row1['lane'],
                            'relo_lane2': relo_row2['lane'],
                            'fmer_lane1': x_pattern1,
                            'fmer_lane2': x_pattern2,
                            'cross_over_description': f"{relo_row1['lane']} and {relo_row2['lane']} with {x_pattern1} and {x_pattern2}",
                            'date': date
                        })
                        used_relo_identifiers_for_crossovers.update([relo_row1['identifier'], relo_row2['identifier']])
                        used_fmer_identifiers_for_crossovers.update([fmer1.iloc[0]['identifier'], fmer2.iloc[0]['identifier']])
                        break

    # Convert cross-overs to DataFrame and ensure no duplicates
    if cross_overs_list:
        output_cross_overs = pd.DataFrame(cross_overs_list).drop_duplicates(subset=['relo_identifier1', 'relo_identifier2', 'fmer_identifier1', 'fmer_identifier2'])
        all_cross_overs.append(output_cross_overs)
    else:
        print(f"No cross-overs found for date: {date}")

    # Identify relo loads that cannot be flipped
    relo_ids_in_cross_overs = set(used_relo_identifiers_for_crossovers)
    non_flipped_relo = relo_filtered[~relo_filtered['identifier'].isin(relo_ids_in_cross_overs)]
    non_flipped_relo['date'] = date
    all_non_flipped.append(non_flipped_relo[['identifier', 'lane', 'date']])

    # Add to summary list
    summary_list.append({
        'date': date,
        'cross_overs': len(cross_overs_list),
        'common_lanes': len(common_lanes),
        'non_flippable_relo': len(non_flipped_relo)
    })

# Consolidate results into single DataFrames
common_lanes_df = pd.concat(all_common_lanes).reset_index(drop=True)
try:
    cross_overs_df = pd.DataFrame()
    cross_overs_df = pd.concat(all_cross_overs).reset_index(drop=True)
except ValueError:
    cross_overs_df = pd.DataFrame()
    print("No Cross-overs")

non_flipped_df = pd.concat(all_non_flipped).reset_index(drop=True)
summary_df = pd.DataFrame(summary_list)

filename = f'FleetLite_carts_run_2_{formatted_date}.xlsx'
# Write the consolidated outputs to an Excel file
with pd.ExcelWriter(filename) as writer:
    common_lanes_df.to_excel(writer, sheet_name='Common_Lanes', index=False)
    cross_overs_df.to_excel(writer, sheet_name='Cross_Overs', index=False)
    non_flipped_df.to_excel(writer, sheet_name='Non_Flipped', index=False)
    summary_df.to_excel(writer, sheet_name='Summary', index=False)

print("Data processing complete. Output saved to consolidated_cross_overs_and_common_lanes.xlsx")
