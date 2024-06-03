# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 07:56:32 2024

@author: ankikul
"""

import pandas as pd

# Load the actual input dataframes
relo_df = pd.read_excel('relo_df.xlsx')
fmer_df = pd.read_excel('fmer_df.xlsx')

# Ensure 'date' columns are in datetime format
relo_df['date'] = pd.to_datetime(relo_df['date'])
fmer_df['date'] = pd.to_datetime(fmer_df['date'])

# Find unique dates in both dataframes
unique_dates = pd.concat([relo_df['date'], fmer_df['date']]).unique()

# Prepare dictionaries to store the outputs for each date
cross_overs_dict = {}
common_lanes_dict = {}
relo_non_flipped_dict = {}
summary_list = []

for date in unique_dates:
    print(f"Processing date: {date}")
    
    # Filter data for the specific date
    relo_filtered = relo_df[relo_df['date'] == date].copy()
    fmer_filtered = fmer_df[fmer_df['date'] == date].copy()

    # Split lanes into start and end components
    relo_filtered[['start_relo', 'end_relo']] = relo_filtered['lane'].str.split('->', expand=True)
    fmer_filtered[['start_fmer', 'end_fmer']] = fmer_filtered['lane'].str.split('->', expand=True)

    # Identify common lanes ensuring no duplicates and one-to-one mapping
    common_lanes = pd.merge(relo_filtered, fmer_filtered, left_on=['start_relo', 'end_relo'], right_on=['start_fmer', 'end_fmer'])
    common_lanes = common_lanes[['identifier_x', 'identifier_y', 'lane_x']]
    common_lanes.columns = ['relo_identifier', 'fmer_identifier', 'lane']
    common_lanes = common_lanes.drop_duplicates(subset=['relo_identifier', 'fmer_identifier'])

    used_relo_identifiers = set()
    used_fmer_identifiers = set()
    common_lanes_list = []
    for _, row in common_lanes.iterrows():
        if row['relo_identifier'] not in used_relo_identifiers and row['fmer_identifier'] not in used_fmer_identifiers:
            common_lanes_list.append(row)
            used_relo_identifiers.add(row['relo_identifier'])
            used_fmer_identifiers.add(row['fmer_identifier'])

    common_lanes = pd.DataFrame(common_lanes_list)
    common_lanes_dict[date] = common_lanes.reset_index(drop=True)
    
    print(f"Common lanes found: {len(common_lanes)}")

    # Keep track of used identifiers to avoid duplicates
    used_relo_identifiers.update(common_lanes['relo_identifier'])
    used_fmer_identifiers.update(common_lanes['fmer_identifier'])

    # Remove common lanes from consideration in cross-overs
    relo_filtered = relo_filtered[~relo_filtered['identifier'].isin(used_relo_identifiers)]
    fmer_filtered = fmer_filtered[~fmer_filtered['identifier'].isin(used_fmer_identifiers)]

    cross_overs_list = []

    # Create a set of fmer lanes for quick lookup
    fmer_lanes = set(f"{row['start_fmer']}->{row['end_fmer']}" for _, row in fmer_filtered.iterrows())

    # Identify cross-overs ensuring no duplicates
    for i, relo_row1 in relo_filtered.iterrows():
        if relo_row1['identifier'] in used_relo_identifiers:
            continue
        for j, relo_row2 in relo_filtered.iterrows():
            if i >= j or relo_row2['identifier'] in used_relo_identifiers:
                continue

            # Check if we can form an "X" pattern with these two `relo` lanes
            if relo_row1['start_relo'] != relo_row2['start_relo'] and relo_row1['end_relo'] != relo_row2['end_relo']:
                x_pattern1 = f"{relo_row1['start_relo']}->{relo_row2['end_relo']}"
                x_pattern2 = f"{relo_row2['start_relo']}->{relo_row1['end_relo']}"

                # Check if the "X" pattern exists in the `fmer` lanes
                if x_pattern1 in fmer_lanes and x_pattern2 in fmer_lanes:
                    cross_overs_list.append({
                        'relo_identifier1': relo_row1['identifier'],
                        'relo_identifier2': relo_row2['identifier'],
                        'fmer_identifier1': fmer_filtered[fmer_filtered['lane'] == x_pattern1].iloc[0]['identifier'],
                        'fmer_identifier2': fmer_filtered[fmer_filtered['lane'] == x_pattern2].iloc[0]['identifier'],
                        'relo_lane1': relo_row1['lane'],
                        'relo_lane2': relo_row2['lane'],
                        'fmer_lane1': x_pattern1,
                        'fmer_lane2': x_pattern2,
                        'cross_over_description': f"{relo_row1['lane']} and {relo_row2['lane']} with {x_pattern1} and {x_pattern2}"
                    })
                    used_relo_identifiers.update([relo_row1['identifier'], relo_row2['identifier']])
                    used_fmer_identifiers.update([
                        fmer_filtered[fmer_filtered['lane'] == x_pattern1].iloc[0]['identifier'],
                        fmer_filtered[fmer_filtered['lane'] == x_pattern2].iloc[0]['identifier']
                    ])
                    break

    # Convert cross-overs to DataFrame and ensure no duplicates
    if cross_overs_list:
        output_cross_overs = pd.DataFrame(cross_overs_list).drop_duplicates(subset=['relo_identifier1', 'relo_identifier2', 'fmer_identifier1', 'fmer_identifier2'])
        cross_overs_dict[date] = output_cross_overs.reset_index(drop=True)
    else:
        print(f"No cross-overs found for date: {date}")

    # Identify relo loads that cannot be flipped
    relo_ids_in_cross_overs = set(used_relo_identifiers)
    non_flipped_relo = relo_filtered[~relo_filtered['identifier'].isin(relo_ids_in_cross_overs)]
    relo_non_flipped_dict[date] = non_flipped_relo[['identifier', 'lane']].reset_index(drop=True)

    # Add to summary list
    summary_list.append({
        'date': date,
        'cross_overs': len(cross_overs_dict.get(date, [])),
        'common_lanes': len(common_lanes_dict.get(date, [])),
        'non_flippable_relo': len(relo_non_flipped_dict.get(date, []))
    })

# Write the outputs to an Excel file
with pd.ExcelWriter('cross_overs_and_common_lanes.xlsx') as writer:
    for date in unique_dates:
        date_str = pd.Timestamp(date).strftime('%Y-%m-%d')
        if date in cross_overs_dict:
            cross_overs_dict[date].to_excel(writer, sheet_name=f'Cross_Overs_{date_str}', index=False)
        if date in common_lanes_dict:
            common_lanes_dict[date].to_excel(writer, sheet_name=f'Common_Lanes_{date_str}', index=False)
        if date in relo_non_flipped_dict:
            relo_non_flipped_dict[date].to_excel(writer, sheet_name=f'Non_Flipped_{date_str}', index=False)
    
    # Write summary sheet
    summary_df = pd.DataFrame(summary_list)
    summary_df.to_excel(writer, sheet_name='Summary', index=False)

print("Data processing complete. Output saved to cross_overs_and_common_lanes.xlsx")
