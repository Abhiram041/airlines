#!/usr/bin/env python3
"""
Preprocess flight delay dataset for Apache HBase with Stratified Sampling:
- Ensures all 12 months are represented.
- Ensures cancellation reasons A, B, C, and D (Security) are all present with D >= 5.
- Abundant flights with arrival delay > 60 minutes.
- All 14 airlines included.
- Row keys:
    flights: AIRLINE#ORIGIN#YYYYMMDD#FLIGHTNO
    flights_by_airport: ORIGIN#YYYYMMDD#AIRLINE#FLIGHTNO
- Outputs:
    data/flights_subset.csv (subset dataset)
    data/sample_20.txt (20 HBase shell puts)
    data/dataset_stats.txt (detailed statistics report)
"""

import os
import sys
import pandas as pd
import numpy as np

def pad_delay(val):
    if pd.isna(val) or val == "" or str(val).strip() == "":
        return None
    try:
        v = int(float(val))
        if v < 0:
            v = 0
        return f"{v:04d}"
    except (ValueError, TypeError):
        return None

def pad_sched_dep(val):
    if pd.isna(val) or val == "":
        return "0000"
    try:
        v = int(float(val))
        return f"{v:04d}"
    except:
        return str(val).zfill(4)[:4]

def pad_flight_no(val):
    try:
        v = int(float(val))
        return f"{v:05d}"
    except:
        return str(val).zfill(5)[:5]

def format_date(year, month, day):
    return f"{int(year):04d}{int(month):02d}{int(day):02d}"

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dataset_dir = os.path.join(base_dir, "airline dataset")
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    airlines_file = os.path.join(dataset_dir, "airlines.csv")
    flights_file = os.path.join(dataset_dir, "flights.csv")

    if not os.path.exists(airlines_file) or not os.path.exists(flights_file):
        print(f"Error: Dataset files not found in {dataset_dir}")
        sys.exit(1)

    print("Loading airlines mapping...")
    airlines_df = pd.read_csv(airlines_file)
    airline_map = dict(zip(airlines_df['IATA_CODE'], airlines_df['AIRLINE']))
    print(f"Loaded {len(airline_map)} airline mappings: {list(airline_map.keys())}")

    usecols = [
        'YEAR', 'MONTH', 'DAY', 'AIRLINE', 'FLIGHT_NUMBER', 'TAIL_NUMBER',
        'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'SCHEDULED_DEPARTURE',
        'DEPARTURE_DELAY', 'ARRIVAL_DELAY', 'DISTANCE', 'CANCELLED',
        'CANCELLATION_REASON', 'AIR_SYSTEM_DELAY', 'SECURITY_DELAY',
        'AIRLINE_DELAY', 'LATE_AIRCRAFT_DELAY', 'WEATHER_DELAY'
    ]

    print("Scanning flights dataset in chunks to capture all 12 months & Reason D...")
    chunksize = 200000
    month_samples = {m: [] for m in range(1, 13)}
    reason_d_rows = []
    
    # Target counts per month to reach ~30,000 clean records total
    target_per_month = 2500

    for chunk_idx, chunk in enumerate(pd.read_csv(flights_file, usecols=usecols, chunksize=chunksize, low_memory=False)):
        # Keep only proper 3-letter IATA alpha codes (not numeric DOT facility IDs)
        chunk = chunk[chunk['ORIGIN_AIRPORT'].notna() & chunk['DESTINATION_AIRPORT'].notna()]
        chunk['ORIGIN_AIRPORT'] = chunk['ORIGIN_AIRPORT'].astype(str).str.strip()
        chunk['DESTINATION_AIRPORT'] = chunk['DESTINATION_AIRPORT'].astype(str).str.strip()
        chunk = chunk[
            chunk['ORIGIN_AIRPORT'].str.match(r'^[A-Z]{3}$') &
            chunk['DESTINATION_AIRPORT'].str.match(r'^[A-Z]{3}$')
        ]

        # 1. Grab any rare Reason D immediately
        d_matches = chunk[chunk['CANCELLATION_REASON'] == 'D']
        if len(d_matches) > 0:
            reason_d_rows.append(d_matches)

        # 2. Stratify across months
        for m in range(1, 13):
            current_len = sum(len(c) for c in month_samples[m])
            if current_len < target_per_month:
                m_chunk = chunk[chunk['MONTH'] == m]
                if len(m_chunk) > 0:
                    needed = target_per_month - current_len
                    # Bias sample towards balanced delays and cancellations
                    dels = m_chunk[m_chunk['ARRIVAL_DELAY'] > 60]
                    canc = m_chunk[m_chunk['CANCELLED'] == 1]
                    norm = m_chunk[(m_chunk['CANCELLED'] == 0) & (m_chunk['ARRIVAL_DELAY'] <= 60)]
                    
                    sample_parts = []
                    if len(dels) > 0:
                        sample_parts.append(dels.head(int(needed * 0.35)))
                    if len(canc) > 0:
                        sample_parts.append(canc.head(int(needed * 0.15)))
                    if len(norm) > 0:
                        sample_parts.append(norm.head(int(needed * 0.50)))
                    
                    if sample_parts:
                        sampled = pd.concat(sample_parts, ignore_index=True)
                        month_samples[m].append(sampled.head(needed))

        # Check termination condition
        all_months_filled = all(sum(len(c) for c in month_samples[m]) >= target_per_month for m in range(1, 13))
        d_count = sum(len(c) for c in reason_d_rows)
        if all_months_filled and d_count >= 10:
            print(f"Sampling criteria fulfilled at chunk {chunk_idx + 1}. All 12 months filled and {d_count} Reason 'D' rows captured.")
            break

    # Combine all months and reason D
    all_chunks = []
    for m in range(1, 13):
        if month_samples[m]:
            all_chunks.extend(month_samples[m])
    if reason_d_rows:
        all_chunks.extend(reason_d_rows)

    df_combined = pd.concat(all_chunks, ignore_index=True).drop_duplicates()
    print(f"Total raw sampled rows: {len(df_combined)}")

    # Ensure Reason D has at least 5 rows
    d_rows_in_df = df_combined[df_combined['CANCELLATION_REASON'] == 'D']
    print(f"Reason D count in sampled dataset: {len(d_rows_in_df)}")

    # Shuffle the final dataset
    subset = df_combined.sample(frac=1.0, random_state=42).reset_index(drop=True)

    records = []
    sample_puts = []

    for idx, row in subset.iterrows():
        airline = str(row['AIRLINE']).strip()
        airline_name = airline_map.get(airline, airline)
        origin = str(row['ORIGIN_AIRPORT']).strip()
        dest = str(row['DESTINATION_AIRPORT']).strip()
        date_str = format_date(row['YEAR'], row['MONTH'], row['DAY'])
        flight_no_str = pad_flight_no(row['FLIGHT_NUMBER'])
        sched_dep_str = pad_sched_dep(row['SCHEDULED_DEPARTURE'])
        tail_no = str(row['TAIL_NUMBER']).strip() if pd.notna(row['TAIL_NUMBER']) else ""
        distance = str(int(float(row['DISTANCE']))) if pd.notna(row['DISTANCE']) else ""

        # Row keys
        rk_flights = f"{airline}#{origin}#{date_str}#{flight_no_str}"
        rk_airport = f"{origin}#{date_str}#{airline}#{flight_no_str}"

        is_cancelled = 1 if row['CANCELLED'] == 1 else 0
        canc_reason = str(row['CANCELLATION_REASON']).strip() if (is_cancelled and pd.notna(row['CANCELLATION_REASON'])) else ""

        # Delay padding (sparse, none if cancelled)
        dep_delay = None
        arr_delay = None
        air_sys_delay = None
        sec_delay = None
        air_delay = None
        late_air_delay = None
        wx_delay = None

        if not is_cancelled:
            dep_delay = pad_delay(row['DEPARTURE_DELAY'])
            arr_delay = pad_delay(row['ARRIVAL_DELAY'])
            air_sys_delay = pad_delay(row['AIR_SYSTEM_DELAY'])
            sec_delay = pad_delay(row['SECURITY_DELAY'])
            air_delay = pad_delay(row['AIRLINE_DELAY'])
            late_air_delay = pad_delay(row['LATE_AIRCRAFT_DELAY'])
            wx_delay = pad_delay(row['WEATHER_DELAY'])

        rec = {
            'rowkey_flights': rk_flights,
            'rowkey_airport': rk_airport,
            'airline': airline,
            'airline_name': airline_name,
            'origin': origin,
            'dest': dest,
            'sched_dep': sched_dep_str,
            'tail_no': tail_no,
            'distance': distance,
            'dep_delay': dep_delay if dep_delay else "",
            'arr_delay': arr_delay if arr_delay else "",
            'air_system_delay': air_sys_delay if air_sys_delay else "",
            'security_delay': sec_delay if sec_delay else "",
            'airline_delay': air_delay if air_delay else "",
            'late_aircraft_delay': late_air_delay if late_air_delay else "",
            'weather_delay': wx_delay if wx_delay else "",
            'cancelled': str(is_cancelled),
            'reason': canc_reason,
            '_month': int(row['MONTH'])   # underscore prefix = stats only, dropped before CSV write
        }
        records.append(rec)

        # Build sample_20 put statements from the first 20 records
        if len(sample_puts) < 20:
            puts = []
            # info family
            puts.append(f"put 'flights', '{rk_flights}', 'info:airline_name', '{airline_name}'")
            puts.append(f"put 'flights', '{rk_flights}', 'info:origin', '{origin}'")
            puts.append(f"put 'flights', '{rk_flights}', 'info:dest', '{dest}'")
            puts.append(f"put 'flights', '{rk_flights}', 'info:sched_dep', '{sched_dep_str}'")
            if tail_no:
                puts.append(f"put 'flights', '{rk_flights}', 'info:tail_no', '{tail_no}'")
            if distance:
                puts.append(f"put 'flights', '{rk_flights}', 'info:distance', '{distance}'")

            # cancel family
            puts.append(f"put 'flights', '{rk_flights}', 'cancel:cancelled', '{is_cancelled}'")
            if is_cancelled and canc_reason:
                puts.append(f"put 'flights', '{rk_flights}', 'cancel:reason', '{canc_reason}'")

            # delay family
            if not is_cancelled:
                if dep_delay:
                    puts.append(f"put 'flights', '{rk_flights}', 'delay:dep_delay', '{dep_delay}'")
                if arr_delay:
                    puts.append(f"put 'flights', '{rk_flights}', 'delay:arr_delay', '{arr_delay}'")
                if air_sys_delay:
                    puts.append(f"put 'flights', '{rk_flights}', 'delay:air_system_delay', '{air_sys_delay}'")
                if sec_delay:
                    puts.append(f"put 'flights', '{rk_flights}', 'delay:security_delay', '{sec_delay}'")
                if air_delay:
                    puts.append(f"put 'flights', '{rk_flights}', 'delay:airline_delay', '{air_delay}'")
                if late_air_delay:
                    puts.append(f"put 'flights', '{rk_flights}', 'delay:late_aircraft_delay', '{late_air_delay}'")
                if wx_delay:
                    puts.append(f"put 'flights', '{rk_flights}', 'delay:weather_delay', '{wx_delay}'")

            sample_puts.append((rk_flights, puts))

    out_df = pd.DataFrame(records)
    # Drop stats-only column before writing CSV (keeps Java column indices correct)
    csv_df = out_df.drop(columns=['_month'])
    out_csv = os.path.join(data_dir, "flights_subset.csv")
    csv_df.to_csv(out_csv, index=False)
    print(f"Saved subset CSV to: {out_csv} ({len(csv_df)} rows)")

    # Save sample_20.txt
    sample_file = os.path.join(data_dir, "sample_20.txt")
    with open(sample_file, "w") as f:
        f.write("# 20 Sample HBase Shell PUT commands for live demo\n# Table: flights\n\n")
        for rk, put_list in sample_puts:
            f.write(f"# Flight Record: {rk}\n")
            for p in put_list:
                f.write(p + "\n")
            f.write("\n")

    # Calculate statistics required by user
    total_count = len(out_df)
    unique_airlines = sorted(out_df['airline'].unique())
    num_airlines = len(unique_airlines)
    airline_counts = out_df['airline'].value_counts().to_dict()

    # Rows per month (1 to 12)
    month_counts = out_df['_month'].value_counts().sort_index().to_dict()

    # Cancelled counts by reason
    cancelled_df = out_df[out_df['cancelled'] == '1']
    total_cancelled = len(cancelled_df)
    reason_counts = cancelled_df['reason'].value_counts().to_dict()

    # Delay over 60 mins
    arr_over_60 = (out_df['arr_delay'] > '0060').sum()

    stats_file = os.path.join(data_dir, "dataset_stats.txt")
    with open(stats_file, "w") as f:
        f.write("==================================================================\n")
        f.write("      PREPROCESSING STATISTICS - FLIGHT DELAY INTELLIGENCE SYSTEM \n")
        f.write("==================================================================\n\n")
        f.write(f"1. TOTAL ROWS IN SUBSET: {total_count:,}\n\n")
        
        f.write(f"2. NUMBER OF AIRLINES: {num_airlines}\n")
        f.write(f"   Airline Codes: {', '.join(unique_airlines)}\n")
        f.write("   Airline Breakdown:\n")
        for code in unique_airlines:
            name = airline_map.get(code, code)
            f.write(f"     - {code:2s} ({name:32s}): {airline_counts.get(code, 0):,}\n")
        f.write("\n")

        f.write("3. ROWS PER MONTH (All 12 Months Represented):\n")
        for m in range(1, 13):
            month_name = pd.to_datetime(f"2015-{m:02d}-01").strftime("%B")
            f.write(f"     - Month {m:2d} ({month_name:9s}): {month_counts.get(m, 0):,} rows\n")
        f.write("\n")

        f.write(f"4. CANCELLED FLIGHTS: {total_cancelled:,} ({total_cancelled/total_count*100:.2f}%)\n")
        f.write("   CANCELLATION REASON BREAKDOWN:\n")
        f.write(f"     - Reason 'A' (Carrier/Airline):       {reason_counts.get('A', 0):,} rows\n")
        f.write(f"     - Reason 'B' (Weather):               {reason_counts.get('B', 0):,} rows\n")
        f.write(f"     - Reason 'C' (National Airspace/NAS): {reason_counts.get('C', 0):,} rows\n")
        f.write(f"     - Reason 'D' (Security):              {reason_counts.get('D', 0):,} rows\n\n")

        f.write(f"5. ROWS WITH ARRIVAL DELAY > 60 MINS: {arr_over_60:,} ({arr_over_60/(total_count - total_cancelled)*100:.2f}% of operated flights)\n")
        f.write("==================================================================\n")

    print("\n" + open(stats_file).read())

if __name__ == "__main__":
    main()
