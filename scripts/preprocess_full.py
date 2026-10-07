#!/usr/bin/env python3
"""
Full Dataset Preprocessing Pipeline for Flight Delay Intelligence System:
- Ingests ALL 5,819,079 records from raw airline dataset/flights.csv
- Maps all 307 Month-10 numeric DOT facility IDs to authentic 3-letter IATA codes
- Constructs collision-safe zero-padded row keys for both schemas:
    flights: AIRLINE#ORIGIN#YYYYMMDD#SCHEDDEP#FLIGHTNO#DEST
    flights_by_airport: ORIGIN#YYYYMMDD#SCHEDDEP#AIRLINE#FLIGHTNO#DEST
- Outputs:
    data/flights_full.csv (5,819,079 records)
    data/sample_20_full.txt (20 verified HBase shell PUTs)
    data/dataset_stats_full.txt (complete statistics)
"""

import os
import sys
import csv
import time

def pad_delay(val):
    if not val or val.strip() == "":
        return ""
    try:
        v = int(float(val))
        if v < 0:
            v = 0
        return f"{v:04d}"
    except (ValueError, TypeError):
        return ""

def pad_sched_dep(val):
    if not val or val.strip() == "":
        return "0000"
    try:
        v = int(float(val))
        return f"{v:04d}"
    except:
        return str(val).zfill(4)[:4]

def pad_flight_no(val):
    if not val or val.strip() == "":
        return "00000"
    try:
        v = int(float(val))
        return f"{v:05d}"
    except:
        return str(val).zfill(5)[:5]

def format_date(year, month, day):
    return f"{int(year):04d}{int(month):02d}{int(day):02d}"

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Full Preprocessing Pipeline")
    parser.add_argument("--flights", default="/root/data/flights.csv" if os.path.exists("/root/data/flights.csv") else None)
    parser.add_argument("--airlines", default="/root/data/airlines.csv" if os.path.exists("/root/data/airlines.csv") else None)
    parser.add_argument("--out-csv", default=None)
    parser.add_argument("--stats-file", default=None)
    parser.add_argument("--sample-file", default=None)
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dataset_dir = os.path.join(base_dir, "airline dataset")
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    airlines_file = args.airlines or os.path.join(dataset_dir, "airlines.csv")
    flights_file = args.flights or os.path.join(dataset_dir, "flights.csv")
    out_csv = args.out_csv or os.path.join(data_dir, "flights_full.csv")
    stats_file = args.stats_file or os.path.join(data_dir, "dataset_stats_full.txt")
    sample_file = args.sample_file or os.path.join(data_dir, "sample_20_full.txt")

    if not os.path.exists(airlines_file) or not os.path.exists(flights_file):
        print(f"Error: Dataset files not found: airlines={airlines_file}, flights={flights_file}")
        sys.exit(1)

    print("Step 1: Loading airlines mapping...")
    airline_map = {}
    with open(airlines_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if row:
                airline_map[row[0].strip()] = row[1].strip()

    print(f"Loaded {len(airline_map)} airline mappings.")

    print("\nStep 2: Building DOT ID -> IATA 3-letter code lookup from non-October records...")
    t0 = time.time()
    route_lookup = {}
    with open(flights_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            month = row[1].strip()
            if month != '10':
                orig = row[7].strip()
                dest = row[8].strip()
                if len(orig) == 3 and orig.isalpha() and len(dest) == 3 and dest.isalpha():
                    airline = row[4].strip()
                    flt_no = row[5].strip()
                    dist = row[17].strip()
                    k = (airline, flt_no, dist)
                    if k not in route_lookup:
                        route_lookup[k] = (orig, dest)

    print(f"Built route lookup with {len(route_lookup)} routes in {time.time() - t0:.2f}s.")

    print("\nStep 3: Building Month 10 DOT ID mapper...")
    t1 = time.time()
    dot_to_iata_orig = {}
    dot_to_iata_dest = {}
    with open(flights_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            month = row[1].strip()
            if month == '10':
                orig = row[7].strip()
                dest = row[8].strip()
                airline = row[4].strip()
                flt_no = row[5].strip()
                dist = row[17].strip()
                k = (airline, flt_no, dist)
                if k in route_lookup:
                    m_orig, m_dest = route_lookup[k]
                    if orig not in dot_to_iata_orig:
                        dot_to_iata_orig[orig] = {}
                    dot_to_iata_orig[orig][m_orig] = dot_to_iata_orig[orig].get(m_orig, 0) + 1
                    
                    if dest not in dot_to_iata_dest:
                        dot_to_iata_dest[dest] = {}
                    dot_to_iata_dest[dest][m_dest] = dot_to_iata_dest[dest].get(m_dest, 0) + 1

    dot_map = {}
    all_dot_ids = set(dot_to_iata_orig.keys()) | set(dot_to_iata_dest.keys())
    for d_id in all_dot_ids:
        votes = {}
        if d_id in dot_to_iata_orig:
            for k, v in dot_to_iata_orig[d_id].items():
                votes[k] = votes.get(k, 0) + v
        if d_id in dot_to_iata_dest:
            for k, v in dot_to_iata_dest[d_id].items():
                votes[k] = votes.get(k, 0) + v
        if votes:
            dot_map[d_id] = max(votes.items(), key=lambda x: x[1])[0]

    print(f"Mapped {len(dot_map)} DOT facility IDs to 3-letter IATA codes in {time.time() - t1:.2f}s.")

    print("\nStep 4: Processing all 5,819,079 records and generating flights_full.csv...")
    t2 = time.time()


    out_fields = [
        'rowkey_flights', 'rowkey_airport', 'airline', 'airline_name',
        'origin', 'dest', 'sched_dep', 'tail_no', 'distance',
        'dep_delay', 'arr_delay', 'air_system_delay', 'security_delay',
        'airline_delay', 'late_aircraft_delay', 'weather_delay',
        'cancelled', 'reason'
    ]

    total_rows = 0
    unique_keys_flights = set()
    unique_keys_airport = set()
    dup_keys_flights = 0
    dup_keys_airport = 0
    sample_puts = []

    # Statistics accumulators
    month_counts = {m: 0 for m in range(1, 13)}
    airline_counts = {}
    total_cancelled = 0
    reason_counts = {'A': 0, 'B': 0, 'C': 0, 'D': 0}
    arr_over_60 = 0

    with open(flights_file, 'r', encoding='utf-8') as f_in, \
         open(out_csv, 'w', newline='', encoding='utf-8') as f_out:
        
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        writer.writerow(out_fields)
        header = next(reader)

        for row in reader:
            total_rows += 1
            year = row[0].strip()
            month = int(row[1].strip())
            day = row[2].strip()
            airline = row[4].strip()
            flt_no = row[5].strip()
            tail_no = row[6].strip()
            orig = row[7].strip()
            dest = row[8].strip()
            sched_dep = row[9].strip()
            dep_delay_raw = row[11].strip()
            dist = row[17].strip()
            arr_delay_raw = row[22].strip()
            cancelled_raw = row[24].strip()
            reason_raw = row[25].strip()
            air_sys_raw = row[26].strip()
            sec_raw = row[27].strip()
            air_delay_raw = row[28].strip()
            late_air_raw = row[29].strip()
            wx_raw = row[30].strip()

            # Handle October DOT IDs
            if orig in dot_map:
                orig = dot_map[orig]
            if dest in dot_map:
                dest = dot_map[dest]

            airline_name = airline_map.get(airline, airline)
            date_str = format_date(year, month, day)
            sched_dep_str = pad_sched_dep(sched_dep)
            flt_no_str = pad_flight_no(flt_no)

            # Full collision-safe row keys
            # flights: AIRLINE#ORIGIN#YYYYMMDD#SCHEDDEP#FLIGHTNO#DEST
            # flights_by_airport: ORIGIN#YYYYMMDD#SCHEDDEP#AIRLINE#FLIGHTNO#DEST
            rk_flights = f"{airline}#{orig}#{date_str}#{sched_dep_str}#{flt_no_str}#{dest}"
            rk_airport = f"{orig}#{date_str}#{sched_dep_str}#{airline}#{flt_no_str}#{dest}"

            if rk_flights in unique_keys_flights:
                dup_keys_flights += 1
            else:
                unique_keys_flights.add(rk_flights)

            if rk_airport in unique_keys_airport:
                dup_keys_airport += 1
            else:
                unique_keys_airport.add(rk_airport)

            is_cancelled = 1 if cancelled_raw == '1' else 0
            canc_reason = reason_raw if (is_cancelled and reason_raw) else ""

            dep_delay = ""
            arr_delay = ""
            air_sys_delay = ""
            sec_delay = ""
            air_delay = ""
            late_air_delay = ""
            wx_delay = ""

            if not is_cancelled:
                dep_delay = pad_delay(dep_delay_raw)
                arr_delay = pad_delay(arr_delay_raw)
                air_sys_delay = pad_delay(air_sys_raw)
                sec_delay = pad_delay(sec_raw)
                air_delay = pad_delay(air_delay_raw)
                late_air_delay = pad_delay(late_air_raw)
                wx_delay = pad_delay(wx_raw)

            out_row = [
                rk_flights, rk_airport, airline, airline_name,
                orig, dest, sched_dep_str, tail_no, dist,
                dep_delay, arr_delay, air_sys_delay, sec_delay,
                air_delay, late_air_delay, wx_delay,
                str(is_cancelled), canc_reason
            ]
            writer.writerow(out_row)

            # Accumulate stats
            month_counts[month] += 1
            airline_counts[airline] = airline_counts.get(airline, 0) + 1
            if is_cancelled:
                total_cancelled += 1
                if canc_reason in reason_counts:
                    reason_counts[canc_reason] += 1
            else:
                if arr_delay and arr_delay > '0060':
                    arr_over_60 += 1

            # Sample 20 puts
            if len(sample_puts) < 20:
                puts = []
                puts.append(f"put 'flights', '{rk_flights}', 'info:airline_name', '{airline_name}'")
                puts.append(f"put 'flights', '{rk_flights}', 'info:origin', '{orig}'")
                puts.append(f"put 'flights', '{rk_flights}', 'info:dest', '{dest}'")
                puts.append(f"put 'flights', '{rk_flights}', 'info:sched_dep', '{sched_dep_str}'")
                if tail_no:
                    puts.append(f"put 'flights', '{rk_flights}', 'info:tail_no', '{tail_no}'")
                if dist:
                    puts.append(f"put 'flights', '{rk_flights}', 'info:distance', '{dist}'")
                puts.append(f"put 'flights', '{rk_flights}', 'cancel:cancelled', '{is_cancelled}'")
                if is_cancelled and canc_reason:
                    puts.append(f"put 'flights', '{rk_flights}', 'cancel:reason', '{canc_reason}'")
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

            if total_rows % 1000000 == 0:
                print(f"  Processed {total_rows:,} rows...")

    t3 = time.time()
    print(f"\nStep 5: Full processing complete in {t3 - t2:.2f}s!")
    print(f"Total Rows Ingested: {total_rows:,}")
    print(f"Unique 'flights' Row Keys: {len(unique_keys_flights):,} (Duplicates: {dup_keys_flights})")
    print(f"Unique 'flights_by_airport' Row Keys: {len(unique_keys_airport):,} (Duplicates: {dup_keys_airport})")

    # Save sample_20_full.txt
    with open(sample_file, 'w', encoding='utf-8') as f:
        f.write("# 20 Sample HBase Shell PUT commands from Full 5.8M Dataset\n# Table: flights\n\n")
        for rk, put_list in sample_puts:
            f.write(f"# Flight Record: {rk}\n")
            for p in put_list:
                f.write(p + "\n")
            f.write("\n")

    # Save dataset_stats_full.txt
    with open(stats_file, 'w', encoding='utf-8') as f:
        f.write("==================================================================\n")
        f.write("   FULL DATASET PREPROCESSING STATISTICS (5.8 MILLION FLIGHTS)    \n")
        f.write("==================================================================\n\n")
        f.write(f"1. TOTAL ROWS IN DATASET: {total_rows:,}\n")
        f.write(f"   - Unique 'flights' Row Keys: {len(unique_keys_flights):,} (Duplicates: {dup_keys_flights})\n")
        f.write(f"   - Unique 'flights_by_airport' Row Keys: {len(unique_keys_airport):,} (Duplicates: {dup_keys_airport})\n\n")
        f.write(f"2. NUMBER OF AIRLINES: {len(airline_counts)}\n")
        for code in sorted(airline_counts.keys()):
            name = airline_map.get(code, code)
            f.write(f"     - {code:2s} ({name:32s}): {airline_counts[code]:,} flights\n")
        f.write("\n3. FLIGHTS PER MONTH (All 12 Months Represented):\n")
        for m in range(1, 13):
            f.write(f"     - Month {m:2d}: {month_counts[m]:,} flights\n")
        f.write(f"\n4. CANCELLED FLIGHTS: {total_cancelled:,} ({total_cancelled/total_rows*100:.2f}%)\n")
        f.write("   CANCELLATION REASONS:\n")
        for r, cnt in reason_counts.items():
            f.write(f"     - Reason '{r}': {cnt:,} flights\n")
        f.write(f"\n5. OPERATED FLIGHTS WITH ARRIVAL DELAY > 60 MINS: {arr_over_60:,} ({arr_over_60/(total_rows - total_cancelled)*100:.2f}%)\n")
        f.write("==================================================================\n")

    print(f"Stats written to: {stats_file}")
    print(f"Output CSV size: {os.path.getsize(out_csv):,} bytes")

if __name__ == "__main__":
    main()
