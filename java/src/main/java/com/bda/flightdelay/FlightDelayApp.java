package com.bda.flightdelay;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlightDelayApp {

    private static final String TABLE_FLIGHTS_NAME = "flights";
    private static final String TABLE_AIRPORT_NAME = "flights_by_airport";

    private static final byte[] CF_INFO = Bytes.toBytes("info");
    private static final byte[] CF_DELAY = Bytes.toBytes("delay");
    private static final byte[] CF_CANCEL = Bytes.toBytes("cancel");

    public static Configuration getHBaseConfig() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        return config;
    }

    public static void createTablesIfNotExist(Admin admin) throws IOException {
        TableName[] tables = {
            TableName.valueOf(TABLE_FLIGHTS_NAME),
            TableName.valueOf(TABLE_AIRPORT_NAME)
        };

        for (TableName tn : tables) {
            if (!admin.tableExists(tn)) {
                System.out.println("[INFO] Creating table: " + tn.getNameAsString());
                TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
                tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF_INFO).build());
                tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF_DELAY).build());
                tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF_CANCEL).build());
                admin.createTable(tdb.build());
                System.out.println("[INFO] Table " + tn.getNameAsString() + " created successfully with families: info, delay, cancel.");
            } else {
                System.out.println("[INFO] Table " + tn.getNameAsString() + " already exists.");
            }
        }
    }

    public static String insertSampleFlight(Connection conn, String csvPath) throws IOException {
        TableName tn = TableName.valueOf(TABLE_FLIGHTS_NAME);
        String insertedKey = null;
        try (Table table = conn.getTable(tn);
             BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            String header = br.readLine();
            String firstRow = br.readLine();
            if (firstRow != null) {
                String[] vals = firstRow.split(",", -1);
                String rkFlights = vals[0].trim();
                String airlineName = vals[3].trim();
                String origin = vals[4].trim();
                String dest = vals[5].trim();
                String schedDep = vals[6].trim();
                String tailNo = vals[7].trim();
                String distance = vals[8].trim();
                String depDelay = vals[9].trim();
                String arrDelay = vals[10].trim();
                String airSysDelay = vals[11].trim();
                String secDelay = vals[12].trim();
                String airDelay = vals[13].trim();
                String lateAirDelay = vals[14].trim();
                String wxDelay = vals[15].trim();
                String cancelled = vals[16].trim();
                String reason = vals[17].trim();

                Put put = new Put(Bytes.toBytes(rkFlights));
                addCells(put, airlineName, origin, dest, schedDep, tailNo, distance,
                         cancelled, reason, depDelay, arrDelay, airSysDelay, secDelay,
                         airDelay, lateAirDelay, wxDelay);
                table.put(put);
                insertedKey = rkFlights;
                System.out.println("[SUCCESS] Inserted sample real flight from CSV via Put API: " + rkFlights);
            }
        }
        return insertedKey;
    }

    public static void bulkLoadCsv(Connection conn, String csvPath) throws IOException {
        System.out.println("[INFO] Starting bulk load from CSV: " + csvPath);
        TableName flightsTable = TableName.valueOf(TABLE_FLIGHTS_NAME);
        TableName airportTable = TableName.valueOf(TABLE_AIRPORT_NAME);

        long startTime = System.currentTimeMillis();
        int count = 0;

        BufferedMutatorParams paramsFlights = new BufferedMutatorParams(flightsTable).writeBufferSize(16 * 1024 * 1024);
        BufferedMutatorParams paramsAirport = new BufferedMutatorParams(airportTable).writeBufferSize(16 * 1024 * 1024);

        try (BufferedMutator mutatorFlights = conn.getBufferedMutator(paramsFlights);
             BufferedMutator mutatorAirport = conn.getBufferedMutator(paramsAirport);
             BufferedReader br = new BufferedReader(new FileReader(csvPath), 1024 * 1024)) {

            String headerLine = br.readLine();
            if (headerLine == null) {
                System.out.println("[WARN] CSV file is empty!");
                return;
            }

            String[] headers = headerLine.split(",");
            String line;
            while ((line = br.readLine()) != null) {
                String[] vals = line.split(",", -1);
                if (vals.length < headers.length) continue;

                String rkFlights = vals[0].trim();
                String rkAirport = vals[1].trim();
                String airline = vals[2].trim();
                String airlineName = vals[3].trim();
                String origin = vals[4].trim();
                String dest = vals[5].trim();
                String schedDep = vals[6].trim();
                String tailNo = vals[7].trim();
                String distance = vals[8].trim();
                String depDelay = vals[9].trim();
                String arrDelay = vals[10].trim();
                String airSysDelay = vals[11].trim();
                String secDelay = vals[12].trim();
                String airDelay = vals[13].trim();
                String lateAirDelay = vals[14].trim();
                String wxDelay = vals[15].trim();
                String cancelled = vals[16].trim();
                String reason = vals[17].trim();

                Put putF = new Put(Bytes.toBytes(rkFlights));
                Put putA = new Put(Bytes.toBytes(rkAirport));

                addCells(putF, airlineName, origin, dest, schedDep, tailNo, distance,
                         cancelled, reason, depDelay, arrDelay, airSysDelay, secDelay,
                         airDelay, lateAirDelay, wxDelay);
                addCells(putA, airlineName, origin, dest, schedDep, tailNo, distance,
                         cancelled, reason, depDelay, arrDelay, airSysDelay, secDelay,
                         airDelay, lateAirDelay, wxDelay);

                mutatorFlights.mutate(putF);
                mutatorAirport.mutate(putA);

                count++;
                if (count % 100000 == 0) {
                    long now = System.currentTimeMillis();
                    double rate = count * 1000.0 / Math.max(1, (now - startTime));
                    System.out.println(String.format("  Loaded %,d records... (%.1f records/sec)", count, rate));
                }
            }
            mutatorFlights.flush();
            mutatorAirport.flush();
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.println(String.format("[SUCCESS] Bulk loaded %,d records into both tables in %d ms (%.1f records/sec)",
                count, duration, (count * 1000.0 / Math.max(1, duration))));
    }

    private static void addCells(Put put, String airlineName, String origin, String dest,
                                 String schedDep, String tailNo, String distance,
                                 String cancelled, String reason, String depDelay,
                                 String arrDelay, String airSysDelay, String secDelay,
                                 String airDelay, String lateAirDelay, String wxDelay) {
        if (!airlineName.isEmpty()) put.addColumn(CF_INFO, Bytes.toBytes("airline_name"), Bytes.toBytes(airlineName));
        if (!origin.isEmpty()) put.addColumn(CF_INFO, Bytes.toBytes("origin"), Bytes.toBytes(origin));
        if (!dest.isEmpty()) put.addColumn(CF_INFO, Bytes.toBytes("dest"), Bytes.toBytes(dest));
        if (!schedDep.isEmpty()) put.addColumn(CF_INFO, Bytes.toBytes("sched_dep"), Bytes.toBytes(schedDep));
        if (!tailNo.isEmpty()) put.addColumn(CF_INFO, Bytes.toBytes("tail_no"), Bytes.toBytes(tailNo));
        if (!distance.isEmpty()) put.addColumn(CF_INFO, Bytes.toBytes("distance"), Bytes.toBytes(distance));

        if (!cancelled.isEmpty()) put.addColumn(CF_CANCEL, Bytes.toBytes("cancelled"), Bytes.toBytes(cancelled));
        if (!reason.isEmpty()) put.addColumn(CF_CANCEL, Bytes.toBytes("reason"), Bytes.toBytes(reason));

        if (!depDelay.isEmpty()) put.addColumn(CF_DELAY, Bytes.toBytes("dep_delay"), Bytes.toBytes(depDelay));
        if (!arrDelay.isEmpty()) put.addColumn(CF_DELAY, Bytes.toBytes("arr_delay"), Bytes.toBytes(arrDelay));
        if (!airSysDelay.isEmpty()) put.addColumn(CF_DELAY, Bytes.toBytes("air_system_delay"), Bytes.toBytes(airSysDelay));
        if (!secDelay.isEmpty()) put.addColumn(CF_DELAY, Bytes.toBytes("security_delay"), Bytes.toBytes(secDelay));
        if (!airDelay.isEmpty()) put.addColumn(CF_DELAY, Bytes.toBytes("airline_delay"), Bytes.toBytes(airDelay));
        if (!lateAirDelay.isEmpty()) put.addColumn(CF_DELAY, Bytes.toBytes("late_aircraft_delay"), Bytes.toBytes(lateAirDelay));
        if (!wxDelay.isEmpty()) put.addColumn(CF_DELAY, Bytes.toBytes("weather_delay"), Bytes.toBytes(wxDelay));
    }

    public static void getFlightRecord(Connection conn, String rowKey) throws IOException {
        System.out.println("\n=== OPERATION 1: GET SINGLE ROW (Direct Key Lookup) ===");
        System.out.println("Query Target Row Key: " + rowKey);
        TableName tn = TableName.valueOf(TABLE_FLIGHTS_NAME);
        try (Table table = conn.getTable(tn)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result res = table.get(get);
            if (res.isEmpty()) {
                System.out.println("[RESULT] No record found for key: " + rowKey);
                return;
            }
            printResult(res);
        }
    }

    public static void scanFilteredFlights(Connection conn) throws IOException {
        System.out.println("\n=== OPERATION 2: SCAN WITH FILTERLIST (PrefixFilter + SingleColumnValueFilter) ===");
        System.out.println("Criteria: American Airlines ('AA#') AND Arrival Delay > 60 mins ('binary:0060')");
        TableName tn = TableName.valueOf(TABLE_FLIGHTS_NAME);
        try (Table table = conn.getTable(tn)) {
            Scan scan = new Scan();

            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            // 1. PrefixFilter for American Airlines
            filterList.addFilter(new PrefixFilter(Bytes.toBytes("AA#")));

            // 2. SingleColumnValueFilter for arrival delay > 60 mins with filterIfMissing=true
            SingleColumnValueFilter delayFilter = new SingleColumnValueFilter(
                    CF_DELAY,
                    Bytes.toBytes("arr_delay"),
                    CompareOperator.GREATER,
                    new BinaryComparator(Bytes.toBytes("0060"))
            );
            delayFilter.setFilterIfMissing(true);
            filterList.addFilter(delayFilter);

            scan.setFilter(filterList);
            scan.setLimit(10); // Display top 10 matches

            int matched = 0;
            long start = System.currentTimeMillis();
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result r : scanner) {
                    matched++;
                    System.out.println(String.format("Result #%d:", matched));
                    printResult(r);
                }
            }
            long elapsed = System.currentTimeMillis() - start;
            System.out.println(String.format("[SUMMARY] Scan matched %d rows in %d ms.", matched, elapsed));
        }
    }

    public static void compareRowKeyTradeoff(Connection conn, String airport, String targetDate) throws IOException {
        System.out.println("\n=== OPERATION 3: ROW-KEY DESIGN TRADE-OFF COMPARISON ===");
        System.out.println(String.format("Query: 'All flights departing %s on %s' (1 warm-up run + 5 timed runs without LIMIT)", airport, targetDate));

        int numRuns = 5;
        long[] timesOpt = new long[numRuns];
        long[] timesUnopt = new long[numRuns];
        int countOpt = 0;
        int countUnopt = 0;

        String prefixAirportTable = airport + "#" + targetDate + "#";

        // 1. Optimized Table 'flights_by_airport' (Prefix Scan on row-key)
        try (Table tableA = conn.getTable(TableName.valueOf(TABLE_AIRPORT_NAME))) {
            // Warm-up run (discarded)
            Scan warmScan = new Scan();
            warmScan.setFilter(new PrefixFilter(Bytes.toBytes(prefixAirportTable)));
            try (ResultScanner scanner = tableA.getScanner(warmScan)) {
                for (Result r : scanner) { /* warm-up */ }
            }

            for (int i = 0; i < numRuns; i++) {
                Scan scan1 = new Scan();
                scan1.setFilter(new PrefixFilter(Bytes.toBytes(prefixAirportTable)));
                countOpt = 0;
                long start1 = System.currentTimeMillis();
                try (ResultScanner scanner = tableA.getScanner(scan1)) {
                    for (Result r : scanner) {
                        countOpt++;
                    }
                }
                timesOpt[i] = System.currentTimeMillis() - start1;
            }
        }

        // 2. Unoptimized Table 'flights' (Full Table Scan with origin and date filters)
        try (Table tableF = conn.getTable(TableName.valueOf(TABLE_FLIGHTS_NAME))) {
            // Warm-up run (discarded)
            Scan warmScan2 = new Scan();
            FilterList warmFl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            SingleColumnValueFilter wOrig = new SingleColumnValueFilter(CF_INFO, Bytes.toBytes("origin"), CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(airport)));
            wOrig.setFilterIfMissing(true);
            warmFl.addFilter(wOrig);
            warmFl.addFilter(new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(".*#" + airport + "#" + targetDate + "#.*")));
            warmScan2.setFilter(warmFl);
            try (ResultScanner scanner = tableF.getScanner(warmScan2)) {
                for (Result r : scanner) { /* warm-up */ }
            }

            for (int i = 0; i < numRuns; i++) {
                Scan scan2 = new Scan();
                FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                SingleColumnValueFilter originFilter = new SingleColumnValueFilter(
                        CF_INFO,
                        Bytes.toBytes("origin"),
                        CompareOperator.EQUAL,
                        new BinaryComparator(Bytes.toBytes(airport))
                );
                originFilter.setFilterIfMissing(true);
                fl.addFilter(originFilter);

                RowFilter dateFilter = new RowFilter(
                        CompareOperator.EQUAL,
                        new RegexStringComparator(".*#" + airport + "#" + targetDate + "#.*")
                );
                fl.addFilter(dateFilter);

                scan2.setFilter(fl);
                countUnopt = 0;
                long start2 = System.currentTimeMillis();
                try (ResultScanner scanner = tableF.getScanner(scan2)) {
                    for (Result r : scanner) {
                        countUnopt++;
                    }
                }
                timesUnopt[i] = System.currentTimeMillis() - start2;
            }
        }

        long minOpt = Arrays.stream(timesOpt).min().getAsLong();
        long maxOpt = Arrays.stream(timesOpt).max().getAsLong();
        long minUnopt = Arrays.stream(timesUnopt).min().getAsLong();
        long maxUnopt = Arrays.stream(timesUnopt).max().getAsLong();

        long[] sortedOpt = timesOpt.clone();
        long[] sortedUnopt = timesUnopt.clone();
        Arrays.sort(sortedOpt);
        Arrays.sort(sortedUnopt);
        long medianOpt = sortedOpt[numRuns / 2];
        long medianUnopt = sortedUnopt[numRuns / 2];

        double minSpeedup = (double) minUnopt / Math.max(1, maxOpt);
        double medianSpeedup = (double) medianUnopt / Math.max(1, medianOpt);
        double maxSpeedup = (double) maxUnopt / Math.max(1, minOpt);

        System.out.println("\nPerformance Benchmark Results (1 Warm-up Discarded + 5 Timed Runs):");
        System.out.println(String.format("1. [Optimized Table] 'flights_by_airport' (Prefix Scan):"));
        System.out.println(String.format("   Runs: %s ms | Min: %d ms | Median: %d ms | Max: %d ms | Rows matched: %d",
                Arrays.toString(timesOpt), minOpt, medianOpt, maxOpt, countOpt));
        System.out.println(String.format("2. [Unoptimized Table] 'flights' (Full Scan + Column/Row Filters):"));
        System.out.println(String.format("   Runs: %s ms | Min: %d ms | Median: %d ms | Max: %d ms | Rows matched: %d",
                Arrays.toString(timesUnopt), minUnopt, medianUnopt, maxUnopt, countUnopt));
        System.out.println(String.format("--> Measured Speedup Range: %.2fx to %.2fx (Median: %.2fx faster, %d ms vs %d ms).",
                minSpeedup, maxSpeedup, medianSpeedup, medianOpt, medianUnopt));
        System.out.println("    Note: Results vary across test runs depending on OS cache state; the performance gap expands further as table volume grows.");
    }

    private static void printResult(Result res) {
        String rk = Bytes.toString(res.getRow());
        String airline = Bytes.toString(res.getValue(CF_INFO, Bytes.toBytes("airline_name")));
        String origin = Bytes.toString(res.getValue(CF_INFO, Bytes.toBytes("origin")));
        String dest = Bytes.toString(res.getValue(CF_INFO, Bytes.toBytes("dest")));
        String schedDep = Bytes.toString(res.getValue(CF_INFO, Bytes.toBytes("sched_dep")));
        String arrDelay = Bytes.toString(res.getValue(CF_DELAY, Bytes.toBytes("arr_delay")));
        String cancelled = Bytes.toString(res.getValue(CF_CANCEL, Bytes.toBytes("cancelled")));
        String reason = Bytes.toString(res.getValue(CF_CANCEL, Bytes.toBytes("reason")));

        System.out.println("  RowKey: " + rk);
        System.out.println("    Airline: " + (airline != null ? airline : "N/A"));
        System.out.println("    Route:   " + (origin != null ? origin : "?") + " -> " + (dest != null ? dest : "?"));
        System.out.println("    SchedDep:" + (schedDep != null ? schedDep : "N/A"));
        System.out.println("    ArrDelay:" + (arrDelay != null ? arrDelay + " min" : "None"));
        System.out.println("    Status:  " + ("1".equals(cancelled) ? "CANCELLED (Reason: " + reason + ")" : "OPERATED"));
    }

    public static void main(String[] args) {
        System.out.println("==================================================================");
        System.out.println("  FLIGHT DELAY INTELLIGENCE SYSTEM - APACHE HBASE JAVA CLIENT     ");
        System.out.println("  Course: 23CSE352 Big Data Analytics                             ");
        System.out.println("==================================================================");

        Configuration config = getHBaseConfig();
        try (Connection conn = ConnectionFactory.createConnection(config);
             Admin admin = conn.getAdmin()) {

            System.out.println("[INFO] Connected successfully to Apache HBase Cluster.");

            // 1. Create tables if missing
            createTablesIfNotExist(admin);

            String defaultCsv = new java.io.File("/root/data/flights_full.csv").exists() ? "/root/data/flights_full.csv" :
                               (new java.io.File("../data/flights_full.csv").exists() ? "../data/flights_full.csv" : "../data/flights_subset.csv");
            String bulkLoadPath = null;
            for (String arg : args) {
                if (arg.startsWith("--bulk-load=")) {
                    bulkLoadPath = arg.substring("--bulk-load=".length());
                    defaultCsv = bulkLoadPath;
                }
            }

            // 2. Insert sample flight from CSV via Put API
            String sampleKey = insertSampleFlight(conn, defaultCsv);

            // 3. Optional Bulk Load if requested
            if (bulkLoadPath != null) {
                bulkLoadCsv(conn, bulkLoadPath);
            }

            // 4. GET Operation on real row
            getFlightRecord(conn, sampleKey != null ? sampleKey : "AS#ANC#20150101#0005#00098#SEA");

            // 5. Scan with FilterList (PrefixFilter + SingleColumnValueFilter)
            scanFilteredFlights(conn);

            // 6. Demonstrate row-key design trade-off with 5 runs & median
            compareRowKeyTradeoff(conn, "ORD", "20150201");

            System.out.println("\n[INFO] All Java API operations completed successfully. Closing connection.");

        } catch (Exception e) {
            System.err.println("[ERROR] Java HBase execution failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
