package de.twiechert.linroad.kafka.stream;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that the sample data file can be parsed correctly by a pure Java reader,
 * validating the data format that JavaDataFeeder will consume.
 */
class JavaDataFeederTest {

    @Test
    void sampleDataFile_canBeParsed() throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream("sample.dat");
        assertNotNull(is, "sample.dat should be on classpath");

        List<String[]> positionReports = new ArrayList<>();
        List<String[]> accountBalanceRequests = new ArrayList<>();
        List<String[]> dailyExpenditureRequests = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] tuple = line.split(",");
                int type = Integer.parseInt(tuple[0].trim());
                switch (type) {
                    case 0:
                        positionReports.add(tuple);
                        break;
                    case 2:
                        accountBalanceRequests.add(tuple);
                        break;
                    case 3:
                        dailyExpenditureRequests.add(tuple);
                        break;
                }
            }
        }

        assertEquals(15, positionReports.size(), "Should have 15 position reports");
        assertEquals(1, accountBalanceRequests.size(), "Should have 1 account balance request");
        assertEquals(1, dailyExpenditureRequests.size(), "Should have 1 daily expenditure request");

        // Verify first position report fields
        String[] first = positionReports.get(0);
        assertEquals("0", first[0].trim()); // type
        assertEquals("0", first[1].trim()); // time
        assertEquals("100", first[2].trim()); // vehicleId
        assertEquals("60", first[3].trim()); // speed
        assertEquals("1", first[4].trim()); // xway
        assertEquals("1", first[5].trim()); // lane
        assertEquals("0", first[6].trim()); // direction
        assertEquals("5", first[7].trim()); // segment
        assertEquals("500", first[8].trim()); // position
    }

    @Test
    void sampleTollHistoryFile_canBeParsed() throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream("sample.dat.tolls.dat");
        assertNotNull(is, "sample.dat.tolls.dat should be on classpath");

        List<String[]> tolls = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                tolls.add(line.split(","));
            }
        }

        assertEquals(5, tolls.size(), "Should have 5 toll history entries");

        // Verify first toll entry: vehicleId,day,xway,toll
        String[] first = tolls.get(0);
        assertEquals("100", first[0].trim()); // vehicleId
        assertEquals("1", first[1].trim()); // day
        assertEquals("1", first[2].trim()); // xway
        assertEquals("5.50", first[3].trim()); // toll amount
    }

    @Test
    void positionReports_spanMultipleMinutes() throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream("sample.dat");
        assertNotNull(is);

        List<Long> timestamps = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] tuple = line.split(",");
                if ("0".equals(tuple[0].trim())) {
                    timestamps.add(Long.parseLong(tuple[1].trim()));
                }
            }
        }

        long minTime = timestamps.stream().mapToLong(Long::longValue).min().orElse(0);
        long maxTime = timestamps.stream().mapToLong(Long::longValue).max().orElse(0);

        assertEquals(0, minTime, "First timestamp should be 0");
        assertEquals(120, maxTime, "Last timestamp should be 120 (spans 3 minutes)");

        // Check distinct vehicles
        long distinctVehicles = 3; // vehicles 100, 200, 300
        assertEquals(5, timestamps.stream().distinct().count(), "Should have 5 distinct timestamps (0,30,60,90,120)");
    }
}
