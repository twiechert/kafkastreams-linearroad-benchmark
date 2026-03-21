package de.twiechert.linroad.kafka.benchmark;

import java.io.*;
import java.util.*;

/**
 * Lightweight Linear Road data generator for testing and CI benchmarking.
 * Generates valid position reports, account balance requests, and daily expenditure requests
 * following the Linear Road benchmark format.
 *
 * Unlike the WalmartLabs LinearGenerator (which produces ~1GB/xway), this generator creates
 * compact datasets suitable for automated testing while preserving correct benchmark semantics.
 */
public class LinearRoadDataGenerator {

    private static final int REPORTS_PER_MINUTE = 2; // 30s interval
    private static final int SEGMENTS_PER_XWAY = 100;
    private static final int LANES = 3; // lanes 0-2, plus exit lane 4
    private static final Random random = new Random(42); // fixed seed for reproducibility

    /**
     * Generates a Linear Road data file.
     *
     * @param outputFile     path to write the .dat file
     * @param numXways       number of expressways (L-rating target)
     * @param durationMinutes how many minutes of simulated data to generate
     * @param vehiclesPerXway number of vehicles per expressway
     */
    public static void generateDataFile(File outputFile, int numXways, int durationMinutes, int vehiclesPerXway) throws IOException {
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)))) {
            int totalSeconds = durationMinutes * 60;
            int queryId = 1000;

            for (int t = 0; t < totalSeconds; t += 30) {
                for (int xway = 0; xway < numXways; xway++) {
                    for (int vid = 0; vid < vehiclesPerXway; vid++) {
                        int vehicleId = xway * 1000 + vid;
                        int segment = (vid + t / 30) % SEGMENTS_PER_XWAY;
                        int speed = 30 + random.nextInt(50); // 30-79 mph
                        int lane = random.nextInt(LANES);
                        boolean direction = random.nextBoolean();
                        int position = segment * 5280 + random.nextInt(5280); // feet within segment

                        // Type 0: Position report
                        // Format: type,time,vehicleId,speed,xway,lane,dir,seg,pos,0,0,0,0,0,0
                        writer.printf("0,%d,%d,%d,%d,%d,%s,%d,%d,0,0,0,0,0,0%n",
                                t, vehicleId, speed, xway, lane,
                                direction ? "0" : "1", segment, position);
                    }
                }

                // Sprinkle in some type 2 (account balance) and type 3 (daily expenditure) queries
                if (t > 0 && t % 60 == 0) {
                    int queryVehicle = random.nextInt(numXways * vehiclesPerXway);
                    // Type 2: account balance request
                    // Format: type,time,0,vehicleId,0,0,0,0,0,queryId,0,0,0,0,0
                    writer.printf("2,%d,0,%d,0,0,0,0,0,%d,0,0,0,0,0%n",
                            t, queryVehicle, queryId++);

                    // Type 3: daily expenditure request
                    // Format: type,time,vehicleId,0,xway,0,0,0,0,queryId,0,0,0,0,day
                    int queryXway = random.nextInt(numXways);
                    int day = 1 + random.nextInt(5);
                    writer.printf("3,%d,%d,0,%d,0,0,0,0,%d,0,0,0,0,%d%n",
                            t, queryVehicle, queryXway, queryId++, day);
                }
            }
        }
    }

    /**
     * Generates a historical toll data file.
     *
     * @param outputFile      path to write the tolls .dat file
     * @param numXways        number of expressways
     * @param vehiclesPerXway number of vehicles per expressway
     * @param numDays         number of historical days
     */
    public static void generateTollHistoryFile(File outputFile, int numXways, int vehiclesPerXway, int numDays) throws IOException {
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)))) {
            for (int xway = 0; xway < numXways; xway++) {
                for (int vid = 0; vid < vehiclesPerXway; vid++) {
                    int vehicleId = xway * 1000 + vid;
                    for (int day = 1; day <= numDays; day++) {
                        double toll = 1.0 + random.nextDouble() * 10.0;
                        // Format: vehicleId,day,xway,toll
                        writer.printf("%d,%d,%d,%.2f%n", vehicleId, day, xway, toll);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int numXways = 1;
        int durationMinutes = 3;
        int vehiclesPerXway = 20;
        String outputPath = "test-data.dat";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-x": numXways = Integer.parseInt(args[++i]); break;
                case "-d": durationMinutes = Integer.parseInt(args[++i]); break;
                case "-v": vehiclesPerXway = Integer.parseInt(args[++i]); break;
                case "-o": outputPath = args[++i]; break;
            }
        }

        File dataFile = new File(outputPath);
        File tollFile = new File(outputPath + ".tolls.dat");

        System.out.printf("Generating L=%d data: %d minutes, %d vehicles/xway%n",
                numXways, durationMinutes, vehiclesPerXway);

        generateDataFile(dataFile, numXways, durationMinutes, vehiclesPerXway);
        generateTollHistoryFile(tollFile, numXways, vehiclesPerXway, 5);

        System.out.printf("Generated: %s (%d KB), %s (%d KB)%n",
                dataFile.getName(), dataFile.length() / 1024,
                tollFile.getName(), tollFile.length() / 1024);
    }
}
