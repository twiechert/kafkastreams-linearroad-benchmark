package de.twiechert.linroad.kafka.benchmark;

import java.io.*;
import java.util.*;

/**
 * Lightweight Linear Road data generator for testing and benchmarking.
 * Generates valid position reports, account balance requests, and daily expenditure requests
 * following the Linear Road benchmark format.
 *
 * Vehicles are clustered into a small range of segments to ensure toll and accident
 * conditions are triggered:
 * - Tolls require >50 distinct vehicles per (xway, segment, direction) in a 1-min window
 * - Accidents require 2+ vehicles stopped (speed=0) at the same position for 4 consecutive reports
 */
public class LinearRoadDataGenerator {

    private static final int SEGMENTS_PER_XWAY = 100;
    private static final int LANES = 3; // lanes 0-2 (lane 4 = exit)
    private static final Random random = new Random(42);

    /**
     * Generates a Linear Road data file with realistic traffic patterns.
     */
    public static void generateDataFile(File outputFile, int numXways, int durationMinutes, int vehiclesPerXway) throws IOException {
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)))) {
            int totalSeconds = durationMinutes * 60;
            int queryId = 1000;

            // Each vehicle has persistent state: current segment, direction, lane
            int totalVehicles = numXways * vehiclesPerXway;
            int[] vehSegment = new int[totalVehicles];
            boolean[] vehDirection = new boolean[totalVehicles];
            int[] vehLane = new int[totalVehicles];
            int[] vehSpeed = new int[totalVehicles];
            int[] vehPosition = new int[totalVehicles]; // fixed position within segment

            // Initialize: cluster vehicles into 3 segments for high density
            // With 500 veh/xway, 2 dirs, 3 segments → ~83 per (seg, dir) — well above the >50 toll threshold
            int CLUSTER_START = 15;
            int CLUSTER_SIZE = 3;
            // Accident positions: a few fixed positions per segment so stopped vehicles collide
            int[] accidentPositions = {2640, 2641, 2642};

            for (int i = 0; i < totalVehicles; i++) {
                vehSegment[i] = CLUSTER_START + (i % CLUSTER_SIZE);
                vehDirection[i] = i % 2 == 0;
                vehLane[i] = i % LANES;
                vehSpeed[i] = 20 + random.nextInt(20); // 20-39 mph (all below 40 → tolls apply)
                vehPosition[i] = vehSegment[i] * 5280 + accidentPositions[i % accidentPositions.length];
            }

            for (int t = 0; t < totalSeconds; t += 30) {
                for (int xway = 0; xway < numXways; xway++) {
                    for (int vid = 0; vid < vehiclesPerXway; vid++) {
                        int globalVid = xway * 100_000 + vid;
                        int idx = xway * vehiclesPerXway + vid;

                        // ~15% of vehicles stop (for accident detection)
                        // Stopped vehicles keep their fixed position so they collide
                        if (random.nextDouble() < 0.15) {
                            vehSpeed[idx] = 0;
                        } else {
                            vehSpeed[idx] = 20 + random.nextInt(20); // 20-39 mph (below 40 threshold)
                            // Some vehicles advance segments over time
                            if (t > 0 && random.nextDouble() < 0.08) {
                                vehSegment[idx] = Math.min(vehSegment[idx] + 1, SEGMENTS_PER_XWAY - 1);
                                vehPosition[idx] = vehSegment[idx] * 5280 + accidentPositions[idx % accidentPositions.length];
                            }
                        }

                        int position = vehPosition[idx];

                        // Type 0: Position report
                        writer.printf("0,%d,%d,%d,%d,%d,%s,%d,%d,0,0,0,0,0,0%n",
                                t, globalVid, vehSpeed[idx], xway, vehLane[idx],
                                vehDirection[idx] ? "0" : "1", vehSegment[idx], position);
                    }
                }

                // Type 2 (account balance) and Type 3 (daily expenditure) queries every minute
                if (t > 0 && t % 60 == 0) {
                    for (int q = 0; q < numXways; q++) {
                        int queryVehicle = random.nextInt(numXways) * 100_000 + random.nextInt(vehiclesPerXway);
                        writer.printf("2,%d,0,%d,0,0,0,0,0,%d,0,0,0,0,0%n",
                                t, queryVehicle, queryId++);

                        int queryXway = random.nextInt(numXways);
                        int day = 1 + random.nextInt(5);
                        writer.printf("3,%d,%d,0,%d,0,0,0,0,%d,0,0,0,0,%d%n",
                                t, queryVehicle, queryXway, queryId++, day);
                    }
                }
            }
        }
    }

    /**
     * Generates a historical toll data file.
     */
    public static void generateTollHistoryFile(File outputFile, int numXways, int vehiclesPerXway, int numDays) throws IOException {
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)))) {
            for (int xway = 0; xway < numXways; xway++) {
                for (int vid = 0; vid < vehiclesPerXway; vid++) {
                    int vehicleId = xway * 100_000 + vid;
                    for (int day = 1; day <= numDays; day++) {
                        double toll = 1.0 + random.nextDouble() * 10.0;
                        writer.printf("%d,%d,%d,%.2f%n", vehicleId, day, xway, toll);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int numXways = 1;
        int durationMinutes = 3;
        int vehiclesPerXway = 200;
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
