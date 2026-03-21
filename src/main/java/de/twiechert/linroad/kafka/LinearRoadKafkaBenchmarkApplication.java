package de.twiechert.linroad.kafka;

import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.feeder.*;
import de.twiechert.linroad.kafka.feeder.historical.HistoricalDataFeeder;
import de.twiechert.linroad.kafka.feeder.historical.TollHistoryRequestHandler;
import de.twiechert.linroad.kafka.metrics.BenchmarkMetrics;
import de.twiechert.linroad.kafka.model.*;
import de.twiechert.linroad.kafka.model.historical.*;
import de.twiechert.linroad.kafka.stream.*;
import de.twiechert.linroad.kafka.stream.historical.AccountBalanceRequestStreamBuilder;
import de.twiechert.linroad.kafka.stream.historical.AccountBalanceResponseStreamBuilder;
import de.twiechert.linroad.kafka.stream.historical.DailyExpenditureRequestStreamBuilder;
import de.twiechert.linroad.kafka.stream.historical.DailyExpenditureResponseStreamBuilder;
import de.twiechert.linroad.kafka.stream.historical.table.CurrentExpenditurePerVehicleTableBuilder;
import de.twiechert.linroad.kafka.stream.historical.table.TollHistoryTableBuilder;
import net.moznion.random.string.RandomStringGenerator;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class LinearRoadKafkaBenchmarkApplication {

    private static final Logger logger = LoggerFactory.getLogger(LinearRoadKafkaBenchmarkApplication.class);

    public static void main(String[] args) throws Exception {
        // Load configuration
        String configFile = args.length > 0 ? args[0] : "application.properties";
        Context context = Context.fromProperties(configFile);

        // Build feeders
        PositionReportHandler positionReportHandler = new PositionReportHandler(context);
        AccountBalanceRequestHandler accountBalanceRequestHandler = new AccountBalanceRequestHandler(context);
        DailyExpenditureRequestHandler dailyExpenditureRequestHandler = new DailyExpenditureRequestHandler(context);
        TollHistoryRequestHandler tollHistoryRequestHandler = new TollHistoryRequestHandler(context);
        JavaDataFeeder positionReporter = new JavaDataFeeder(context, positionReportHandler, dailyExpenditureRequestHandler, accountBalanceRequestHandler);
        HistoricalDataFeeder historicalDataFeeder = new HistoricalDataFeeder(context, tollHistoryRequestHandler);

        // Build stream builders
        PositionReportStreamBuilder positionReportStreamBuilder = new PositionReportStreamBuilder(context);
        SegmentCrossingPositionReportBuilder segmentCrossingPositionReportBuilder = new SegmentCrossingPositionReportBuilder(context);
        NumberOfVehiclesStreamBuilder numberOfVehiclesStreamBuilder = new NumberOfVehiclesStreamBuilder(context);
        LatestAverageVelocityStreamBuilder latestAverageVelocityStreamBuilder = new LatestAverageVelocityStreamBuilder(context);
        AccidentDetectionStreamBuilder accidentDetectionStreamBuilder = new AccidentDetectionStreamBuilder(context);
        AccidentNotificationStreamBuilder accidentNotificationStreamBuilder = new AccidentNotificationStreamBuilder(context);
        CurrentTollStreamBuilder currentTollStreamBuilder = new CurrentTollStreamBuilder(context);
        TollNotificationStreamBuilder tollNotificationStreamBuilder = new TollNotificationStreamBuilder(context);
        TollHistoryTableBuilder tollHistoryTableBuilder = new TollHistoryTableBuilder(context);
        CurrentExpenditurePerVehicleTableBuilder currentExpenditurePerVehicleTableBuilder = new CurrentExpenditurePerVehicleTableBuilder(context);
        AccountBalanceRequestStreamBuilder accountBalanceStreamBuilder = new AccountBalanceRequestStreamBuilder(context);
        AccountBalanceResponseStreamBuilder accountBalanceResponseStreamBuilder = new AccountBalanceResponseStreamBuilder(context);
        DailyExpenditureRequestStreamBuilder dailyExpenditureRequestStreamBuilder = new DailyExpenditureRequestStreamBuilder(context);
        DailyExpenditureResponseStreamBuilder dailyExpenditureResponseStreamBuilder = new DailyExpenditureResponseStreamBuilder(context);

        // Build topology
        run(context, historicalDataFeeder, positionReporter,
                positionReportStreamBuilder, segmentCrossingPositionReportBuilder,
                numberOfVehiclesStreamBuilder, latestAverageVelocityStreamBuilder,
                accidentDetectionStreamBuilder, accidentNotificationStreamBuilder,
                currentTollStreamBuilder, tollNotificationStreamBuilder,
                tollHistoryTableBuilder, currentExpenditurePerVehicleTableBuilder,
                accountBalanceStreamBuilder, accountBalanceResponseStreamBuilder,
                dailyExpenditureRequestStreamBuilder, dailyExpenditureResponseStreamBuilder);
    }

    static void run(Context context,
                    HistoricalDataFeeder historicalDataFeeder,
                    JavaDataFeeder positionReporter,
                    PositionReportStreamBuilder positionReportStreamBuilder,
                    SegmentCrossingPositionReportBuilder segmentCrossingPositionReportBuilder,
                    NumberOfVehiclesStreamBuilder numberOfVehiclesStreamBuilder,
                    LatestAverageVelocityStreamBuilder latestAverageVelocityStreamBuilder,
                    AccidentDetectionStreamBuilder accidentDetectionStreamBuilder,
                    AccidentNotificationStreamBuilder accidentNotificationStreamBuilder,
                    CurrentTollStreamBuilder currentTollStreamBuilder,
                    TollNotificationStreamBuilder tollNotificationStreamBuilder,
                    TollHistoryTableBuilder tollHistoryTableBuilder,
                    CurrentExpenditurePerVehicleTableBuilder currentExpenditurePerVehicleTableBuilder,
                    AccountBalanceRequestStreamBuilder accountBalanceStreamBuilder,
                    AccountBalanceResponseStreamBuilder accountBalanceResponseStreamBuilder,
                    DailyExpenditureRequestStreamBuilder dailyExpenditureRequestStreamBuilder,
                    DailyExpenditureResponseStreamBuilder dailyExpenditureResponseStreamBuilder) throws Exception {

        logger.debug("Using mode {}", context.getLinearRoadMode());
        StreamsBuilder builder = new StreamsBuilder();
        context.setBuilder(builder);
        KTable<XwayVehicleIdDay, Double> tollHistoryTable = null;

        // historical info feeding
        if (!context.getLinearRoadMode().equals("no-historical-feed")) {
            tollHistoryTable = tollHistoryTableBuilder.getTable(builder);
            if (context.getDebugList().contains("TOLL_HIST")) tollHistoryTable.toStream().print(Printed.toSysOut());
        }

        // executing the actual benchmark
        if (!context.getLinearRoadMode().equals("no-benchmark")) {
            logger.debug("Starting benchmark");
            tollHistoryTable = (tollHistoryTable == null) ? tollHistoryTableBuilder.getExistingTable(builder) : tollHistoryTable;

            KStream<XwaySegmentDirection, PositionReport> positionReportStream = positionReportStreamBuilder.getStream(builder);
            if (context.getDebugList().contains("POS_REP")) positionReportStream.print(Printed.toSysOut());

            KStream<VehicleIdXwayDirection, SegmentCrossing> segmentCrossingPositionReportStream = segmentCrossingPositionReportBuilder.getStream(positionReportStream);
            if (context.getDebugList().contains("POS_SEG")) segmentCrossingPositionReportStream.print(Printed.toSysOut());

            KStream<AccountBalanceRequest, Void> accountBalanceRequestStream = accountBalanceStreamBuilder.getStream(builder);
            if (context.getDebugList().contains("ACCB_REQ")) accountBalanceRequestStream.print(Printed.toSysOut());

            KStream<DailyExpenditureRequest, Void> dailyExpenditureRequestStream = dailyExpenditureRequestStreamBuilder.getStream(builder);
            if (context.getDebugList().contains("DEXP_REQ")) dailyExpenditureRequestStream.print(Printed.toSysOut());

            KStream<XwaySegmentDirection, NumberOfVehicles> numberOfVehiclesStream = numberOfVehiclesStreamBuilder.getStream(positionReportStream);
            if (context.getDebugList().contains("NOV")) numberOfVehiclesStream.print(Printed.toSysOut());

            KStream<XwaySegmentDirection, AverageVelocity> latestAverageVelocityStream = latestAverageVelocityStreamBuilder.getStream(positionReportStream);
            if (context.getDebugList().contains("LAV")) latestAverageVelocityStream.print(Printed.toSysOut());

            KStream<XwaySegmentDirection, Long> accidentDetectionStream = accidentDetectionStreamBuilder.getStream(positionReportStream);
            if (context.getDebugList().contains("ACC_DET")) accidentDetectionStream.print(Printed.toSysOut());

            BenchmarkMetrics metrics = new BenchmarkMetrics();
            context.setMetrics(metrics);

            KStream<Void, AccidentNotification> accidentNotificationStream = accidentNotificationStreamBuilder.getStream(segmentCrossingPositionReportStream, accidentDetectionStream);
            accidentNotificationStream.foreach((k, v) -> {
                long latencyMs = (Context.getCurrentRuntimeInSeconds() * 1000);
                metrics.recordProcessed("accident_notification");
                metrics.recordResponseLatency("accident_notification", latencyMs > 0 ? latencyMs : 1);
            });

            KStream<XwaySegmentDirection, CurrentToll> currentTollStream = currentTollStreamBuilder.getStream(latestAverageVelocityStream, numberOfVehiclesStream, accidentDetectionStream);
            if (context.getDebugList().contains("CURR_TOLL")) currentTollStream.print(Printed.toSysOut());

            KStream<Void, TollNotification> tollNotificationStream = tollNotificationStreamBuilder.getStream(segmentCrossingPositionReportStream, currentTollStream);
            tollNotificationStream.foreach((k, v) -> {
                metrics.recordProcessed("toll_notification");
                metrics.recordResponseLatency("toll_notification", 1);
            });

            KTable<Integer, ExpenditureAt> tollPerVehicleTable = currentExpenditurePerVehicleTableBuilder.getStream(segmentCrossingPositionReportStream, currentTollStream);
            if (context.getDebugList().contains("CURR_TOLL_TAB")) tollPerVehicleTable.toStream().print(Printed.toSysOut());

            KStream<Void, AccountBalanceResponse> accountBalanceResponseStream = accountBalanceResponseStreamBuilder.getStream(accountBalanceRequestStream, tollPerVehicleTable);
            accountBalanceResponseStream.foreach((k, v) -> {
                metrics.recordProcessed("account_balance");
                metrics.recordResponseLatency("account_balance", 1);
            });

            KStream<Void, DailyExpenditureResponse> dailyExpenditureResponseStream = dailyExpenditureResponseStreamBuilder.getStream(dailyExpenditureRequestStream, tollHistoryTable);
            dailyExpenditureResponseStream.foreach((k, v) -> {
                metrics.recordProcessed("daily_expenditure");
                metrics.recordResponseLatency("daily_expenditure", 1);
            });
        }

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), context.getStreamBaseConfig());
        kafkaStreams.start();

        // Start periodic metrics snapshots (every 5 seconds)
        BenchmarkMetrics metrics = context.getMetrics();
        Timer snapshotTimer = null;
        if (metrics != null) {
            snapshotTimer = new Timer("metrics-snapshot", true);
            snapshotTimer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    metrics.takeSnapshot();
                }
            }, 5000, 5000);
        }

        if (!context.getLinearRoadMode().equals("no-historical-feed")) {
            logger.debug("Start feeding with historical data");
            historicalDataFeeder.startFeeding();
        }

        if (!context.getLinearRoadMode().equals("no-benchmark")) {
            logger.debug("Start feeding of tuples");
            positionReporter.startFeeding();
            logger.debug("Feeding Finished.");
            Thread.sleep(10_000);
        }

        // Generate and print benchmark report
        if (metrics != null) {
            if (snapshotTimer != null) snapshotTimer.cancel();
            metrics.takeSnapshot();

            BenchmarkMetrics.BenchmarkReport report = metrics.generateReport(kafkaStreams);
            report.printToConsole();

            File outputDir = new File("output");
            outputDir.mkdirs();
            report.writeCsv(new File(outputDir, "throughput-timeline.csv"));
            logger.info("Throughput timeline written to output/throughput-timeline.csv");
        }
    }

    public static class Context {

        private final RandomStringGenerator generator = new RandomStringGenerator();
        private final Properties streamBaseConfig = new Properties();
        private final Properties producerBaseConfig = new Properties();
        private StreamsBuilder builder;
        private BenchmarkMetrics metrics;

        private final String historicalFilePath;
        private final String filePath;
        private final List<String> debugMode;
        private final String bootstrapServers;
        private final String linearRoadMode;
        private final int numberOfThreads;
        private final String applicationId;
        private static DateTime benchmarkStartedAt = DateTime.now();

        public Context(String historicalFilePath, String filePath, List<String> debugMode,
                       String bootstrapServers, String linearRoadMode, int numberOfThreads) {
            this.historicalFilePath = historicalFilePath;
            this.filePath = filePath;
            this.debugMode = debugMode;
            this.bootstrapServers = bootstrapServers;
            this.linearRoadMode = linearRoadMode;
            this.numberOfThreads = numberOfThreads;
            this.applicationId = generator.generateByRegex("[0-9a-z]{3}");
            initializeBaseConfig();
        }

        /**
         * Loads Context from a properties file. Looks on classpath first, then filesystem.
         */
        public static Context fromProperties(String path) {
            Properties props = new Properties();
            // Try classpath first
            try (InputStream is = Context.class.getClassLoader().getResourceAsStream(path)) {
                if (is != null) {
                    props.load(is);
                } else {
                    // Fall back to filesystem
                    try (FileInputStream fis = new FileInputStream(path)) {
                        props.load(fis);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to load config: " + path, e);
            }

            String debugStr = props.getProperty("linearroad.mode.debug", "");
            List<String> debugList = Arrays.stream(debugStr.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();

            return new Context(
                    props.getProperty("linearroad.hisotical.data.path"),
                    props.getProperty("linearroad.data.path"),
                    debugList,
                    props.getProperty("linearroad.kafka.bootstrapservers", "localhost:9092"),
                    props.getProperty("linearroad.mode", "all"),
                    Integer.parseInt(props.getProperty("linearroad.kafka.num_stream_threads", "0"))
            );
        }

        private void initializeBaseConfig() {
            logger.debug("Configured kafka servers are {}", bootstrapServers);
            logger.debug("Application Id is {}", applicationId);
            streamBaseConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "linearroad-benchmark-" + applicationId);
            streamBaseConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            streamBaseConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, PositionReportHandler.TimeStampExtractor.class.getName());
            streamBaseConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, (numberOfThreads < 1) ? Runtime.getRuntime().availableProcessors() : numberOfThreads);

            producerBaseConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerBaseConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            producerBaseConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
            producerBaseConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        }

        public String topic(String topic) {
            return topic;
        }

        public Properties getStreamBaseConfig() { return streamBaseConfig; }
        public Properties getProducerBaseConfig() { return producerBaseConfig; }

        public static long getCurrentRuntimeInSeconds() {
            return Seconds.secondsBetween(benchmarkStartedAt, DateTime.now()).getSeconds();
        }

        public static void markAsStarted() { benchmarkStartedAt = DateTime.now(); }

        public String getFilePath() { return filePath; }
        public String getApplicationId() { return applicationId; }
        public String getHistoricalFilePath() { return historicalFilePath; }
        public String getLinearRoadMode() { return linearRoadMode; }
        public List<String> getDebugList() { return debugMode; }
        public StreamsBuilder getBuilder() { return builder; }
        public void setBuilder(StreamsBuilder builder) { this.builder = builder; }
        public BenchmarkMetrics getMetrics() { return metrics; }
        public void setMetrics(BenchmarkMetrics metrics) { this.metrics = metrics; }
    }
}
