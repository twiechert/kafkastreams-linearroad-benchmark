package de.twiechert.linroad.kafka;

import de.twiechert.linroad.jdriver.DataDriver;
import de.twiechert.linroad.jdriver.DataDriverLibrary;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.feeder.DataFeeder;
import de.twiechert.linroad.kafka.feeder.PositionReportHandler;
import de.twiechert.linroad.kafka.feeder.historical.HistoricalDataFeeder;
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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Properties;

/**
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@SpringBootApplication
@EnableAutoConfiguration
@EnableAsync
@Configuration
public class LinearRoadKafkaBenchmarkApplication {

    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(LinearRoadKafkaBenchmarkApplication.class);

    public static void main(String[] args) {

        SpringApplication.run(LinearRoadKafkaBenchmarkApplication.class, args);
    }

    @Component
    public static class BenchmarkRunner implements CommandLineRunner {

        @Autowired
        private Context context;

        @Autowired
        private HistoricalDataFeeder historicalDataFeeder;

        @Autowired
        private DataFeeder positionReporter;

        @Autowired
        private LatestAverageVelocityStreamBuilder latestAverageVelocityStreamBuilder;

        @Autowired
        private AccidentDetectionStreamBuilder accidentDetectionStreamBuilder;

        @Autowired
        private AccidentNotificationStreamBuilder accidentNotificationStreamBuilder;

        @Autowired
        private SegmentCrossingPositionReportBuilder segmentCrossingPositionReportBuilder;

        @Autowired
        private PositionReportStreamBuilder positionReportStreamBuilder;

        @Autowired
        private NumberOfVehiclesStreamBuilder numberOfVehiclesStreamBuilder;

        @Autowired
        private CurrentTollStreamBuilder currentTollStreamBuilder;

        @Autowired
        private TollNotificationStreamBuilder tollNotificationStreamBuilder;

        @Autowired
        private TollHistoryTableBuilder tollHistoryTableBuilder;

        @Autowired
        private CurrentExpenditurePerVehicleTableBuilder currentExpenditurePerVehicleTableBuilder;

        @Autowired
        private AccountBalanceRequestStreamBuilder accountBalanceStreamBuilder;

        @Autowired
        private AccountBalanceResponseStreamBuilder accountBalanceResponseStreamBuilder;

        @Autowired
        private DailyExpenditureRequestStreamBuilder dailyExpenditureRequestStreamBuilder;

        @Autowired
        private DailyExpenditureResponseStreamBuilder dailyExpenditureResponseStreamBuilder;

        @Override
        public void run(String... var1) throws Exception {
            logger.debug("Using mode {}", context.getLinearRoadMode());
            KStreamBuilder builder = new KStreamBuilder();
            context.setBuilder(builder);
            KTable<XwayVehicleIdDay, Double> tollHistoryTable = null;

            // historical info feeding
            if (!context.getLinearRoadMode().equals("no-historical-feed")) {
                tollHistoryTable = tollHistoryTableBuilder.getTable(builder);
                if (context.getDebugList().contains("TOLL_HIST")) tollHistoryTable.print();

            }

            // executing the actual benchmark
            if (!context.getLinearRoadMode().equals("no-benchmark")) {

                // a certain delay is required, because kafka streams will fail if reading from non-existent topic...

                logger.debug("Starting benchmark");
                tollHistoryTable = (tollHistoryTable == null) ? tollHistoryTableBuilder.getExistingTable(builder) : tollHistoryTable;

                /*
                  Converting position reports to processable Kafka stream
                 */
                KStream<XwaySegmentDirection, PositionReport> positionReportStream = positionReportStreamBuilder.getStream(builder);
                if (context.getDebugList().contains("POS_REP")) positionReportStream.print();

                /*
                  A reduced version of the position report stream, that only considers the first position report within a segment per vehicle.
                  This is required for both the accident and toll notification, which are only triggered, if a position report has no predecessor from the same segment.
                 */
                KStream<VehicleIdXwayDirection, SegmentCrossing> segmentCrossingPositionReportStream = segmentCrossingPositionReportBuilder.getStream(positionReportStream);
                if (context.getDebugList().contains("POS_SEG")) segmentCrossingPositionReportStream.print();

                /*
                  Converting account balance request to processable Kafka stream
                 */
                KStream<AccountBalanceRequest, Void> accountBalanceRequestStream = accountBalanceStreamBuilder.getStream(builder);
                if (context.getDebugList().contains("ACCB_REQ")) accountBalanceRequestStream.print();

                /*
                  Converting daily expenditure request to processable Kafka stream
                 */
                KStream<DailyExpenditureRequest, Void> dailyExpenditureRequestStream = dailyExpenditureRequestStreamBuilder.getStream(builder);
                if (context.getDebugList().contains("DEXP_REQ")) dailyExpenditureRequestStream.print();

                /*
                  Building NOV stream
                 */
                KStream<XwaySegmentDirection, NumberOfVehicles> numberOfVehiclesStream = numberOfVehiclesStreamBuilder.getStream(positionReportStream);
                if (context.getDebugList().contains("NOV")) numberOfVehiclesStream.print();

                /*
                  Building LAV stream
                 */
                KStream<XwaySegmentDirection, AverageVelocity> latestAverageVelocityStream = latestAverageVelocityStreamBuilder.getStream(positionReportStream);
                if (context.getDebugList().contains("LAV")) latestAverageVelocityStream.print();

                /*
                  Building Accident detection stream
                 */
                KStream<XwaySegmentDirection, Long> accidentDetectionStream = accidentDetectionStreamBuilder.getStream(positionReportStream);
                if (context.getDebugList().contains("ACC_DET")) accidentDetectionStream.print();

                /*
                  Building Accident notification stream
                 */
                KStream<Void, AccidentNotification> accidentNotificationStream = accidentNotificationStreamBuilder.getStream(segmentCrossingPositionReportStream, accidentDetectionStream);
                accidentNotificationStream.writeAsText("output/" + accidentNotificationStreamBuilder.getOutputTopic() + ".csv", accidentNotificationStreamBuilder.getKeySerde(), accidentNotificationStreamBuilder.getValueSerde());

                /*
                  Building current toll per Xway-Segmen-Directon tuple stream
                 */
                KStream<XwaySegmentDirection, CurrentToll> currentTollStream = currentTollStreamBuilder.getStream(latestAverageVelocityStream, numberOfVehiclesStream, accidentDetectionStream);
                if (context.getDebugList().contains("CURR_TOLL")) currentTollStream.print();

                /*
                  Building stream to notify driver about tolls
                 */
                KStream<Void, TollNotification> tollNotificationStream = tollNotificationStreamBuilder.getStream(segmentCrossingPositionReportStream, currentTollStream);
                tollNotificationStream.writeAsText("output/" + tollNotificationStreamBuilder.getOutputTopic() + ".csv", tollNotificationStreamBuilder.getKeySerde(), tollNotificationStreamBuilder.getValueSerde());


                /*
                  Creating tables to retain the latest state about tolls
                  (a) recent toll per vehicle
                  (b) toll per vehicle, per day, per expressway
                 */
                // this table may be derived from the above (how to realize in Kafka streams?)
                KTable<Integer, ExpenditureAt> tollPerVehicleTable = currentExpenditurePerVehicleTableBuilder.getStream(segmentCrossingPositionReportStream, currentTollStream);
                if (context.getDebugList().contains("CURR_TOLL_TAB")) tollPerVehicleTable.print();

                /*
                  Building stream to answer account balance requests
                 */
                KStream<Void, AccountBalanceResponse> accountBalanceResponseStream = accountBalanceResponseStreamBuilder.getStream(accountBalanceRequestStream, tollPerVehicleTable);
                accountBalanceResponseStream.writeAsText("output/" + accountBalanceResponseStreamBuilder.getOutputTopic() + ".csv", accountBalanceResponseStreamBuilder.getKeySerde(), accountBalanceResponseStreamBuilder.getValueSerde());

                /*
                  Building stream to answer daily expenditure requests
                 */
                KStream<Void, DailyExpenditureResponse> dailyExpenditureResponseStream = dailyExpenditureResponseStreamBuilder.getStream(dailyExpenditureRequestStream, tollHistoryTable);
                dailyExpenditureResponseStream.writeAsText("output/" + dailyExpenditureResponseStreamBuilder.getOutputTopic() + ".csv", dailyExpenditureResponseStreamBuilder.getKeySerde(), dailyExpenditureResponseStreamBuilder.getValueSerde());

            }


            KafkaStreams kafkaStreams = new KafkaStreams(builder, context.getStreamBaseConfig());
            kafkaStreams.start();

            if (!context.getLinearRoadMode().equals("no-historical-feed")) {
                logger.debug("Start feeding with historical data");

                // must be synchronous -> actual benchmark must not begin before
                historicalDataFeeder.startFeeding();
            }

            if (!context.getLinearRoadMode().equals("no-benchmark")) {
                logger.debug("Start feeding of tuples");
                positionReporter.startFeeding();
                logger.debug("Feeding Finished.");

            }
        }
    }

    @Component
    public static class Context {

        private final RandomStringGenerator generator = new RandomStringGenerator();

        private final Properties streamBaseConfig = new Properties();

        private final Properties producerBaseConfig = new Properties();

        private KStreamBuilder builder;

        @Value("${linearroad.hisotical.data.path}")
        private String historicalFilePath;

        @Value("${linearroad.data.path}")
        private String filePath;

        @Value("#{'${linearroad.mode.debug}'.split(',')}")
        private List<String> debugMode;

        @Value("${linearroad.datadriver.path}")
        private String dataDriverPath;

        @Value("${linearroad.kafka.bootstrapservers}")
        private String bootstrapServers;

        @Value("${linearroad.zookeeper.server}")
        private String zookeeperServer;

        @Value("${spring.profiles.active}")
        private String env;

        @Value("${linearroad.mode}")
        private String linearRoadMode;

        @Value("${linearroad.kafka.num_stream_threads}")
        private int numberOfThreads;

        private final String applicationId = generator.generateByRegex("[0-9a-z]{3}");
        private static DateTime benchmarkStartedAt = DateTime.now(); // will be overriden, when first element is written to topic ....

        public Context() {
        }

        public String topic(String topic) {
            return topic;//+"-"+applicationId;
        }

        @PostConstruct
        private void initializeBaseConfig() {
            logger.debug("Configured kafka servers are {} and zookeeper {}", bootstrapServers, zookeeperServer);
            logger.debug("Application Id is {} and running in env {}", applicationId, env);
            streamBaseConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "linearroad-benchmark-" + this.getApplicationId());
            streamBaseConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            streamBaseConfig.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperServer);
            streamBaseConfig.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, PositionReportHandler.TimeStampExtractor.class.getName());
            streamBaseConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, (numberOfThreads < 1) ? Runtime.getRuntime().availableProcessors() : numberOfThreads);
            // streamBaseConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 0);

            producerBaseConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            producerBaseConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            producerBaseConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
            //producerBaseConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, 0); //16384);

            //producerBaseConfig.put(ProducerConfig.LINGER_MS_CONFIG, 1);
            producerBaseConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        }

        public Properties getStreamBaseConfig() {
            return streamBaseConfig;
        }

        public Properties getProducerBaseConfig() {
            return producerBaseConfig;
        }

        public static long getCurrentRuntimeInSeconds() {
            return Seconds.secondsBetween(benchmarkStartedAt, DateTime.now()).getSeconds();
        }

        public static void markAsStarted() {
            benchmarkStartedAt = DateTime.now();
        }

        public String getFilePath() {
            return filePath;
        }

        public String getApplicationId() {
            return applicationId;
        }

        public String getHistoricalFilePath() {
            return historicalFilePath;
        }

        public String getLinearRoadMode() {
            return linearRoadMode;
        }

        public String getDataDriverPath() {
            return dataDriverPath;
        }

        public List<String> getDebugList() {
            return debugMode;
        }

        public KStreamBuilder getBuilder() {
            return builder;
        }

        public void setBuilder(KStreamBuilder builder) {
            this.builder = builder;
        }
    }


    @Bean
    public DataDriverLibrary getDataDriverLibrary(DataDriver dataDriver) {
        return dataDriver.getLibrary();
    }


    @Bean
    public DataDriver getDataDriver(Context context) {
        logger.debug("Path to file is {}", context.getFilePath());
        return new DataDriver(context.getFilePath(), context.getDataDriverPath(), DataDriver.Architecture.X64_LINUX);
    }


}
