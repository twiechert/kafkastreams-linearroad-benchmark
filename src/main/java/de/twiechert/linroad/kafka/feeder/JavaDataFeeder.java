package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

/**
 * Pure Java replacement for the native C DataDriver-based DataFeeder.
 * Reads .dat files line by line and routes tuples to the appropriate handlers
 * based on the tuple type (field 0): 0=position report, 2=account balance, 3=daily expenditure.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class JavaDataFeeder {

    private static final Logger logger = LoggerFactory.getLogger(JavaDataFeeder.class);

    private final String filePath;
    private final PositionReportHandler positionReportHandler;
    private final DailyExpenditureRequestHandler dailyExpenditureRequestHandler;
    private final AccountBalanceRequestHandler accountBalanceRequestHandler;

    public JavaDataFeeder(LinearRoadKafkaBenchmarkApplication.Context context,
                          PositionReportHandler positionReportHandler,
                          DailyExpenditureRequestHandler dailyExpenditureRequestHandler,
                          AccountBalanceRequestHandler accountBalanceRequestHandler) {
        this.filePath = context.getFilePath();
        this.positionReportHandler = positionReportHandler;
        this.dailyExpenditureRequestHandler = dailyExpenditureRequestHandler;
        this.accountBalanceRequestHandler = accountBalanceRequestHandler;
    }

    /**
     * Reads the data file line by line and feeds each tuple to the appropriate handler.
     * This method is synchronous — it blocks until the entire file has been processed.
     */
    public void startFeeding() {
        TupleHandler<?, ?>[] handlers = {positionReportHandler, dailyExpenditureRequestHandler, accountBalanceRequestHandler};

        long linesProcessed = 0;
        boolean firstArrived = false;

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                if (!firstArrived) {
                    logger.debug("First element has arrived, starting timer.");
                    LinearRoadKafkaBenchmarkApplication.Context.markAsStarted();
                    firstArrived = true;
                }

                String[] tuple = line.split(",");
                Arrays.stream(handlers).forEach(handler -> handler.handle(tuple));

                linesProcessed++;
                if (linesProcessed % 100000 == 0) {
                    logger.debug("Processed {} lines of input data.", linesProcessed);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read data file: " + filePath, e);
        }

        logger.debug("Finished feeding {} lines.", linesProcessed);
        Arrays.stream(handlers).forEach(TupleHandler::close);
    }
}
