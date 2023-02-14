package com.optiva.tools.addevents;

import com.optiva.tools.load.NeverClosingSingleConsumer;
import com.optiva.tools.load.ScheduleBasedConsumer;
import com.optiva.tools.load.TimebasedLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

/**
 * NatsReader reads messages from NATS server. It reads a batch of messages
 * to prevent it bloating the service as there are  millions of Event
 * messages stored in NATS.
 */
public final class NatsReader {
    private static final Logger logger = LogManager.getLogger(NatsReader.class);
    private static final Integer CONNECT_BYTE_BUFFER_SIZE = 20 * 1024 * 1024;
    // 1 second
    private static final Integer INITIAL_MAX_WAIT_TIME_MS = 1000;
    private static final Integer MAX_WAIT_TIME_MS = 100;
    @CommandLine.Option(names = {"-s",
                                 "--server"}, required = true, paramLabel = "NatsServer", description = "the nats server endpoint, this must also include the prefix nats and the port for example nats://192.168.49.2:30409")
    public String endpoint;
    @CommandLine.Option(names = {"-consName",
                                 "--consumerName"}, defaultValue = "zdataDurName", paramLabel = "Name of the Durable Consumer", description = "The Consumer Name that will be used to consume messages.")
    public String consumerName;
    @CommandLine.Option(names = {"-bs", "--batchSize"}, defaultValue = "720000", paramLabel = "Batch Size", description = "Number of Messages to consumer per iteration.")
    public Integer batchSize;
    @CommandLine.Option(names = {"-stream", "--StreamName"}, defaultValue = "Events", paramLabel = "Stream Name", description = "Name of the JetStream Stream to connect to.")
    public String streamName;
    @CommandLine.Option(names = {"-subject", "--subjectName"}, defaultValue = "Events.>", paramLabel = "Subject Name", description = "Name of the JetStream Subject to connect to.")
    public String subjectName;
    @CommandLine.Option(names = {"-ttm",
                                 "--totalTimeMinutes"}, defaultValue = "1", paramLabel = "Total Time In Minutes", description = "When running a load test, this specifies how long the test should execute.")
    public Integer totalTimeInMinutes;
    @CommandLine.Option(names = {"-mps",
                                 "--messagesPerSecond"}, defaultValue = "1", paramLabel = "Messages Per Second", description = "Number of Messages that each publisher should write per second.")
    public Integer messagesPerSecond;

    @CommandLine.Option(names = {"-np",
                                 "--numberOfPublishers"}, defaultValue = "1", paramLabel = "Number of Publishers", description = "The Number of publishers that should be simultaneously when writing messages.")
    public Integer numberOfPublishers;

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help message")
    boolean usageHelpRequested;
    @CommandLine.Option(names = {"-tst", "--testName"}, required = true, paramLabel = "TestNameToExecute", description = "Test to execute must be one of these values: ${COMPLETION-CANDIDATES}")
    TestCases tstName = null;
    private NatsConfiguration configuration;


    public NatsReader(NatsConfiguration configuration) {
        this.configuration = configuration;
    }

    public static void main(String[] args) {
        NatsReader reader = new NatsReader(null);
        CommandLine commandLine = new CommandLine(reader);
        try {
            commandLine.parseArgs(args);
            if (commandLine.isUsageHelpRequested()) {
                commandLine.usage(System.out);
                return;
            }
        } catch (CommandLine.ParameterException ex) {
            logger.error(ex.getMessage());
            commandLine.usage(System.out);
            return;
        }

        logger.error("Test to execute : {}", reader.tstName);
        logger.error("Nats Server(s) :{}", reader.endpoint);
        logger.error("consumerName : {}", reader.consumerName);
        logger.error("batchSize : {}", reader.batchSize);
        logger.error("total time in minutes : {}", reader.totalTimeInMinutes);
        logger.error("messages per second : {}", reader.messagesPerSecond);
        logger.error("number of publishers : {}", reader.numberOfPublishers);
        logger.error("stream name : {}", reader.streamName);
        logger.error("subject name : {}", reader.getSubject(reader));

        reader.configuration = new NatsReaderConfiguration(reader.getSubject(reader),
                                                           reader.endpoint,
                                                           reader.streamName,
                                                           reader.consumerName,
                                                           reader.batchSize,
                                                           CONNECT_BYTE_BUFFER_SIZE);
        switch (reader.tstName) {
            case loadTest:
                TimebasedLoader tbl = new TimebasedLoader(reader.totalTimeInMinutes, reader.messagesPerSecond, reader.numberOfPublishers, reader.configuration);
                tbl.executeLoad();
                break;
            case consumeOnSchedule:
                ScheduleBasedConsumer sbc = new ScheduleBasedConsumer(reader.configuration);
                sbc.consume();
                break;
            case neverClosingSingleConsumer:
                NeverClosingSingleConsumer neverClosingSingleConsumer = new NeverClosingSingleConsumer(reader.configuration);
                neverClosingSingleConsumer.consume();
                break;
        }
    }

    /**
     * Defaults to Events.> if not provided on the command line
     * @param reader
     * @return
     */
    private String getSubject(NatsReader reader) {
        if (null == reader.subjectName || reader.subjectName.isEmpty()) {
            return "Events.>";
        } else {
            return reader.subjectName;
        }
    }

    enum TestCases {
        loadTest,
        consumeOnSchedule,
        neverClosingSingleConsumer
    }
}
