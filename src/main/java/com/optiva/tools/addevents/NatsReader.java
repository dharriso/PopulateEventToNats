package com.optiva.tools.addevents;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsMessage;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * NatsReader reads messages from NATS server. It reads a batch of messages
 * to prevent it bloating the service as there are  millions of Event
 * messages stored in NATS.
 */
public final class NatsReader implements Supplier<List<EventMessage>>, NatsConsumer {
    public static final String TRANSFERS = "Transfers";
    public static final int MAX_ACK_PENDING = -1;
    private static final Logger logger = LogManager.getLogger(NatsReader.class);
    private static final int MAX_ALLOWED_BATCH_SIZE = 256;
    private static final Integer connectByteBufferSize = 20 * 1024 * 1024;
    // 1 second
    private static final Integer initialMaxWaitTimeMs = 1000;
    private static final Integer maxWaitTimeMs = 100;
    private final EventSerialization event;
    @CommandLine.Option(names = {"-s", "--server"}, required = true, paramLabel = "NatsServer",
            description = "the nats server endpoint, this must also include the prefix nats and the port for example nats://192.168.49.2:30409")
    public String endpoint;
    @CommandLine.Option(names = {"-partitions"}, defaultValue = "256", showDefaultValue = CommandLine.Help.Visibility.ALWAYS, paramLabel = "NumPartitions",
            description = "number of partitions to create in NATS, valid values from 0-256")
    public Integer numPartitions;
    @CommandLine.Option(names = {"-mc"}, defaultValue = "1", showDefaultValue = CommandLine.Help.Visibility.ALWAYS, paramLabel = "NumMsgs",
            description = "number of dummy message to send to each partition")
    public Integer msgCount;
    @CommandLine.Option(names = {"-timeId"}, defaultValue = "1", showDefaultValue = CommandLine.Help.Visibility.ALWAYS, paramLabel = "TimeId",
            description = "TimeId for the Dummy Events")
    public Integer timeId;
    @CommandLine.Option(names = {"-useId"}, defaultValue = "1", showDefaultValue = CommandLine.Help.Visibility.ALWAYS, paramLabel = "UseId",
            description = "useId for the Dummy Events")
    public Integer useId;
    @CommandLine.Option(names = {"-group"}, defaultValue = "1", paramLabel = "Group",
            description = "Group for the writing to the TRANSFERABLE Partition this writes out the number of dummy messages published.")
    public Integer group;
    @CommandLine.Option(names = {"-cdbip", "--customerDBIP"}, paramLabel = "CustomerDBIPToPullFrom", description = "The IP Address of the CUSTDB that holds the events that should be published to NATS")
    public String custDbIp;
    @CommandLine.Option(names = {"-cdbport", "--customerDBPort"}, paramLabel = "CustomerDBPortToPullFrom", description = "The Port of the CUSTDB that holds the events that should be published to NATS")
    public Integer custDbPort;
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help message")
    boolean usageHelpRequested;
    @CommandLine.Option(names = {"-tst", "--testName"}, required = true, paramLabel = "TestNameToExecute",
            description = "Test to execute must be one of these values: ${COMPLETION-CANDIDATES}")
    TestCases tstName = null;
    private NatsConfiguration configuration;
    private int totalRead;
    private boolean noMoreMsgs;

    public NatsReader(NatsConfiguration configuration) {
        this.configuration = configuration;
        totalRead = 0;
        noMoreMsgs = true;
        event = new JsonEvent();
    }

    public static void main(String[] args) {
        NatsReader reader = new NatsReader(null);
        CommandLine commandLine = new CommandLine(reader);
        try {
            commandLine.parseArgs(args);
            reader.validate();
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
        logger.error("Nats Server :{}", reader.endpoint);
        logger.error("Number of partitions :{} ", reader.numPartitions);
        logger.error("Number of message to send to each partitions :{} ", reader.msgCount);
        logger.error("timeId :{} useId :{} ", reader.timeId, reader.useId);
        logger.error("custDbIp :{} ", reader.custDbIp);
        logger.error("custDbPort :{} ", reader.custDbPort);

        reader.configuration = new NatsReaderConfiguration(true, "Events.>",
                null,
                "PULL", true,
                reader.endpoint, "queueName",
                "Events", "durableName",
                1000, 3, 40, connectByteBufferSize,
                initialMaxWaitTimeMs, maxWaitTimeMs);

        switch (reader.tstName) {
            case createPartitionedStreams:
                reader.createPartitionedEventStreams(reader.numPartitions);
                break;
            case deletePartitionedStreams:
                reader.deletePartitionedEventStreams(reader.numPartitions);
                break;
            case publishPartitionedStreams:
                reader.publishAllPartitions(reader.numPartitions, reader.msgCount, reader.useId, reader.timeId, reader.group);
                break;
            case publishFromDb:
                reader.publishFromDb(reader.custDbIp, reader.custDbPort);
                break;
            case getMessages:
                throw new RuntimeException("TODO - NEEDS TO BE IMPLEMENTED!");

        }
    }

    /**
     * Validate the Parameters that have been passed via the CLI, the only required parameter is {-tst, --testName}
     * so anything else that is needed must be validated. although I supposed we could also add in endpoint
     */
    void validate() {

        if (tstName == TestCases.publishFromDb) {
            if (missing(custDbIp) || missing(custDbPort)) {
                throw new CommandLine.ParameterException(spec.commandLine(),
                        "Missing options: when trying to publish db events to streams the  " +
                                "{-s, --server} option must be specified, " +
                                "{-cdbip, --customerDBIP} option must be specified , " +
                                "{-cdbport, --customerDBPort} option must also be specified, " +
                                "ensure all of these are included. \n" +
                                "To see the default values for the rest of the parameters please provide -h or --help to print the usage info.");
            }
        }
    }

    boolean missing(Object parameter) {
        return ObjectUtils.isEmpty(parameter);
    }

    /**
     * Consumes up to fetchBatchSize messages from NATS.
     * Populates the Messages extracted into a container, see getMessages API
     * call to return these Messages.
     *
     * @return list of the message consumed from NATS
     */
    public List<EventMessage> get() {
        Options options = new Options.Builder().
                server(configuration.getUrl()).
                connectionListener(new NatsConnectionListener(this)).
                reconnectBufferSize(configuration.getConnectionByteBufferSize()).  // Set buffer in bytes
                        build();
        List<EventMessage> messages = new ArrayList<>();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = getJetStream(nc, jsm);
            consumeMessages(js, messages);
        } catch (IOException e) {
            logger.error("I/O error communicating to the NATS server.", e);
        } catch (InterruptedException | JetStreamApiException | TimeoutException e) {
            logger.error("Processing JetStream messages error.", e);
        }
        return messages;
    }

    public void publish(ByteArrayOutputStream output, int numEvents, String subject) {
        Options options = new Options.Builder().
                server(configuration.getUrl()).
                connectionListener(new NatsConnectionListener(this)).
                reconnectBufferSize(configuration.getConnectionByteBufferSize()).  // Set buffer in bytes
                        build();
        try (Connection nc = Nats.connect(options)) {
            JetStream js = nc.jetStream();
            for (int x = 0; x < numEvents; x++) {
                Message msg = NatsMessage.builder()
                        .subject(subject)
                        .data(output.toByteArray())
                        .build();
                js.publish(msg);
            }
        } catch (IOException e) {
            System.err.println("Error I/O [" + e.getLocalizedMessage());
        } catch (Exception e) {
            System.err.println("Error  [" + e.getLocalizedMessage());
        }
    }

    /**
     * This consolidates the serialization to a ByteArrayOutputStream, and since the JSONEvent implements EventSerialization,
     * this convenience method should be used to simplify the publishing code.
     * @param event the event that needs to be serialized (not sure how this could be multiples)
     * @param numEvents the number of events - this really should just be 1
     * @param subject the subject to be used when publishing the events
     */
    public void publish(EventSerialization event, int numEvents, String subject) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            event.serialize(output);
            publish(output, numEvents, subject);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Has Nats indicated that no more messages to read
     *
     * @return true if no more messages to read otherwise false
     */
    public boolean noMoreMsgs() {
        return noMoreMsgs;
    }

    /**
     * Get the total message this consumer has gotten from the NATS server
     *
     * @return total number of messages consumer by  this subscriber.
     */
    public int getTotalRead() {
        return totalRead;
    }

    /**
     * Create a Pull subscriber using the durable name.
     *
     * @param js
     * @return handle to the pull consumer.
     * @throws JetStreamApiException
     * @throws IOException
     */
    private JetStreamSubscription getPullSubscribeOptions(JetStream js) throws JetStreamApiException, IOException {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(configuration.getDurableName())
                .maxAckPending(MAX_ACK_PENDING) // as we are pull we should ste to -1 to allow us to scale out
                .build();
        PullSubscribeOptions pullOptions = PullSubscribeOptions.builder().configuration(cc).build();
        /*
         * Consume all the events in the stream
         */
        System.out.println("Creating subscriber for subject: " + configuration.getSubjectName() + " with durablename: " + configuration.getDurableName());
        return js.subscribe(configuration.getSubjectName(), pullOptions);
    }

    /**
     * Create a pull subscriber with a durableName and the filter subject set.
     *
     * @param js
     * @return handle to the filtered pull consumer.
     * @throws JetStreamApiException
     * @throws IOException
     */
    private JetStreamSubscription getFilteredPullSubscribeOptions(JetStream js) throws JetStreamApiException, IOException {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .filterSubject(configuration.getFilter())
                .durable(configuration.getDurableName())
                .maxAckPending(MAX_ACK_PENDING) // as we are pull we should ste to -1 to allow us to scale out
                .build();
        PullSubscribeOptions pullOptions = PullSubscribeOptions.builder().configuration(cc).build();
        logger.info("filter-consumer filtered on {} ", configuration.getFilter());
        /*
         * Consume all the events in the stream
         */
        return js.subscribe(configuration.getSubjectName(), pullOptions);
    }

    /**
     * Consume the message from NATS.
     * <p>
     * May require more than one iteration if the size is greater than the max size (256) of
     * a NATS pull batch.
     *
     * @param js
     * @param messages
     * @throws JetStreamApiException
     * @throws IOException
     */
    private void consumeMessages(JetStream js, List<EventMessage> messages) throws JetStreamApiException, IOException {
        JetStreamSubscription pullSub;
        if (configuration.getFilter() != null &&
                configuration.getFilter().length() > 0) {
            pullSub = getFilteredPullSubscribeOptions(js);
        } else {
            pullSub = getPullSubscribeOptions(js);
        }
        /*
         * dont block waiting on the complete batch size of messages
         * if we receive less than batch size then we should return
         *
         * no point polling nats
         */
        try {
            boolean noMoreToRead = false;
            do {
                /*
                 * NATS mandates batch size of 256 messages max in a batch if the fetch size is >
                 * then we need to chunk the response from NATS and grab it in chunks of 256
                 */
                int batchSize = calculateBatchSize();
                pullSub.pullNoWait(batchSize);
                /*
                 * if noMoreToRead false indicates that we have exhausted this batch and there might be more
                 * to retrieve from NATS
                 * true means that NATS has sent a 404 indicating no more messages at this time
                 * lets exit, regardless
                 */
                noMoreToRead = fetchBatch(messages, pullSub);
            } while (!noMoreToRead && totalRead < configuration.getBatchSize());
        } catch (InterruptedException e) {
            logger.error("Error reading message of the NATS server.", e);
        }
    }

    /**
     * fetchBatch
     * fetches a batch of messages from NATS
     *
     * @param messages
     * @param pullSub
     * @return true iff NATS has no more messages available.
     */
    private boolean fetchBatch(List<EventMessage> messages, Subscription pullSub) throws InterruptedException {
        /*
         * wait a period for the first one
         */
        noMoreMsgs = true;
        Message msg = pullSub.nextMessage(Duration.ofMillis(configuration.getInitialMaxWaitTimeMs()));
        if (msg != null) {
            noMoreMsgs = false;
        }
        while (msg != null) {
            if (msg.isJetStream()) {
                EventMessage pojo = null;
                try {
                    pojo = event.deserialize(new ByteArrayInputStream(msg.getData()));
                    messages.add(pojo);
                    totalRead++;
                    msg.ack();
                    /*
                     * message should be here.
                     */
                    msg = pullSub.nextMessage(Duration.ofMillis(configuration.getMaxWaitTimeMs()));
                } catch (IOException e) {
                    logger.error("Problem serializing the Nats message to JSON, continuing", e);
                }

            } else if (msg.isStatusMessage()) {
                /*
                 * This indicates batch has nothing more to send
                 *
                 * m.getStatus().getCode should == 404
                 * m.getStatus().getMessage should be "No Messages"
                 */
                msg = null;
                //
                noMoreMsgs = true;
            }
        }
        return noMoreMsgs;
    }

    /**
     * Determines the size of a batch request sent to NATS.
     *
     * @return size of the batch to use in request to  NATS
     */
    private int calculateBatchSize() {
        int outStanding = configuration.getBatchSize() - totalRead;

        return outStanding > MAX_ALLOWED_BATCH_SIZE ? MAX_ALLOWED_BATCH_SIZE : outStanding;
    }

    /**
     * Get the JetStream handle from NATS
     *
     * @param nc
     * @param jsm
     * @return
     * @throws JetStreamApiException
     * @throws IOException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public JetStream getJetStream(Connection nc, JetStreamManagement jsm) throws
            JetStreamApiException, IOException, InterruptedException, TimeoutException {
        /*
         * Perhaps we should drive this from the command line to build the
         * stream up front using nats.cli.
         */
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(configuration.getStreamName())
                .subjects(configuration.getSubjectName())
                .storageType(StorageType.File)
                .replicas(configuration.getNumberOfReplicas())
                .maxMessagesPerSubject(Long.MAX_VALUE)
                .build();
        // Create the stream
        StreamInfo streamInfo = getStreamInfo(jsm, configuration.getStreamName(), false);
        if (streamInfo == null) {
            jsm.addStream(streamConfig);
        }
        JetStream js = nc.jetStream();
        nc.flush(Duration.ofSeconds(1));
        return js;
    }

    /**
     * Get the stream information from NATS JetStream
     *
     * @param jsm
     * @param streamName
     * @param deleteStr
     * @return
     */
    private StreamInfo getStreamInfo(JetStreamManagement jsm, String streamName, boolean deleteStr) {
        StreamInfo strDetails = null;
        try {
            strDetails = jsm.getStreamInfo(streamName);
            if (strDetails != null && deleteStr) {
                jsm.deleteStream(streamName);
                strDetails = null;
            }
        } catch (JetStreamApiException | IOException jsae) {
            logger.info("Error NATS Management API stream", jsae);
            return null;
        }
        return strDetails;
    }

    /**
     * Create a Stream for each parition.
     *
     * @param numberPartitions
     */
    public void createPartitionedEventStreams(int numberPartitions) {
        Options options = new Options.Builder().
                server(configuration.getUrl()).
                connectionListener(new NatsConnectionListener(this)).
                reconnectBufferSize(configuration.getConnectionByteBufferSize()).  // Set buffer in bytes
                        build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            /*
             * Perhaps we should drive this from the command line to build the
             * stream up front using nats.cli.
             */
            for (int part = 0; part < numberPartitions; ++part) {
                String partition = Integer.toString(part);
                String streamName = configuration.getStreamName() + "_" + partition;
                String subjectName = "Events." + partition + ".>";
                StreamConfiguration streamConfig = StreamConfiguration.builder()
                        .name(streamName)
                        .subjects(subjectName)
                        .storageType(StorageType.File)
                        .replicas(configuration.getNumberOfReplicas())
                        .maxMessagesPerSubject(Long.MAX_VALUE)
                        .build();
                // Create the stream
                jsm.addStream(streamConfig);
                logger.error("Created stream: {} subject : {} ", streamName, subjectName);

/**
 *

 if (streamInfo == null) {
 jsm.addStream(streamConfig);
 logger.error("Created stream: {} subject : {} ", streamName, subjectName);
 } else {
 logger.error("Stream already present. Not Created: Stream :{} subject : {} ", streamName, subjectName);
 }
 */
                Thread.sleep(10);
                ;
            }
            createTransferStream();
        } catch (IOException e) {
            logger.error("I/O error", e);
        } catch (InterruptedException | JetStreamApiException e) {
            logger.error("NATS Error ", e);
        } catch (Exception e) {
            logger.error("Error  ", e);
        }
    }

    public void deletePartitionedEventStreams(int numberPartitions) {
        Options options = new Options.Builder().
                server(configuration.getUrl()).
                reconnectBufferSize(configuration.getConnectionByteBufferSize()).  // Set buffer in bytes
                        build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            for (int part = 0; part < numberPartitions; ++part) {
                String partition = Integer.toString(part);
                String streamName = configuration.getStreamName() + "_" + partition;
                String subjectName = "Events." + partition + ".>";
                StreamInfo strDetails = jsm.getStreamInfo(streamName);
                if (strDetails != null) {
                    jsm.deleteStream(streamName);
                    System.out.println("Deleted Stream: " + streamName);
                } else {
                    System.out.println("Stream: " + streamName + " doest not exist. Cannot delete");
                }
            }
            deleteTransfersStream();
        } catch (IOException e) {
            logger.error("Error  ", e);
        } catch (InterruptedException jsa) {
            logger.error("Error  ", jsa);
        } catch (JetStreamApiException e) {
            logger.error("Error stream not found ", e);
        }
    }

    private void createTransferStream() {
        Options options = new Options.Builder().
                server(configuration.getUrl()).
                connectionListener(new NatsConnectionListener(this)).
                reconnectBufferSize(configuration.getConnectionByteBufferSize()).  // Set buffer in bytes
                        build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            /*
             * Perhaps we should drive this from the command line to build the
             * stream up front using nats.cli.
             */

            String streamName = TRANSFERS;
            String subjectName = streamName + ".>";
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(streamName)
                    .subjects(subjectName)
                    .storageType(StorageType.File)
                    .replicas(configuration.getNumberOfReplicas())
                    .maxMessagesPerSubject(Long.MAX_VALUE)
                    .build();
            // Create the stream
            StreamInfo streamInfo = getStreamInfo(jsm, streamName, false);
            if (streamInfo == null) {
                jsm.addStream(streamConfig);
                logger.error("Created stream: {} subject : {} ", streamName, subjectName);
            } else {
                logger.error("Stream already present. Not Created: Stream :{} subject : {} ", streamName, subjectName);
            }
        } catch (IOException e) {
            logger.error("I/O error", e);
        } catch (InterruptedException | JetStreamApiException e) {
            logger.error("NATS Error ", e);
        } catch (Exception e) {
            logger.error("Error  ", e);
        }

    }

    private void deleteTransfersStream() {
        Options options = new Options.Builder().
                server(configuration.getUrl()).
                reconnectBufferSize(configuration.getConnectionByteBufferSize()).  // Set buffer in bytes
                        build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            String streamName = TRANSFERS;
            StreamInfo strDetails = jsm.getStreamInfo(streamName);
            if (strDetails != null) {
                jsm.deleteStream(streamName);
                System.out.println("Deleted Stream: " + streamName);
            } else {
                System.out.println("Stream: " + streamName + " doest not exist. Cannot delete");
            }
        } catch (IOException | JetStreamApiException | InterruptedException e) {
            logger.error("Error  ", e);
        }
    }

    /**
     * This test runner will pull all events from the configured CUSTDB and push them 1 by 1 into NATs
     *
     * @param custDbIp   the oracle customer db IP address
     * @param custDbPort the oracle customer db port
     */
    public void publishFromDb(String custDbIp, int custDbPort) {
        String oracleJdbcUrl = String.format("jdbc:oracle:thin:@%s:%d:CUST1DB", custDbIp, custDbPort);
        String username = "adv";
        String password = "2Ab}7C";

        try (java.sql.Connection conn = DriverManager.getConnection(oracleJdbcUrl, username, password)) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM EVENT e WHERE e.EVENTTYPE > 0 AND e.INTERNALRATINGRELEVANT0 IS NOT NULL");

            while (rs.next()) {
                JsonEvent event = new JsonEvent();
                event.setEventId(rs.getInt("EVENTID"));
                event.setAccessKey(rs.getString("ACCESSKEY"));
                event.setAccessKeyType(rs.getInt("ACCESSKEYTYPE"));
                event.setOwningCustomerID(rs.getString("OWNINGCUSTOMERID"));
                event.setRootCustomerID(rs.getString("ROOTCUSTOMERID"));
                event.setRootCustomerIDHash(rs.getInt("ROOTCUSTOMERIDHASH"));
                event.setComposedCustomerID(rs.getString("COMPOSEDCUSTOMERID"));
                event.setEventType(rs.getInt("EVENTTYPE"));
                event.setOriginalEventTime(rs.getTimestamp("ORIGEVENTTIME"));
                event.setCreationEventTime(rs.getTimestamp("CREATIONEVENTTIME"));
                event.setEffectiveEventTime(rs.getTimestamp("EFFECTIVEEVENTTIME"));
                event.setExternalCorrelationID(rs.getLong("CORRELATIONID"));
                event.setBillCycleID(rs.getInt("BILLCYCLEID"));
                event.setBillPeriodID(rs.getInt("BILLPERIODID"));
                event.setErrorCode(rs.getInt("ERRORCODE"));
                event.setRateEventType(rs.getString("RATEEVENTTYPE"));
                event.setInternalRatingRelevant0(rs.getBytes("INTERNALRATINGRELEVANT0"));
                event.setInternalRatingRelevant1(rs.getBytes("INTERNALRATINGRELEVANT1"));
                event.setInternalRatingRelevant2(rs.getBytes("INTERNALRATINGRELEVANT2"));
                event.setExternalRatingIrrelevant0(rs.getBytes("EXTERNALRATINGIRRELEVANT0"));
                event.setExternalRatingIrrelevant1(rs.getBytes("EXTERNALRATINGIRRELEVANT1"));
                event.setExternalRatingIrrelevant2(rs.getBytes("EXTERNALRATINGIRRELEVANT2"));
                event.setExternalRatingResult0(rs.getBytes("EXTERNALRATINGRESULTS0"));
                event.setExternalRatingResult1(rs.getBytes("EXTERNALRATINGRESULTS1"));
                event.setExternalRatingResult2(rs.getBytes("EXTERNALRATINGRESULTS2"));
                event.setExternalDataTransp0(rs.getBytes("EXTERNALDATATRANSP0"));
                event.setExternalDataTransp1(rs.getBytes("EXTERNALDATATRANSP1"));
                event.setExternalDataTransp2(rs.getBytes("EXTERNALDATATRANSP2"));
                event.setUniversalAttribute0(rs.getBytes("UNIVERSALATTRIBUTE1"));
                event.setUniversalAttribute1(rs.getBytes("UNIVERSALATTRIBUTE2"));
                event.setUniversalAttribute2(rs.getBytes("UNIVERSALATTRIBUTE3"));
                event.setUniversalAttribute3(rs.getBytes("UNIVERSALATTRIBUTE4"));
                event.setUniversalAttribute4(rs.getBytes("UNIVERSALATTRIBUTE5"));
                event.setRefEventId(rs.getInt("REFEVENTID"));
                event.setRefUseId(rs.getInt("REFUSEID"));
                event.setTimeId(rs.getInt("TIMEID"));
                event.setUseId(rs.getInt("USEID"));

                String natsSubject = "Events." + event.getRootCustomerIDHash() + "." + event.getTimeId() + "." + event.getUseId() + "." + event.getRootCustomerID();
                publish(event, 1, natsSubject);

            }

        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        }
    }

    public void publishAllPartitions(int numberPartitions, int numMsgsPerPartition, int useId, int timeId, int group) {
        Date now = java.util.Calendar.getInstance().getTime();

        for (int partition = 0; partition < numberPartitions; partition++) {
            JsonEvent event = new JsonEvent().withTimeId(timeId)
                    .withUseId(useId)
                    .withEventId(22)
                    .withAccessKey("accessKey")
                    .withOwningCustomerID("owningCustomerID")
                    .withRootCustomerID("rootCustomerID")
                    .withEventType(112)
                    .withOriginalEventTime(now)
                    .withCreationEventTime(now)
                    .withEffectiveEventTime(now)
                    .withBillCycleID(1)
                    .withBillPeriodID(98)
                    .withRateEventType("rateEventType")
                    .withRootCustomerIDHash(partition);
            String subject = "Events." + partition + "." + timeId + "." + useId + event.getRootCustomerID();
            /**
             * publish numMsgsPerPartition for this partition
             */
            publish(event, numMsgsPerPartition, subject);
            System.out.println("Published " + numMsgsPerPartition + " events to NATS  subject: " + subject +
                    " for useId " + useId + " and timeId " + timeId + " and custIdHash " + partition);
        }

        JsonTransfer transfer = new JsonTransfer(333, timeId, useId, now.getTime(),
                now.getTime(), group, "TRANSFERRABLE",
                "EXPORTABLE",
                numMsgsPerPartition * numberPartitions,
                "WRITING", 133444,
                "none", numMsgsPerPartition * numberPartitions);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            transfer.serialize(output);
            publish(output, 1, "Transfers.WRITING");
            System.out.println("Published message with subject [Transfers.WRITING]");
        } catch (IOException e) {
            logger.error("Error unable to publish Transfer entry to NATS ", e);
        }
    }

    @Override
    public NatsConfiguration getConfiguration() {
        return configuration;
    }

    enum TestCases {createPartitionedStreams, deletePartitionedStreams, publishPartitionedStreams, publishFromDb, getMessages}
}
