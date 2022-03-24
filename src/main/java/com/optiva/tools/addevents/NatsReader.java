package com.optiva.tools.addevents;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
 *
 */
public final class NatsReader implements Supplier<List<EventMessage>> , NatsConsumer {
    private static final Logger logger = LogManager.getLogger(NatsReader.class);
    public static final String TRANSFERS = "Transfers";
    @CommandLine.Option(names = { "-s"}, defaultValue = "nats://192.168.49.2:30409", paramLabel = "NatsServer",
            description = "the nats server endpoint")
    public String endpoint;
    @CommandLine.Option(names = { "-partitions" }, defaultValue = "256", paramLabel = "NumPartitions",
            description = "number of partitions to create in NATS 0-255 by default")
    public Integer numPartitions;

    @CommandLine.Option(names = { "-mc" }, required = true, paramLabel = "NumMsgs",
            description = "number of message to send to each partition")
    public Integer msgCount;

    @CommandLine.Option(names = { "-timeId" }, required = true, paramLabel = "TimeId",
            description = "TimeId for the Event")
    public Integer timeId;

    @CommandLine.Option(names = { "-useId" }, required = true, paramLabel = "UseId",
            description = "useId for the Event")
    public Integer useId;

    @CommandLine.Option(names = { "-group" }, required = true, paramLabel = "Group",
            description = "Group for the Event")
    public Integer group;

    @CommandLine.Option(names = { "-custPartId", "--custPartId" }, paramLabel = "custPartId",
            description = "custPartId for the Event")
    public Integer custPartId;

    enum TestCases { createPartitionedStreams, deletePartitionedStreams, publishPartitionedStreams  }
    @CommandLine.Option(names = { "-tst", "--testName" }, required = true, paramLabel = "TestNameToExecute",
            description = "Test to execute must be one of these values: ${COMPLETION-CANDIDATES}")
    TestCases tstName = null;


    private static final int MAX_ALLOWED_BATCH_SIZE = 256;
    public static final int MAX_ACK_PENDING = -1;
    private static final Integer connectByteBufferSize = 20*1024*1024;
    // 1 second
    private static final Integer initialMaxWaitTimeMs = 1000;
    private static final Integer maxWaitTimeMs = 100;
    private final NatsConfiguration configuration;
    private int totalRead;
    private boolean noMoreMsgs;
    private EventSerialization event;

    public NatsReader(NatsConfiguration configuration) {
        this.configuration = configuration;
        totalRead = 0;
        noMoreMsgs =  true;
        event = new JsonEvent();
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
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();
            for (int x = 0; x < numEvents; x++) {
                Message msg = NatsMessage.builder()
                        .subject(subject)
                        .data(output.toByteArray())
                        .build();
                js.publish(msg);
            }
        } catch (IOException e) {
            System.err.println("Error I/O ["+e.getLocalizedMessage());
        } catch (InterruptedException | JetStreamApiException  e) {
            System.err.println("Error  ["+e.getLocalizedMessage());
        } catch (Exception e) {
            System.err.println("Error  ["+e.getLocalizedMessage());

        }
    }
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
        System.out.println("Creating subscriber for subject: "+configuration.getSubjectName()+ " with durablename: "+configuration.getDurableName());
        return js.subscribe(configuration.getSubjectName(), pullOptions);
    }

    /**
     * Create a pull subscriber with a durableName and the filter subject set.
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
     *
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
            } while (noMoreToRead == false && totalRead < configuration.getBatchSize());
        } catch (InterruptedException e) {
            logger.error("Error reading message of the NATS server.",e);
        }
    }

    /**
     * fetchBatch
     * fetches a batch of messages from NATS
     *
     * @return true iff NATS has no more messages available.
     *
     * @param messages
     * @param pullSub */
    private  boolean fetchBatch(List<EventMessage> messages, Subscription pullSub) throws InterruptedException {
        /*
         * wait a period for the first one
         */
        noMoreMsgs =  true;
        Message msg = pullSub.nextMessage(Duration.ofMillis(configuration.getInitialMaxWaitTimeMs()));
        if (msg !=null) {
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
        }
        catch (JetStreamApiException | IOException jsae) {
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
    public void createPartitionedEventStreams(int numberPartitions ) {
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
                String streamName = configuration.getStreamName()+partition;
                String subjectName = partition+".>";
                StreamConfiguration streamConfig = StreamConfiguration.builder()
                        .name(streamName)
                        .subjects(subjectName)
                        .storageType(StorageType.File)
                        .replicas(configuration.getNumberOfReplicas())
                        .maxMessagesPerSubject(Long.MAX_VALUE)
                        .build();
                // Create the stream
                // StreamInfo streamInfo = getStreamInfo(jsm, streamName, false);
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
                Thread.sleep(10);;
            }
            createTransferStream();
        } catch (IOException e) {
            logger.error("I/O error",e);
        } catch (InterruptedException | JetStreamApiException  e) {
            logger.error("NATS Error ", e);
        } catch (Exception e) {
            logger.error("Error  ",e);
        }
    }
    public void deletePartitionedEventStreams(int numberPartitions ) {
        Options options = new Options.Builder().
                server(configuration.getUrl()).
                reconnectBufferSize(configuration.getConnectionByteBufferSize()).  // Set buffer in bytes
                        build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            for (int part = 0; part < numberPartitions; ++part) {
                String partition = Integer.toString(part);
                String streamName = configuration.getStreamName() + partition;
                StreamInfo strDetails = jsm.getStreamInfo(streamName);
                if (strDetails != null ) {
                    jsm.deleteStream(streamName);
                    System.out.println("Deleted Stream: "+streamName);
                } else{
                    System.out.println("Stream: "+streamName+" doest not exist. Cannot delete");
                }
            }
            deleteTransfersStream();
        } catch (IOException e) {
            logger.error("Error  ",e);
        } catch (InterruptedException jsa) {
            logger.error("Error  ",jsa);
        } catch (JetStreamApiException e) {
            logger.error("Error stream not found ",e);
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
            String subjectName = streamName+".>";
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
            logger.error("I/O error",e);
        } catch (InterruptedException | JetStreamApiException  e) {
            logger.error("NATS Error ", e);
        } catch (Exception e) {
            logger.error("Error  ",e);
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
            if (strDetails != null ) {
                jsm.deleteStream(streamName);
                System.out.println("Deleted Stream: "+streamName);
            } else{
                System.out.println("Stream: "+streamName+" doest not exist. Cannot delete");
            }
        } catch (IOException e) {
            logger.error("Error  ",e);
        } catch (InterruptedException jsa) {
            logger.error("Error  ",jsa);
        } catch (JetStreamApiException e) {
            logger.error("Error  ",e);
        }
    }
    public void publishAllPartitions(int numberPartitions, int numMsgsPerPartition, int useId, int timeId) {
        Date now = java.util.Calendar.getInstance().getTime();

        for (int partition = 0; partition < numberPartitions; partition++){
            JsonEvent event  = new JsonEvent().withTimeId(timeId)
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
            String subject = Integer.toString(partition)+"."+Integer.toString(timeId)+"."+useId;
            /**
             * publish numMsgsPerPartition for this partition
             */
            publish(event, numMsgsPerPartition, subject);
            System.out.println("Published "+numMsgsPerPartition +" events to NATS  subject: "+ subject+
                    " for useId "+useId + " and timeId "+timeId+ " and custIdHash "+partition);
        }

        JsonTransfer transfer = new JsonTransfer(333,timeId, useId, now.getTime(),
                now.getTime(), group, "TRANSFERRABLE",
                "EXPORTABLE",
                numMsgsPerPartition*numberPartitions,
                "WRITING",133444,
                "none",numMsgsPerPartition*numberPartitions );

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            transfer.serialize(output);
            publish(output, 1, "Transfers.WRITING");
        } catch (IOException e) {
            logger.error("Error unable to publish Transfer entry to NATS ",e);
        }
    }
    public static void main(String[] args){
        NatsReader reader = new NatsReader(null);
        CommandLine commandLine = null;
        try {
            commandLine = new CommandLine(reader);
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
        logger.error("server :{}", reader.endpoint);
        logger.error("number of partitions :{} ", reader.numPartitions);
        logger.error("number of message to send to each partitions :{} ", reader.msgCount);
        logger.error("timeId :{} useId :{} ", reader.timeId, reader.useId);

        logger.error("Test to execute : {}", reader.tstName);

        NatsReaderConfiguration config = new NatsReaderConfiguration(true, "Events.>",
                null,
                "PULL", true ,
                reader.endpoint, "queueName",
                "Events", "durableName",
                1000, 3,  40, connectByteBufferSize,
                initialMaxWaitTimeMs, maxWaitTimeMs);
        NatsReader natsReader = new NatsReader(config);

        switch (reader.tstName) {
            case createPartitionedStreams:
                natsReader.createPartitionedEventStreams(reader.numPartitions);
                break;
            case deletePartitionedStreams:
                natsReader.deletePartitionedEventStreams(reader.numPartitions);
                break;
            case publishPartitionedStreams:
                natsReader.publishAllPartitions(reader.numPartitions, reader.msgCount, reader.useId, reader.timeId);
                break;
        }
    }

    @Override
    public NatsConfiguration getConfiguration() {
        return configuration;
    }
}
