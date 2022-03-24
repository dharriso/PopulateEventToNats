package com.optiva.tools.addevents;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class NatsTransferState implements Supplier<JsonTransfer>, NatsConsumer {
    private static final int MAX_ACK_PENDING = -1;
    private final NatsConfiguration configuration;
    private static final Logger logger = LogManager.getLogger(NatsTransferState.class);
    private final JsonTransfer transfer;


    public NatsTransferState(final NatsConfiguration configuration) {

        this.configuration = configuration;
        transfer =  new JsonTransfer();
    }

    @Override
    /**
     * gets the oldest Transfer in table
     */
    public JsonTransfer get() {
        Options options = new Options.Builder().
                server(configuration.getUrl()).
                connectionListener(new NatsConnectionListener(this)).
                reconnectBufferSize(configuration.getConnectionByteBufferSize()).  // Set buffer in bytes
                        build();
        Message msg = null;
        JsonTransfer pojo = null;

        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = getJetStream(nc, jsm);
            JetStreamSubscription pullSub = getPullSubscribeOptions(js);
            pullSub.pullNoWait(1);
            msg = pullSub.nextMessage(Duration.ofMillis(configuration.getInitialMaxWaitTimeMs()));
            if (msg != null && msg.isJetStream()) {
                try {
                    pojo = transfer.deserialize(new ByteArrayInputStream(msg.getData()));
                    msg.ack();
                } catch (IOException e) {
                    logger.error("Problem serializing the Nats message to JSON, continuing", e);
                }
            }
        } catch (IOException e) {
            logger.error("I/O error communicating to the NATS server.", e);
        } catch (InterruptedException | JetStreamApiException e) {
            logger.error("Processing JetStream messages error.", e);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        return pojo;
    }

    public NatsConfiguration getConfiguration() {
        return configuration;
    }
    public void publish(JsonTransfer event, String subject) {
        Options options = new Options.Builder().
                server(configuration.getUrl()).
                connectionListener(new NatsConnectionListener(this)).
                reconnectBufferSize(configuration.getConnectionByteBufferSize()).  // Set buffer in bytes
                        build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = getJetStream(nc, jsm);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            event.serialize(output);
            Message msg = NatsMessage.builder()
                    .subject(subject)
                    .data(output.toByteArray())
                    .build();
            js.publish(msg);
        } catch (IOException e) {
            System.err.println("Error I/O ["+e.getLocalizedMessage());
        } catch (InterruptedException | JetStreamApiException  e) {
            System.err.println("Error  ["+e.getLocalizedMessage());
        } catch (Exception e) {
            System.err.println("Error  ["+e.getLocalizedMessage());

        }
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
        return js.subscribe(configuration.getSubjectName(), pullOptions);
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
    private JetStream getJetStream(Connection nc, JetStreamManagement jsm) throws
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
            logger.error("Error NATS Management API stream", jsae);
            return null;
        }
        return strDetails;
    }
}
