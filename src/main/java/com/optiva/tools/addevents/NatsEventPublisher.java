package com.optiva.tools.addevents;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamOptions;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;

public class NatsEventPublisher {

    public static final JetStreamOptions JET_STREAM_OPTIONS = JetStreamOptions.builder().publishNoAck(false).requestTimeout(Duration.ofSeconds(30)).build();

    private final NatsConfiguration configuration;

    private final Connection connection;

    public NatsEventPublisher(final NatsConfiguration configuration) {
        this.configuration = configuration;
        Options connectionOptions = new Options.Builder().servers(((NatsReaderConfiguration) this.configuration).getUrls())
                                                         .connectionTimeout(Duration.ofSeconds(30))
                                                         .maxReconnects(-1)
                                                         .reconnectBufferSize(configuration.getConnectionByteBufferSize())
                                                         .turnOnAdvancedStats()
                                                         .build();

        try {
            connection = Nats.connect(connectionOptions);
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(new NatsEventException("NatsConnection#initialize Error with connection ", e));
        }
    }

    /**
     * Publish the message to Nats using the JetStream server
     *
     * @param event   serialized JSON event
     * @param subject String representation of message subject e.g. 100.23.66.66
     * @throws NatsEventException custom exception to hold any errors while publishing
     */
    public void publish(ByteArrayOutputStream event, String subject) throws NatsEventException {
        try {
            Message msg = NatsMessage.builder().subject(subject).data(event.toByteArray()).build();
            PublishOptions publishOptions = PublishOptions.builder().streamTimeout(Duration.ofSeconds(30)).stream(configuration.getStreamName()).build();
            PublishAck ack = getNatsConnection().jetStream(JET_STREAM_OPTIONS).publish(msg, publishOptions);
            if (ack != null) {
                if (ack.hasError()) {
                    String errMsg = "NatsEventPublisher#publish --> Message publishing ack returned an error : ";

                    throw new NatsEventException(errMsg + ack.getError());
                }
            } else {
                String errMsg = "NatsEventPublisher#publish --> PublishAck object was NULL ";
                throw new NatsEventException(errMsg);
            }
        } catch (IOException | JetStreamApiException e) {
            String errMsg = "NatsEventPublisher#publish --> exception when publishing : ";
            throw new NatsEventException(errMsg + e.getMessage(), e);
        }
    }

    public Connection getNatsConnection() {
        return connection;
    }

    public void close() {
        try {
            connection.close();
        } catch (Exception e) {
            //
        }
    }

    public void printConnectionStats() {
        System.out.println(connection.getStatistics());
    }
}
