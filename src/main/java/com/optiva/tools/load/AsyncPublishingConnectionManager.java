package com.optiva.tools.load;

import com.optiva.tools.EventProperties;
import com.optiva.tools.addevents.NatsConfiguration;
import com.optiva.tools.addevents.NatsReaderConfiguration;
import io.nats.client.Connection;
import io.nats.client.JetStream;
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
import java.util.concurrent.CompletableFuture;

public enum AsyncPublishingConnectionManager {
    INSTANCE;

    private final Connection natsConnection;
    private final JetStream jetStream;
    private final NatsConfiguration natsConfiguration;

    AsyncPublishingConnectionManager()
    {
        natsConfiguration = createConfiguration();
        natsConnection = createConnection();
        jetStream = createJetstream();
    }

    public static AsyncPublishingConnectionManager getInstance()
    {
        return INSTANCE;
    }

    public CompletableFuture<PublishAck> publishAysnc(ByteArrayOutputStream event, String subject, String streamName) {
        Message msg = NatsMessage.builder().subject(subject).data(event.toByteArray()).build();
        PublishOptions publishOptions = PublishOptions.builder().streamTimeout(Duration.ofSeconds(30)).stream(streamName).build();
        return jetStream.publishAsync(msg, publishOptions);
    }

    private NatsReaderConfiguration createConfiguration() {
        return new NatsReaderConfiguration(EventProperties.getNatsEventSubjectName(),
                                                        EventProperties.getNatsHostsList(),
                                                        EventProperties.getNatsEventStreamName(),
                                                        EventProperties.getNatsEventDurableName(),
                                                        EventProperties.getNatsBatchSize(),
                                                        EventProperties.getNatsConnectBufferByteSize());
    }

    private Connection createConnection() {
        Options connectionOptions = new Options.Builder().servers(((NatsReaderConfiguration) natsConfiguration).getUrls())
                                                         .connectionTimeout(Duration.ofSeconds(30))
                                                         .maxReconnects(-1)
                                                         .reconnectBufferSize(natsConfiguration.getConnectionByteBufferSize())
                                                         .build();
        try {
            return Nats.connect(connectionOptions);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private JetStream createJetstream() {
        try {
            return natsConnection.jetStream(JetStreamOptions.builder().publishNoAck(false).requestTimeout(Duration.ofSeconds(30)).build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
