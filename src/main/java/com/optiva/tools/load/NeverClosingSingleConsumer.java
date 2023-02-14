package com.optiva.tools.load;

import com.optiva.tools.addevents.NatsConfiguration;
import com.optiva.tools.addevents.NatsEventPublisher;
import com.optiva.tools.addevents.NatsReaderConfiguration;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class NeverClosingSingleConsumer {
    private final NatsConfiguration natsConfiguration;
    private Connection connection;
    private JetStreamSubscription pullSub;

    public NeverClosingSingleConsumer(NatsConfiguration natsConfiguration) {
        this.natsConfiguration = natsConfiguration;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));
    }

    public void consume() {
        try {
            JetStream js = getConnection().jetStream(NatsEventPublisher.JET_STREAM_OPTIONS);
            System.out.printf("Stream = %s , Consumer Name = %s, Subject Name = %s%n", natsConfiguration.getStreamName(), natsConfiguration.getDurableName(), natsConfiguration.getSubjectName());
            ConsumerInfo consumerInfo =
                    getConnection().jetStreamManagement(NatsEventPublisher.JET_STREAM_OPTIONS).getConsumerInfo(natsConfiguration.getStreamName(), natsConfiguration.getDurableName());
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder().configuration(consumerInfo.getConsumerConfiguration()).build();
            pullSub = js.subscribe(natsConfiguration.getSubjectName(), pullOptions);

            while (true) {
                consumeMessages();
                System.out.printf("Subscription pendingMsgCount %s, dropped Count %s, Delivered Count %s %n ",
                                  pullSub.getPendingMessageCount(),
                                  pullSub.getDroppedCount(),
                                  pullSub.getDeliveredCount());
                Thread.sleep(1000);

            }
        } catch (Exception e) {
            close();
            System.out.printf("I/O error communicating to the NATS server :: %s%n", e.getLocalizedMessage());
        }
    }

    private Connection getConnection() {
        if (connection != null && connection.getStatus() != Connection.Status.CONNECTED) {
            close();
            connection = null;
        }
        return connection != null ? connection : createNatsConnection();
    }

    private Connection createNatsConnection() {
        Arrays.stream(((NatsReaderConfiguration) natsConfiguration).getUrls()).forEach(System.out::println);
        Options connectionOptions = new Options.Builder().servers(((NatsReaderConfiguration) natsConfiguration).getUrls())
                                                         .connectionTimeout(Duration.ofSeconds(30))
                                                         .maxReconnects(-1)
                                                         .reconnectBufferSize(natsConfiguration.getConnectionByteBufferSize())
                                                         .build();
        try {
            connection = Nats.connect(connectionOptions);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        return connection;
    }

    private void close() {
        try {
            if (pullSub != null) {
                pullSub.unsubscribe();
            }
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (InterruptedException e) {
            connection = null;
            Thread.currentThread().interrupt();
        }
    }

    private void consumeMessages() {
        List<Message> messages = pullSub.fetch(natsConfiguration.getBatchSize(), Duration.ofSeconds(1));

        if (messages != null && !messages.isEmpty()) {
            for (Message msg : messages) {
                msg.getData();
                msg.ack();
            }
        }
    }
}