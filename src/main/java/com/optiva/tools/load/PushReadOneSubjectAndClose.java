package com.optiva.tools.load;

import com.optiva.tools.addevents.NatsConfiguration;
import com.optiva.tools.addevents.NatsEventPublisher;
import com.optiva.tools.addevents.NatsReaderConfiguration;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

public class PushReadOneSubjectAndClose {
    private final NatsConfiguration natsConfiguration;
    private Connection connection;

    public PushReadOneSubjectAndClose(NatsConfiguration natsConfiguration) {
        this.natsConfiguration = natsConfiguration;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));
    }

    public void consume() {
        Dispatcher dispatcher = null;
        JetStreamSubscription pushSub = null;
        try {
            JetStream js = getConnection().jetStream(NatsEventPublisher.JET_STREAM_OPTIONS);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                                                            .filterSubject(natsConfiguration.getSubjectName())
                                                            .deliverPolicy(DeliverPolicy.All)
                                                            .replayPolicy(ReplayPolicy.Instant)
                                                            .ackPolicy(AckPolicy.None)
                                                            .build();

            PushSubscribeOptions pushSubscribeOptions = PushSubscribeOptions.builder().configuration(cc).ordered(true).build();
            dispatcher = connection.createDispatcher();

            pushSub = js.subscribe(natsConfiguration.getSubjectName(), dispatcher, new NatsMessageHandler(), false, pushSubscribeOptions);
            while (pushSub.isActive())
                ;

        } catch (Exception e) {
            System.out.printf("I/O error communicating to the NATS server :: %s%n", e.getLocalizedMessage());
        } finally {
            try {
                if (dispatcher != null) {
                    dispatcher.drain(Duration.ofMillis(100));
                }

                if (pushSub != null) {
                    pushSub.drain(Duration.ofMillis(100));
                }
            } catch (Exception e) {
                // ignore as we don't really care.
            }
            close();
            System.exit(0);
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
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (InterruptedException e) {
            connection = null;
            Thread.currentThread().interrupt();
        }
    }

    class NatsMessageHandler implements MessageHandler {

        /**
         * Called to deliver a message to the handler. This call is in the dispatcher's thread
         * and can block all other messages being delivered.
         *
         * <p>The thread used to call onMessage will be interrupted if the connection is closed, or the dispatcher is stopped.
         *
         * @param msg the received Message
         * @throws InterruptedException if the dispatcher interrupts this handler
         */
        @Override
        public void onMessage(Message msg) {
            System.out.println("  " + msg.metaData());
            msg.ack();
        }
    }
}