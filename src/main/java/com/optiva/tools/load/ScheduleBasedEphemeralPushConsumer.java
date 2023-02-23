package com.optiva.tools.load;

import com.optiva.tools.addevents.NatsConfiguration;
import com.optiva.tools.addevents.NatsEventPublisher;
import com.optiva.tools.addevents.NatsReaderConfiguration;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ScheduleBasedEphemeralPushConsumer {
    private final NatsConfiguration natsConfiguration;
    private final ScheduledExecutorService executorService;

    private Long startSequence = null;

    public ScheduleBasedEphemeralPushConsumer(NatsConfiguration natsConfiguration) {
        this.natsConfiguration = natsConfiguration;
        this.executorService = Executors.newScheduledThreadPool(8);

        Thread ensureExecutorsClosed = new Thread(() -> {
            System.out.println("Shutting down executors and closing the connections.");
            executorService.shutdown();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // we are ending the process anyway, no need to exit abnormally
            }
        });

        Runtime.getRuntime().addShutdownHook(ensureExecutorsClosed);

    }

    public void consume() {
        Thread consumerThread = new Thread(new Consumer());
        consumerThread.start();

        // wait for all threads to finish
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //executorService.scheduleAtFixedRate(new ScheduleBasedEphemeralPushConsumer.Consumer(), 0, 15, TimeUnit.SECONDS);
    }

    class Consumer implements Runnable {
        private Connection connection;
        private int totalRead = 0;

        @Override
        public void run() {
            try {
                JetStream js = getConnection().jetStream(NatsEventPublisher.JET_STREAM_OPTIONS);
                while (true) {
                    consumeMessages(getConnection(), js);
                }
            } catch (Exception e) {
                System.out.printf("I/O error communicating to the NATS server :: %s%n", e.getLocalizedMessage());
            } finally {
                close();
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
            Options connectionOptions = new Options.Builder().servers(((NatsReaderConfiguration) natsConfiguration).getUrls())
                                                             .connectionTimeout(Duration.ofSeconds(120))
                                                             .maxReconnects(-1)
                                                             .reconnectBufferSize(natsConfiguration.getConnectionByteBufferSize())
                                                             .turnOnAdvancedStats()
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
                    System.out.println(connection.getStatistics());
                    connection.close();
                    connection = null;
                }
            } catch (InterruptedException e) {
                connection = null;
                Thread.currentThread().interrupt();
            }
        }

        /**
         * Consume the message from NATS.
         * <p>
         * May require more than one iteration if the size is greater than the max size (256) of
         * a NATS pull batch.
         *
         * @param js
         * @throws JetStreamApiException
         * @throws IOException
         */
        /**
         * Consume the message from NATS.
         * <p>
         * May require more than one iteration if the size is greater than the max size (256) of
         * a NATS pull batch.
         *
         * @param js
         * @throws JetStreamApiException
         * @throws IOException
         */
        private void consumeMessages(Connection connection, JetStream js) throws JetStreamApiException, IOException, InterruptedException, TimeoutException {
            ConsumerConfiguration cc;
            if (startSequence == null) {
                cc = ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.All).headersOnly(true).maxDeliver(natsConfiguration.getBatchSize()).flowControl(Duration.ofMillis(1000)).build();
            } else {
                cc = ConsumerConfiguration.builder()
                                          .deliverPolicy(DeliverPolicy.ByStartSequence)
                                          .startSequence(startSequence + 1)
                                          .headersOnly(true)
                                          .maxDeliver(natsConfiguration.getBatchSize())
                                          .flowControl(Duration.ofMillis(1000))
                                          .build();
            }

            Dispatcher dispatcher = connection.createDispatcher();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().stream(natsConfiguration.getStreamName()).configuration(cc).build();
            JetStreamSubscription pushSub = js.subscribe(natsConfiguration.getSubjectName(), dispatcher, new NatsMessageHandler(), false, pso);
            dispatcher.unsubscribe(pushSub, natsConfiguration.getBatchSize());

            while (totalRead < dispatcher.getDeliveredCount()) {

            }


            dispatcher.drain(Duration.ofMillis(100));
            connection.drain(Duration.ofMillis(100));

            System.out.printf("Subscription pendingMsgCount %s, dropped Count %s, Delivered Count %s %n ",
                              pushSub.getPendingMessageCount(),
                              pushSub.getDroppedCount(),
                              pushSub.getDeliveredCount());
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
            public void onMessage(Message msg) throws InterruptedException {
                System.out.println("  " + msg.metaData());
                startSequence = msg.metaData().streamSequence();
                msg.ack();
                totalRead += 1;
            }
        }
    }

}
