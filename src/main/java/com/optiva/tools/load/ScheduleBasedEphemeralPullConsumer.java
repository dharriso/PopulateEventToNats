package com.optiva.tools.load;

import com.optiva.tools.addevents.NatsConfiguration;
import com.optiva.tools.addevents.NatsEventPublisher;
import com.optiva.tools.addevents.NatsReaderConfiguration;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class ScheduleBasedEphemeralPullConsumer {
    private final NatsConfiguration natsConfiguration;
    private final ScheduledExecutorService executorService;
    private AtomicLong totalMessagesRead = new AtomicLong(0);

    private Long startSequence = null;

    public ScheduleBasedEphemeralPullConsumer(NatsConfiguration natsConfiguration) {
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

        Thread consumerThread = new Thread(new ScheduleBasedEphemeralPullConsumer.Consumer());
        consumerThread.start();

        // wait for all threads to finish
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    class Consumer implements Runnable {
        private int totalRead;
        private Connection connection;

        @Override
        public void run() {
            totalRead = 0;
            try {
                JetStream js = getConnection().jetStream(NatsEventPublisher.JET_STREAM_OPTIONS);
                consumeMessages(getConnection(), js);
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
            ConsumerConfiguration cc = ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.All).build();

            PullSubscribeOptions pso = PullSubscribeOptions.builder().stream(natsConfiguration.getStreamName()).configuration(cc).build();
            JetStreamSubscription pullSub = js.subscribe(natsConfiguration.getSubjectName(), pso);
            System.out.printf("Subscription pendingMsgCount %s, dropped Count %s, Delivered Count %s %n ",
                              pullSub.getPendingMessageCount(),
                              pullSub.getDroppedCount(),
                              pullSub.getDeliveredCount());
            try {
                boolean noMoreMessages = false;
                while (!noMoreMessages) {
                    noMoreMessages = readBatch(pullSub);
                }
            } catch (Exception e) {
                System.out.printf("Error reading message of the NATS server %s%n", e.getLocalizedMessage());
            } finally {
                System.out.printf("Subscription pendingMsgCount %s, dropped Count %s, Delivered Count %s %n ",
                                  pullSub.getPendingMessageCount(),
                                  pullSub.getDroppedCount(),
                                  pullSub.getDeliveredCount());
                pullSub.drain(Duration.ofMillis(100));
                pullSub.unsubscribe();
                connection.drain(Duration.ofMillis(100));

                System.out.printf("Total Messages Read & Acked %s %n ", totalMessagesRead.get());
            }
        }

        /**
         * fetchBatch
         * fetches a batch of messages from NATS
         *
         * @param pullSub
         * @return true iff NATS has no more messages available.
         */
        private boolean readBatch(JetStreamSubscription pullSub) {
            List<Message> msgs = pullSub.fetch(1, Duration.ofMinutes(3));

            if (msgs == null || msgs.isEmpty()) {
                return true;
            }

            for (Message msg : msgs) {
                totalMessagesRead.incrementAndGet();
                msg.ack();
                startSequence = msg.metaData().streamSequence();
                totalRead++;
                System.out.println("  " + msg.metaData());
            }

            return false;
        }

        /**
         * Determines the size of a batch request sent to NATS.
         *
         * @return size of the batch to use in request to  NATS
         */
        private int calculateBatchSize() {
            int outStanding = natsConfiguration.getBatchSize() - totalRead;
            return Math.min(outStanding, 100);
        }
    }
}
