package com.optiva.tools.load;

import com.optiva.tools.addevents.JsonEvent;
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
import io.nats.client.api.ConsumerInfo;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduleBasedConsumer {
    private final NatsConfiguration natsConfiguration;
    private final ScheduledExecutorService executorService;

    public ScheduleBasedConsumer(NatsConfiguration natsConfiguration) {
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
        executorService.scheduleAtFixedRate(new ScheduleBasedConsumer.Consumer(), 1, 5, TimeUnit.SECONDS);
    }

    class Consumer implements Runnable {
        private int totalRead;
        private Connection connection;

        @Override
        public void run() {
            Instant start = Instant.now();
            Instant end;

            System.out.printf("Starting consumer on thread %s%n", Thread.currentThread().getName());
            totalRead = 0;
            try {
                JetStream js = getConnection().jetStream(NatsEventPublisher.JET_STREAM_OPTIONS);
                consumeMessages(js);
            } catch (Exception e) {
                close();
                System.out.printf("I/O error communicating to the NATS server :: %s%n", e.getLocalizedMessage());
            } finally {
                close();
                end = Instant.now();
                System.out.printf("Ending consumer on thread %s took %s ms %n", Thread.currentThread().getName(), Duration.between(start, end).toMillis());
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
            Options connectionOptions = new Options.Builder().servers(((NatsReaderConfiguration)natsConfiguration).getUrls())
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
        private void consumeMessages(JetStream js) throws JetStreamApiException, IOException {

            System.out.printf("Stream = %s , Consumer Name = %s, Subject Name = %s%n", natsConfiguration.getStreamName(), natsConfiguration.getDurableName(), natsConfiguration.getSubjectName());
            ConsumerInfo consumerInfo =
                    getConnection().jetStreamManagement(NatsEventPublisher.JET_STREAM_OPTIONS).getConsumerInfo(natsConfiguration.getStreamName(), natsConfiguration.getDurableName());
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder().configuration(consumerInfo.getConsumerConfiguration()).build();
            JetStreamSubscription pullSub = js.subscribe(natsConfiguration.getSubjectName(), pullOptions);
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
                pullSub.unsubscribe();
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
            List<Message> msgs = pullSub.fetch(calculateBatchSize(), Duration.ofMillis(250));
            if (msgs == null || msgs.isEmpty()) {
                return true;
            }

            for (Message msg : msgs) {
                msg.getData();
                totalRead++;
                msg.ack();
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
