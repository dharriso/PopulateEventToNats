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
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public class ScheduleBasedEphemeralPullConsumerIterateOverSubjects {
    private final NatsConfiguration natsConfiguration;

    public ScheduleBasedEphemeralPullConsumerIterateOverSubjects(NatsConfiguration natsConfiguration) {
        this.natsConfiguration = natsConfiguration;
    }

    public void consume() {
        Thread consumerThread = new Thread(new ScheduleBasedEphemeralPullConsumerIterateOverSubjects.Consumer());
        consumerThread.start();
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    class Consumer implements Runnable {
        private Connection connection;
        @Override
        public void run() {
            try {
                Options connectionOptions = new Options.Builder().servers(((NatsReaderConfiguration) natsConfiguration).getUrls())
                                                                 .connectionTimeout(Duration.ofMinutes(3))
                                                                 .maxReconnects(-1)
                                                                 .reconnectBufferSize(natsConfiguration.getConnectionByteBufferSize())
                                                                 .turnOnAdvancedStats()
                                                                 .traceConnection()
                                                                 .pedantic()
                                                                 .build();
                connection = Nats.connect(connectionOptions);
                JetStream js = connection.jetStream(NatsEventPublisher.JET_STREAM_OPTIONS);
                consumeMessages(js);
            } catch (Exception e) {
                System.out.printf("I/O error communicating to the NATS server :: %s%n", e.getLocalizedMessage());
            } finally {
                try {
                    connection.close();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void consumeMessages(JetStream js) {
            for (int i = 0; i < 65535; i++) {
                String subjectName = natsConfiguration.getSubjectName() + i;
                try {
                    ConsumerConfiguration cc = ConsumerConfiguration.builder()
                                                                    .filterSubject(subjectName)
                                                                    .deliverPolicy(DeliverPolicy.All)
                                                                    .replayPolicy(ReplayPolicy.Instant)
                                                                    .ackPolicy(AckPolicy.Explicit)
                                                                    .build();

                    PullSubscribeOptions pso = PullSubscribeOptions.builder().stream(natsConfiguration.getStreamName()).configuration(cc).build();
                    JetStreamSubscription pullSub = js.subscribe(subjectName, pso);
                    List<Message> msgs = pullSub.fetch(10, Duration.ofSeconds(30));
                    if (msgs != null && !msgs.isEmpty()) {
                        for (Message msg : msgs) {
                            msg.ack();
                            System.out.printf("Current Subject %s, metadata= %s%n  ", subjectName, msg.metaData());
                        }
                    }
                    pullSub.drain(Duration.ofSeconds(30));
                    pullSub.unsubscribe();
                } catch (Exception e) {
                    System.err.printf("Error reading message of the NATS server %s%n", e.getLocalizedMessage());
                }
            }
        }
    }
}
