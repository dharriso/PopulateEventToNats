package com.optiva.tools.load;

import com.optiva.tools.addevents.ConvertKeyTo;
import com.optiva.tools.addevents.JsonEvent;
import com.optiva.tools.addevents.NatsConfiguration;
import com.optiva.tools.addevents.NatsEventException;
import com.optiva.tools.addevents.NatsEventPublisher;
import com.optiva.tools.addevents.NatsEventSerializer;
import com.optiva.tools.addevents.Period;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load Test Class that will execute at a fixed number of messages per second, across a specified number of publishers, for a fixed amount of time,
 * <p>
 * Minimum of 1 message per second and for a minimum of 1 minute
 */
public class TimebasedLoader {

    private final Random rnd = new Random(System.currentTimeMillis());
    private final ScheduledExecutorService executorService;
    private final int lengthOfRunInMinutes;
    private final int messagesPerSecond;
    private final NatsConfiguration natsConfiguration;
    private final int numberOfPublishers;
    private final AtomicLong idSequence = new AtomicLong(1);
    Timer timer = new Timer();
    TimerTask endRun = new TimerTask() {
        @Override
        public void run() {
            executorService.shutdown();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // we are ending the process anyway, no need to exit abnormally
            }
            System.exit(0);
        }
    };
    private List<NatsEventPublisher> publishers;

    public TimebasedLoader(int lengthOfRunInMinutes, int messagesPerSecond, int numberOfPublishers, NatsConfiguration config) {
        this.lengthOfRunInMinutes = lengthOfRunInMinutes;
        this.messagesPerSecond = messagesPerSecond;
        this.natsConfiguration = config;
        this.numberOfPublishers = numberOfPublishers;
        this.executorService = Executors.newSingleThreadScheduledExecutor();

        Thread ensureExecutorsClosed = new Thread(() -> {
            executorService.shutdown();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // we are ending the process anyway, no need to exit abnormally
            }
        });

        Runtime.getRuntime().addShutdownHook(ensureExecutorsClosed);
    }

    public void executeLoad() {
        publishers = new ArrayList<>(numberOfPublishers);
        for (int i = 0; i < numberOfPublishers; i++) {
            publishers.add(new NatsEventPublisher(natsConfiguration));
        }
        timer.schedule(endRun, new Date(System.currentTimeMillis() + (lengthOfRunInMinutes * (60 * 1000))));
        executorService.scheduleAtFixedRate(new MultiThreadedPublisher(numberOfPublishers, messagesPerSecond), 10, 1000, TimeUnit.MILLISECONDS);
    }

    class MultiThreadedPublisher implements Runnable {
        private final int numberOfPublishers;
        private final int numberOfMessages;
        private final ExecutorService publisherExecutorService;

        public MultiThreadedPublisher(int numberOfPublishers, int numberOfMessages) {
            this.numberOfPublishers = numberOfPublishers;
            this.numberOfMessages = numberOfMessages;
            publisherExecutorService = Executors.newFixedThreadPool(numberOfPublishers);
        }

        @Override
        public void run() {

            for (int p = 0; p < numberOfPublishers; p++) {
                NatsEventPublisher publisher = publishers.get(p);
                publisherExecutorService.execute(() -> {
                    for (int i = 0; i < numberOfMessages; i++) {
                        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                            JsonEvent event = getEvent();
                            String subject = String.format(natsConfiguration.getSubjectName(), ConvertKeyTo.bucketedCRC16("" + rnd.nextLong(), 65535));
                            NatsEventSerializer.getInstance().serialize(event, baos);
                            publisher.publish(baos, subject);
                        } catch (IOException | NatsEventException e) {
                            System.out.println(e.getMessage());
                        }
                    }
                });
            }
        }

        private JsonEvent getEvent() {
            Period period = Period.now();
            Date now = java.util.Calendar.getInstance().getTime();
            return new JsonEvent().withTimeId((int)period.getTimeId())
                                  .withUseId((int)period.getUseId())
                                  .withEventId(System.currentTimeMillis() + idSequence.incrementAndGet())
                                  .withInternalRatingRelevant0(RandomStringUtils.randomAlphabetic(1000).getBytes(StandardCharsets.UTF_8))
                                  .withAccessKey("0")
                                  .withOwningCustomerID("owningCustomerID")
                                  .withRootCustomerID(""+period.getTimeId())
                                  .withEventType(40000)
                                  .withOriginalEventTime(now)
                                  .withCreationEventTime(now)
                                  .withEffectiveEventTime(now)
                                  .withBillCycleID(1)
                                  .withBillPeriodID(98)
                                  .withRateEventType("rateEventType")
                                  .withRootCustomerIDHash(ConvertKeyTo.bucketedCRC16(""+period.getTimeId(), 256));
        }
    }
}
