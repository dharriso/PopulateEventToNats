package com.optiva.tools.addevents;

import java.util.Objects;

public final class NatsReaderConfiguration implements NatsConfiguration {
    private final String subjectName;
    private final boolean createEventStream;
    private final Integer numberOfConsumers;
    private final boolean readerEnabled;
    private final String filter;
    private final String url;
    private final String queueName;
    private final String streamName;
    private final String durableName;
    private final Integer batchSize;
    private final Integer numberOfReplicas;
    private final boolean isPullBasedConsumer;
    private final Integer connectionByteBufferSize;
    private final Integer initialMaxWaitTimeMs;
    private final Integer maxWaitTimeMs;

    /**
     *  @param subjectName   subject that is used to extract messages from NATS
     * @param filter        Filter a subset of the results returned from NATS
     * @param url    NATS server endpoint
     * @param queueName             Used for Push based subscriptions to ensure at-most-once delivery
     * @param streamName            Name of the stream if we need to create it on the fly.
     * @param durableName               Durable name
     * @param batchSize        How many message to extract from NATs. This ensure we dont bload service
     * @param numberOfReplicas              Only required if we create the stream on the fly
     * @param connectionByteBufferSize
     */
    public NatsReaderConfiguration(boolean readerEnabled, String subjectName, String filter,
                                   String subscriberType, boolean createEventStream,
                                   String url, String queueName,
                                   String streamName, String durableName,
                                   int batchSize, int numberOfReplicas, int numberOfConsumers,
                                   int connectionByteBufferSize,
                                   int initialMaxWaitTimeMs,
                                   int maxWaitTimeMs) {
        this.connectionByteBufferSize = connectionByteBufferSize;
        this.subjectName = Objects.requireNonNull(subjectName);
        this.durableName = Objects.requireNonNull(durableName);
        this.url = Objects.requireNonNull(url);;

        if (subjectName.isEmpty() ) {
            throw new IllegalArgumentException("Subject Name must be a valid string");
        }
        if (durableName.isEmpty() ) {
            throw new IllegalArgumentException("Durable Name must be a valid string");
        }
        if (url.isEmpty() ) {
            throw new IllegalArgumentException("NATS URL must be a valid string");
        }
        if (!(batchSize >0)) {
            throw new IllegalArgumentException("Batch size must be greater than zero");
        }
        this.readerEnabled = readerEnabled;
        this.filter = filter;
        this.queueName = queueName;
        this.streamName = streamName;
        this.batchSize = batchSize;
        this.numberOfReplicas = numberOfReplicas;
        this.createEventStream = createEventStream;
        isPullBasedConsumer = subscriberType == null || !subscriberType.equalsIgnoreCase("PUSH");
        this.numberOfConsumers = numberOfConsumers;
        this.initialMaxWaitTimeMs = initialMaxWaitTimeMs;
        this.maxWaitTimeMs = maxWaitTimeMs;
    }

    @Override
    public final boolean isReaderEnabled() {
        return readerEnabled;
    }

    @Override
    public final boolean isPullBasedConsumer() {
        return isPullBasedConsumer;
    }

    @Override
    public final boolean isCreateEventStream() {
        return createEventStream;
    }

    @Override
    public final String getSubjectName() {
        return subjectName;
    }

    @Override
    public final String getFilter() {
        return filter;
    }

    @Override
    public final String getDurableName() {
        return durableName;
    }

    @Override
    public final String getQueueName() {
        return queueName;
    }

    @Override
    public final String getUrl() {
        return url;
    }

    @Override
    public final int getNumberOfConsumers() {
        return numberOfConsumers;
    }

    @Override
    public final int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    @Override
    public final int getBatchSize() {
        return batchSize;
    }

    @Override
    public final String getStreamName() {
        return streamName;
    }
    /**
     * Returns the size of the connection buffer in bytes
     *
     * @return size of buffer in bytest
     */
    @Override
    public final int getConnectionByteBufferSize() {
        return connectionByteBufferSize;
    }
    /**
     * Get the initial wait time in ms for pull from nats
     *
     * @return initial wait time in milliseconds
     */
    @Override
    public final int getInitialMaxWaitTimeMs() {
        return initialMaxWaitTimeMs;
    }
    /**
     * Get the wait time in ms for pull from nats
     *
     * @return wait time in milliseconds
     */
    @Override
    public final int getMaxWaitTimeMs() {
        return maxWaitTimeMs;
    }
}
