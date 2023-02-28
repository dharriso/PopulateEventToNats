package com.optiva.tools.addevents;

import java.util.Objects;

public final class NatsReaderConfiguration implements NatsConfiguration {
    private final String subjectName;
    private String[] url;
    private final String streamName;
    private final String durableName;
    private final Integer batchSize;
    private final Integer connectionByteBufferSize;

    /**
     * @param subjectName              subject that is used to extract messages from NATS
     * @param url                      NATS server endpoint
     * @param streamName               Name of the stream if we need to create it on the fly.
     * @param durableName              Durable name
     * @param batchSize                How many message to extract from NATs. This ensure we dont bload service
     * @param connectionByteBufferSize
     */
    public NatsReaderConfiguration(String subjectName,
                                   String[] url,
                                   String streamName,
                                   String durableName,
                                   int batchSize,
                                   int connectionByteBufferSize) {
        this.connectionByteBufferSize = connectionByteBufferSize;
        this.subjectName = Objects.requireNonNull(subjectName);
        this.durableName = Objects.requireNonNull(durableName);

        if (subjectName.isEmpty()) {
            throw new IllegalArgumentException("Subject Name must be a valid string");
        }
        if (durableName.isEmpty()) {
            throw new IllegalArgumentException("Durable Name must be a valid string");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be greater than zero");
        }

        this.streamName = streamName;
        this.batchSize = batchSize;
        this.url = url;
    }

    @Override
    public String getSubjectName() {
        return subjectName;
    }

    @Override
    public String getDurableName() {
        return durableName;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public String getStreamName() {
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

    public String[] getUrls() {
        return url;
    }
}
