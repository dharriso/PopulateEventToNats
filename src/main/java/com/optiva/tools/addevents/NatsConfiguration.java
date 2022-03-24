package com.optiva.tools.addevents;

public interface NatsConfiguration {
    public boolean isReaderEnabled();
    public boolean isPullBasedConsumer();
    public boolean isCreateEventStream();
    public String getSubjectName();
    public String getFilter();
    public String getDurableName();
    public String getQueueName();
    public String getUrl();
    public int getNumberOfConsumers();
    public int getNumberOfReplicas();
    public int getBatchSize();
    public String getStreamName();
    /**
     * Returns the size of the connection buffer in bytes
     *
     * @return size of buffer in bytest
     */
    public int getConnectionByteBufferSize();
    /**
     * Get the initial wait time in ms for pull from nats
     *
     * @return initial wait time in milliseconds
     */
    public int getInitialMaxWaitTimeMs();
    /**
     * Get the wait time in ms for pull from nats
     *
     * @return wait time in milliseconds
     */
    public int getMaxWaitTimeMs();
}
