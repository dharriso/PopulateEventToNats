package com.optiva.tools.addevents;

public interface NatsConfiguration {

    String getSubjectName();

    String getDurableName();

    int getBatchSize();

    String getStreamName();

    /**
     * Returns the size of the connection buffer in bytes
     *
     * @return size of buffer in bytest
     */
    int getConnectionByteBufferSize();

}
