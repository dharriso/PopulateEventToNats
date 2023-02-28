package com.optiva.tools;

import com.optiva.tools.addevents.NatsReader;

public class EventProperties {

    private static String natsEventSubjectName = "";
    private static String namespacePath = "";
    private static String namespace = "";

    private static String stream = "";

    public static String[] hosts = new String[0];
    private static String durableName = "";

    public static String getNatsEventSubjectName() {
        return natsEventSubjectName;
    }

    public static String getNamespacePath() {
        return namespacePath;
    }

    public static String getNamespace(Object namespacePath) {
        return namespace;
    }

    public static String getNatsEventStreamName() {
        return stream;
    }

    public static String getNatsEventDurableName() {
        return durableName;
    }

    public static int getNatsBatchSize() {
        return 256;
    }

    public static int getNatsConnectBufferByteSize() {
        return (20 * 1024 * 1024);
    }

    public static String[] getNatsHostsList() {
        return hosts;
    }

    public static void setAll(NatsReader reader) {
        natsEventSubjectName = reader.subjectName;
        hosts = reader.endpoint.trim().split(" ");
        durableName = reader.consumerName;
        stream = reader.streamName;

    }
}
