package com.optiva.tools.addevents;

import com.dslplatform.json.DslJson;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class NatsEventSerializer {

    private static final NatsEventSerializer INSTANCE = new NatsEventSerializer();
    private final DslJson<Object> dsl;
    private NatsEventSerializer() {
        this.dsl = new DslJson<>();
    }

    public static NatsEventSerializer getInstance() {
        return INSTANCE;
    }

    public void serialize(JsonEvent message, ByteArrayOutputStream output) throws IOException {
        dsl.serialize(message, output);
    }

    public JsonEvent deserialize(ByteArrayInputStream input) throws IOException {
        return dsl.deserialize(JsonEvent.class, input);
    }
}
