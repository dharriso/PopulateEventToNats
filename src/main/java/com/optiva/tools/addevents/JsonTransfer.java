package com.optiva.tools.addevents;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonAttribute;
import com.dslplatform.json.JsonWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

@CompiledJson(onUnknown = CompiledJson.Behavior.IGNORE)
public class JsonTransfer {
    private int id;
    private int timeId;
    private int useId;
    private long activatedTime;
    private long time;
    private int group;
    private String transferState;
    private String exportState;
    private int count;
    private String state;
    private long stateTimeout;
    private String errors;
    private long events;

    private final DslJson<Object> dsl;
    private final JsonWriter writer;

    public JsonTransfer() {
        dsl =  new DslJson<>();
        writer = dsl.newWriter();
    }

    public JsonTransfer(final int id, final int timeId, final int useId, final long activatedTime,
                        final long time, final int group, final String transferState, final String exportState,
                        final int count, final String state, final long stateTimeout,
                        final String errors, final long events) {
        this.id = id;
        this.timeId = timeId;
        this.useId = useId;
        this.activatedTime = activatedTime;
        this.time = time;
        this.group = group;
        this.transferState = transferState;
        this.exportState = exportState;
        this.count = count;
        this.state = state;
        this.stateTimeout = stateTimeout;
        this.errors = errors;
        this.events = events;
        dsl =  new DslJson<>();
        writer = dsl.newWriter();
    }

    public void serialize(ByteArrayOutputStream output) throws IOException {
        dsl.serialize(this, output);
    }
    public JsonTransfer deserialize(ByteArrayInputStream input) throws IOException {
        return dsl.deserialize(JsonTransfer.class, input);
    }

    @JsonAttribute(index = 1)
    public int getId() {
        return id;
    }

    @JsonAttribute(index = 2)
    public int getTimeId() {
        return timeId;
    }

    @JsonAttribute(index = 3)
    public int getUseId() {
        return useId;
    }

    @JsonAttribute(index = 4)
    public long getActivatedTime() {
        return activatedTime;
    }
    @JsonAttribute(index = 5)
    public long getTime() {
        return time;
    }
    @JsonAttribute(index = 6)
    public int getGroup() {
        return group;
    }
    @JsonAttribute(index = 7)
    public String getTransferState() {
        return transferState;
    }

    @JsonAttribute(index = 8)
    public String getExportState() {
        return exportState;
    }

    @JsonAttribute(index = 9)
    public int getCount() {
        return count;
    }

    @JsonAttribute(index = 10)
    public String getState() {
        return state;
    }
    @JsonAttribute(index = 11)
    public long getStateTimeout() {
        return stateTimeout;
    }

    @JsonAttribute(index = 12)
    public String getErrors() {
        return errors;
    }

    @JsonAttribute(index = 13)
    public long getEvents() {
        return events;
    }

    public void setId(final int id) {
        this.id = id;
    }

    public void setTimeId(final int timeId) {
        this.timeId = timeId;
    }

    public void setUseId(final int useId) {
        this.useId = useId;
    }

    public void setActivatedTime(final long activatedTime) {
        this.activatedTime = activatedTime;
    }

    public void setTime(final long time) {
        this.time = time;
    }

    public void setGroup(final int group) {
        this.group = group;
    }

    public void setTransferState(final String transferState) {
        this.transferState = transferState;
    }

    public void setExportState(final String exportState) {
        this.exportState = exportState;
    }

    public void setCount(final int count) {
        this.count = count;
    }

    public void setState(final String state) {
        this.state = state;
    }

    public void setStateTimeout(final long stateTimeout) {
        this.stateTimeout = stateTimeout;
    }

    public void setErrors(final String errors) {
        this.errors = errors;
    }

    public void setEvents(final long events) {
        this.events = events;
    }
}
