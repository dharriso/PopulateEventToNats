package com.optiva.tools.addevents;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonAttribute;
import com.dslplatform.json.JsonWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

@CompiledJson(onUnknown = CompiledJson.Behavior.IGNORE, objectFormatPolicy = CompiledJson.ObjectFormatPolicy.MINIMAL)
public final class JsonEvent implements EventMessage, EventSerialization {
    private final DslJson<Object> dsl;
    private int timeId;
    private int useId;
    private long eventId;
    private String accessKey;
    // optional
    private Integer accessKeyType;
    private String owningCustomerID;
    private String rootCustomerID;
    private String composedCustomerID;
    private long eventType;
    private Date originalEventTime;
    private Date creationEventTime;
    private Date effectiveEventTime;
    private int billCycleID;
    private int billPeriodID;
    // optional
    private Integer errorCode;
    private String rateEventType;
    // optional
    private Long externalCorrelationID;
    private int rootCustomerIDHash;
    private String projectAddOn;
    // additional option
    private int refEventId;
    private int refUseId;
    // these are optional attributes
    private byte[] internalRatingRelevant0;
    private byte[] internalRatingRelevant1;
    private byte[] internalRatingRelevant2;
    private byte[] externalRatingIrrelevant0;
    private byte[] externalRatingIrrelevant1;
    private byte[] externalRatingIrrelevant2;
    private byte[] externalRatingResult0;
    private byte[] externalRatingResult1;
    private byte[] externalRatingResult2;
    private byte[] externalDataTransp0;
    private byte[] externalDataTransp1;
    private byte[] externalDataTransp2;
    private byte[] universalAttribute0;
    private byte[] universalAttribute1;
    private byte[] universalAttribute2;
    private byte[] universalAttribute3;
    private byte[] universalAttribute4;
    private int dayId;

    public JsonEvent() {
        dsl = new DslJson<>();
        JsonWriter writer = dsl.newWriter();
    }

    public void serialize(ByteArrayOutputStream output) throws IOException {
        dsl.serialize(this, output);
    }

    public JsonEvent deserialize(ByteArrayInputStream input) throws IOException {
        return dsl.deserialize(JsonEvent.class, input);
    }

    @JsonAttribute(name = "tid", index = 1)
    public int getTimeId() {
        return timeId;
    }

    public void setTimeId(final int timeId) {
        this.timeId = timeId;
    }

    @JsonAttribute(name = "uid", index = 2)
    public int getUseId() {
        return useId;
    }

    public void setUseId(final int useId) {
        this.useId = useId;
    }

    @JsonAttribute(name = "eid", index = 3)
    public long getEventId() {
        return eventId;
    }

    public void setEventId(final long eventId) {
        this.eventId = eventId;
    }

    @JsonAttribute(name = "ak", index = 4)
    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(final String accessKey) {
        if (null != accessKey) {
            this.accessKey = accessKey;
        }
    }

    @JsonAttribute(name = "akt", index = 19)
    public Integer getAccessKeyType() {
        return accessKeyType == null ? 0 : accessKeyType;
    }

    public void setAccessKeyType(final Integer accessKeyType) {
        if (null != accessKeyType) {
            this.accessKeyType = accessKeyType;
        }
    }

    public boolean hasAccessKeyType() {
        return accessKeyType != null;
    }

    @JsonAttribute(name = "ocid", index = 5)
    public String getOwningCustomerID() {
        return owningCustomerID;
    }

    public void setOwningCustomerID(final String owningCustomerID) {

        if (null != owningCustomerID) {
            this.owningCustomerID = owningCustomerID;
        }
    }

    @JsonAttribute(name = "rcid", index = 6)
    public String getRootCustomerID() {
        return rootCustomerID;
    }

    public void setRootCustomerID(final String rootCustomerID) {
        this.rootCustomerID = rootCustomerID;
    }

    @JsonAttribute(name = "ccid", index = 7)
    public String getComposedCustomerID() {
        return composedCustomerID;
    }

    public void setComposedCustomerID(final String composedCustomerID) {
        if (null != composedCustomerID) {
            this.composedCustomerID = composedCustomerID;
        }
    }

    @JsonAttribute(name = "et", index = 8)
    public long getEventType() {
        return eventType;
    }

    public void setEventType(final long eventType) {
        this.eventType = eventType;
    }

    @JsonAttribute(name = "oet", index = 9)
    public Date getOriginalEventTime() {
        return originalEventTime;
    }

    public void setOriginalEventTime(final Date originalEventTime) {
        if (null != originalEventTime) {
            this.originalEventTime = originalEventTime;
        }
    }

    @JsonAttribute(name = "cet", index = 10)
    public Date getCreationEventTime() {
        return creationEventTime;
    }

    public void setCreationEventTime(final Date creationEventTime) {
        if (null != creationEventTime) {
            this.creationEventTime = creationEventTime;
        }
    }

    @JsonAttribute(name = "eet", index = 11)
    public Date getEffectiveEventTime() {
        return effectiveEventTime;
    }

    public void setEffectiveEventTime(final Date effectiveEventTime) {
        if (null != effectiveEventTime) {
            this.effectiveEventTime = effectiveEventTime;
        }
    }

    @JsonAttribute(name = "bcid", index = 12)
    public int getBillCycleID() {
        return billCycleID;
    }

    public void setBillCycleID(final int billCycleID) {
        this.billCycleID = billCycleID;
    }

    @JsonAttribute(name = "bpid", index = 13)
    public int getBillPeriodID() {
        return billPeriodID;
    }

    public void setBillPeriodID(final int billPeriodID) {
        this.billPeriodID = billPeriodID;
    }

    @JsonAttribute(name = "ec", index = 20)
    public Integer getErrorCode() {
        return errorCode == null ? 0 : errorCode;
    }

    public void setErrorCode(final Integer errorCode) {
        if (null != errorCode) {
            this.errorCode = errorCode;
        }
    }

    public boolean hasErrorCode() {
        return errorCode != null;
    }

    @JsonAttribute(name = "ret", index = 14)
    public String getRateEventType() {
        return rateEventType;
    }

    public void setRateEventType(final String rateEventType) {
        if (null != rateEventType) {
            this.rateEventType = rateEventType;
        }
    }

    @JsonAttribute(name = "ecid", index = 18)
    public Long getExternalCorrelationID() {
        return externalCorrelationID == null ? 0L : externalCorrelationID;
    }

    public void setExternalCorrelationID(final Long externalCorrelationID) {
        if (null != externalCorrelationID) {
            this.externalCorrelationID = externalCorrelationID;
        }
    }

    public boolean hasCorrelationId() {
        return externalCorrelationID != null;
    }

    @JsonAttribute(name = "rcidh", index = 15)
    public int getRootCustomerIDHash() {
        return rootCustomerIDHash;
    }

    public void setRootCustomerIDHash(final int rootCustomerIDHash) {
        this.rootCustomerIDHash = rootCustomerIDHash;
    }

    @JsonAttribute(name = "pao", index = 39)
    public String getProjectAddOn() {
        return projectAddOn;
    }

    public void setProjectAddOn(final String projectAddOn) {
        if (null != projectAddOn) {
            this.projectAddOn = projectAddOn;
        }
    }

    @JsonAttribute(name = "reid", index = 16)
    public int getRefEventId() {
        return refEventId;
    }

    public void setRefEventId(final int refEventId) {
        this.refEventId = refEventId;
    }

    @JsonAttribute(name = "ruid", index = 17)
    public int getRefUseId() {
        return refUseId;
    }

    public void setRefUseId(final int refUseId) {
        this.refUseId = refUseId;
    }

    @JsonAttribute(name = "irr0", index = 21)
    public byte[] getInternalRatingRelevant0() {
        return internalRatingRelevant0;
    }

    public void setInternalRatingRelevant0(final byte[] internalRatingRelevant0) {
        if (null != internalRatingRelevant0) {
            this.internalRatingRelevant0 = internalRatingRelevant0;
        }
    }

    @JsonAttribute(name = "irr1", index = 22)
    public byte[] getInternalRatingRelevant1() {
        return internalRatingRelevant1;
    }

    public void setInternalRatingRelevant1(final byte[] internalRatingRelevant1) {
        if (null != internalRatingRelevant1) {
            this.internalRatingRelevant1 = internalRatingRelevant1;
        }
    }

    @JsonAttribute(name = "irr2", index = 23)
    public byte[] getInternalRatingRelevant2() {
        return internalRatingRelevant2;
    }

    public void setInternalRatingRelevant2(final byte[] internalRatingRelevant2) {
        if (null != internalRatingRelevant2) {
            this.internalRatingRelevant2 = internalRatingRelevant2;
        }
    }

    @JsonAttribute(name = "eri0", index = 24)
    public byte[] getExternalRatingIrrelevant0() {
        return externalRatingIrrelevant0;
    }

    public void setExternalRatingIrrelevant0(final byte[] externalRatingIrrelevant0) {
        if (null != externalRatingIrrelevant0) {
            this.externalRatingIrrelevant0 = externalRatingIrrelevant0;
        }
    }

    @JsonAttribute(name = "eri1",index = 25)
    public byte[] getExternalRatingIrrelevant1() {
        return externalRatingIrrelevant1;
    }

    public void setExternalRatingIrrelevant1(final byte[] externalRatingIrrelevant1) {
        if (null != externalRatingIrrelevant1) {
            this.externalRatingIrrelevant1 = externalRatingIrrelevant1;
        }
    }

    @JsonAttribute(name = "eri2",index = 26)
    public byte[] getExternalRatingIrrelevant2() {
        return externalRatingIrrelevant2;
    }

    public void setExternalRatingIrrelevant2(final byte[] externalRatingIrrelevant2) {
        if (null != externalRatingIrrelevant2) {
            this.externalRatingIrrelevant2 = externalRatingIrrelevant2;
        }
    }

    @JsonAttribute(name = "err0",index = 27)
    public byte[] getExternalRatingResult0() {
        return externalRatingResult0;
    }

    public void setExternalRatingResult0(final byte[] externalRatingResult0) {
        if (null != externalRatingResult0) {
            this.externalRatingResult0 = externalRatingResult0;
        }
    }

    @JsonAttribute(name = "err1",index = 28)
    public byte[] getExternalRatingResult1() {
        return externalRatingResult1;
    }

    public void setExternalRatingResult1(final byte[] externalRatingResult1) {
        if (null != externalRatingResult1) {
            this.externalRatingResult1 = externalRatingResult1;
        }
    }

    @JsonAttribute(name = "err2",index = 29)
    public byte[] getExternalRatingResult2() {
        return externalRatingResult2;
    }

    public void setExternalRatingResult2(final byte[] externalRatingResult2) {
        if (null != externalRatingResult2) {
            this.externalRatingResult2 = externalRatingResult2;
        }
    }

    @JsonAttribute(name = "edt0",index = 30)
    public byte[] getExternalDataTransp0() {
        return externalDataTransp0;
    }

    public void setExternalDataTransp0(final byte[] externalDataTransp0) {
        if (null != externalDataTransp0) {
            this.externalDataTransp0 = externalDataTransp0;
        }
    }

    @JsonAttribute(name = "edt1",index = 31)
    public byte[] getExternalDataTransp1() {
        return externalDataTransp1;
    }

    public void setExternalDataTransp1(final byte[] externalDataTransp1) {
        if (null != externalDataTransp1) {
            this.externalDataTransp1 = externalDataTransp1;
        }
    }

    @JsonAttribute(name = "edt2",index = 32)
    public byte[] getExternalDataTransp2() {
        return externalDataTransp2;
    }

    public void setExternalDataTransp2(final byte[] externalDataTransp2) {
        if (null != externalDataTransp2) {
            this.externalDataTransp2 = externalDataTransp2;
        }
    }

    @JsonAttribute(name = "ua0",index = 33)
    public byte[] getUniversalAttribute0() {
        return universalAttribute0;
    }

    public void setUniversalAttribute0(final byte[] universalAttribute0) {
        if (null != universalAttribute0) {
            this.universalAttribute0 = universalAttribute0;
        }
    }

    @JsonAttribute(name = "ua1",index = 34)
    public byte[] getUniversalAttribute1() {
        return universalAttribute1;
    }

    public void setUniversalAttribute1(final byte[] universalAttribute1) {
        if (null != universalAttribute1) {
            this.universalAttribute1 = universalAttribute1;
        }
    }

    @JsonAttribute(name = "ua2",index = 35)
    public byte[] getUniversalAttribute2() {
        return universalAttribute2;
    }

    public void setUniversalAttribute2(final byte[] universalAttribute2) {
        if (null != universalAttribute2) {
            this.universalAttribute2 = universalAttribute2;
        }
    }

    @JsonAttribute(name = "ua3",index = 36)
    public byte[] getUniversalAttribute3() {
        return universalAttribute3;
    }

    public void setUniversalAttribute3(final byte[] universalAttribute3) {
        if (null != universalAttribute3) {
            this.universalAttribute3 = universalAttribute3;
        }
    }

    @JsonAttribute(name = "ua4",index = 37)
    public byte[] getUniversalAttribute4() {
        return universalAttribute4;
    }

    public void setUniversalAttribute4(final byte[] universalAttribute4) {
        if (null != universalAttribute4) {
            this.universalAttribute4 = universalAttribute4;
        }
    }

    @JsonAttribute(name = "did",index = 38)
    public int getDayId() {
        return dayId;
    }

    @Override
    public byte[] getInternalRatingRelevant() {
        return new byte[0];
    }

    @Override
    public byte[] getExternalRatingIrrelevant() {
        return new byte[0];
    }

    @Override
    public byte[] getExternalRatingResult() {
        return new byte[0];
    }

    @Override
    public byte[] getExternalDataTransp() {
        return new byte[0];
    }

    public void setDayId(final int dayId) {
        this.dayId = dayId;
    }

    public  JsonEvent withTimeId(int timeId) {
        this.timeId = timeId;
        return this;
    }

    public  JsonEvent withUseId(int useId) {
        this.useId = useId;
        return this;
    }

    public  JsonEvent withEventId(long eventId) {
        this.eventId = eventId;
        return this;
    }

    public  JsonEvent withAccessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public  JsonEvent withAccessKeyType(int accessKeyType) {
        this.accessKeyType = accessKeyType;
        return this;
    }

    public  JsonEvent withOwningCustomerID(String owningCustomerID) {
        this.owningCustomerID = owningCustomerID;
        return this;
    }

    public  JsonEvent withRootCustomerID(String rootCustomerID) {
        this.rootCustomerID = rootCustomerID;
        return this;
    }

    public  JsonEvent withComposedCustomerID(String composedCustomerID) {
        this.composedCustomerID = composedCustomerID;
        return this;
    }

    public  JsonEvent withEventType(long eventType) {
        this.eventType = eventType;
        return this;
    }

    public  JsonEvent withOriginalEventTime(Date originalEventTime) {
        this.originalEventTime = originalEventTime;
        return this;
    }

    public  JsonEvent withCreationEventTime(Date creationEventTime) {
        this.creationEventTime = creationEventTime;
        // '2009-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
        Calendar cal = Calendar.getInstance();
        cal.setTime(creationEventTime);
        dayId = cal.get(Calendar.DAY_OF_MONTH);
        return this;
    }

    public  JsonEvent withEffectiveEventTime(Date effectiveEventTime) {
        this.effectiveEventTime = effectiveEventTime;
        return this;

    }

    public  JsonEvent withBillCycleID(int billCycleID) {
        this.billCycleID = billCycleID;
        return this;
    }

    public  JsonEvent withBillPeriodID(int billPeriodID) {
        this.billPeriodID = billPeriodID;
        return this;
    }

    public  JsonEvent withErrorCode(int errorCode) {
        this.errorCode = errorCode;
        return this;
    }

    public  JsonEvent withRateEventType(String rateEventType) {
        this.rateEventType = rateEventType;
        return this;
    }

    public  JsonEvent withExternalCorrelationID(long externalCorrelationID) {
        this.externalCorrelationID = externalCorrelationID;
        return this;
    }

    public JsonEvent withRootCustomerIDHash(int rootCustomerIDHash) {
        this.rootCustomerIDHash = rootCustomerIDHash;
        return this;
    }

    public JsonEvent withProjectAddOn(String projectAddOn) {
        this.projectAddOn = projectAddOn;
        return this;
    }

    public JsonEvent withRefEventId(int refEventId) {
        this.refEventId = refEventId;
        return this;
    }

    public JsonEvent withRefUseId(int refUseId) {
        this.refUseId = refUseId;
        return this;
    }

    public JsonEvent withInternalRatingRelevant0(byte[] internalRatingRelevant0) {
        this.internalRatingRelevant0 = internalRatingRelevant0;
        return this;
    }

    public JsonEvent withInternalRatingRelevant1(byte[] internalRatingRelevant1) {
        this.internalRatingRelevant1 = internalRatingRelevant1;
        return this;
    }

    public JsonEvent withInternalRatingRelevant2(byte[] internalRatingRelevant2) {
        this.internalRatingRelevant2 = internalRatingRelevant2;
        return this;
    }

    public JsonEvent withExternalRatingIrrelevant0(byte[] externalRatingIrrelevant0) {
        this.externalRatingIrrelevant0 = externalRatingIrrelevant0;
        return this;
    }

    public JsonEvent withExternalRatingIrrelevant1(byte[] externalRatingIrrelevant1) {
        this.externalRatingIrrelevant1 = externalRatingIrrelevant1;
        return this;
    }

    public JsonEvent withExternalRatingIrrelevant2(byte[] externalRatingIrrelevant2) {
        this.externalRatingIrrelevant2 = externalRatingIrrelevant2;
        return this;
    }

    public JsonEvent withExternalRatingResult0(byte[] externalRatingResult0) {
        this.externalRatingResult0 = externalRatingResult0;
        return this;
    }

    public JsonEvent withExternalRatingResult1(byte[] externalRatingResult1) {
        this.externalRatingResult1 = externalRatingResult1;
        return this;
    }

    public JsonEvent withExternalRatingResult2(byte[] externalRatingResult2) {
        this.externalRatingResult2 = externalRatingResult2;
        return this;
    }

    public JsonEvent withExternalDataTransp0(byte[] externalDataTransp0) {
        this.externalDataTransp0 = externalDataTransp0;
        return this;
    }

    public JsonEvent withExternalDataTransp1(byte[] externalDataTransp1) {
        this.externalDataTransp1 = externalDataTransp1;
        return this;
    }

    public JsonEvent withExternalDataTransp2(byte[] externalDataTransp2) {
        this.externalDataTransp2 = externalDataTransp2;
        return this;
    }

    public JsonEvent withUniversalAttribute0(byte[] universalAttribute0) {
        this.universalAttribute0 = universalAttribute0;
        return this;
    }

    public JsonEvent withUniversalAttribute1(byte[] universalAttribute1) {
        this.universalAttribute1 = universalAttribute1;
        return this;
    }

    public JsonEvent withUniversalAttribute2(byte[] universalAttribute2) {
        this.universalAttribute2 = universalAttribute2;
        return this;
    }

    public JsonEvent withUniversalAttribute3(byte[] universalAttribute3) {
        this.universalAttribute3 = universalAttribute3;
        return this;
    }

    public JsonEvent withUniversalAttribute4(byte[] universalAttribute4) {
        this.universalAttribute4 = universalAttribute4;
        return this;
    }

    @Override
    public String toString() {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            this.serialize(baos);
            return baos.toString();
        } catch (IOException e) {
            return "";
        }

    }
}
