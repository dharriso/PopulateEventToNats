package com.optiva.tools.addevents;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonAttribute;
import com.dslplatform.json.JsonWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

@CompiledJson(onUnknown = CompiledJson.Behavior.IGNORE)
public final class JsonEvent implements EventMessage, EventSerialization {
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
    private  String projectAddOn;
    // additional option
    private int refEventId;
    private int refUseId;
    // these are optional attributes
    private byte[] internalRatingRelevant;
    private byte[] externalRatingIrrelevant;
    private byte[] externalRatingResult;
    private byte[] externalDataTransp;
    private byte[] universalAttribute0;
    private byte[] universalAttribute1;
    private byte[] universalAttribute2;
    private byte[] universalAttribute3;
    private byte[] universalAttribute4;
    private int dayId;

    private final DslJson<Object> dsl;
    private final JsonWriter writer;

    public JsonEvent() {
       dsl =  new DslJson<>();
       writer = dsl.newWriter();
    }

    public void serialize(ByteArrayOutputStream output) throws IOException {
        dsl.serialize(this, output);
    }
    public JsonEvent deserialize(ByteArrayInputStream input) throws IOException {
        return dsl.deserialize(JsonEvent.class, input);
    }

    @JsonAttribute(index = 1)
    public int getTimeId() {
        return timeId;
    }

    @JsonAttribute(index = 2)
    public int getUseId() {
        return useId;
    }

    @JsonAttribute(index = 3)
    public long getEventId() {
        return eventId;
    }

    @JsonAttribute(index = 4)
    public String getAccessKey() {
        return accessKey;
    }

    @JsonAttribute(index = 19)
    public Integer getAccessKeyType() {
        return accessKeyType==null?0:accessKeyType;
    }

    public boolean hasAccessKeyType() {
        return accessKeyType != null;
    }

    @JsonAttribute(index = 5)
    public String getOwningCustomerID() {
        return owningCustomerID;
    }

    @JsonAttribute(index = 6)
    public String getRootCustomerID() {
        return rootCustomerID;
    }

    @JsonAttribute(index = 7)
    public String getComposedCustomerID() {
        return composedCustomerID;
    }

    @JsonAttribute(index = 8)
    public long getEventType() {
        return eventType;
    }

    @JsonAttribute(index = 9)
    public Date getOriginalEventTime() {
        return originalEventTime;
    }

    @JsonAttribute(index = 10)
    public Date getCreationEventTime() {
        return creationEventTime;
    }

    @JsonAttribute(index = 11)
    public Date getEffectiveEventTime() {
        return effectiveEventTime;
    }

    @JsonAttribute(index = 12)
    public int getBillCycleID() {
        return billCycleID;
    }

    @JsonAttribute(index = 13)
    public int getBillPeriodID() {
        return billPeriodID;
    }

    @JsonAttribute(index = 20)
    public Integer getErrorCode() {
        return errorCode==null?0:errorCode;
    }

    public boolean hasErrorCode() {
        return errorCode != null;
    }

    @JsonAttribute(index = 14)
    public String getRateEventType() {
        return rateEventType;
    }

    @JsonAttribute(index = 18)
    public Long getExternalCorrelationID() {
        return externalCorrelationID==null?0L:externalCorrelationID;
    }
    public boolean hasCorrelationId() {
        return externalCorrelationID != null;
    }

    @JsonAttribute(index = 15)
    public int getRootCustomerIDHash() {
        return rootCustomerIDHash;
    }

    public String getProjectAddOn() {
        return projectAddOn;
    }

    @JsonAttribute(index = 16)
    public int getRefEventId() {
        return refEventId;
    }

    @JsonAttribute(index = 17)
    public int getRefUseId() {
        return refUseId;
    }

    public byte[] getInternalRatingRelevant() {
        return internalRatingRelevant;
    }

    public byte[] getExternalRatingIrrelevant() {
        return externalRatingIrrelevant;
    }

    public byte[] getExternalRatingResult() {
        return externalRatingResult;
    }

    public byte[] getExternalDataTransp() {
        return externalDataTransp;
    }

    public byte[] getUniversalAttribute0() {
        return universalAttribute0;
    }

    public byte[] getUniversalAttribute1() {
        return universalAttribute1;
    }

    public byte[] getUniversalAttribute2() {
        return universalAttribute2;
    }

    public byte[] getUniversalAttribute3() {
        return universalAttribute3;
    }

    public byte[] getUniversalAttribute4() {
        return universalAttribute4;
    }

    JsonEvent withTimeId(int timeId) {
        this.timeId = timeId;
        return this;
    }

    JsonEvent withUseId(int useId) {
        this.useId = useId;
        return this;
    }

    JsonEvent withEventId(long eventId) {
        this.eventId = eventId;
        return this;
    }

    JsonEvent withAccessKey(String accessKey){
        this.accessKey = accessKey;
        return this;
    }

    JsonEvent withAccessKeyType(int accessKeyType){
        this.accessKeyType = accessKeyType;
        return this;
    }

    JsonEvent withOwningCustomerID(String owningCustomerID){
        this.owningCustomerID = owningCustomerID;
        return this;
    }

    JsonEvent withRootCustomerID(String rootCustomerID){
        this.rootCustomerID = rootCustomerID;
        return this;
    }

    JsonEvent withComposedCustomerID(String composedCustomerID){
        this.composedCustomerID = composedCustomerID;
        return this;
    }

    JsonEvent withEventType(long eventType){
        this.eventType = eventType;
        return this;
    }

    JsonEvent withOriginalEventTime(Date originalEventTime){
        this.originalEventTime = originalEventTime;
        return this;
    }

    JsonEvent withCreationEventTime(Date creationEventTime){
        this.creationEventTime = creationEventTime;
        // '2009-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
        Calendar cal = Calendar.getInstance();
        cal.setTime(creationEventTime);
        dayId = cal.get(Calendar.DAY_OF_MONTH);
        return this;
    }

    JsonEvent withEffectiveEventTime(Date effectiveEventTime){
        this.effectiveEventTime = effectiveEventTime;
        return this;

    }

    JsonEvent withBillCycleID(int billCycleID){
        this.billCycleID = billCycleID;
        return this;
    }

    JsonEvent withBillPeriodID(int billPeriodID){
        this.billPeriodID = billPeriodID;
        return this;
    }

    JsonEvent withErrorCode(int errorCode){
        this.errorCode = errorCode;
        return this;
    }

    JsonEvent withRateEventType(String rateEventType){
        this.rateEventType = rateEventType;
        return this;
    }
    public JsonEvent withExternalCorrelationID(long externalCorrelationID){
        this.externalCorrelationID = externalCorrelationID;
        return this;
    }

    public JsonEvent withRootCustomerIDHash(int rootCustomerIDHash){
        this.rootCustomerIDHash = rootCustomerIDHash;
        return this;
    }

    public JsonEvent withProjectAddOn(String projectAddOn){
        this.projectAddOn = projectAddOn;
        return this;
    }

    public JsonEvent withRefEventId(int refEventId){
        this.refEventId = refEventId;
        return this;
    }

    public JsonEvent withRefUseId(int refUseId){
        this.refUseId = refUseId;
        return this;
    }

    public JsonEvent withInternalRatingRelevant(byte[] internalRatingRelevant){
        this.internalRatingRelevant = internalRatingRelevant;
        return this;
    }

    public JsonEvent withExternalRatingIrrelevant(byte[] externalRatingIrrelevant){
        this.externalRatingIrrelevant = externalRatingIrrelevant;
        return this;
    }

    public JsonEvent withExternalRatingResult(byte[] externalRatingResult){
        this.externalRatingResult = externalRatingResult;
        return this;
    }


    public JsonEvent withExternalDataTransp(byte[] externalDataTransp0){
        this.externalDataTransp = externalDataTransp0;
        return this;
    }


    public JsonEvent withUniversalAttribute0(byte[] universalAttribute0){
        this.universalAttribute0 = universalAttribute0;
        return this;
    }

    public JsonEvent withUniversalAttribute1(byte[] universalAttribute1){
        this.universalAttribute1 = universalAttribute1;
        return this;
    }

    public JsonEvent withUniversalAttribute2(byte[] universalAttribute2){
        this.universalAttribute2 = universalAttribute2;
        return this;
    }

    public JsonEvent withUniversalAttribute3(byte[] universalAttribute3){
        this.universalAttribute3 = universalAttribute3;
        return this;
    }

    public JsonEvent withUniversalAttribute4(byte[] universalAttribute4){
        this.universalAttribute4 = universalAttribute4;
        return this;
    }

    public void setTimeId(final int timeId) {
        this.timeId = timeId;
    }

    public void setUseId(final int useId) {
        this.useId = useId;
    }

    public void setEventId(final long eventId) {
        this.eventId = eventId;
    }

    public void setAccessKey(final String accessKey) {
        this.accessKey = accessKey;
    }

    public void setAccessKeyType(final Integer accessKeyType) {
        this.accessKeyType = accessKeyType;
    }

    public void setOwningCustomerID(final String owningCustomerID) {
        this.owningCustomerID = owningCustomerID;
    }

    public void setRootCustomerID(final String rootCustomerID) {
        this.rootCustomerID = rootCustomerID;
    }

    public void setComposedCustomerID(final String composedCustomerID) {
        this.composedCustomerID = composedCustomerID;
    }

    public void setEventType(final long eventType) {
        this.eventType = eventType;
    }

    public void setOriginalEventTime(final Date originalEventTime) {
        this.originalEventTime = originalEventTime;
    }

    public void setCreationEventTime(final Date creationEventTime) {
        this.creationEventTime = creationEventTime;
    }

    public void setEffectiveEventTime(final Date effectiveEventTime) {
        this.effectiveEventTime = effectiveEventTime;
    }

    public void setBillCycleID(final int billCycleID) {
        this.billCycleID = billCycleID;
    }

    public void setBillPeriodID(final int billPeriodID) {
        this.billPeriodID = billPeriodID;
    }

    public void setErrorCode(final Integer errorCode) {
        this.errorCode = errorCode;
    }

    public void setRateEventType(final String rateEventType) {
        this.rateEventType = rateEventType;
    }

    public void setExternalCorrelationID(final Long externalCorrelationID) {
        this.externalCorrelationID = externalCorrelationID;
    }

    public void setRootCustomerIDHash(final int rootCustomerIDHash) {
        this.rootCustomerIDHash = rootCustomerIDHash;
    }

    public void setProjectAddOn(final String projectAddOn) {
        this.projectAddOn = projectAddOn;
    }

    public void setRefEventId(final int refEventId) {
        this.refEventId = refEventId;
    }

    public void setRefUseId(final int refUseId) {
        this.refUseId = refUseId;
    }

    public void setInternalRatingRelevant(final byte[] internalRatingRelevant) {
        this.internalRatingRelevant = internalRatingRelevant;
    }


    public void setExternalRatingIrrelevant(final byte[] externalRatingIrrelevant) {
        this.externalRatingIrrelevant = externalRatingIrrelevant;
    }

    public void setExternalRatingResult(final byte[] externalRatingResult) {
        this.externalRatingResult = externalRatingResult;
    }

    public void setExternalDataTransp(final byte[] externalDataTransp) {
        this.externalDataTransp = externalDataTransp;
    }

    public void setUniversalAttribute0(final byte[] universalAttribute0) {
        this.universalAttribute0 = universalAttribute0;
    }

    public void setUniversalAttribute1(final byte[] universalAttribute1) {
        this.universalAttribute1 = universalAttribute1;
    }

    public void setUniversalAttribute2(final byte[] universalAttribute2) {
        this.universalAttribute2 = universalAttribute2;
    }

    public void setUniversalAttribute3(final byte[] universalAttribute3) {
        this.universalAttribute3 = universalAttribute3;
    }

    public void setUniversalAttribute4(final byte[] universalAttribute4) {
        this.universalAttribute4 = universalAttribute4;
    }

    public void setDayId(final int dayId) {
        this.dayId = dayId;
    }

    @JsonAttribute(index = 21)
    public int getDayId() {
        return dayId;
    }
}
