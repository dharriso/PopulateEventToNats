package com.optiva.tools.addevents;

import java.util.Calendar;
import java.util.Date;

public final class NatsEvent implements EventMessage {

    private final Builder builder;
    private final int dayId;

    /**
     * Builder provides only to build this wee yoke
     */
    public static final class Builder {
        private final int timeId; //This is important, so we'll pass it to the constructor.
        private final int useId;
        private final long eventId;
        private final String accessKey;
        // optional
        private Integer accessKeyType;
        private final String owningCustomerID;
        private final String rootCustomerID;
        private String composedCustomerID;
        private final long eventType;
        private final Date originalEventTime;
        private final Date creationEventTime;
        private final Date effectiveEventTime;
        private final int billCycleID;
        private final int billPeriodID;
        // optional
        private Integer errorCode;
        private final String rateEventType;
        // optional
        private Long externalCorrelationID;
        private final int rootCustomerIDHash;
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

        public Builder(int timeId, int useId, long eventId, String accessKey,
                       String owningCustomerID, String rootCustomerID,
                       long eventType, Date originalEventTime, Date creationEventTime, Date effectiveEventTime,
                       int billCycleID, int billPeriodID, String rateEventType, int rootCustomerIDHash) {
            this.timeId = timeId;
            this.useId = useId;
            this.eventId = eventId;
            this.accessKey = accessKey;
            this.owningCustomerID = owningCustomerID;
            this.rootCustomerID = rootCustomerID;
            this.eventType = eventType;
            this.originalEventTime = originalEventTime;
            this.creationEventTime = creationEventTime;
            this.effectiveEventTime = effectiveEventTime;
            this.billCycleID = billCycleID;
            this.billPeriodID = billPeriodID;
            this.rateEventType = rateEventType;
            this.rootCustomerIDHash = rootCustomerIDHash;
        }

        public Builder withAccessKeyType(int accessKeyType){
            this.accessKeyType = accessKeyType;
            return this;
        }

        public Builder withComposedCustomerID(String composedCustomerID){
            this.composedCustomerID = composedCustomerID;
            return this;
        }
        public Builder withErrorCode(int errorCode){
            this.errorCode = errorCode;
            return this;
        }

        public Builder withExternalCorrelationID(long externalCorrelationID){
            this.externalCorrelationID = externalCorrelationID;
            return this;
        }

        public Builder withProjectAddOn(String projectAddOn){
            this.projectAddOn = projectAddOn;
            return this;
        }

        public Builder withRefEventId(int refEventId){
            this.refEventId = refEventId;
            return this;
        }

        public Builder withRefUseId(int refUseId){
            this.refUseId = refUseId;
            return this;
        }

        public Builder withInternalRatingRelevant(byte[] internalRatingRelevant){
            this.internalRatingRelevant = internalRatingRelevant;
            return this;
        }

        public Builder withExternalRatingIrrelevant(byte[] externalRatingIrrelevant){
            this.externalRatingIrrelevant = externalRatingIrrelevant;
            return this;
        }

        public Builder withExternalRatingResult(byte[] externalRatingResult){
            this.externalRatingResult = externalRatingResult;
            return this;
        }

        public Builder withExternalDataTransp(byte[] externalDataTransp){
            this.externalDataTransp = externalDataTransp;
            return this;
        }

        public Builder withUniversalAttribute0(byte[] universalAttribute0){
            this.universalAttribute0 = universalAttribute0;
            return this;
        }
        public Builder withUniversalAttribute1(byte[] universalAttribute1){
            this.universalAttribute1 = universalAttribute1;
            return this;
        }

        public Builder withUniversalAttribute2(byte[] universalAttribute2){
            this.universalAttribute2 = universalAttribute2;
            return this;
        }

        public Builder withUniversalAttribute3(byte[] universalAttribute3){
            this.universalAttribute3 = universalAttribute3;
            return this;
        }

        public Builder withUniversalAttribute4(byte[] universalAttribute4){
            this.universalAttribute4 = universalAttribute4;
            return this;
        }

        public int getTimeId() {
            return timeId;
        }

        public int getUseId() {
            return useId;
        }

        public long getEventId() {
            return eventId;
        }

        public String getAccessKey() {
            return accessKey;
        }

        public Integer getAccessKeyType() {

            return accessKeyType==null?0:accessKeyType;
        }

        public boolean hasAccessKeyType() {
            return accessKeyType != null;
        }
        public String getOwningCustomerID() {
            return owningCustomerID;
        }

        public String getRootCustomerID() {
            return rootCustomerID;
        }

        public String getComposedCustomerID() {
            return composedCustomerID;
        }

        public long getEventType() {
            return eventType;
        }

        public Date getOriginalEventTime() {
            return originalEventTime;
        }

        public Date getCreationEventTime() {
            return creationEventTime;
        }

        public Date getEffectiveEventTime() {
            return effectiveEventTime;
        }

        public int getBillCycleID() {
            return billCycleID;
        }

        public int getBillPeriodID() {
            return billPeriodID;
        }

        public Integer getErrorCode() {
            return errorCode==null?0:errorCode;
        }

        public boolean hasErrorCode() {
            return errorCode != null;
        }

        public String getRateEventType() {
            return rateEventType;
        }

        public Long getExternalCorrelationID() {
            return externalCorrelationID==null?0:externalCorrelationID;
        }

        public boolean hasCorrelationId() {
            return externalCorrelationID != null;
        }

        public int getRootCustomerIDHash() {
            return rootCustomerIDHash;
        }

        public String getProjectAddOn() {
            return projectAddOn;
        }

        public int getRefEventId() {
            return refEventId;
        }

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
        public NatsEvent build(){
            return new NatsEvent(this);
        }
    }

    /**
     * Must use the builder to create the NatsEvent
     *
     * @param builder
     */
    private NatsEvent(Builder builder) {
        this.builder = builder;
        // '2009-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
        Calendar cal = Calendar.getInstance();
        cal.setTime(builder.getCreationEventTime());
        dayId = cal.get(Calendar.DAY_OF_MONTH);
    }

    public int getTimeId() {
        return builder.getTimeId();
    }

    public int getUseId() {
        return builder.getUseId();
    }

    public long getEventId() {
        return builder.getEventId();
    }

    public String getAccessKey() {
        return builder.getAccessKey();
    }

    public Integer getAccessKeyType() {
        return builder.getAccessKeyType();
    }

    public boolean hasAccessKeyType() {
        return builder.hasAccessKeyType();
    }

    public String getOwningCustomerID() {
        return builder.getOwningCustomerID();
    }

    public String getRootCustomerID() {
        return builder.getRootCustomerID();
    }

    public String getComposedCustomerID() {
        return builder.getComposedCustomerID();
    }

    public long getEventType() {
        return builder.getEventType();
    }

    public Date getOriginalEventTime() {
        return builder.getOriginalEventTime();
    }

    public Date getCreationEventTime() {
        return builder.getCreationEventTime();
    }

    public Date getEffectiveEventTime() {
        return builder.getEffectiveEventTime();
    }

    public int getBillCycleID() {
        return builder.getBillCycleID();
    }

    public int getBillPeriodID() {
        return builder.getBillPeriodID();
    }

    public Integer getErrorCode() {
        return builder.getErrorCode();
    }

    public boolean hasErrorCode() {
        return builder.hasErrorCode();
    }

    public String getRateEventType() {
        return builder.getRateEventType();
    }

    public Long getExternalCorrelationID() {
        return builder.getExternalCorrelationID();
    }

    public boolean hasCorrelationId() {
        return builder.hasCorrelationId();
    }

    public int getRootCustomerIDHash() {
        return builder.getRootCustomerIDHash();
    }

    public String getProjectAddOn() {
        return builder.getProjectAddOn();
    }

    public int getRefEventId() {
        return builder.getRefEventId();
    }

    public int getRefUseId() {
        return builder.getRefUseId();
    }

    public int getDayId() {
        return dayId;
    }

    public byte[] getInternalRatingRelevant() {
        return builder.getInternalRatingRelevant();
    }

    public byte[] getExternalRatingIrrelevant() {
        return builder.getExternalRatingIrrelevant();
    }

    public byte[] getExternalRatingResult() {
        return builder.getExternalRatingResult();
    }

    public byte[] getExternalDataTransp() {
        return builder.getExternalDataTransp();
    }

    public byte[] getUniversalAttribute0() {
        return builder.getUniversalAttribute0();
    }

    public byte[] getUniversalAttribute1() {
        return builder.getUniversalAttribute1();
    }

    public byte[] getUniversalAttribute2() {
        return builder.getUniversalAttribute2();
    }

    public byte[] getUniversalAttribute3() {
        return builder.getUniversalAttribute3();
    }

    public byte[] getUniversalAttribute4() {
        return builder.getUniversalAttribute4();
    }
}