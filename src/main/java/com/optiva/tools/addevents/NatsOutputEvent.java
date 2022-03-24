package com.optiva.tools.addevents;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public final class NatsOutputEvent implements OutputEvent, OutputEventExtensionV2 {
    private final EventMessage natsEvent;

    public NatsOutputEvent(EventMessage natsEvent) {
        this.natsEvent = natsEvent;
    }

    /**
     * This is the event Id
     * @return the eventId associated with this record
     *
     * @throws Exception
     */
    @Override
    public long getId() throws Exception {
        return natsEvent.getEventId();
    }

    @Override
    public String getAccessKey() throws Exception {
        return natsEvent.getAccessKey();
    }

    @Override
    public int getAccessKeyType() throws SQLException {
        return natsEvent.getAccessKeyType();
    }

    @Override
    public boolean hasAccessKeyType() throws SQLException {
        return  natsEvent.hasAccessKeyType() ;
    }

    @Override
    public String getCustomerId() throws SQLException {
        return natsEvent.getOwningCustomerID();
    }

    @Override
    public String getRootCustomerId() throws SQLException {
        return natsEvent.getRootCustomerID();
    }

    @Override
    public int getEventUseId() throws SQLException {
        return natsEvent.getRefUseId();
    }

    @Override
    public long getEventId() throws SQLException {
        return natsEvent.getRefEventId();
    }

    /**
     * This is derived from the actual creation event time to get the day id
     *
     * @return
     * @throws SQLException
     */
    public long getDayId() throws SQLException {
        return natsEvent.getDayId();
    }
    @Override
    public int getRootCustomerIdHash() throws SQLException {
        return natsEvent.getRootCustomerIDHash();
    }

    @Override
    public String getComposedCustomerId() throws SQLException {
        return natsEvent.getComposedCustomerID();
    }

    @Override
    public long getType() throws SQLException {
        return natsEvent.getEventType();
    }

    @Override
    public long getOriginalTime() throws SQLException {
        return natsEvent.getOriginalEventTime().getTime();
    }

    @Override
    public long getCreationTime() throws SQLException {
        return natsEvent.getCreationEventTime().getTime();
    }

    @Override
    public long getEffectiveTime() throws SQLException {
        return natsEvent.getEffectiveEventTime().getTime();
    }

    @Override
    public boolean hasCorrelationId() throws SQLException {
        return  natsEvent.hasCorrelationId();
    }

    @Override
    public long getCorrelationId() {
        return natsEvent.getExternalCorrelationID();
    }

    @Override
    public int getBillCycleId() throws SQLException {
        return natsEvent.getBillCycleID();
    }

    @Override
    public int getBillPeriodId() throws SQLException {
        return natsEvent.getBillPeriodID();
    }

    @Override
    public boolean hasErrorCode() throws SQLException {
        return  natsEvent.hasErrorCode();
    }

    @Override
    public int getErrorCode() {
        return natsEvent.getErrorCode();
    }

    @Override
    public String getRateEventType() throws SQLException {
        return natsEvent.getRateEventType();
    }
    public byte[] getInternalRatingRelevant() throws SQLException {
        return natsEvent.getInternalRatingRelevant();
    }
    @Override
    public byte[] getExternalRatingIrrelevant() throws SQLException {
        return natsEvent.getExternalRatingIrrelevant();
    }

    @Override
    public byte[] getExternalRatingResult() throws SQLException {
        return natsEvent.getExternalRatingResult();
    }


    @Override
    public byte[] getExternalDataTransp() throws SQLException {
        return natsEvent.getExternalDataTransp();
    }

    @Override
    public String getUniversalAttribute0() throws SQLException {
        return bytesToUtfString(natsEvent.getUniversalAttribute0());
    }

    @Override
    public String getUniversalAttribute1() throws SQLException {
        return bytesToUtfString(natsEvent.getUniversalAttribute1());
    }

    @Override
    public String getUniversalAttribute2() throws SQLException {
        return bytesToUtfString(natsEvent.getUniversalAttribute2());
    }

    @Override
    public String getUniversalAttribute3() throws SQLException {
        return bytesToUtfString(natsEvent.getUniversalAttribute3());
    }

    @Override
    public String getUniversalAttribute4() throws SQLException {
        return bytesToUtfString(natsEvent.getUniversalAttribute4());
    }

    @Override
    public int getTimeId() throws SQLException {
        return natsEvent.getTimeId();
    }

    @Override
    public int getUseId() throws SQLException {
        return natsEvent.getUseId();
    }

    /**
     * Convert buye array to a string.
     *
     * @param bytes
     * @return
     */
    private String bytesToUtfString(byte[] bytes) {
        return bytes==null?null:new String(bytes, StandardCharsets.UTF_8);
    }
}