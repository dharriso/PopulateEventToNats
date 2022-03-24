package com.optiva.tools.addevents;

import java.sql.SQLException;

public interface OutputEvent {
    long getId() throws Exception;

    String getAccessKey() throws Exception;
    int getAccessKeyType() throws SQLException;
    boolean hasAccessKeyType() throws SQLException;

    String getCustomerId() throws SQLException;
    String getRootCustomerId() throws SQLException;
    int getRootCustomerIdHash() throws SQLException;
    String getComposedCustomerId() throws SQLException;

    long getType() throws SQLException;

    long getOriginalTime() throws SQLException;
    long getCreationTime() throws SQLException;
    long getEffectiveTime() throws SQLException;

    boolean hasCorrelationId() throws SQLException;
    long getCorrelationId();

    int getBillCycleId() throws SQLException;
    int getBillPeriodId() throws SQLException;

    boolean hasErrorCode() throws SQLException;
    int getErrorCode();

    String getRateEventType() throws SQLException;
    byte [] getInternalRatingRelevant() throws SQLException;

    byte [] getExternalRatingIrrelevant() throws SQLException;

    byte [] getExternalRatingResult() throws SQLException;

    byte [] getExternalDataTransp() throws SQLException;

    String getUniversalAttribute0() throws SQLException;
    String getUniversalAttribute1() throws SQLException;
    String getUniversalAttribute2() throws SQLException;
    String getUniversalAttribute3() throws SQLException;
    String getUniversalAttribute4() throws SQLException;

    int getTimeId() throws SQLException;

    int getUseId() throws SQLException;
}
