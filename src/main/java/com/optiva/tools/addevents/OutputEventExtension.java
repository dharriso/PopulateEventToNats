package com.optiva.tools.addevents;

import java.sql.SQLException;

public interface OutputEventExtension {
    int getEventUseId() throws SQLException;
    long getEventId() throws SQLException;

    int getRootCustomerIdHash() throws SQLException;

    long getCreationTime() throws SQLException;

    int getBillCycleId() throws SQLException;
    int getBillPeriodId() throws SQLException;
    byte [] getExternalRatingResult() throws SQLException;
    byte [] getExternalDataTransp() throws SQLException;

}
