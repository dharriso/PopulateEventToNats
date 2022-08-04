package com.optiva.tools.addevents;

import java.util.Date;

public interface EventMessage {
    int getTimeId();

    int getUseId();

    long getEventId();

    String getAccessKey();

    Integer getAccessKeyType();

    boolean hasAccessKeyType();

    String getOwningCustomerID();

    String getRootCustomerID();

    String getComposedCustomerID();

    long getEventType();

    Date getOriginalEventTime();

    Date getCreationEventTime();

    Date getEffectiveEventTime();

    int getBillCycleID();

    int getBillPeriodID();

    Integer getErrorCode();

    boolean hasErrorCode();

    String getRateEventType();

    Long getExternalCorrelationID();

    boolean hasCorrelationId();

    int getRootCustomerIDHash();

    String getProjectAddOn();

    int getRefEventId();

    int getRefUseId();

    int getDayId();
    byte[] getInternalRatingRelevant() ;
    byte[] getExternalRatingIrrelevant();
    byte[] getExternalRatingResult();
    byte[] getExternalDataTransp();
    byte[] getUniversalAttribute0();
    byte[] getUniversalAttribute1();
    byte[] getUniversalAttribute2();
    byte[] getUniversalAttribute3();
    byte[] getUniversalAttribute4();

    String toString();
}
