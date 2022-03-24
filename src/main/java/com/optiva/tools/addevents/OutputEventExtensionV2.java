package com.optiva.tools.addevents;

import java.sql.SQLException;

public interface OutputEventExtensionV2 extends OutputEventExtension {
    String getRootCustomerId() throws SQLException;

    long getType() throws SQLException;

    byte [] getInternalRatingRelevant() throws SQLException;
}
