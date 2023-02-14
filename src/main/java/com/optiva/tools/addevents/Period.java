package com.optiva.tools.addevents;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * periodSinceOrigin is a count of (variable) minute intervals since 2009-01-1. Also known as UseId.
 * periodsSinceMidnight is a count of (variable) minute intervals since beginning of the day. Also known as TimeId.
 *
 * Currently, statically using 15-minute intervals, this probably needs to be using the EventProperty.AdHocDB_SwitchPeriodSeconds
 */
public class Period {

    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    /**
     * this is the actual USEID
     */
    private final long periodsSinceOrigin;
    /**
     * this is the actual TIMEID
     */
    private final long periodsSinceMidnight;

    public Period() {
        this(System.currentTimeMillis());
    }

    public Period(long currentTime) {
        Calendar sinceOriginCal = Calendar.getInstance(GMT);
        sinceOriginCal.set(2009, Calendar.JANUARY, 1);
        sinceOriginCal.set(Calendar.HOUR_OF_DAY, 0);
        sinceOriginCal.set(Calendar.MINUTE, 0);
        sinceOriginCal.set(Calendar.SECOND, 0);
        sinceOriginCal.set(Calendar.MILLISECOND, 0);
        long millisSinceOrigin = currentTime - sinceOriginCal.getTimeInMillis();
        periodsSinceOrigin = millisSinceOrigin / (180 * 1000);

        LocalDate date = Instant.ofEpochMilli(currentTime).atOffset(ZoneOffset.UTC).toLocalDate();

        Calendar sinceMidnightCal = Calendar.getInstance(GMT);
        sinceMidnightCal.set(date.getYear(), date.getMonthValue()-1, date.getDayOfMonth());

        sinceMidnightCal.set(Calendar.HOUR_OF_DAY, 0);
        sinceMidnightCal.set(Calendar.MINUTE, 0);
        sinceMidnightCal.set(Calendar.SECOND, 0);
        sinceMidnightCal.set(Calendar.MILLISECOND, 0);
        long millisSinceMidnight = currentTime - sinceMidnightCal.getTimeInMillis();
        periodsSinceMidnight = millisSinceMidnight / (180 * 1000);
    }

    public static Period now() {
        return new Period();
    }

    public long getUseId() {
        return periodsSinceOrigin;
    }

    public long getTimeId() {
        return periodsSinceMidnight;
    }

    @Override
    public String toString() {
        return "Period{" +
                "periodsSinceOrigin (USEID)=" + periodsSinceOrigin +
                ", periodsSinceMidnight (TIMEID)=" + periodsSinceMidnight +
                '}';
    }
}
