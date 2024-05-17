package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 12:18 PM
 **/

public final class DateTimeUtils {
    public static final DateFormat ISO_DATE_TIME = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    private DateTimeUtils() {}

    public static String format(long millis, DateFormat dateFormat) {
        return dateFormat.format(new Date(millis));
    }

    public static String format(Date date, DateFormat dateFormat) {
        return dateFormat.format(date);
    }

    public static Date nowInUtc() {
        return Date.from(LocalDateTime.now()
                .atZone(ZoneOffset.UTC)
                .toInstant());
    }

    public static Date now() {
        return new Date();
    }

}
