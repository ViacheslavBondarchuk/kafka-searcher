package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
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

}
