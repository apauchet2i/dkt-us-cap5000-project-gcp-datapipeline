package org.datapipeline.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class DateNow {

    public static String dateNow() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'H:mm:ss", Locale.getDefault());
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }
}
