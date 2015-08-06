package com.palantir.util;

import java.util.Iterator;

public class StringUtils {

    public static String join(Iterable<?> items, String delimiter) {
        Iterator<?> iterator = items.iterator();
        if (!iterator.hasNext()) {
            return "";
        }

        StringBuilder builder = new StringBuilder(iterator.next().toString());
        while (iterator.hasNext()) {
            builder.append(delimiter).append(iterator.next());
        }
        return builder.toString();
    }
}
