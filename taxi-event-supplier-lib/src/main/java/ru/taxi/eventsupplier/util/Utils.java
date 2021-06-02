package ru.taxi.eventsupplier.util;

import java.util.Map;

public class Utils {

    private Utils() {
        throw new UnsupportedOperationException("Can not be initialized");
    }

    public static boolean isEmpty(String s) {
        return s == null || "".equals(s.trim());
    }

    public static void setPropertyIfNotNull(Map<String, Object> properties, String key, String value) {
        if (!isEmpty(value)) {
            properties.put(key, value);
        }
    }
}
