package com.palantir.atlasdb.rdbms.impl.service;

public final class AtlasRdbmsUtils {
    private AtlasRdbmsUtils() {
        // not instantiable
    }
    public static int countFields(String fieldList) {
        return fieldList.split("\\s*,\\s*").length; //$NON-NLS-1$
    }

    public static String nArguments(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("Number of arguments must be positive");
        }
        StringBuilder sb = new StringBuilder("?");
        for (int i = 1; i < n; i++) {
            sb.append(",?");
        }
        return sb.toString();
    }
}
