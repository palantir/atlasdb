package com.palantir.atlasdb.sql.grammar;

import java.util.EnumSet;
import java.util.Map;

import com.google.common.collect.Maps;

public enum AggregationType {
    IDENTITY,
    COUNT("count"),
    MAX("max"),
    MIN("min");

    private final String strRep;

    AggregationType() {
        this(null);
    }

    AggregationType(String strRep) {
        this.strRep = strRep;
    }

    public String getStringRep() {
        return strRep;
    }

    public static AggregationType from(String str) {
        if (lookup.containsKey(str)) {
            return lookup.get(str);
        }
        throw new IllegalArgumentException(String.format("Unknown aggregator '%s'.", str));
    }

    private static final Map<String, AggregationType> lookup = Maps.newHashMap();
    static {
        for(AggregationType f : EnumSet.allOf(AggregationType.class)) {
            if (f.getStringRep() != null) {
                lookup.put(f.getStringRep(), f);
            }
        }
    }

}
