package com.palantir.atlasdb.performance;

/**
 * Enum for keeping track of JMH parameters {@see @Param}. Edit this enum with caution.
 * The keys must match the name of their associated instance variable.
 */
public enum BenchmarkParam {
    BACKEND("backend");

    private final String key;

    BenchmarkParam(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
