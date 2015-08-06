package com.palantir.atlasdb.rdbms.impl.config;

public class AtlasRdbmsConfiguration {

    private final String jdbcUrl;
    private final String jdbcDriverClassName;
    private final int maxConnections;
    private final String systemPropertiesTableName;

    public AtlasRdbmsConfiguration(String jdbcUrl, String jdbcDriverClassName, int maxConnections, String sytemPropertiesTableName) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcDriverClassName = jdbcDriverClassName;
        this.maxConnections = maxConnections;
        this.systemPropertiesTableName = sytemPropertiesTableName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getJdbcDriverClassName() {
        return jdbcDriverClassName;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public String getSystemPropertiesTableName() {
        return systemPropertiesTableName;
    }

    @Override
    public String toString() {
        return "AtlasRdbmsConfiguration [jdbcUrl=" + jdbcUrl + ", jdbcDriverClassName="
                + jdbcDriverClassName + ", maxConnections=" + maxConnections
                + ", systemPropertiesTableName=" + systemPropertiesTableName + "]";
    }
}
