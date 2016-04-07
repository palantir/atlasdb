package com.palantir.logger;

public class SqlStackLogWrapper {
    private String query;
    private String key;
    private boolean isRegistered;

    public SqlStackLogWrapper(String query) {
        this.query = query;
        isRegistered = false;
    }

    public SqlStackLogWrapper(String query, String key) {
        this.query = query;
        this.key = key;
        isRegistered = true;
    }

    public String getQuery() {
        return query;
    }

    public String getKey() {
        return key;
    }

    public boolean isRegistered() {
        return isRegistered;
    }

    @Override
    public String toString() {
        return query.toString();
    }
}
