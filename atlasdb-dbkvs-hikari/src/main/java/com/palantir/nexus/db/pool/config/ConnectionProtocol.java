package com.palantir.nexus.db.pool.config;

public enum ConnectionProtocol {
    TCP("tcp"), TCPS("tcps");

    private final String urlStr;

    ConnectionProtocol(String urlStr) {
        this.urlStr = urlStr;
    }

    public String getUrlString() {
        return urlStr;
    }

    public static ConnectionProtocol fromUrlString(String val) {
        for (ConnectionProtocol cp : ConnectionProtocol.values()) {
            if (cp.getUrlString().toLowerCase().equals(val.toLowerCase())) {
                return cp;
            }
        }
        throw new IllegalArgumentException(val + " does not correspond to a known ConnectionProtocol");
    }
}
