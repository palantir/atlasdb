package com.palantir.atlasdb.shell;

public class DbConnectionInfo {
    private final String type;
    private final String host;
    private final String port;
    private final String identifier;
    private final String username;
    private final String password;

    public static DbConnectionInfo create(String host,
                             String port,
                             String identifier,
                             String type,
                             String username,
                             String password) {
        return new DbConnectionInfo(host, port, identifier, type, username, password);
    }

    private DbConnectionInfo(String host,
                             String port,
                             String identifier,
                             String type,
                             String username,
                             String password) {
        this.host = host;
        this.port = port;
        this.identifier = identifier;
        this.type = type;
        this.username = username;
        this.password = password;
    }

    public String getType() {
        return type;
    }
    public String getHost() {
        return host;
    }
    public String getPort() {
        return port;
    }
    public String getIdentifier() {
        return identifier;
    }
    public String getUsername() {
        return username;
    }
    public String getPassword() {
        return password;
    }
}
