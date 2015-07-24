package com.palantir.atlasdb.shell;

import java.util.List;

import com.google.common.collect.Lists;

enum IDENTIFIER_REQUIRED {
    YES,
    NO
}

public enum AtlasShellConnectionType {
    MEMORY(IDENTIFIER_REQUIRED.NO, null, null, null, null),
    CASSANDRA(IDENTIFIER_REQUIRED.YES, "keyspace", "atlasdb", "localhost", "9160"),
//    ORACLE(IDENTIFIER_REQUIRED.YES, "SID", "localhost", 1521),
//    POSTGRESQL(IDENTIFIER_REQUIRED.YES, "dbname", "localhost", 5432),
//    DISPATCH(IDENTIFIER_REQUIRED.NO, null, "localhost", 3280)
    ;

    private final IDENTIFIER_REQUIRED identifierRequired;
    private final String identifierName;
    private final String defaultIdentifier;
    private final String defaultHostname;
    private final String defaultPort;

    public static List<String> getTypeList() {
        List<String> typeList = Lists.newArrayList();
        for (AtlasShellConnectionType type : AtlasShellConnectionType.values()) {
            typeList.add(type.name());
        }
        return typeList;
    }

    private AtlasShellConnectionType(IDENTIFIER_REQUIRED identifierRequired,
                                     String identifierText,
                                     String defaultIdentifier,
                                     String defaultHostname,
                                     String defaultPort) {
        this.identifierRequired = identifierRequired;
        this.identifierName = identifierText;
        this.defaultIdentifier = defaultIdentifier;
        this.defaultHostname = defaultHostname;
        this.defaultPort = defaultPort;
    }

    public boolean isIdentifierRequired() {
        return identifierRequired == IDENTIFIER_REQUIRED.YES;
    }

    public String getIdentifierName() {
        return identifierName;
    }

    public String getDefaultHostname() {
        return defaultHostname;
    }

    public String getDefaultIdentifier() {
        return defaultIdentifier;
    }

    public String getDefaultPort() {
        return defaultPort;
    }

}
