package com.palantir.atlasdb.shell;

import java.util.List;

import com.google.common.collect.Lists;

enum IDENTIFIER_REQUIRED {
    YES,
    NO
}

public enum AtlasShellConnectionType {

    MEMORY(IDENTIFIER_REQUIRED.NO, null, "localhost", 1521),
//    ORACLE(IDENTIFIER_REQUIRED.YES, "SID", "localhost", 1521),
//    POSTGRESQL(IDENTIFIER_REQUIRED.YES, "dbname", "localhost", 5432),
//    DISPATCH(IDENTIFIER_REQUIRED.NO, null, "localhost", 3280)
    ;

    private final IDENTIFIER_REQUIRED identifierRequired;
    private final String identifierName;
    private final String defaultHostname;
    private final Integer defaultPort;

    public static List<String> getTypeList() {
        List<String> typeList = Lists.newArrayList();
        for (AtlasShellConnectionType type : AtlasShellConnectionType.values()) {
            typeList.add(type.name());
        }
        return typeList;
    }

    private AtlasShellConnectionType(IDENTIFIER_REQUIRED identifierRequired,
                                     String identifierText,
                                     String defaultHostname,
                                     Integer defaultPort) {
        this.identifierRequired = identifierRequired;
        this.identifierName = identifierText;
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

    public Integer getDefaultPort() {
        return defaultPort;
    }

}
