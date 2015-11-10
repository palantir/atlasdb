/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    ROCKSDB(IDENTIFIER_REQUIRED.NO, null, null, null, null),
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
