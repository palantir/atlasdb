/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.table.description;

import com.palantir.atlasdb.AtlasDbConstants;

/**
 * CharacterLimitType which indicates the different types of character limits imposed on table names.
 * These depend on the KVS being used.
 */
public enum CharacterLimitType {
    CASSANDRA("Cassandra", AtlasDbConstants.CASSANDRA_TABLE_NAME_CHAR_LIMIT),
    POSTGRES("Postgres", AtlasDbConstants.POSTGRES_TABLE_NAME_CHAR_LIMIT);

    private final String kvsName;
    private final int charLimit;

    CharacterLimitType(String name, int limit) {
        kvsName = name;
        charLimit = limit;
    }

    @Override
    public String toString() {
        return String.format("%s (%s characters)", kvsName, charLimit);
    }
}
