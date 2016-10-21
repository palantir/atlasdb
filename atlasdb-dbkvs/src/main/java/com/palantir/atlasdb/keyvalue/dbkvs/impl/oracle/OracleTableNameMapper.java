/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import java.util.UUID;

import com.palantir.atlasdb.keyvalue.dbkvs.TableNameMapper;

public final class OracleTableNameMapper implements TableNameMapper {
    public static final int ORACLE_TABLE_NAME_LIMIT = 30; // pk_ + name must be 30 characters
    private static final int RANDOM_SUFFIX_LENGTH = 5; // pk_ + name must be 30 characters
    private static final int TABLE_NAME_LENGTH = ORACLE_TABLE_NAME_LIMIT - RANDOM_SUFFIX_LENGTH;

    @Override
    public String getShortPrefixedTableName(String tablePrefix, String tableName) {
        if (tablePrefix.length() + tableName.length() < ORACLE_TABLE_NAME_LIMIT) {
            return tablePrefix + tableName;
        }
        int unPrefixedHashSize = TABLE_NAME_LENGTH - tablePrefix.length();
        String randomHash = UUID.randomUUID().toString().substring(0, RANDOM_SUFFIX_LENGTH);

        if (tableName.length() < unPrefixedHashSize) {
            return tablePrefix + tableName + randomHash;
        } else {
            String prefixedTableName = tablePrefix + tableName.replaceAll("[AaEeIiOoUu]", "");
            return prefixedTableName.substring(0, Math.min(prefixedTableName.length(), TABLE_NAME_LENGTH)) + randomHash;
        }
    }
}
