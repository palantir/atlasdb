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

import com.palantir.atlasdb.keyvalue.dbkvs.TableNameMapper;

public final class OracleTableNameMapper implements TableNameMapper {
    static final int ORACLE_TABLE_NAME_LIMIT = 27; // pk_ + name must be 30 characters

    public OracleTableNameMapper() {
        //Utility class
    }

    @Override
    public String hashTableNameToFitOracleTableNameLimits(String tablePrefix, String tableName) {
        System.out.println("Params are : tableName: " + tableName + ", prefix: " + tablePrefix);
        int unPrefixedHashSize = ORACLE_TABLE_NAME_LIMIT - tablePrefix.length();
        if (tableName.length() < unPrefixedHashSize) {
            return tablePrefix + tableName;
        } else {
            String prefixedTableName = tablePrefix + tableName.replaceAll("[AaEeIiOoUu]", "");
            return  prefixedTableName.substring(0, Math.min(prefixedTableName.length(), ORACLE_TABLE_NAME_LIMIT));
        }
    }
}
