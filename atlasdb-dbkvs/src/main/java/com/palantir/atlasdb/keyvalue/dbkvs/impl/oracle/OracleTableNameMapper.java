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

import org.apache.commons.lang3.RandomStringUtils;

import com.palantir.atlasdb.keyvalue.dbkvs.TableNameMapper;

public final class OracleTableNameMapper implements TableNameMapper {
    public static final int ORACLE_TABLE_NAME_LENGTH = 30;
    public static final int RANDOM_SUFFIX_LENGTH = 5;
    public static final int PREFIXED_TABLE_NAME_LENGTH = ORACLE_TABLE_NAME_LENGTH - RANDOM_SUFFIX_LENGTH;

    @Override
    public String getShortPrefixedTableName(String tablePrefix, String tableName) {
        String fullTableName = tablePrefix + tableName;
        if (fullTableName.length() < ORACLE_TABLE_NAME_LENGTH) {
            return fullTableName;
        }

        int numCharactersToDrop = fullTableName.length() - PREFIXED_TABLE_NAME_LENGTH;
        return tablePrefix + getShortTableName(tableName, numCharactersToDrop) + getRandomSuffix();
    }

    private String getShortTableName(String tableName, int numCharactersToDrop) {
        int noChangeUntilIndex = 0;
        String tableNameSubstring = tableName;
        while (noChangeUntilIndex < tableName.length()
                && tableNameSubstring.length() - tableNameSubstring.replaceAll("a|e|i|o|u|", "").length() > numCharactersToDrop) {
            noChangeUntilIndex++;
            tableNameSubstring = tableNameSubstring.substring(1);
        }

        String tableNameAfterDroppingVowels = tableName.substring(0, noChangeUntilIndex)
                + tableNameSubstring.replaceAll("a|e|i|o|u|", "");
        int numCharactersDropped = tableName.length() - tableNameAfterDroppingVowels.length();
        final int numExtraCharacters = numCharactersToDrop - numCharactersDropped;
        if (numExtraCharacters > 0) {
            tableNameAfterDroppingVowels = tableNameAfterDroppingVowels.substring(0,
                    tableNameAfterDroppingVowels.length() - numExtraCharacters);
        }
        return tableNameAfterDroppingVowels;
    }

    private String getRandomSuffix() {
        return RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH);
    }
}
