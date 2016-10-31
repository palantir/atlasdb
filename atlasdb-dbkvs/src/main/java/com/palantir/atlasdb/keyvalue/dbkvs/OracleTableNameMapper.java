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
package com.palantir.atlasdb.keyvalue.dbkvs;

import java.nio.charset.StandardCharsets;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.util.crypto.Sha256Hash;

public final class OracleTableNameMapper {
    public static final int ORACLE_MAX_TABLE_NAME_LENGTH = 30;
    public static final int RANDOM_SUFFIX_LENGTH = 5;
    private static final int PREFIXED_TABLE_NAME_LENGTH = ORACLE_MAX_TABLE_NAME_LENGTH - RANDOM_SUFFIX_LENGTH;

    public String getShortPrefixedTableName(String tablePrefix, TableReference tableRef) {
        Preconditions.checkState(tablePrefix.length() <= 7, "The tablePrefix can be at most 7 characters long");
        String fullTableName = tablePrefix + DbKvs.internalTableName(tableRef);
        if (fullTableName.length() <= ORACLE_MAX_TABLE_NAME_LENGTH) {
            return fullTableName;
        }

        int maxTableNameLength = PREFIXED_TABLE_NAME_LENGTH - tablePrefix.length();
        return tablePrefix
                + getShortTableName(DbKvs.internalTableName(tableRef), maxTableNameLength)
                + getHashSuffix(fullTableName);
    }

    private String getShortTableName(String tableName, int maximumLength) {
        int numCharactersToDrop = tableName.length() - maximumLength;
        String tableNameWithoutLastVowels = removeLastKVowels(tableName, numCharactersToDrop);
        if (tableNameWithoutLastVowels.length() <= maximumLength) {
            return tableNameWithoutLastVowels;
        }
        return tableNameWithoutLastVowels.substring(0, maximumLength);
    }

    private String removeLastKVowels(String tableName, int numVowelsToDrop) {
        final int kthLastVowelIndex = findKthLastVowelIndex(tableName, numVowelsToDrop);
        String head = tableName.substring(0, kthLastVowelIndex);
        String tail = tableName.substring(kthLastVowelIndex);
        return head + removeAllLowerCaseVowels(tail);
    }

    private int findKthLastVowelIndex(String tableName, int k) {
        final String vowels = "aeiou";
        int vowelsFound = 0;
        for (int i = tableName.length() - 1; i > 0; i--)
        {
            if (vowels.indexOf(tableName.toLowerCase().charAt(i)) >= 0) {
                vowelsFound++;
            }
            if (vowelsFound == k) {
                return i;
            }
        }
        return 0;
    }

    private String removeAllLowerCaseVowels(String tableName) {
        return tableName.replaceAll("a|e|i|o|u", "");
    }

    private String getHashSuffix(String fullTableName) {
        Sha256Hash hash = Sha256Hash.computeHash(fullTableName.getBytes(StandardCharsets.UTF_8));
        return hash.serializeToHexString().substring(0, 5);
    }
}
