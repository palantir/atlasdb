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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import com.google.common.collect.ImmutableList;
import java.util.List;

public final class OracleQueryHelpers {

    private OracleQueryHelpers() {}

    public static String getValueSubselect(boolean haveOverflow, String tableAlias, boolean includeValue) {
        if (includeValue) {
            List<String> colNames = getValueColumnNames(haveOverflow);
            StringBuilder ret = new StringBuilder(10 * colNames.size());
            for (String colName : colNames) {
                // e.g., ", m.val"
                ret.append(", ").append(tableAlias).append('.').append(colName);
            }
            return ret.toString();
        } else {
            return "";
        }
    }

    public static String getValueSubselectForGroupBy(boolean haveOverflow, String tableAlias) {
        List<String> colNames = getValueColumnNames(haveOverflow);
        StringBuilder ret = new StringBuilder(70 * colNames.size());
        for (String colName : colNames) {
            // E.g., ", MAX(m.val) KEEP (DENSE_RANK LAST ORDER BY m.ts ASC) AS val".
            // How this works, assuming we have a "GROUP BY row_name, col_name" clause:
            //  1) Among each group of rows with the same (row_name, col_name) pair,
            //     "KEEP (DENSE_RANK LAST ORDER BY m.ts ASC)" will select the subset
            //     of rows with the biggest 'ts' value. Since (row_name, col_name, ts)
            //     is the primary key, that subset will always contain exactly one
            //     element in our case.
            //  2) For that subset of rows, we take an aggregate function "MAX(m.val)".
            //     It doesn't make a difference which function we use since our subset
            //     is a singleton, so MAX will simply return the only element in that set.
            //  To sum it up: for each (row_name, col_name) group, this will select
            //  the value of the row that has the largest 'ts' value within that group.
            ret.append(", MAX(")
                    .append(tableAlias)
                    .append('.')
                    .append(colName)
                    .append(") KEEP (DENSE_RANK LAST ORDER BY ")
                    .append(tableAlias)
                    .append(".ts ASC) AS ")
                    .append(colName);
        }
        return ret.toString();
    }

    private static List<String> getValueColumnNames(boolean haveOverflow) {
        if (haveOverflow) {
            return VAL_AND_OVERFLOW;
        } else {
            return VAL_ONLY;
        }
    }

    private static final ImmutableList<String> VAL_ONLY = ImmutableList.of("val");
    private static final ImmutableList<String> VAL_AND_OVERFLOW = ImmutableList.of("val", "overflow");
}
