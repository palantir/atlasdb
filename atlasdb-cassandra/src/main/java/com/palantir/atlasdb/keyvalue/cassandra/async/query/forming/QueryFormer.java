/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async.query.forming;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public interface QueryFormer {

    enum SupportedQueries {
        GET
    }

    class FieldNameProvider {
        public static String row = "key";
        public static String column = "column1";
        public static String timestamp = "column2";
        public static String value = "value";

        protected FieldNameProvider() {}
    }

    String TIME_PATTERN = "SELECT dateof(now()) FROM system.local ;";
    String GET_PATTERN =
            "SELECT " + FieldNameProvider.value + ',' + FieldNameProvider.timestamp + " FROM %s "
                    + "WHERE " + FieldNameProvider.row + " = :" + FieldNameProvider.row
                    + " AND " + FieldNameProvider.column + " = :" + FieldNameProvider.column
                    + " AND " + FieldNameProvider.timestamp + " > :" + FieldNameProvider.timestamp + " ;";

    Map<SupportedQueries, String> QUERY_FORMATS_MAP = ImmutableMap.of(
            SupportedQueries.GET, GET_PATTERN);


    default String formTimeQuery() {
        return TIME_PATTERN;
    }

    String formQuery(SupportedQueries supportedQuery, String keySpace, TableReference tableReference);
}
