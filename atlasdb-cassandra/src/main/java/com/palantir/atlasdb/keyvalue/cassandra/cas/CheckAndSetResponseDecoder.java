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
package com.palantir.atlasdb.keyvalue.cassandra.cas;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.atlasdb.keyvalue.impl.ImmutableCheckAndSetResult;
import java.util.Arrays;
import java.util.List;
import okio.ByteString;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;

final class CheckAndSetResponseDecoder {
    private static final String APPLIED_COLUMN = "[applied]";
    private static final String VALUE_COLUMN = "value";

    private static final byte[] SUCCESSFUL_OPERATION = {1};

    private CheckAndSetResponseDecoder() {
        // Utility Class
    }

    static CheckAndSetResult<ByteString> decodeCqlResult(CqlResult cqlResult) {
        CqlRow resultRow = Iterables.getOnlyElement(cqlResult.getRows());
        return ImmutableCheckAndSetResult.of(isResultSuccessful(resultRow), existingValues(resultRow));
    }

    private static boolean isResultSuccessful(CqlRow cqlRow) {
        Column appliedColumn = cqlRow.getColumns()
                .stream()
                .filter(column -> APPLIED_COLUMN.equals(decodeCqlColumnName(column)))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("CQL row " + cqlRow + " was missing an [applied] column"));
        return Arrays.equals(SUCCESSFUL_OPERATION, appliedColumn.getValue());
    }

    private static List<ByteString> existingValues(CqlRow cqlRow) {
        return cqlRow.getColumns()
                .stream()
                .filter(column -> VALUE_COLUMN.equals(decodeCqlColumnName(column)))
                .findFirst()
                .map(column -> ImmutableList.of(ByteString.of(column.getValue())))
                .orElseGet(ImmutableList::of);
    }

    private static String decodeCqlColumnName(Column column) {
        return PtBytes.toString(column.getName());
    }
}
