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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Stopwatch;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CqlQuery;
import com.palantir.logsafe.Arg;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class CheckAndSetQueriesTest {
    private static final TableReference TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("ns.table");
    private static final Cell CELL = Cell.create(PtBytes.toBytes("abc"), PtBytes.toBytes("123"));
    private static final CheckAndSetRequest NEW_CELL_REQUEST =
            CheckAndSetRequest.newCell(TABLE_REFERENCE, CELL, PtBytes.toBytes("ptpt"));
    private static final CheckAndSetRequest UPDATE_REQUEST =
            CheckAndSetRequest.singleCell(TABLE_REFERENCE, CELL, PtBytes.toBytes("aaa"), PtBytes.toBytes("bbb"));

    @Test
    public void valuesCreatedAtCorrectLogSafetyLevelsForNewCells() {
        CqlQuery query = CheckAndSetQueries.getQueryForRequest(NEW_CELL_REQUEST);
        AtomicReference<Object[]> objects = new AtomicReference<>();
        query.logSlowResult((format, args) -> objects.set(args), Stopwatch.createStarted());

        Object[] loggedObjects = objects.get();
        Map<String, Boolean> argumentSafety = new HashMap<>();
        Arrays.stream(loggedObjects).forEach(object -> {
            Arg<?> arg = (Arg<?>) object;
            argumentSafety.put(arg.getName(), arg.isSafeForLogging());
        });

        assertThat(argumentSafety)
                .containsEntry("row", false)
                .containsEntry("column", false)
                .containsEntry("cassandraTimestamp", true)
                .containsEntry("newValue", false)
                .containsEntry("unsafeTableRef", false)
                .doesNotContainKey("tableRef"); // the table wasn't marked as safe
        assertThat(query.toString())
                .isEqualTo("INSERT INTO \"ns__table\" (key, column1, column2, value)"
                        + " VALUES (0x616263, 0x313233, -1, 0x70747074) IF NOT EXISTS;");
    }

    @Test
    public void valuesCreatedAtCorrectLogSafetyLevelsForUpdates() {
        CqlQuery query = CheckAndSetQueries.getQueryForRequest(UPDATE_REQUEST);
        AtomicReference<Object[]> objects = new AtomicReference<>();
        query.logSlowResult((format, args) -> objects.set(args), Stopwatch.createStarted());

        Object[] loggedObjects = objects.get();
        Map<String, Boolean> argumentSafety = new HashMap<>();
        Arrays.stream(loggedObjects).forEach(object -> {
            Arg<?> arg = (Arg<?>) object;
            argumentSafety.put(arg.getName(), arg.isSafeForLogging());
        });

        assertThat(argumentSafety)
                .containsEntry("row", false)
                .containsEntry("column", false)
                .containsEntry("cassandraTimestamp", true)
                .containsEntry("oldValue", false)
                .containsEntry("newValue", false)
                .containsEntry("unsafeTableRef", false)
                .doesNotContainKey("tableRef"); // the table wasn't marked as safe
        assertThat(query.toString())
                .isEqualTo("UPDATE \"ns__table\" SET value=0x626262"
                        + " WHERE key=0x616263 AND column1=0x313233 AND column2=-1 IF value=0x616161;");
    }
}
