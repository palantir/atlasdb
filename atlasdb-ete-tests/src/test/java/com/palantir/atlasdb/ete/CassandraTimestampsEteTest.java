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
package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.ete.cassandra.util.CassandraCommands;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.atlasdb.todo.TodoSchema;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CassandraTimestampsEteTest {
    private static final Todo TODO = ImmutableTodo.of("todo");
    private static final Todo TODO_2 = ImmutableTodo.of("todo_two");
    private static final String CASSANDRA_CONTAINER_NAME = "cassandra";
    private static final long ID = 1L;
    private static final String NAMESPACE = "tom";

    private TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);

    @Before
    public void setup() {
        todoClient.truncate();
    }

    @After
    public void cleanup() {
        todoClient.truncate();
    }

    @Test
    public void timestampsForRangeTombstonesAreCurrentInThoroughTables() throws IOException, InterruptedException {
        long firstWriteTimestamp = todoClient.addTodoWithIdAndReturnTimestamp(ID, TODO);
        long secondWriteTimestamp = todoClient.addTodoWithIdAndReturnTimestamp(ID, TODO_2);

        sweepUntilNoValueExistsAtTimestamp(ID, firstWriteTimestamp);

        CassandraCommands.nodetoolFlush(CASSANDRA_CONTAINER_NAME);
        CassandraCommands.nodetoolCompact(CASSANDRA_CONTAINER_NAME);

        List<String> ssTables = CassandraCommands.nodetoolGetSsTables(
                CASSANDRA_CONTAINER_NAME, "atlasete", TodoSchema.todoTable(), ValueType.FIXED_LONG.convertFromJava(ID));
        String sstableMetadata =
                CassandraCommands.ssTableMetadata(CASSANDRA_CONTAINER_NAME, Iterables.getOnlyElement(ssTables));

        // Failure here implies that a range tombstone is being written at a very conservative timestamp.
        assertMinimumTimestampIsAtLeast(sstableMetadata, secondWriteTimestamp);

        // If the range tombstone is taken at a fresh timestamp, it must have timestamp at least 2 greater than
        // the write timestamp of the second value. Failure here implies the range tombstone was not taken using
        // a fresh timestamp.
        assertMaximumTimestampIsAheadByAtLeast(sstableMetadata, 2);

        // Failure here implies that the tombstone was dropped in the compaction, which shouldn't happen.
        assertEstimatedTombstonesPositive(sstableMetadata);
    }

    @Test
    public void timestampsForSentinelsAndTombstonesAreCurrentInConservativeTables()
            throws IOException, InterruptedException {
        long firstWriteTimestamp = todoClient.addNamespacedTodoWithIdAndReturnTimestamp(ID, NAMESPACE, TODO);
        todoClient.addNamespacedTodoWithIdAndReturnTimestamp(ID, NAMESPACE, TODO_2);
        sweepUntilNoValueExistsForNamespaceAtTimestamp(ID, firstWriteTimestamp, NAMESPACE);

        CassandraCommands.nodetoolFlush(CASSANDRA_CONTAINER_NAME);

        List<String> ssTables = CassandraCommands.nodetoolGetSsTables(
                CASSANDRA_CONTAINER_NAME,
                "atlasete",
                TodoSchema.namespacedTodoTable(),
                ValueType.STRING.convertFromJava(NAMESPACE));
        String sstableMetadata =
                CassandraCommands.ssTableMetadata(CASSANDRA_CONTAINER_NAME, Iterables.getOnlyElement(ssTables));

        // The table should contain the first value, and timestamps thereafter should be higher.
        // Failure here means either that the range tombstone was written at a lower Cassandra timestamp than the
        // original value (unlikely since we confirmed no value existed at the first write timestamp), or
        // that the sweep sentinel was written at a lower Cassandra timestamp than the original value (and hence not
        // a fresh timestamp).
        assertMinimumTimestampIsAtLeast(sstableMetadata, firstWriteTimestamp);

        // Failure here means that the range tombstone was already dropped, which shouldn't happen (gc_grace will
        // prevent it from being removed)
        assertEstimatedTombstonesPositive(sstableMetadata);
    }

    private void sweepUntilNoValueExistsAtTimestamp(long id, long timestamp) {
        sweepUntilConditionSatisfied(() -> todoClient.doesNotExistBeforeTimestamp(id, timestamp));
    }

    private void sweepUntilNoValueExistsForNamespaceAtTimestamp(long id, long timestamp, String namespace) {
        sweepUntilConditionSatisfied(
                () -> todoClient.namespacedTodoDoesNotExistBeforeTimestamp(id, timestamp, namespace));
    }

    private void sweepUntilConditionSatisfied(BooleanSupplier predicate) {
        Awaitility.waitAtMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    todoClient.runIterationOfTargetedSweep();
                    return predicate.getAsBoolean();
                });
    }

    private void assertMinimumTimestampIsAtLeast(String sstableMetadata, long expectedMinimum) {
        assertThat(parseMinimumTimestamp(sstableMetadata)).isGreaterThanOrEqualTo(expectedMinimum);
    }

    private void assertMaximumTimestampIsAheadByAtLeast(String sstableMetadata, long expectedMinimumDelta) {
        long range = parseMaximumTimestamp(sstableMetadata) - parseMinimumTimestamp(sstableMetadata);
        assertThat(range).isGreaterThanOrEqualTo(expectedMinimumDelta);
    }

    private long parseMinimumTimestamp(String sstableMetadata) {
        return parseNumericField("Minimum timestamp", sstableMetadata);
    }

    private long parseMaximumTimestamp(String sstableMetadata) {
        return parseNumericField("Maximum timestamp", sstableMetadata);
    }

    private long parseNumericField(String field, String sstableMetadata) {
        Pattern pattern = Pattern.compile(field + ": (-?[0-9]*)");
        Matcher timestampMatcher = pattern.matcher(sstableMetadata);
        assertThat(timestampMatcher.find())
                .as("SSTableMetadata output contains %s", field)
                .isTrue();
        return Long.parseLong(timestampMatcher.group(1));
    }

    private boolean hasEstimatedTombstones(String sstableMetadata) {
        Iterator<String> it =
                Arrays.stream(sstableMetadata.split(System.lineSeparator())).iterator();
        while (it.hasNext()) {
            String nextLine = it.next();
            if (nextLine.startsWith("Estimated tombstone drop times:")) {
                // If the following line starts with "Count", then no information about tombstones was printed
                return !it.next().startsWith("Count");
            }
        }
        return false;
    }

    private void assertEstimatedTombstonesPositive(String sstableMetadata) {
        assertThat(hasEstimatedTombstones(sstableMetadata)).isTrue();
    }
}
