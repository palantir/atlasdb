/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.ete.cassandra.util.CassandraCommands;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.atlasdb.todo.generated.TodoTable;

public class CassandraTimestampsEteTest {
    private static final Todo TODO = ImmutableTodo.of("todo");
    private static final Todo TODO_2 = ImmutableTodo.of("todo_two");
    public static final String CASSANDRA_CONTAINER_NAME = "cassandra";
    public static final long ID = 1L;

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
    public void tombstonesInOneSSTableAreCurrent() throws IOException, InterruptedException {
        long firstWriteTimestamp = todoClient.addTodoWithIdAndReturnTimestamp(ID, TODO);
        todoClient.addTodoWithIdAndReturnTimestamp(ID, TODO_2);

        // Wait for targeted sweep to sweep the old value
        Awaitility.waitAtMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> todoClient.doesNotExistBeforeTimestamp(ID, firstWriteTimestamp));

        CassandraCommands.nodetoolFlush(CASSANDRA_CONTAINER_NAME);

        List<String> ssTables = CassandraCommands.nodetoolGetSSTables(
                CASSANDRA_CONTAINER_NAME,
                "atlasete",
                TableReference.create(Namespace.DEFAULT_NAMESPACE, TodoTable.getRawTableName()),
                ValueType.FIXED_LONG.convertFromJava(ID));
        String sstableMetadata = CassandraCommands.ssTableMetadata(
                CASSANDRA_CONTAINER_NAME, Iterables.getOnlyElement(ssTables));

        assertMinimumTimestampIsAtLeast(sstableMetadata, firstWriteTimestamp);
        assertDroppableTombstoneRatioPositive(sstableMetadata);
    }

    private void assertMinimumTimestampIsAtLeast(String sstableMetadata, long expectedMinimum) {
        Pattern pattern = Pattern.compile("Minimum timestamp: ([0-9]*)");
        Matcher timestampMatcher = pattern.matcher(sstableMetadata);
        assertThat(timestampMatcher.find()).isTrue();
        assertThat(Long.parseLong(timestampMatcher.group(1))).isGreaterThanOrEqualTo(expectedMinimum);
    }

    private void assertDroppableTombstoneRatioPositive(String sstableMetadata) {
        Pattern pattern = Pattern.compile("Estimated droppable tombstones: ([0-9]*\\.[0-9]*)");
        Matcher droppableTombstoneRatioMatcher = pattern.matcher(sstableMetadata);
        assertThat(droppableTombstoneRatioMatcher.find()).isTrue();
        assertThat(Double.parseDouble(droppableTombstoneRatioMatcher.group(1))).isGreaterThan(0);
    }
}
