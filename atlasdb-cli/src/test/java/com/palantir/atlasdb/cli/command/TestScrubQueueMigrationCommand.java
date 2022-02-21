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
package com.palantir.atlasdb.cli.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.KeyValueServiceScrubberStore;
import com.palantir.atlasdb.cleaner.ScrubberStore;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.SortedMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestScrubQueueMigrationCommand {
    private KeyValueService kvs;
    private ScrubberStore scrubStore;

    @Before
    public void before() {
        kvs = new InMemoryKeyValueService(false, MoreExecutors.newDirectExecutorService());
        scrubStore = KeyValueServiceScrubberStore.create(kvs);
    }

    @After
    public void after() {
        kvs.close();
    }

    @Test
    public void testScrubQueueMigration() {
        TableReference foo = TableReference.createWithEmptyNamespace("foo");
        TableReference bar = TableReference.createWithEmptyNamespace("bar");
        TableReference baz = TableReference.createWithEmptyNamespace("baz");
        Cell cell1 = Cell.create(new byte[] {1, 2, 3}, new byte[] {4, 5, 6});
        Cell cell2 = Cell.create(new byte[] {7, 8, 9}, new byte[] {4, 5, 6});
        Cell cell3 = Cell.create(new byte[] {1, 2, 3}, new byte[] {7, 8, 9});
        kvs.createTable(AtlasDbConstants.OLD_SCRUB_TABLE, new byte[] {0});
        kvs.putWithTimestamps(
                AtlasDbConstants.OLD_SCRUB_TABLE,
                ImmutableMultimap.of(
                        cell1, Value.create("foo\0bar".getBytes(StandardCharsets.UTF_8), 10),
                        cell1, Value.create("baz".getBytes(StandardCharsets.UTF_8), 20),
                        cell2, Value.create("foo".getBytes(StandardCharsets.UTF_8), 30),
                        cell3, Value.create("foo\0bar\0baz".getBytes(StandardCharsets.UTF_8), 40)));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ScrubQueueMigrationCommand.run(
                kvs,
                new PrintWriter(new BufferedWriter(new OutputStreamWriter(baos, StandardCharsets.UTF_8)), true),
                1000);
        BatchingVisitable<SortedMap<Long, Multimap<TableReference, Cell>>> visitable =
                scrubStore.getBatchingVisitableScrubQueue(
                        Long.MAX_VALUE, PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY);
        assertThat(Iterables.getOnlyElement(BatchingVisitables.copyToList(visitable)))
                .containsExactlyInAnyOrderEntriesOf(ImmutableSortedMap.of(
                        10L, ImmutableMultimap.of(foo, cell1, bar, cell1),
                        20L, ImmutableMultimap.of(baz, cell1),
                        30L, ImmutableMultimap.of(foo, cell2),
                        40L, ImmutableMultimap.of(foo, cell3, bar, cell3, baz, cell3)));
        String output = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        assertThat(output).describedAs(output).contains("Starting iteration 2");
        assertThat(output).describedAs(output).contains("Moved 4 cells");
        assertThat(output).describedAs(output).contains("into 7 cells");
        assertThat(output).describedAs(output).contains("Finished all iterations");
    }
}
