/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.cli.command;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.SortedMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        kvs.putWithTimestamps(AtlasDbConstants.OLD_SCRUB_TABLE, ImmutableMultimap.of(
                cell1, Value.create("foo\0bar".getBytes(), 10),
                cell1, Value.create("baz".getBytes(), 20),
                cell2, Value.create("foo".getBytes(), 30),
                cell3, Value.create("foo\0bar\0baz".getBytes(), 40)));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ScrubQueueMigrationCommand.run(kvs, new PrintWriter(baos, true), 1000);
        BatchingVisitable<SortedMap<Long, Multimap<TableReference, Cell>>> visitable =
                scrubStore.getBatchingVisitableScrubQueue(
                        Long.MAX_VALUE, PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY);
        Assert.assertEquals(ImmutableSortedMap.of(
                10L, ImmutableMultimap.of(foo, cell1, bar, cell1),
                20L, ImmutableMultimap.of(baz, cell1),
                30L, ImmutableMultimap.of(foo, cell2),
                40L, ImmutableMultimap.of(foo, cell3, bar, cell3, baz, cell3)),
                Iterables.getOnlyElement(BatchingVisitables.copyToList(visitable)));
        String output = new String(baos.toByteArray());
        Assert.assertTrue(output, output.contains("Starting iteration 2"));
        Assert.assertTrue(output, output.contains("Moved 4 cells"));
        Assert.assertTrue(output, output.contains("into 7 cells"));
        Assert.assertTrue(output, output.contains("Finished all iterations"));
    }
}
