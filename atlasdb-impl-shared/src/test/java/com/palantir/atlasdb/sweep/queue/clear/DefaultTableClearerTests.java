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

package com.palantir.atlasdb.sweep.queue.clear;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.sweep.queue.WriteInfo;
import com.palantir.atlasdb.table.description.TableMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class DefaultTableClearerTests {
    private static final TableReference TABLE = TableReference.create(Namespace.create("foo"), "bar");
    private static final Cell CELL = Cell.create(new byte[1], new byte[2]);
    private long immutableTimestamp = 0;

    @Mock private KeyValueService kvs;
    @Mock private ConservativeSweepWatermarkStore watermarkStore;

    private DefaultTableClearer clearer;

    @Before
    public void before() {
        clearer = new DefaultTableClearer(watermarkStore, kvs, () -> immutableTimestamp);
    }

    @Test
    public void testFilter_ignoresIfThorough() {
        createTable(SweepStrategy.THOROUGH);
        WriteInfo write = writeInfo(1);
        assertThat(clearer.filter(ImmutableList.of(write))).containsExactly(write);
    }

    @Test
    public void testFilter_ignoresIfNoSweep() {
        createTable(SweepStrategy.NOTHING);
        WriteInfo write = writeInfo(1);
        assertThat(clearer.filter(ImmutableList.of(write))).containsExactly(write);
    }

    @Test
    public void testFilter_filtersIfConservativeSweepAndBehindWatermark() {
        createTable(SweepStrategy.CONSERVATIVE);
        when(watermarkStore.getWatermarks(ImmutableSet.of(TABLE))).thenReturn(ImmutableMap.of(TABLE, 2L));
        WriteInfo write = writeInfo(1);
        assertThat(clearer.filter(ImmutableList.of(write))).isEmpty();
    }

    @Test
    public void testFilter_doesNotFilterIfAheadOfWatermark() {
        createTable(SweepStrategy.CONSERVATIVE);
        when(watermarkStore.getWatermarks(ImmutableSet.of(TABLE))).thenReturn(ImmutableMap.of(TABLE, 2L));
        WriteInfo write = writeInfo(3);
        assertThat(clearer.filter(ImmutableList.of(write))).containsExactly(write);
    }

    @Test
    public void testDestructiveAction_watermarkUpdateIfConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        immutableTimestamp = 4;
        clearer.truncateTables(ImmutableSet.of(TABLE));
        verify(watermarkStore).updateWatermarks(4, ImmutableSet.of(TABLE));
    }

    @Test
    public void testDestructiveAction_noWatermarkUpdateIfThorough() {
        createTable(SweepStrategy.THOROUGH);
        immutableTimestamp = 4;
        clearer.truncateTables(ImmutableSet.of(TABLE));
        verifyZeroInteractions(watermarkStore);
    }

    private static WriteInfo writeInfo(long timestamp) {
        return WriteInfo.write(TABLE, CELL, timestamp);
    }

    private void createTable(SweepStrategy sweepStrategy) {
        TableMetadata metadata = TableMetadata.builder()
                .sweepStrategy(sweepStrategy)
                .build();
        when(kvs.getMetadataForTable(TABLE)).thenReturn(metadata.persistToBytes());
    }
}
