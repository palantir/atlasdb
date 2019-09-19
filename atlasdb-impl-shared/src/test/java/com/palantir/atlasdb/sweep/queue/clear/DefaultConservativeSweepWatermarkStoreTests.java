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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.table.description.Schemas;
import org.junit.Before;
import org.junit.Test;

public class DefaultConservativeSweepWatermarkStoreTests {
    private static final TableReference TABLE = TableReference.create(Namespace.create("foo"), "bar");
    private final KeyValueService kvs = new InMemoryKeyValueService(false);
    private final ConservativeSweepWatermarkStore watermarkStore = new DefaultConservativeSweepWatermarkStore(kvs);

    @Before
    public void before() {
        Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    @Test
    public void testWatermarkUpdates_newWatermark() {
        watermarkStore.updateWatermarks(2, ImmutableSet.of(TABLE));
        assertThat(watermarkStore.getWatermarks(ImmutableSet.of(TABLE))).containsEntry(TABLE, 2L);
    }

    @Test
    public void testWatermarkUpdates_laterWatermarkUnchanged() {
        watermarkStore.updateWatermarks(4, ImmutableSet.of(TABLE));
        watermarkStore.updateWatermarks(3, ImmutableSet.of(TABLE));
        assertThat(watermarkStore.getWatermarks(ImmutableSet.of(TABLE))).containsEntry(TABLE, 4L);
    }

    @Test
    public void testWatermarkUpdates_updates() {
        watermarkStore.updateWatermarks(3, ImmutableSet.of(TABLE));
        watermarkStore.updateWatermarks(4, ImmutableSet.of(TABLE));
        assertThat(watermarkStore.getWatermarks(ImmutableSet.of(TABLE))).containsEntry(TABLE, 4L);
    }
}
