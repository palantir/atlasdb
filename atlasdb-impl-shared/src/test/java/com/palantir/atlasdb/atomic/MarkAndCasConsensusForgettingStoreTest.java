/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.atomic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class MarkAndCasConsensusForgettingStoreTest {
    private static final byte[] SAD = PtBytes.toBytes("sad");
    private static final byte[] HAPPY = PtBytes.toBytes("happy");
    private static final Cell CELL = Cell.create(PtBytes.toBytes("a"), PtBytes.toBytes("b"));
    public static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private static final byte[] IN_PROGRESS_MARKER = new byte[0];

    private final InMemoryKeyValueService kvs = new InMemoryKeyValueService(true);
    private final MarkAndCasConsensusForgettingStore store =
            new MarkAndCasConsensusForgettingStore(IN_PROGRESS_MARKER, kvs, TABLE);

    @Test
    public void canMarkCell() throws ExecutionException, InterruptedException {

        store.mark(CELL);
        assertThat(store.get(CELL).get()).hasValue(IN_PROGRESS_MARKER);
        assertThat(kvs.getAllTimestamps(TABLE, ImmutableSet.of(CELL), Long.MAX_VALUE)
                        .size())
                .isEqualTo(1);
    }

    @Test
    public void updatesMarkedCell() throws ExecutionException, InterruptedException {
        store.mark(CELL);
        store.atomicUpdate(CELL, HAPPY);
        assertThat(store.get(CELL).get()).hasValue(HAPPY);
    }

    @Test
    public void cannotUpdateUnmarkedCell() throws ExecutionException, InterruptedException {
        assertThatThrownBy(() -> store.atomicUpdate(CELL, SAD))
                .isInstanceOf(CheckAndSetException.class)
                .hasMessageContaining(
                        "Unexpected value observed in table test.table. If this is happening repeatedly, your program"
                                + " may be out of sync with the database");
        assertThat(store.get(CELL).get()).isEmpty();
    }
}
