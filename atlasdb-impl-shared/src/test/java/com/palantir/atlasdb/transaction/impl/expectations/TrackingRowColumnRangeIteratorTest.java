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

package com.palantir.atlasdb.transaction.impl.expectations;

import static com.palantir.logsafe.testing.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.Test;

public class TrackingRowColumnRangeIteratorTest {
    public static final ImmutableMap<Cell, Value> VALUE_BY_CELL = ImmutableMap.<Cell, Value>builder()
            .put(Cell.create(bytes("row1"), bytes("col1")), Value.create(bytes("contents1"), 1L))
            .put(Cell.create(bytes("row20"), bytes("column20")), Value.create(bytes("contents20"), 20L))
            .put(Cell.create(bytes("r3"), bytes("c3")), Value.create(bytes("c3"), 3L))
            .buildOrThrow();

    @Test
    public void trackingIteratorForwardsData() {
        Iterator<Map.Entry<Cell, Value>> iterator = spawnIterator();
        TrackingRowColumnRangeIterator trackingIterator = new TrackingRowColumnRangeIterator(spawnIterator(), noOp());

        trackingIterator.forEachRemaining(entry -> {
            assertThat(iterator.hasNext()).isTrue();
            assertThat(entry).isEqualTo(iterator.next());
        });

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void trackingIteratorTracksData() {
        TrackingRowColumnRangeIterator trackingIterator =
                new TrackingRowColumnRangeIterator(spawnIterator(), new Consumer<>() {
                    final Iterator<Map.Entry<Cell, Value>> baseIterator = spawnIterator();

                    @Override
                    public void accept(Long bytes) {
                        assertThat(bytes).isEqualTo(ExpectationsMeasuringUtils.byteSize(baseIterator.next()));
                    }
                });

        trackingIterator.forEachRemaining(noOp());
    }

    private static Iterator<Map.Entry<Cell, Value>> spawnIterator() {
        return VALUE_BY_CELL.entrySet().stream().iterator();
    }

    private static <T> Consumer<T> noOp() {
        return t -> {};
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
