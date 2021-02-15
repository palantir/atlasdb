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
package com.palantir.atlasdb.sweep.queue.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import java.util.NoSuchElementException;
import org.junit.Test;

public class SweepTableIndicesTest {
    private final KeyValueService kvs = new InMemoryKeyValueService(true);
    private final IdsToNames idsToNames = new IdsToNames(kvs);
    private final NamesToIds namesToIds = new NamesToIds(kvs);
    private final SweepTableIndices tableIndices = new SweepTableIndices(idsToNames, namesToIds);

    @Test
    public void testThrowsIfUnknownTableId() {
        assertThatExceptionOfType(NoSuchElementException.class)
                .isThrownBy(() -> tableIndices.getTableReference(3))
                .withMessage("Id 3 does not exist");
    }

    @Test
    public void testSucceedsOnFirstAttempt() {
        assertThat(tableIndices.getTableId(table(1))).isEqualTo(1);
        assertThat(tableIndices.getTableReference(1)).isEqualTo(table(1));
    }

    @Test
    public void testConcurrentCreationOfSameTable() {
        namesToIds.storeAsPending(table(1), 2);
        assertThat(tableIndices.getTableId(table(1))).isEqualTo(2);
    }

    @Test
    public void testConcurrentCreationOfDifferentTable() {
        idsToNames.storeNewMapping(table(2), 1);
        assertThat(tableIndices.getTableId(table(1))).isEqualTo(2);
    }

    @Test
    public void testConcurrentCreationOfMultipleTables() {
        namesToIds.storeAsPending(table(1), 1);
        idsToNames.storeNewMapping(table(2), 1);
        assertThat(tableIndices.getTableId(table(1))).isEqualTo(2);
    }

    private static TableReference table(int id) {
        return TableReference.create(Namespace.create(Integer.toString(id)), Integer.toString(id));
    }
}
