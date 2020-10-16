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
import org.junit.Test;

public final class NamesToIdsTest {
    private static final TableReference TABLE = TableReference.create(Namespace.create("namespace"), "table");
    private final KeyValueService kvs = new InMemoryKeyValueService(true);
    private final NamesToIds namesToIds = new NamesToIds(kvs);

    @Test
    public void testWorkflow() {
        assertThat(namesToIds.storeAsPending(TABLE, 1)).isEqualTo(SweepTableIdentifier.pending(1));
        namesToIds.storeAsPending(TABLE, 1, 2);
        assertThat(namesToIds.currentMapping(TABLE)).contains(SweepTableIdentifier.pending(2));
        namesToIds.moveToComplete(TABLE, 2);
        assertThat(namesToIds.currentMapping(TABLE)).contains(SweepTableIdentifier.identified(2));
    }

    @Test
    public void testConflicts() {
        assertThat(namesToIds.storeAsPending(TABLE, 1)).isEqualTo(SweepTableIdentifier.pending(1));
        assertThat(namesToIds.storeAsPending(TABLE, 2)).isEqualTo(SweepTableIdentifier.pending(1));
        namesToIds.storeAsPending(TABLE, 2, 3);
        assertThat(namesToIds.currentMapping(TABLE)).contains(SweepTableIdentifier.pending(1));
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> namesToIds.moveToComplete(TABLE, 2))
                .withMessage("Unexpectedly we state changed from pending(id) to not(identified(id)) after "
                        + "identifying id as the correct value");
        assertThat(namesToIds.currentMapping(TABLE)).contains(SweepTableIdentifier.pending(1));
        namesToIds.moveToComplete(TABLE, 1);
        assertThat(namesToIds.storeAsPending(TABLE, 1)).isEqualTo(SweepTableIdentifier.identified(1));
    }
}
