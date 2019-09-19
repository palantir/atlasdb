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

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import org.junit.Test;

public final class IdsToNamesTest {
    private final KeyValueService kvs = new InMemoryKeyValueService(true);
    private final IdsToNames idsToNames = new IdsToNames(kvs);

    @Test
    public void testWorkflow() {
        assertThat(idsToNames.getNextId()).isEqualTo(1);
        assertThat(idsToNames.get(1)).isEmpty();
        assertThat(idsToNames.storeNewMapping(tableRef(0), 1)).isTrue();
        assertThat(idsToNames.storeNewMapping(tableRef(1), 1)).isFalse();
        assertThat(idsToNames.get(1)).contains(tableRef(0));
        assertThat(idsToNames.getNextId()).isEqualTo(2);
        assertThat(idsToNames.storeNewMapping(tableRef(1), 2)).isTrue();
        assertThat(idsToNames.getNextId()).isEqualTo(3);
    }

    private static TableReference tableRef(int index) {
        return TableReference.create(Namespace.create(Integer.toString(index)), Integer.toString(index));
    }
}
