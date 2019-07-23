/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.migration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.Test;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class KvsProgressCheckPointImplTest {
    private final KeyValueService kvs = new InMemoryKeyValueService(false);

    private KvsProgressCheckPointImpl kvsProgressCheckPoint = new KvsProgressCheckPointImpl(kvs);

    @Test
    public void testFirstReadReturnsEmpty() {
        assertThat(kvsProgressCheckPoint.getNextStartRow()).isEmpty();
    }

    @Test
    public void testWriteThenRead() {
        byte[] checkpointValue = PtBytes.toBytes("something");

        kvsProgressCheckPoint.setNextStartRow(Optional.of(checkpointValue));

        assertThat(kvsProgressCheckPoint.getNextStartRow()).contains(checkpointValue);
    }

    @Test
    public void testMultipleWritesThenRead() {
        byte[] expectedCheckpointValue = PtBytes.toBytes("something");

        kvsProgressCheckPoint.setNextStartRow(Optional.of(PtBytes.toBytes("something-else")));
        kvsProgressCheckPoint.setNextStartRow(Optional.of(PtBytes.toBytes("something-else-else")));
        kvsProgressCheckPoint.setNextStartRow(Optional.of(expectedCheckpointValue));

        assertThat(kvsProgressCheckPoint.getNextStartRow()).contains(expectedCheckpointValue);
    }
}