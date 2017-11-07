/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.benchmarks.benchmarks;

import java.util.Map;

import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.timelock.benchmarks.RandomBytes;
import com.palantir.atlasdb.timelock.benchmarks.config.BenchmarkSettings;
import com.palantir.atlasdb.timelock.benchmarks.schema.BenchmarksSchema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public final class KvsReadBenchmark extends AbstractBenchmark<KvsReadBenchmark.Settings> {

    private static final TableReference TABLE = BenchmarksSchema.BLOBS_TABLE_REF;

    @Value.Immutable
    public interface Settings extends BenchmarkSettings { }

    private final byte[] data = RandomBytes.ofLength(16);
    private final Cell cell = Cell.create(data, data);
    private final KeyValueService keyValueService;

    public static Map<String, Object> execute(SerializableTransactionManager txnManager, Settings settings) {
        return new KvsReadBenchmark(txnManager.getKeyValueService(), settings).execute();
    }

    private KvsReadBenchmark(KeyValueService keyValueService, Settings settings) {
        super(settings);
        this.keyValueService = keyValueService;
    }

    @Override
    public void setup() {
        keyValueService.put(TABLE, ImmutableMap.of(cell, data), 100L);
    }

    @Override
    protected void performOneCall() {
        byte[] result = keyValueService.get(TABLE, ImmutableMap.of(cell, 200L))
                .get(data).getContents();
        Preconditions.checkState(result.length == data.length);
    }
}
