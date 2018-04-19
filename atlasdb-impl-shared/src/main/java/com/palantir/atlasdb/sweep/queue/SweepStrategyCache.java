/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;

public class SweepStrategyCache {
        private final KeyValueService kvs;

        private ConcurrentMap<TableReference, TableMetadataPersistence.SweepStrategy> cache = new ConcurrentHashMap<>();

        public SweepStrategyCache(KeyValueService kvs) {
            this.kvs = kvs;
        }

        public TableMetadataPersistence.SweepStrategy getStrategy(WriteInfo writeInfo) {
            return cache.computeIfAbsent(writeInfo.tableRefCell().tableRef(), this::getStrategyFromKvs);
        }

        private TableMetadataPersistence.SweepStrategy getStrategyFromKvs(TableReference tableRef) {
            // todo(gmaretic): fail gracefully if we cannot hydrate? How -- return NOTHING?
            return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(kvs.getMetadataForTable(tableRef)).getSweepStrategy();
        }
}
