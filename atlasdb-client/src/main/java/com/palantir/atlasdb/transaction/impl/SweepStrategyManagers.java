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
package com.palantir.atlasdb.transaction.impl;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;

public class SweepStrategyManagers {
    private SweepStrategyManagers() {
        //
    }

    public static SweepStrategyManager createDefault(KeyValueService kvs) {
        RecomputingSupplier<Map<TableReference, SweepStrategy>> supplier =
                RecomputingSupplier.create(() -> getSweepStrategies(kvs));
        return tableRef -> {
            SweepStrategy strategy = supplier.get().get(tableRef);
            if (strategy == null) {
                strategy = supplier.recompute().get(tableRef);
            }
            return Preconditions.checkNotNull(strategy, "unknown table", SafeArg.of("tableRef", tableRef));
        };
    }

    public static SweepStrategyManager createFromSchema(Schema schema) {
        return tableRef -> {
            TableMetadata tableMeta = Preconditions.checkNotNull(
                    schema.getAllTablesAndIndexMetadata().get(tableRef),
                    "unknown table",
                    SafeArg.of("tableRef", tableRef));
            return tableMeta.getSweepStrategy();
        };
    }

    public static SweepStrategyManager fromMap(final Map<TableReference, SweepStrategy> map) {
        return tableRef -> Preconditions.checkNotNull(
                map.get(tableRef),
                "unknown table",
                SafeArg.of("tableRef", tableRef));
    }

    public static SweepStrategyManager completelyConservative() {
        return tableRef -> SweepStrategy.CONSERVATIVE;
    }

    private static Map<TableReference, SweepStrategy> getSweepStrategies(KeyValueService kvs) {
        return ImmutableMap.copyOf(Maps.transformEntries(kvs.getMetadataForTables(), (tableRef, tableMeta) -> {
            if (tableMeta != null && tableMeta.length > 0) {
                return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(tableMeta).getSweepStrategy();
            } else {
                return SweepStrategy.CONSERVATIVE;
            }
        }));
    }
}
