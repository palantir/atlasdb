/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.impl;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Functions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableMetadata;

public class SweepStrategyManagers {
    private SweepStrategyManagers() {
        //
    }

    public static SweepStrategyManager createDefault(KeyValueService kvs) {
        return new SweepStrategyManager(getSweepStrategySupplier(kvs));
    }

    public static SweepStrategyManager createFromSchema(Schema schema) {
        return new SweepStrategyManager(RecomputingSupplier.create(Suppliers.ofInstance(getSweepStrategies(schema))));
    }

    public static SweepStrategyManager fromMap(final Map<TableReference, SweepStrategy> map) {
        return new SweepStrategyManager(RecomputingSupplier.create(Suppliers.ofInstance(map)));
    }

    public static SweepStrategyManager completelyConservative(KeyValueService kvs) {
        return new SweepStrategyManager(getConservativeManager(kvs));
    }

    private static RecomputingSupplier<Map<TableReference, SweepStrategy>> getConservativeManager(final KeyValueService kvs) {
        return RecomputingSupplier.create(() -> {
            Set<TableReference> tables = kvs.getAllTableNames();
            return Maps.asMap(tables, Functions.constant(SweepStrategy.CONSERVATIVE));
        });
    }

    private static RecomputingSupplier<Map<TableReference, SweepStrategy>> getSweepStrategySupplier(final KeyValueService keyValueService) {
        return RecomputingSupplier.create(() -> getSweepStrategies(keyValueService));
    }

    private static Map<TableReference, SweepStrategy> getSweepStrategies(KeyValueService kvs) {
        return ImmutableMap.copyOf(Maps.transformEntries(kvs.getMetadataForTables(), (tableRef, tableMetadata) -> {
            if (tableMetadata != null && tableMetadata.length > 0) {
                return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(tableMetadata).getSweepStrategy();
            } else {
                return SweepStrategy.CONSERVATIVE;
            }
        }));
    }

    private static Map<TableReference, SweepStrategy> getSweepStrategies(Schema schema) {
        Map<TableReference, SweepStrategy> ret = Maps.newHashMap();
        for (Map.Entry<TableReference, TableMetadata> e : schema.getAllTablesAndIndexMetadata().entrySet()) {
            ret.put(e.getKey(), e.getValue().getSweepStrategy());
        }
        return ret;
    }
}
