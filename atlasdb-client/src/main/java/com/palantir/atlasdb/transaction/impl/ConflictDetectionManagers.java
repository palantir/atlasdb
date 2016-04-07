/**
 * Copyright 2015 Palantir Technologies
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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class ConflictDetectionManagers {
    private ConflictDetectionManagers() {
        //
    }

    public static ConflictDetectionManager createDefault(KeyValueService kvs) {
        return new ConflictDetectionManager(getTablesToConflictDetectSupplier(kvs));
    }

    public static ConflictDetectionManager createFromSchema(Schema schema) {
        return new ConflictDetectionManager(RecomputingSupplier.create(Suppliers.ofInstance(getTablesToConflictDetect(schema))));
    }

    public static ConflictDetectionManager fromMap(final Map<TableReference, ConflictHandler> map) {
        return new ConflictDetectionManager(RecomputingSupplier.create(Suppliers.ofInstance(map)));
    }

    /**
     * Don't do conflict detection. Use this for read only transactions.
     */
    public static ConflictDetectionManager withoutConflictDetection(KeyValueService kvs) {
        return new ConflictDetectionManager(getNoConflictDetectSupplier(kvs));
    }

    private static RecomputingSupplier<Map<TableReference, ConflictHandler>> getNoConflictDetectSupplier(final KeyValueService kvs) {
        return RecomputingSupplier.create(new Supplier<Map<TableReference, ConflictHandler>>() {
            @Override
            public Map<TableReference, ConflictHandler> get() {
                Set<TableReference> tables = kvs.getAllTableNames();
                return Maps.asMap(tables, Functions.constant(ConflictHandler.IGNORE_ALL));
            }
        });
    }

    private static RecomputingSupplier<Map<TableReference, ConflictHandler>> getTablesToConflictDetectSupplier(final KeyValueService keyValueService) {
        return RecomputingSupplier.create(new Supplier<Map<TableReference, ConflictHandler>>() {
            @Override
            public Map<TableReference, ConflictHandler> get() {
                return getTablesToConflictDetect(keyValueService);
            }
        });
    }

    private static Map<TableReference, ConflictHandler> getTablesToConflictDetect(KeyValueService kvs) {
        return ImmutableMap.copyOf(Maps.transformValues(kvs.getMetadataForTables(), new Function<byte[], ConflictHandler>() {
            @Override
            public ConflictHandler apply(byte[] metadataForTable) {
                if (metadataForTable != null && metadataForTable.length > 0) {
                    return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadataForTable).getConflictHandler();
                } else {
                    return ConflictHandler.RETRY_ON_WRITE_WRITE;
                }
            }
        }));
    }

    private static Map<TableReference, ConflictHandler> getTablesToConflictDetect(Schema schema) {
        Map<TableReference, ConflictHandler> ret = Maps.newHashMap();
        for (Map.Entry<TableReference, TableMetadata> e : schema.getAllTablesAndIndexMetadata().entrySet()) {
            ret.put(e.getKey(), e.getValue().getConflictHandler());
        }
        return ret;
    }
}
