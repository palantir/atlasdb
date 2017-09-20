/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.impl;

import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.AutoDelegate_TableMappingService;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.exception.NotInitializedException;
import com.palantir.processors.AutoDelegate;

@AutoDelegate(typeToExtend = TableMappingService.class)
public final class StaticTableMappingService extends AbstractTableMappingService {
    private class InitializingWrapper extends AsyncInitializer implements AutoDelegate_TableMappingService {
        @Override
        public TableMappingService delegate() {
            checkInitialized();
            return StaticTableMappingService.this;
        }

        @Override
        protected void tryInitialize() {
            StaticTableMappingService.this.updateTableMap();
        }

        @Override
        protected String getClassName() {
            return "StaticTableMappingService";
        }
    }

    private final InitializingWrapper wrapper = new InitializingWrapper();
    private final KeyValueService kv;

    public static TableMappingService create(KeyValueService kv) {
        return create(kv, AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static TableMappingService create(KeyValueService kv, boolean initializeAsync) {
        StaticTableMappingService tableMappingService = new StaticTableMappingService(kv);
        tableMappingService.wrapper.initialize(initializeAsync);
        return tableMappingService.wrapper.isInitialized() ? tableMappingService : tableMappingService.wrapper;
    }

    private StaticTableMappingService(KeyValueService kv) {
        this.kv = kv;
    }

    @Override
    public TableReference addTable(TableReference tableRef) {
        return tableRef;
    }

    @Override
    public void removeTable(TableReference tableRef) {
        // We don't need to do any work here because we don't store a mapping.
    }

    @Override
    protected BiMap<TableReference, TableReference> readTableMap() {
        return HashBiMap.create(
                kv.getAllTableNames()
                        .stream()
                        .collect(
                                Collectors.toMap(
                                        Function.identity(),
                                        Function.identity())
                        )
        );
    }

    @Override
    protected void validateShortName(TableReference tableRef, TableReference shortName) {
        // any name is ok for the static mapper
    }
}
