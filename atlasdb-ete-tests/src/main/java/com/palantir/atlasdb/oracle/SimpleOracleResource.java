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
package com.palantir.atlasdb.oracle;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SimpleOracleResource implements OracleResource {
    private static final Set<String> TABLE_NAMES = Set.of("test", "test2", "test3", "test4");
    private KeyValueService atlas;
    private Optional<Namespace> maybeNamespace;

    public SimpleOracleResource(KeyValueService atlas, Optional<Namespace> maybeNamespace) {
        this.atlas = atlas;
        this.maybeNamespace = maybeNamespace;
    }

    @Override
    public List<String> getTables() {
        return atlas.getAllTableNames().stream()
                .map(TableReference::getTableName)
                .collect(Collectors.toList());
    }

    @Override
    public void createTables() {
        TABLE_NAMES.forEach(tableName -> atlas.createTable(
                maybeNamespace
                        .map(namespace -> TableReference.create(namespace, tableName))
                        .orElseGet(() -> TableReference.createWithEmptyNamespace(tableName)),
                AtlasDbConstants.GENERIC_TABLE_METADATA));
    }
}
