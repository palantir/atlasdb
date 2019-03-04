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

package com.palantir.atlasdb.factory;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public class DeprecatedTablesCleaner extends Callback<TransactionManager> {

    private static final Logger log = LoggerFactory.getLogger(DeprecatedTablesCleaner.class);

    private final Set<TableReference> deprecatedTables;

    DeprecatedTablesCleaner(Set<Schema> schemas) {
        deprecatedTables = deprecatedTables(schemas);
    }

    @Override
    public void init(TransactionManager resource) {
        try {
            resource.getKeyValueService().dropTables(deprecatedTables);
            log.info("Successfully dropped deprecated tables on startup.");
        } catch (Throwable e) {
            log.info("Could not drop deprecated tables from the underlying KeyValueService.", e);
        }
    }

    private static Set<TableReference> deprecatedTables(Set<Schema> schemas) {
        return schemas.stream()
                .peek(Schema::validate)
                .map(Schema::getDeprecatedTables)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

}
