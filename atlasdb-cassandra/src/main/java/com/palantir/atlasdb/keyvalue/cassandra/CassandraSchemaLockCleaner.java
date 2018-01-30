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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Optional;
import java.util.Set;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;

public class CassandraSchemaLockCleaner {
    private static final Logger log = LoggerFactory.getLogger(CassandraSchemaLockCleaner.class);

    private final SchemaMutationLockTables lockTables;
    private final CassandraTableDropper cassandraTableDropper;
    private final SchemaMutationLock schemaMutationLock;

    public CassandraSchemaLockCleaner(SchemaMutationLockTables lockTables,
            SchemaMutationLock schemaMutationLock,
            CassandraTableDropper cassandraTableDropper) {
        this.lockTables = lockTables;
        this.schemaMutationLock = schemaMutationLock;
        this.cassandraTableDropper = cassandraTableDropper;
    }

    public void cleanLocksState() throws TException {
        Set<TableReference> tables = lockTables.getAllLockTables();
        Optional<TableReference> tableToKeep = tables.stream().findFirst();
        if (!tableToKeep.isPresent()) {
            log.info("No lock tables to clean up.");
            return;
        }
        tables.remove(tableToKeep.get());
        if (tables.size() > 0) {
            cassandraTableDropper.dropTables(tables);
            LoggingArgs.SafeAndUnsafeTableReferences safeAndUnsafe = LoggingArgs.tableRefs(tables);
            log.info("Dropped tables {} and {}", safeAndUnsafe.safeTableRefs(), safeAndUnsafe.unsafeTableRefs());
        }

        // TODO We want to make the SchemaMutationLock object, now that we know which table to use

        schemaMutationLock.cleanLockState();
        log.info("Reset the schema mutation lock in table [{}]",
                LoggingArgs.tableRef(tableToKeep.get()));
    }
}
