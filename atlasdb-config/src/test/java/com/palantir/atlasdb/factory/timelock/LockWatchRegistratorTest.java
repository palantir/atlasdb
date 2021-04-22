/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManager;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

public class LockWatchRegistratorTest {
    private static final TableDefinition NO_CACHING_TABLE = new TableDefinition() {
        {
            rowName();
            rowComponent("row", ValueType.BLOB);
            noColumns();
        }
    };
    private static final TableDefinition CACHING_TABLE = new TableDefinition() {
        {
            rowName();
            rowComponent("row", ValueType.BLOB);
            noColumns();
            enableCaching();
            conflictHandler(ConflictHandler.SERIALIZABLE_CELL);
        }
    };

    private final TransactionManager tm = mock(TransactionManager.class);
    private final LockWatchManager lockWatchManager = mock(LockWatchManager.class);

    @Before
    public void setupMocks() {
        when(tm.getLockWatchManager()).thenReturn(lockWatchManager);
    }

    @Test
    public void noCachingTest() {
        Set<Schema> schemas = createSchemas();
        schemas.forEach(schema -> schema.addTableDefinition("no_caching", NO_CACHING_TABLE));

        assertThat(LockWatchRegistrator.maybeCreate(schemas)).isEmpty();
    }

    @Test
    public void cachingTest() {
        Set<Schema> schemas = createSchemas();
        schemas.forEach(schema -> schema.addTableDefinition("no_caching", NO_CACHING_TABLE));

        Set<LockWatchReference> expectedReferences = new HashSet<>();
        Iterator<Schema> schemaIterator = schemas.iterator();
        Schema first = schemaIterator.next();
        first.addTableDefinition("caching", CACHING_TABLE);
        expectedReferences.add(LockWatchReferences.entireTable(getTableName(first, "caching")));
        Schema second = schemaIterator.next();
        second.addTableDefinition("caching_first", CACHING_TABLE);
        expectedReferences.add(LockWatchReferences.entireTable(getTableName(second, "caching_first")));
        second.addTableDefinition("caching_second", CACHING_TABLE);
        expectedReferences.add(LockWatchReferences.entireTable(getTableName(second, "caching_second")));

        Optional<LockWatchRegistrator> maybeRegistrator = LockWatchRegistrator.maybeCreate(schemas);
        assertThat(maybeRegistrator).isNotEmpty();

        try (LockWatchRegistrator registrator = maybeRegistrator.get()) {
            registrator.initialize(tm);
            Awaitility.await("Waiting for lock watch registration")
                    .atMost(Duration.ofSeconds(5))
                    .until(() -> verifyExpectedLockWatchesRegistered(expectedReferences));
        }
    }

    private static Set<Schema> createSchemas() {
        return IntStream.range(0, 3)
                .mapToObj(Integer::toString)
                .map(Namespace::create)
                .map(Schema::new)
                .collect(Collectors.toSet());
    }

    private static String getTableName(Schema first, String tableName) {
        return TableReference.create(first.getNamespace(), tableName).getQualifiedName();
    }

    private boolean verifyExpectedLockWatchesRegistered(Set<LockWatchReference> lockWatchReferences) {
        verify(lockWatchManager, atLeast(1)).registerPreciselyWatches(lockWatchReferences);
        verifyNoMoreInteractions(lockWatchManager);
        return true;
    }
}
