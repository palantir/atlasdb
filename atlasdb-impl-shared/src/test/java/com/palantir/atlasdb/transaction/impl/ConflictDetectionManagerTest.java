/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class ConflictDetectionManagerTest {

    private static final TableReference TABLE_REFERENCE =
            TableReference.create(Namespace.EMPTY_NAMESPACE, "test_table");

    @Mock
    private CacheLoader<TableReference, ConflictHandler> delegate;

    private ConflictDetectionManager conflictDetectionManager;

    @BeforeEach
    public void before() {
        conflictDetectionManager = new ConflictDetectionManager(delegate);
    }

    @Test
    public void testThatConflictDetectionManagerDoesNotCacheNegativeLookup() throws Exception {
        when(delegate.load(TABLE_REFERENCE)).thenReturn(null).thenReturn(ConflictHandler.SERIALIZABLE);
        assertThat(conflictDetectionManager.get(TABLE_REFERENCE)).isEmpty();
        assertThat(conflictDetectionManager.get(TABLE_REFERENCE)).contains(ConflictHandler.SERIALIZABLE);
    }

    @Test
    public void testThatConflictDetectionManagerCachesLookup() throws Exception {
        when(delegate.load(TABLE_REFERENCE))
                .thenReturn(ConflictHandler.SERIALIZABLE)
                .thenReturn(null);
        assertThat(conflictDetectionManager.get(TABLE_REFERENCE)).contains(ConflictHandler.SERIALIZABLE);
        // No cache invalidation is performed, consequently we expect the same result. Should cache invalidation be
        // added in the future, then we need to change this test.
        assertThat(conflictDetectionManager.get(TABLE_REFERENCE)).contains(ConflictHandler.SERIALIZABLE);
    }
}
