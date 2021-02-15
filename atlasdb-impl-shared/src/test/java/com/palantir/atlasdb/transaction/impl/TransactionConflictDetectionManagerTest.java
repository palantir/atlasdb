/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.google.common.cache.CacheLoader;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeNullPointerException;
import javax.annotation.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class TransactionConflictDetectionManagerTest {

    private static final TableReference TABLE_REFERENCE =
            TableReference.create(Namespace.EMPTY_NAMESPACE, "test_table");

    @Mock
    private CacheLoader<TableReference, ConflictHandler> delegate;

    private TransactionConflictDetectionManager conflictDetectionManager;

    @Before
    public void before() {
        conflictDetectionManager = new TransactionConflictDetectionManager(new ConflictDetectionManager(delegate) {
            @Override
            @Nullable
            public ConflictHandler get(TableReference tableReference) {
                try {
                    return delegate.load(tableReference);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Test
    public void testDisableReadWriteConflict_throwsIfDelegateValueIsNull() {
        assertThatLoggableExceptionThrownBy(() -> whenDisableReadWriteConflict(null))
                .isExactlyInstanceOf(SafeNullPointerException.class)
                .hasLogMessage("Conflict handler cannot be null when overwriting");
    }

    @Test
    public void testDisableReadWriteConflict_multipleCalls() throws Exception {
        testDisableReadWriteConflict(ConflictHandler.SERIALIZABLE, ConflictHandler.RETRY_ON_WRITE_WRITE);
        testDisableReadWriteConflict(ConflictHandler.SERIALIZABLE, ConflictHandler.RETRY_ON_WRITE_WRITE);
    }

    @Test
    public void testDisableReadWriteConflict_throwsIfCalledAfterTableWasUsed() throws Exception {
        when(delegate.load(TABLE_REFERENCE)).thenReturn(ConflictHandler.SERIALIZABLE);
        conflictDetectionManager.get(TABLE_REFERENCE);
        assertThatLoggableExceptionThrownBy(() -> conflictDetectionManager.disableReadWriteConflict(TABLE_REFERENCE))
                .isExactlyInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("Cannot overwrite conflict behaviour after the table has already been used");
    }

    @Test
    public void testDisableReadWriteConflict_Serializable() throws Exception {
        testDisableReadWriteConflict(ConflictHandler.SERIALIZABLE, ConflictHandler.RETRY_ON_WRITE_WRITE);
    }

    @Test
    public void testDisableReadWriteConflict_SerializableCell() throws Exception {
        testDisableReadWriteConflict(ConflictHandler.SERIALIZABLE_CELL, ConflictHandler.RETRY_ON_WRITE_WRITE_CELL);
    }

    @Test
    public void testDisableReadWriteConflict_IgnoreAll() throws Exception {
        testDisableReadWriteConflict(ConflictHandler.IGNORE_ALL, ConflictHandler.IGNORE_ALL);
    }

    @Test
    public void testDisableReadWriteConflict_RetryOnWriteWrite() throws Exception {
        testDisableReadWriteConflict(ConflictHandler.RETRY_ON_WRITE_WRITE, ConflictHandler.RETRY_ON_WRITE_WRITE);
    }

    @Test
    public void testDisableReadWriteConflict_RetryOnValueChanges() throws Exception {
        testDisableReadWriteConflict(ConflictHandler.RETRY_ON_VALUE_CHANGED, ConflictHandler.RETRY_ON_VALUE_CHANGED);
    }

    @Test
    public void testDisableReadWriteConflict_RetryOnWriteWriteCell() throws Exception {
        testDisableReadWriteConflict(
                ConflictHandler.RETRY_ON_WRITE_WRITE_CELL, ConflictHandler.RETRY_ON_WRITE_WRITE_CELL);
    }

    @Test
    public void testDisableReadWriteConflict_Unsupported() {
        testDisableReadWriteConflictThrowsUnsupported(ConflictHandler.SERIALIZABLE_INDEX);
        testDisableReadWriteConflictThrowsUnsupported(ConflictHandler.SERIALIZABLE_LOCK_LEVEL_MIGRATION);
    }

    private void testDisableReadWriteConflict(ConflictHandler initial, ConflictHandler overriden) throws Exception {
        whenDisableReadWriteConflict(initial);
        assertThat(conflictDetectionManager.get(TABLE_REFERENCE)).isEqualTo(overriden);
    }

    private void testDisableReadWriteConflictThrowsUnsupported(ConflictHandler initial) {
        assertThatThrownBy(() -> whenDisableReadWriteConflict(initial))
                .isExactlyInstanceOf(UnsupportedOperationException.class);
    }

    private void whenDisableReadWriteConflict(ConflictHandler initial) throws Exception {
        when(delegate.load(TABLE_REFERENCE)).thenReturn(initial);
        conflictDetectionManager.disableReadWriteConflict(TABLE_REFERENCE);
    }
}
