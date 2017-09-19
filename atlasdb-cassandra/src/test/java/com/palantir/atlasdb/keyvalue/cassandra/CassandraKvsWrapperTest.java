/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.exception.NotInitializedException;

public class CassandraKvsWrapperTest {
    private static final CassandraKeyValueServiceImpl kvs = mock(CassandraKeyValueServiceImpl.class);
    private static final CassandraKeyValueService kvsWrapper = spy(kvs.new InitializingWrapper());

    @Test
    public void ifWrapperIsInitializedDelegateIsCalled() {
        when(kvsWrapper.isInitialized()).thenReturn(true);
        TableReference tableRef = TableReference.create(Namespace.DEFAULT_NAMESPACE, "test");
        kvsWrapper.createTable(tableRef, AtlasDbConstants.GENERIC_TABLE_METADATA);
        verify(kvs).createTable(any(TableReference.class), any());
    }

    @Test
    public void ifWrapperIsNotInitializedDelegateIsNotCalled() {
        when(kvsWrapper.isInitialized()).thenReturn(false);
        TableReference tableRef = TableReference.create(Namespace.DEFAULT_NAMESPACE, "test");
        assertThatThrownBy(() -> kvsWrapper.createTable(tableRef, AtlasDbConstants.GENERIC_TABLE_METADATA))
                .isInstanceOf(NotInitializedException.class);
        verify(kvs, never()).createTable(any(TableReference.class), any());
    }
}
