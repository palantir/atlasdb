/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class FailSafeKeyValueServiceDataTrackerTest {
    private static final TableReference TABLE_REFERENCE = TableReference.createWithEmptyNamespace("test-table");
    private static final String METHOD_NAME = "test-method-name";
    private static final long BYTES_READ = 10L;

    @Mock
    private KeyValueServiceDataTracker delegate;

    private KeyValueServiceDataTracker dataTracker;

    @Before
    public void setUp() {
        dataTracker = new FailSafeKeyValueServiceDataTracker(delegate);
    }

    @Test
    public void recordReadForTableDoesNotThrowWhenDelegateRecordReadForTableThrows() {
        doThrow(RuntimeException.class).when(delegate).recordReadForTable(TABLE_REFERENCE, METHOD_NAME, BYTES_READ);
        dataTracker.recordReadForTable(TABLE_REFERENCE, METHOD_NAME, BYTES_READ);
    }

    @Test
    public void recordTableAgnosticReadDoesNotThrowWhenDelegateRecordTableAgnosticReadThrows() {
        doThrow(RuntimeException.class).when(delegate).recordTableAgnosticRead(METHOD_NAME, BYTES_READ);
        dataTracker.recordTableAgnosticRead(METHOD_NAME, BYTES_READ);
    }

    @Test
    public void recordCallForTableDoesNotThrowWhenDelegateRecordCallForTableThrows() {
        when(delegate.recordCallForTable(TABLE_REFERENCE)).thenThrow(RuntimeException.class);
        dataTracker.recordCallForTable(TABLE_REFERENCE);
    }

    @Test
    public void recordCallForTableBytesTrackerRecordDoesNotThrowWhenDelegateRecordForTableBytesTrackerRecordThrows() {
        BytesReadTracker tracker = mock(BytesReadTracker.class);
        doThrow(RuntimeException.class).when(tracker).record(BYTES_READ);
        when(delegate.recordCallForTable(TABLE_REFERENCE)).thenReturn(tracker);
        dataTracker.recordCallForTable(TABLE_REFERENCE).record(BYTES_READ);
    }
}
