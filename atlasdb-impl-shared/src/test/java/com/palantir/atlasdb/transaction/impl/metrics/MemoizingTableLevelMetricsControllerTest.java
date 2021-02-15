/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Counter;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class MemoizingTableLevelMetricsControllerTest {
    private static final String METRIC_NAME = "name";
    private static final Class<String> STRING_CLASS = String.class;
    private static final TableReference TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("a.b");
    private static final TableReference TABLE_REFERENCE_2 = TableReference.createFromFullyQualifiedName("a.bc");

    private final TableLevelMetricsController delegate = mock(TableLevelMetricsController.class);
    private final TableLevelMetricsController memoizing = new MemoizingTableLevelMetricsController(delegate);

    @Before
    public void setUp() {
        when(delegate.createAndRegisterCounter(any(), anyString(), any())).thenAnswer(_invocation -> new Counter());
    }

    @Test
    public void metricIsOnlyRegisteredOnce() {
        Counter c1 = memoizing.createAndRegisterCounter(STRING_CLASS, METRIC_NAME, TABLE_REFERENCE);
        Counter c2 = memoizing.createAndRegisterCounter(STRING_CLASS, METRIC_NAME, TABLE_REFERENCE);

        assertThat(c1).isSameAs(c2);
        verify(delegate, times(1)).createAndRegisterCounter(STRING_CLASS, METRIC_NAME, TABLE_REFERENCE);
    }

    @Test
    public void registersDifferentMetricsSeparately() {
        Counter c1 = memoizing.createAndRegisterCounter(STRING_CLASS, METRIC_NAME, TABLE_REFERENCE);
        Counter c2 = memoizing.createAndRegisterCounter(STRING_CLASS, METRIC_NAME, TABLE_REFERENCE_2);

        assertThat(c1).isNotSameAs(c2);
        verify(delegate).createAndRegisterCounter(STRING_CLASS, METRIC_NAME, TABLE_REFERENCE);
        verify(delegate).createAndRegisterCounter(STRING_CLASS, METRIC_NAME, TABLE_REFERENCE_2);
    }

    @Test
    public void stressTest() {
        int numTableReferences = 1000;
        List<TableReference> tableReferences = IntStream.range(0, numTableReferences)
                .mapToObj(this::getTableReference)
                .collect(Collectors.toList());
        for (int iteration = 0; iteration < 5; iteration++) {
            Collections.shuffle(tableReferences);
            tableReferences.forEach(
                    tableReference -> memoizing.createAndRegisterCounter(STRING_CLASS, METRIC_NAME, tableReference));
        }

        verify(delegate, times(numTableReferences)).createAndRegisterCounter(any(), any(), any());
    }

    private TableReference getTableReference(int tableReferenceIndex) {
        return TableReference.createFromFullyQualifiedName("a.b" + tableReferenceIndex);
    }
}
