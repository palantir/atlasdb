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
package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.SweepStrategy;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ReadTransactionShould {

    private static final TableReference DUMMY_CONSERVATIVE_TABLE =
            TableReference.createWithEmptyNamespace("dummy-conservative");
    private static final TableReference DUMMY_THOROUGH_TABLE =
            TableReference.createWithEmptyNamespace("dummy-thorough");
    private static final Cell DUMMY_CELL = Cell.create("row".getBytes(), "col".getBytes());
    private static final byte[] EMPTY_BYTES = "".getBytes();

    private static final Map<String, Object[]> simpleGets = ImmutableMap.<String, Object[]>builder()
            .put("get", new Object[] {DUMMY_THOROUGH_TABLE, ImmutableSet.of(DUMMY_CELL)})
            .put("getRows", new Object[] {DUMMY_THOROUGH_TABLE, ImmutableList.of(EMPTY_BYTES), ColumnSelection.all()})
            .put("getRange", new Object[] {DUMMY_THOROUGH_TABLE, RangeRequest.all()})
            .put("getRanges", new Object[] {DUMMY_THOROUGH_TABLE, ImmutableList.of(RangeRequest.all())})
            .put("getRangesLazy", new Object[] {DUMMY_THOROUGH_TABLE, ImmutableList.of(RangeRequest.all())})
            .build();

    private ReadTransaction readTransaction;
    private AbstractTransaction delegateTransaction;

    @Before
    public void setUp() {
        delegateTransaction = Mockito.mock(AbstractTransaction.class);
        SweepStrategyManager sweepStrategies = Mockito.mock(SweepStrategyManager.class);
        when(sweepStrategies.get(DUMMY_CONSERVATIVE_TABLE)).thenReturn(SweepStrategy.CONSERVATIVE);
        when(sweepStrategies.get(DUMMY_THOROUGH_TABLE)).thenReturn(SweepStrategy.THOROUGH);
        readTransaction = new ReadTransaction(delegateTransaction, sweepStrategies);
    }

    @Test
    public void notAllowPuts() {
        checkThrowsAndNoInteraction(
                () -> readTransaction.put(DUMMY_CONSERVATIVE_TABLE, ImmutableMap.of(DUMMY_CELL, "value".getBytes())),
                IllegalArgumentException.class,
                "is a read only transaction");
    }

    @Test
    public void notAllowDeletes() {
        checkThrowsAndNoInteraction(
                () -> readTransaction.delete(DUMMY_CONSERVATIVE_TABLE, ImmutableSet.of(DUMMY_CELL)),
                IllegalArgumentException.class,
                "is a read only transaction");
    }

    @Test
    public void allowGetsOnConservativeTables() {
        ImmutableSet<Cell> cellToGet = ImmutableSet.of(DUMMY_CELL);
        readTransaction.get(DUMMY_CONSERVATIVE_TABLE, cellToGet);
        Mockito.verify(delegateTransaction, times(1)).get(eq(DUMMY_CONSERVATIVE_TABLE), eq(cellToGet));
    }

    @Test
    public void notAllowSimpleGetsOnThoroughTables() {
        Method[] declaredMethods = ReadTransaction.class.getDeclaredMethods();

        for (Method method : declaredMethods) {
            // Ignore methods that are either not simple gets or are overloaded
            if (simpleGets.containsKey(method.getName()) && hasExpectedParameterCount(method)) {
                checkThrowsAndNoInteraction(
                        () -> {
                            try {
                                method.invoke(readTransaction, simpleGets.get(method.getName()));
                            } catch (InvocationTargetException e) {
                                Throwables.throwIfInstance(e.getCause(), IllegalStateException.class);
                            } catch (IllegalAccessException e) {
                                Throwables.throwUncheckedException(e);
                            }
                        },
                        SafeIllegalStateException.class,
                        "cannot be read");
            }
        }
    }

    @Test
    public void notAllowBatchColumnRangeGets() {
        checkThrowsAndNoInteraction(
                () -> readTransaction.getRowsColumnRange(
                        DUMMY_THOROUGH_TABLE,
                        ImmutableList.of(EMPTY_BYTES),
                        BatchColumnRangeSelection.create(EMPTY_BYTES, EMPTY_BYTES, 1)),
                SafeIllegalStateException.class,
                "cannot be read");
        checkThrowsAndNoInteraction(
                () -> readTransaction.getRowsColumnRangeIterator(
                        DUMMY_THOROUGH_TABLE,
                        ImmutableList.of(EMPTY_BYTES),
                        BatchColumnRangeSelection.create(EMPTY_BYTES, EMPTY_BYTES, 1)),
                SafeIllegalStateException.class,
                "cannot be read");
    }

    @Test
    public void notAllowColumnRangeGets() {
        checkThrowsAndNoInteraction(
                () -> readTransaction.getRowsColumnRange(
                        DUMMY_THOROUGH_TABLE,
                        ImmutableList.of(EMPTY_BYTES),
                        new ColumnRangeSelection(EMPTY_BYTES, EMPTY_BYTES),
                        1),
                SafeIllegalStateException.class,
                "cannot be read");
    }

    private void checkThrowsAndNoInteraction(
            Runnable thrower, Class<? extends Exception> exception, String errorMessage) {
        try {
            thrower.run();
            fail("fail");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(exception);
            assertThat(e.getMessage()).contains(errorMessage);
            verifyNoMoreInteractions(delegateTransaction);
        }
    }

    private boolean hasExpectedParameterCount(Method method) {
        return simpleGets.get(method.getName()).length == method.getParameterCount();
    }
}
