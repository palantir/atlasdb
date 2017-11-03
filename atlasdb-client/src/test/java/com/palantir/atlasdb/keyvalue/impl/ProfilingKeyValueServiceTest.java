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
package com.palantir.atlasdb.keyvalue.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.common.base.ClosableIterator;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;

/**
 * We test behaviour for every of the three maybeLog(...) methods from ProfilingKeyValueService using public facing
 * methods that use them
 */
public class ProfilingKeyValueServiceTest {
    private static final Namespace NAMESPACE = Namespace.create("test");
    private static final TableReference TABLE_REF = TableReference.create(NAMESPACE, "testTable");
    private static final RangeRequest RANGE_REQUEST = RangeRequest.builder().build();
    private static final CandidateCellForSweepingRequest CANDIDATE_OF_CELLS_FOR_SWEEPING_REQUEST =
            ImmutableCandidateCellForSweepingRequest.builder()
                    .startRowInclusive((byte) 0b00)
                    .batchSizeHint(0)
                    .shouldCheckIfLatestValueIsEmpty(false)
                    .maxTimestampExclusive(0L)
                    .build();
    private static final ColumnRangeSelection COLUMN_RANGE_SELECTION = new ColumnRangeSelection(
            new byte[0], new byte[0]);

    private KeyValueService delegate;
    private KeyValueService kvs;

    private static final Answer<Void> waitASecondAndAHalf = any -> {
        Thread.sleep(1500);
        return (Void) null;
    };

    private static final Supplier<Object> slowLogMatcher = () -> argThat(new ArgumentMatcher() {
        @Override
        public boolean matches(final Object argument) {
            LoggingEvent ev = (LoggingEvent) argument;
            return ev.getLoggerName() == KvsProfilingLogger.SLOW_LOGGER_NAME
                    && ev.getLevel() == Level.WARN;
        }
    });

    private static final Supplier<Object> traceLogMatcher = () -> argThat(new ArgumentMatcher() {
        @Override
        public boolean matches(final Object argument) {
            LoggingEvent ev = (LoggingEvent) argument;
            return ev.getLoggerName() == LoggerFactory.getLogger(KvsProfilingLogger.class).getName()
                    && ev.getLevel() == Level.TRACE;
        }
    });

    private Appender setLogLevelAndGetAppender(Level loglevel) {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(loglevel);

        final Appender mockAppender = mock(Appender.class);
        when(mockAppender.getName()).thenReturn("MOCK");
        root.addAppender(mockAppender);
        return mockAppender;
    }

    @Before
    public void before() throws Exception {
        delegate = mock(KeyValueService.class);
        kvs = ProfilingKeyValueService.create(delegate);
    }

    @After
    public void after() throws Exception {
        kvs.close();
    }

    @Test
    public void delegatesInitializationCheck() {
        when(delegate.isInitialized())
                .thenReturn(false)
                .thenReturn(true);

        assertFalse(kvs.isInitialized());
        assertTrue(kvs.isInitialized());
    }

    @Test
    public void dropTableSlowLogPresentOnInfoLevel() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.INFO);

        doAnswer(waitASecondAndAHalf).when(delegate).dropTable(any());
        kvs.dropTable(TABLE_REF);

        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void dropTableTraceLogPresentOnTraceLevel() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.TRACE);

        kvs.dropTable(TABLE_REF);

        verify(mockAppender).doAppend(traceLogMatcher.get());
    }

    @Test
    public void dropTableTraceLogPresentOnTraceLevelEvenIfQueryIsSlow() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.TRACE);

        doAnswer(waitASecondAndAHalf).when(delegate).dropTable(any());
        kvs.dropTable(TABLE_REF);

        verify(mockAppender).doAppend(traceLogMatcher.get());
        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void dropTableNoLoggingHappensIfQueryFastAndInfoLevel() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.INFO);

        kvs.dropTable(TABLE_REF);

        verifyNoMoreInteractions(mockAppender);
    }

    @Test
    public void getAllTableNamesSlowLogPresentOnInfoLevel() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.INFO);

        doAnswer(waitASecondAndAHalf).when(delegate).getAllTableNames();
        kvs.getAllTableNames();

        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getAllTableNamesTraceLogPresentOnTraceLevel() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.TRACE);

        kvs.getAllTableNames();

        verify(mockAppender).doAppend(traceLogMatcher.get());
    }

    @Test
    public void getAllTableNamesTraceLogPresentOnTraceLevelEvenIfQueryIsSlow() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.TRACE);

        doAnswer(waitASecondAndAHalf).when(delegate).getAllTableNames();
        kvs.getAllTableNames();

        verify(mockAppender).doAppend(traceLogMatcher.get());
        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getAllTableNamesNoLoggingHappensIfQueryFastAndInfoLevel() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.INFO);

        kvs.getAllTableNames();

        verifyNoMoreInteractions(mockAppender);
    }

    private static Map<Cell, Long> timestampByCell = ImmutableMap.of();
    private static Map<Cell, Value> result = ImmutableMap.of();
    private static Answer<Map<Cell, Value>> waitASecondAndAHalfAndReturn = any -> {
        Thread.sleep(1500);
        return result;
    };

    @Test
    public void getSlowLogPresentOnInfoLevel() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.INFO);

        doAnswer(waitASecondAndAHalfAndReturn).when(delegate).get(TABLE_REF, timestampByCell);
        kvs.get(TABLE_REF, timestampByCell);

        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getTraceLogPresentOnTraceLevel() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.TRACE);

        kvs.get(TABLE_REF, timestampByCell);

        verify(mockAppender).doAppend(traceLogMatcher.get());
    }

    @Test
    public void getTraceLogPresentOnTraceLevelEvenIfQueryIsSlow() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.TRACE);

        doAnswer(waitASecondAndAHalfAndReturn).when(delegate).get(TABLE_REF, timestampByCell);
        kvs.get(TABLE_REF, timestampByCell);

        verify(mockAppender).doAppend(traceLogMatcher.get());
        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getNoLoggingHappensIfQueryFastAndInfoLevel() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.INFO);

        kvs.get(TABLE_REF, timestampByCell);

        verifyNoMoreInteractions(mockAppender);
    }

    @Test
    public void getRangeLogsWhenIteratorNextIsSlow() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.TRACE);
        ClosableIterator returnedIterator = slowGetRangeIterator();

        returnedIterator.next();

        verify(mockAppender).doAppend(traceLogMatcher.get());
        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getRangeLogsWhenIteratorHasNextIsSlow() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.TRACE);
        ClosableIterator returnedIterator = slowGetRangeIterator();

        returnedIterator.hasNext();

        verify(mockAppender).doAppend(traceLogMatcher.get());
        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getRangeLogsWhenIteratorForEachRemainingIsSlow() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.TRACE);
        ClosableIterator returnedIterator = slowGetRangeIterator();

        returnedIterator.forEachRemaining((obj) -> {});

        verify(mockAppender).doAppend(traceLogMatcher.get());
        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getRangeOfTimestampsLogsWhenIteratorNextIsSlow() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.TRACE);
        ClosableIterator mockedIterator = buildSlowClosableIterator();

        when(delegate.getRangeOfTimestamps(any(), any(), anyLong())).thenReturn(mockedIterator);
        ClosableIterator returnedIterator = kvs.getRangeOfTimestamps(TABLE_REF, RANGE_REQUEST, 0);
        returnedIterator.next();

        verify(mockAppender).doAppend(traceLogMatcher.get());
        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getCandidateOfCellsForSweepingLogsWhenIteratorNextIsSlow() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.TRACE);
        ClosableIterator mockedIterator = buildSlowClosableIterator();

        when(delegate.getCandidateCellsForSweeping(any(), any())).thenReturn(mockedIterator);
        ClosableIterator returnedIterator = kvs.getCandidateCellsForSweeping(TABLE_REF,
                CANDIDATE_OF_CELLS_FOR_SWEEPING_REQUEST);
        returnedIterator.next();

        verify(mockAppender).doAppend(traceLogMatcher.get());
        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getRowsColumnRangeLogsWhenIteratorIsSlow() {
        Appender mockAppender = setLogLevelAndGetAppender(Level.TRACE);
        RowColumnRangeIterator rowColumnRangeIterator = buildRowColumnRangeIterator();

        when(delegate.getRowsColumnRange(any(), any(), any(), anyInt(), anyLong())).thenReturn(rowColumnRangeIterator);
        RowColumnRangeIterator returnedIterator = kvs.getRowsColumnRange(TABLE_REF, Lists.newArrayList(),
                COLUMN_RANGE_SELECTION, 0, 0);
        returnedIterator.next();

        verify(mockAppender).doAppend(traceLogMatcher.get());
        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    private ClosableIterator slowGetRangeIterator() {
        ClosableIterator mockedIterator = buildSlowClosableIterator();
        when(delegate.getRange(any(), any(), anyLong())).thenReturn(mockedIterator);
        return kvs.getRange(TABLE_REF, RANGE_REQUEST, 0);
    }

    private ClosableIterator buildSlowClosableIterator() {
        ClosableIterator mockedIterator = mock(ClosableIterator.class);
        doAnswer(waitASecondAndAHalf).when(mockedIterator).next();
        doAnswer(waitASecondAndAHalf).when(mockedIterator).hasNext();
        doAnswer(waitASecondAndAHalf).when(mockedIterator).forEachRemaining(any());
        return mockedIterator;
    }

    private RowColumnRangeIterator buildRowColumnRangeIterator() {
        RowColumnRangeIterator mockedIterator = mock(RowColumnRangeIterator.class);
        doAnswer(waitASecondAndAHalf).when(mockedIterator).next();
        return mockedIterator;
    }
}
