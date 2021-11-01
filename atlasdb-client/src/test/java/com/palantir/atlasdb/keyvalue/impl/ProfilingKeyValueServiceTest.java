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
package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

/**
 * We test behaviour for every of the three maybeLog(...) methods from ProfilingKeyValueService using public facing
 * methods that use them
 */
@RunWith(MockitoJUnitRunner.class)
public class ProfilingKeyValueServiceTest {
    private static final Namespace NAMESPACE = Namespace.create("test");
    private static final TableReference TABLE_REF = TableReference.create(NAMESPACE, "testTable");

    private static final Answer<Void> waitASecondAndAHalf = _any -> {
        Thread.sleep(1500);
        return (Void) null;
    };

    private static final Supplier<LoggingEvent> slowLogMatcher = () -> argThat((ArgumentMatcher<LoggingEvent>)
            argument -> KvsProfilingLogger.SLOW_LOGGER_NAME.equals(argument.getLoggerName())
                    && argument.getLevel() == Level.WARN);

    private static final Supplier<LoggingEvent> traceLogMatcher =
            () -> argThat((ArgumentMatcher<LoggingEvent>) argument ->
                    LoggerFactory.getLogger(KvsProfilingLogger.class).getName().equals(argument.getLoggerName())
                            && argument.getLevel() == Level.TRACE);

    private static final Map<Cell, Long> timestampByCell = ImmutableMap.of();
    private static final Map<Cell, Value> result = ImmutableMap.of();
    private static final Answer<Map<Cell, Value>> waitASecondAndAHalfAndReturn = _any -> {
        Thread.sleep(1500);
        return result;
    };

    @Mock
    Appender<ILoggingEvent> mockAppender;

    private KeyValueService delegate;
    private KeyValueService kvs;

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
        when(delegate.isInitialized()).thenReturn(false).thenReturn(true);

        assertThat(kvs.isInitialized()).isFalse();
        assertThat(kvs.isInitialized()).isTrue();
    }

    @Test
    public void dropTableSlowLogPresentOnInfoLevel() {
        setLogLevel(Level.INFO);

        doAnswer(waitASecondAndAHalf).when(delegate).dropTable(any());
        kvs.dropTable(TABLE_REF);

        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void dropTableTraceLogPresentOnTraceLevel() {
        setLogLevel(Level.TRACE);

        kvs.dropTable(TABLE_REF);

        verify(mockAppender).doAppend(traceLogMatcher.get());
    }

    @Test
    public void dropTableTraceLogPresentOnTraceLevelEvenIfQueryIsSlow() {
        setLogLevel(Level.TRACE);

        doAnswer(waitASecondAndAHalf).when(delegate).dropTable(any());
        kvs.dropTable(TABLE_REF);

        verify(mockAppender).doAppend(traceLogMatcher.get());
        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void dropTableNoLoggingHappensIfQueryFastAndInfoLevel() {
        setLogLevel(Level.INFO);

        kvs.dropTable(TABLE_REF);

        verifyNoMoreInteractions(mockAppender);
    }

    @Test
    public void getAllTableNamesSlowLogPresentOnInfoLevel() {
        setLogLevel(Level.INFO);

        doAnswer(waitASecondAndAHalf).when(delegate).getAllTableNames();
        kvs.getAllTableNames();

        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getAllTableNamesTraceLogPresentOnTraceLevel() {
        setLogLevel(Level.TRACE);

        kvs.getAllTableNames();

        verify(mockAppender).doAppend(traceLogMatcher.get());
    }

    @Test
    public void getAllTableNamesTraceLogPresentOnTraceLevelEvenIfQueryIsSlow() {
        setLogLevel(Level.TRACE);

        doAnswer(waitASecondAndAHalf).when(delegate).getAllTableNames();
        kvs.getAllTableNames();

        verify(mockAppender).doAppend(traceLogMatcher.get());
        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getAllTableNamesNoLoggingHappensIfQueryFastAndInfoLevel() {
        setLogLevel(Level.INFO);

        kvs.getAllTableNames();

        verifyNoMoreInteractions(mockAppender);
    }

    @Test
    public void getSlowLogPresentOnInfoLevel() {
        setLogLevel(Level.INFO);

        doAnswer(waitASecondAndAHalfAndReturn).when(delegate).get(TABLE_REF, timestampByCell);
        kvs.get(TABLE_REF, timestampByCell);

        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getTraceLogPresentOnTraceLevel() {
        setLogLevel(Level.TRACE);

        kvs.get(TABLE_REF, timestampByCell);

        verify(mockAppender).doAppend(traceLogMatcher.get());
    }

    @Test
    public void getTraceLogPresentOnTraceLevelEvenIfQueryIsSlow() {
        setLogLevel(Level.TRACE);

        doAnswer(waitASecondAndAHalfAndReturn).when(delegate).get(TABLE_REF, timestampByCell);
        kvs.get(TABLE_REF, timestampByCell);

        verify(mockAppender).doAppend(traceLogMatcher.get());
        verify(mockAppender).doAppend(slowLogMatcher.get());
    }

    @Test
    public void getNoLoggingHappensIfQueryFastAndInfoLevel() {
        setLogLevel(Level.INFO);

        kvs.get(TABLE_REF, timestampByCell);

        verifyNoMoreInteractions(mockAppender);
    }

    private void setLogLevel(Level loglevel) {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(loglevel);
        root.addAppender(mockAppender);
    }
}
