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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
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
import org.mockito.ArgumentMatcher;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

/**
 * We test behaviour for every of the three maybeLog(...) methods from ProfilingKeyValueService using public facing
 * methods that use them
 */
public class ProfilingKeyValueServiceTest {
    private static final Namespace NAMESPACE = Namespace.create("test");
    private static final TableReference TABLE_REF = TableReference.create(NAMESPACE, "testTable");

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
}
