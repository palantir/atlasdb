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

package com.palantir.atlasdb.safetycheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class ServerKillerTest {
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    private final TestAppender appender = new TestAppender();

    @Before
    public void setUp() {
        registerAppender(appender);
    }

    @Test
    public void killsTheServerWithExitCodeOfOne() {
        exit.expectSystemExitWithStatus(1);
        ServerKiller.kill(new RuntimeException());
    }

    @Test
    public void logsTheRelevantExceptionMessage() {
        String errorId = UUID.randomUUID().toString();
        exit.expectSystemExit();

        assertThatThrownBy(() -> ServerKiller.kill(new RuntimeException("Something bad happened - " + errorId)))
                .isInstanceOf(RuntimeException.class);

        Set<String> relevantMessages = appender.logBuffer.stream()
                .filter(message -> message.contains(errorId))
                .collect(Collectors.toSet());
        assertThat(relevantMessages).isNotEmpty();
    }

    @Test
    public void logsSafeLogMessageIfPresent() {
        exit.expectSystemExit();
        String loggableMessage = UUID.randomUUID().toString();

        assertThatThrownBy(() ->
                ServerKiller.kill(new RuntimeException("Something bad happened!"), Optional.of(loggableMessage)))
                .isInstanceOf(RuntimeException.class);

        Set<String> relevantMessages = appender.logBuffer.stream()
                .filter(message -> message.contains(loggableMessage))
                .collect(Collectors.toSet());
        assertThat(relevantMessages).isNotEmpty();
    }

    private static void registerAppender(TestAppender appender) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        appender.setContext(context);
        appender.start();
        context.getLogger(ServerKiller.class).addAppender(appender);
    }

    private static class TestAppender extends AppenderBase<ILoggingEvent> {
        private final List<String> logBuffer = Lists.newArrayList();

        @Override
        protected void append(ILoggingEvent eventObject) {
            StringBuilder logMessageBuilder = new StringBuilder();
            logMessageBuilder.append(eventObject.getFormattedMessage());
            for (Object object : eventObject.getArgumentArray()) {
                logMessageBuilder.append("\n");
                logMessageBuilder.append(object.toString());
            }
            logBuffer.add(logMessageBuilder.toString());
        }
    }
}
