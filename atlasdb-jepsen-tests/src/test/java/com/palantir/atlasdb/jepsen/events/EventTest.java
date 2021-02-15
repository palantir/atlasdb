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
package com.palantir.atlasdb.jepsen.events;

import static org.assertj.core.api.Assertions.assertThat;

import clojure.lang.Keyword;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.jepsen.JepsenConstants;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class EventTest {
    public static final String SOME_LONG_AS_STRING = "136";
    public static final String SOME_STRING = "SOME_STRING";
    public static final Long SOME_LONG = 987L;
    public static final long SOME_TIME = 3029699376L;
    public static final int SOME_PROCESS = 1;
    public static final String READ_OPERATION = "read-operation";

    @Test
    public void makeSureWeCanHaveNullValues() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("info"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern(JepsenConstants.START_FUNCTION));
        keywordMap.put(Keyword.intern("process"), Keyword.intern(JepsenConstants.NEMESIS_PROCESS_NAME));
        keywordMap.put(Keyword.intern("time"), SOME_TIME);
        keywordMap.put(Keyword.intern("value"), null);

        Event event = Event.fromKeywordMap(keywordMap);

        assertThat(event).isInstanceOf(InfoEvent.class);
        assertThat(((InfoEvent) event).value()).isNotPresent();
    }

    @Test
    public void canDeserialiseInfoReadWithoutValue() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("info"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern(JepsenConstants.START_FUNCTION));
        keywordMap.put(Keyword.intern("process"), Keyword.intern(JepsenConstants.NEMESIS_PROCESS_NAME));
        keywordMap.put(Keyword.intern("time"), SOME_TIME);

        Event event = Event.fromKeywordMap(keywordMap);

        assertThat(event).isInstanceOf(InfoEvent.class);
    }

    @Test
    public void canDeserialiseInfoReadWithValue() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("info"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern(JepsenConstants.START_FUNCTION));
        keywordMap.put(Keyword.intern("process"), Keyword.intern(JepsenConstants.NEMESIS_PROCESS_NAME));
        keywordMap.put(Keyword.intern("time"), SOME_TIME);
        keywordMap.put(Keyword.intern("value"), Keyword.intern(SOME_LONG_AS_STRING));

        Event event = Event.fromKeywordMap(keywordMap);

        assertThat(event).isInstanceOf(InfoEvent.class);
    }

    @Test
    public void canDeserialiseInvokeEvent() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("invoke"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern(READ_OPERATION));
        keywordMap.put(Keyword.intern("value"), null);
        keywordMap.put(Keyword.intern("process"), SOME_PROCESS);
        keywordMap.put(Keyword.intern("time"), SOME_TIME);

        Event event = Event.fromKeywordMap(keywordMap);

        InvokeEvent expectedEvent = ImmutableInvokeEvent.builder()
                .process(SOME_PROCESS)
                .time(SOME_TIME)
                .function(READ_OPERATION)
                .build();
        assertThat(event).isEqualTo(expectedEvent);
    }

    @Test
    public void canDeserialiseOkReadWithValueAsStringOfLong() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("ok"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern(READ_OPERATION));
        keywordMap.put(Keyword.intern("value"), SOME_LONG_AS_STRING);
        keywordMap.put(Keyword.intern("process"), SOME_PROCESS);
        keywordMap.put(Keyword.intern("time"), SOME_TIME);

        Event event = Event.fromKeywordMap(keywordMap);

        OkEvent expectedEvent = ImmutableOkEvent.builder()
                .value(SOME_LONG_AS_STRING)
                .process(SOME_PROCESS)
                .time(SOME_TIME)
                .function(READ_OPERATION)
                .build();
        assertThat(event).isEqualTo(expectedEvent);
    }

    @Test
    public void canDeserialiseOkReadWithValueAsAnyString() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("ok"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern(READ_OPERATION));
        keywordMap.put(Keyword.intern("value"), SOME_STRING);
        keywordMap.put(Keyword.intern("process"), SOME_PROCESS);
        keywordMap.put(Keyword.intern("time"), SOME_TIME);

        Event event = Event.fromKeywordMap(keywordMap);

        OkEvent expectedEvent = ImmutableOkEvent.builder()
                .value(SOME_STRING)
                .process(SOME_PROCESS)
                .time(SOME_TIME)
                .function(READ_OPERATION)
                .build();
        assertThat(event).isEqualTo(expectedEvent);
    }

    @Test
    public void canDeserialiseOkReadWithValueAsLong() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("ok"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern(READ_OPERATION));
        keywordMap.put(Keyword.intern("value"), SOME_LONG);
        keywordMap.put(Keyword.intern("process"), SOME_PROCESS);
        keywordMap.put(Keyword.intern("time"), SOME_TIME);

        Event event = Event.fromKeywordMap(keywordMap);

        OkEvent expectedEvent = ImmutableOkEvent.builder()
                .value(SOME_LONG.toString())
                .process(SOME_PROCESS)
                .time(SOME_TIME)
                .function(READ_OPERATION)
                .build();
        assertThat(event).isEqualTo(expectedEvent);
    }

    @Test
    public void canDeserialiseOkReadWithNullValue() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("ok"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern(READ_OPERATION));
        keywordMap.put(Keyword.intern("value"), null);
        keywordMap.put(Keyword.intern("process"), SOME_PROCESS);
        keywordMap.put(Keyword.intern("time"), SOME_TIME);

        Event event = Event.fromKeywordMap(keywordMap);

        OkEvent expectedEvent = ImmutableOkEvent.builder()
                .value(null)
                .process(SOME_PROCESS)
                .time(SOME_TIME)
                .function(READ_OPERATION)
                .build();
        assertThat(event).isEqualTo(expectedEvent);
    }

    @Test
    public void canDeserialiseFailEventWithStacktrace() {
        String exceptionString = new RuntimeException("Error").toString();

        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("fail"));
        keywordMap.put(Keyword.intern("process"), SOME_PROCESS);
        keywordMap.put(Keyword.intern("time"), SOME_TIME);
        keywordMap.put(Keyword.intern("error"), exceptionString);

        Event event = Event.fromKeywordMap(keywordMap);

        FailEvent expectedEvent = ImmutableFailEvent.builder()
                .process(SOME_PROCESS)
                .time(SOME_TIME)
                .error(exceptionString)
                .build();
        assertThat(event).isEqualTo(expectedEvent);
    }

    @Test
    public void canDeserialiseFailEventWithTimeoutKeyword() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("fail"));
        keywordMap.put(Keyword.intern("process"), SOME_PROCESS);
        keywordMap.put(Keyword.intern("time"), SOME_TIME);
        keywordMap.put(Keyword.intern("error"), Keyword.intern("timeout"));

        Event event = Event.fromKeywordMap(keywordMap);

        FailEvent expectedEvent = ImmutableFailEvent.builder()
                .process(SOME_PROCESS)
                .time(SOME_TIME)
                .error("timeout")
                .build();
        assertThat(event).isEqualTo(expectedEvent);
    }

    @Test
    public void canDeserialiseOkReadWhenValueIsMissing() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("ok"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern(READ_OPERATION));
        keywordMap.put(Keyword.intern("process"), SOME_PROCESS);
        keywordMap.put(Keyword.intern("time"), SOME_TIME);

        Event event = Event.fromKeywordMap(keywordMap);

        OkEvent expectedEvent = ImmutableOkEvent.builder()
                .process(SOME_PROCESS)
                .time(SOME_TIME)
                .value(null)
                .function(READ_OPERATION)
                .build();
        assertThat(event).isEqualTo(expectedEvent);
    }

    @Test
    public void canSerialiseInfoEventWithValue() {
        Event infoEvent = ImmutableInfoEvent.builder()
                .function("foo")
                .process(SOME_PROCESS)
                .time(SOME_TIME)
                .value("bar")
                .build();

        Map<Keyword, Object> expected = ImmutableMap.of(
                Keyword.intern("type"), "info",
                Keyword.intern("f"), "foo",
                Keyword.intern("process"), SOME_PROCESS,
                Keyword.intern("time"), SOME_TIME,
                Keyword.intern("value"), "bar");

        assertThat(Event.toKeywordMap(infoEvent)).isEqualTo(expected);
    }

    @Test
    public void canSerialiseInfoEventWithoutValue() {
        Event infoEvent = ImmutableInfoEvent.builder()
                .function("foo")
                .process(SOME_PROCESS)
                .time(SOME_TIME)
                .build();

        Map<Keyword, Object> expected = ImmutableMap.of(
                Keyword.intern("type"),
                "info",
                Keyword.intern("f"),
                "foo",
                Keyword.intern("process"),
                SOME_PROCESS,
                Keyword.intern("time"),
                SOME_TIME);

        assertThat(Event.toKeywordMap(infoEvent)).isEqualTo(expected);
    }
}
