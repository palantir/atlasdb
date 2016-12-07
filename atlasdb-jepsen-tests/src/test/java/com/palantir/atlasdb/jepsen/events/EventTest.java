/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.jepsen.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import clojure.lang.Keyword;

public class EventTest {
    public static final long SOME_VALUE = 136L;
    public static final long SOME_TIME = 3029699376L;
    public static final int SOME_PROCESS = 1;

    @Test
    public void makeSureWeCanHaveNullValues() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("info"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern("start"));
        keywordMap.put(Keyword.intern("process"), Keyword.intern("nemesis"));
        keywordMap.put(Keyword.intern("time"), SOME_TIME);
        keywordMap.put(Keyword.intern("value"), null);

        Event event = Event.fromKeywordMap(keywordMap);

        assertThat(event).isInstanceOf(InfoEvent.class);
    }

    @Test
    public void canDeserialiseInfoReadWithoutValue() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("info"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern("start"));
        keywordMap.put(Keyword.intern("process"), Keyword.intern("nemesis"));
        keywordMap.put(Keyword.intern("time"), SOME_TIME);

        Event event = Event.fromKeywordMap(keywordMap);

        assertThat(event).isInstanceOf(InfoEvent.class);
    }

    @Test
    public void canDeserialiseInfoReadWithValue() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("info"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern("start"));
        keywordMap.put(Keyword.intern("process"), Keyword.intern("nemesis"));
        keywordMap.put(Keyword.intern("time"), SOME_TIME);
        keywordMap.put(Keyword.intern("value"), Keyword.intern(String.valueOf(SOME_VALUE)));

        Event event = Event.fromKeywordMap(keywordMap);

        assertThat(event).isInstanceOf(InfoEvent.class);
    }

    @Test
    public void canDeserialiseInvokeEvent() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("invoke"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern("read-operation"));
        keywordMap.put(Keyword.intern("value"), null);
        keywordMap.put(Keyword.intern("process"), SOME_PROCESS);
        keywordMap.put(Keyword.intern("time"), SOME_TIME);

        Event event = Event.fromKeywordMap(keywordMap);

        InvokeEvent expectedEvent = ImmutableInvokeEvent.builder()
                .process(SOME_PROCESS)
                .time(SOME_TIME)
                .build();
        assertThat(event).isEqualTo(expectedEvent);
    }

    @Test
    public void canDeserialiseOkRead() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("ok"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern("read-operation"));
        keywordMap.put(Keyword.intern("value"), SOME_VALUE);
        keywordMap.put(Keyword.intern("process"), SOME_PROCESS);
        keywordMap.put(Keyword.intern("time"), SOME_TIME);

        Event event = Event.fromKeywordMap(keywordMap);

        OkEvent expectedEvent = ImmutableOkEvent.builder()
                .value(SOME_VALUE)
                .process(SOME_PROCESS)
                .time(SOME_TIME)
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
    public void cannotDeserialiseOkReadWhenValueIsMissing() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("ok"));
        keywordMap.put(Keyword.intern("f"), Keyword.intern("read-operation"));
        keywordMap.put(Keyword.intern("process"), SOME_PROCESS);
        keywordMap.put(Keyword.intern("time"), SOME_TIME);

        assertThatThrownBy(() -> Event.fromKeywordMap(keywordMap)).isInstanceOf(IllegalArgumentException.class);
    }
}
