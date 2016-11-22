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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import clojure.lang.Keyword;

public class EventTest {
    @Test
    public void makeSureWeCanHaveNullValues() {
        Map<Keyword, Object> keywordMap = new HashMap<>();
        keywordMap.put(Keyword.intern("type"), Keyword.intern("info"));
        keywordMap.put(Keyword.intern("value"), null);

        Event event = Event.fromKeywordMap(keywordMap);

        assertThat(event).isInstanceOf(InfoEvent.class);
    }

    @Test
    public void canDeserialiseInfoRead() {
        // TODO
    }

    @Test
    public void canDeserialiseInvokeRead() {
        // TODO
    }

    @Test
    public void canDeserialiseOkRead() {
        // TODO
    }

    @Test
    public void cannotDeserialiseOkReadWhenFieldIsMissing() {
        // TODO
    }
}
