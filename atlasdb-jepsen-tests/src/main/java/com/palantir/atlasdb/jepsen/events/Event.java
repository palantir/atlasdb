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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

import clojure.lang.Keyword;
import one.util.streamex.EntryStream;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(InfoEvent.class),
        @JsonSubTypes.Type(InvokeEvent.class),
        @JsonSubTypes.Type(OkEvent.class)
        })
public interface Event {
    static Event fromKeywordMap(Map<Keyword, ?> map) {
        Map<String, Object> convertedMap = new HashMap<>();
        EntryStream.of(map)
                .mapKeys(Keyword::getName)
                .mapValues(value -> value != null && value instanceof Keyword ? ((Keyword) value).getName() : value)
                .forKeyValue(convertedMap::put);
        return new ObjectMapper().convertValue(convertedMap, Event.class);
    }

    void accept(EventVisitor visitor);
}
