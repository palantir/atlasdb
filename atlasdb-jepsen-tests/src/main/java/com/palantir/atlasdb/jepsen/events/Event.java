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

import clojure.lang.Keyword;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.atlasdb.jepsen.utils.EventUtils;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(InfoEvent.class),
        @JsonSubTypes.Type(InvokeEvent.class),
        @JsonSubTypes.Type(OkEvent.class),
        @JsonSubTypes.Type(FailEvent.class)
        })
public interface Event {
    ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);

    static Event fromKeywordMap(Map<Keyword, ?> map) {
        Map<Keyword, ?> encodedMap = EventUtils.encodeNemesis(map);
        Map<String, Object> convertedMap = KeyedStream.stream(encodedMap)
                .mapKeys(Keyword::getName)
                .map(value -> value != null & value instanceof Keyword ? ((Keyword) value).getName() : value)
                .collectToMap();
        return OBJECT_MAPPER.convertValue(convertedMap, Event.class);
    }

    static Map<Keyword, Object> toKeywordMap(Event event) {
        Map<String, Object> rawStringMap =
                OBJECT_MAPPER.convertValue(event, new TypeReference<Map<String, Object>>() {});
        return KeyedStream.stream(rawStringMap)
                .filter(Objects::nonNull)
                .mapKeys(key -> Keyword.intern(key))
                .collectToMap();
    }

    long time();

    int process();

    void accept(EventVisitor visitor);
}
