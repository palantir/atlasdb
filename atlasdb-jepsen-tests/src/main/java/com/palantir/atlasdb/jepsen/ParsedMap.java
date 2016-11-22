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
package com.palantir.atlasdb.jepsen;

import com.google.common.base.Optional;

import clojure.lang.PersistentArrayMap;

public class ParsedMap {
    public enum Function {
        READ_OPERATION, START
    }

    public enum Type {
        INVOKE, OK, INFO
    }

    Optional<Long> time;
    Optional<String> process;
    Optional<Type> type;
    Optional<Function> function;
    Optional<String> value;

    public ParsedMap(Optional<Long> time, Optional<String> process, Optional<Type> type, Optional<Function> function,
            Optional<String> value) {
        this.time = time;
        this.process = process;
        this.type = type;
        this.function = function;
        this.value = value;
    }

    public Optional<Long> time() {
        return time;
    }

    public Optional<String> process() {
        return process;
    }

    public Optional<Type> type() {
        return type;
    }

    public Optional<Function> function() {
        return function;
    }

    public Optional<String> value() {
        return value;
    }

    public static ParsedMap create(PersistentArrayMap map) {
        Optional<Long> time = Optional.absent();
        if (ClojureObjectUtils.mapContainsKeywordNamed(map, "time")) {
            time = Optional.of(ClojureObjectUtils.getFromKeywordNamed(map, "time", Long.class));
        }

        Optional<String> process = Optional.absent();
        if (ClojureObjectUtils.mapContainsKeywordNamed(map, "process")) {
            Object entry = ClojureObjectUtils.getFromKeywordNamed(map, "process", Object.class);
            if (entry != null) {
                process = Optional.of(entry.toString());
            }
        }

        Optional<Type> type = Optional.absent();
        if (ClojureObjectUtils.mapContainsKeywordNamed(map, "type")) {
            type = Optional.of(ClojureObjectUtils.getEnumFromKeywordNamed(map, "type", Type.class));
        }

        Optional<Function> function = Optional.absent();
        if (ClojureObjectUtils.mapContainsKeywordNamed(map, "f")) {
            function = Optional.of(ClojureObjectUtils.getEnumFromKeywordNamed(map, "f", Function.class));
        }

        Optional<String> value = Optional.absent();
        if (ClojureObjectUtils.mapContainsKeywordNamed(map, "value")) {
            Object entry = ClojureObjectUtils.getFromKeywordNamed(map, "value", Object.class);
            if (entry != null) {
                value = Optional.of(entry.toString());
            }
        }

        return new ParsedMap(time, process, type, function, value);
    }
}
