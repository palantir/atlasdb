/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.http.errors;

import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.remoting2.errors.SerializableStackTraceElement;

@JsonDeserialize(as = ImmutableAtlasDbStackTraceElement.class)
@JsonSerialize(as = ImmutableAtlasDbStackTraceElement.class)
@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class AtlasDbStackTraceElement {
    public static final String UNKNOWN_CLASS = "UnknownClass";
    public static final String UNKNOWN_METHOD = "UnknownMethod";

    public abstract Optional<String> getClassName();

    public abstract Optional<String> getMethodName();

    public abstract Optional<String> getFileName();

    public abstract Optional<Integer> getLineNumber();

    public static AtlasDbStackTraceElement fromSerializableStackTraceElement(SerializableStackTraceElement element) {
        return ImmutableAtlasDbStackTraceElement.builder()
                .className(element.getClassName())
                .methodName(element.getMethodName())
                .fileName(element.getFileName())
                .lineNumber(element.getLineNumber())
                .build();
    }

    @Override
    public final String toString() {
        StackTraceElement element = new StackTraceElement(
                getClassName().orElse(UNKNOWN_CLASS),
                getMethodName().orElse(UNKNOWN_METHOD),
                getFileName().orElse(null),
                getLineNumber().orElse(0));

        return element.toString();
    }
}
